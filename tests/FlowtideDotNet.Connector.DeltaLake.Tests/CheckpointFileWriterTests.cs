// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Apache.Arrow;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.CheckpointReading;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using ParquetSharp;
using Stowage;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class CheckpointFileWriterTests : IDisposable
    {
        private readonly string _directory;

        public CheckpointFileWriterTests()
        {
            _directory = Path.Combine(Path.GetTempPath(), "flowtide-writer-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(_directory);
        }

        public void Dispose()
        {
            Directory.Delete(_directory, true);
        }

        private static DeltaAction Protocol() => new DeltaAction() { Protocol = new DeltaProtocolAction() { MinReaderVersion = 3, MinWriterVersion = 7, ReaderFeatures = new List<string>() { "deletionVectors" }, WriterFeatures = new List<string>() { "deletionVectors", "rowTracking", "domainMetadata" } } };

        private static DeltaAction MetaData() => new DeltaAction()
        {
            MetaData = new DeltaMetadataAction()
            {
                Id = "id",
                SchemaString = DeltaTestKit.UserSchema,
                Format = new DeltaMetadataFormat() { Provider = "parquet", Options = new Dictionary<string, string>() },
                PartitionColumns = new List<string>(),
                Configuration = new Dictionary<string, string>() { ["a"] = "1" },
                CreatedTime = 5
            }
        };

        private static IEnumerable<byte[]> Records(params DeltaAction[] actions) => actions.SelectMany(x => TestRecords.FromAction(x));

        private (string Path, CheckpointWriteResult Result, CheckpointFileWriter Writer) Write(IEnumerable<byte[]> records, string? marker = null, CheckpointWriterLimits? limits = null,
            Compression compression = Compression.Zstd, JobMemory? memory = null, DiskLedger? ledger = null)
        {
            var path = Path.Combine(_directory, Guid.NewGuid().ToString("N") + ".checkpoint.parquet");
            memory ??= new JobMemory();
            ledger ??= new DiskLedger();
            using var writer = new CheckpointFileWriter(path, ledger, memory, compression, marker, limits);
            // Never more on disk than the ledger holds for the file
            writer.AfterWriteForTests = () => Assert.True(new FileInfo(path).Length <= writer.Charged, $"{new FileInfo(path).Length} bytes on disk with {writer.Charged} charged");
            foreach (var record in records)
            {
                writer.Append(record);
            }
            var result = writer.Close();
            return (path, result, writer);
        }

        private static async Task<List<DeltaAction>> ReadActions(string path)
        {
            var storage = Files.Of.LocalDisk(Path.GetDirectoryName(path)!);
            var entry = (await storage.Ls("/")).Single(x => x.Name == Path.GetFileName(path));
            var actions = new List<DeltaAction>();
            await foreach (var action in new ParquetCheckpointReader().ReadCheckpointFile(storage, entry))
            {
                actions.Add(action);
            }
            return actions;
        }

        // Every row of one top-level column, through the Arrow reader
        private static List<StructArray> ReadColumn(string path, string column)
        {
            using var reader = new ParquetSharp.Arrow.FileReader(path);
            using var batches = reader.GetRecordBatchReader();
            var arrays = new List<StructArray>();
            RecordBatch batch;
            while ((batch = batches.ReadNextRecordBatchAsync().AsTask().Result) != null)
            {
                arrays.Add((StructArray)batch.Column(column));
            }
            return arrays;
        }

        [Fact]
        public async Task RemoveRowsKeepRowTrackingFields()
        {
            var remove = new DeltaRemoveFileAction()
            {
                Path = "r.parquet",
                DeletionTimestamp = 99,
                DataChange = true,
                ExtendedFileMetadata = true,
                PartitionValues = new Dictionary<string, string>() { ["p"] = "1" },
                Size = 123,
                DeletionVector = new DeletionVector() { StorageType = "u", PathOrInlineDv = "abcdefghijklmnopqrst", Offset = 3, SizeInBytes = 40, Cardinality = 5 },
                BaseRowId = 11,
                DefaultRowCommitVersion = 12
            };
            var (path, _, _) = Write(Records(Protocol(), MetaData(), new DeltaAction() { Remove = remove }));

            var read = (await ReadActions(path)).Single(x => x.Remove != null).Remove!;
            Assert.Equal("r.parquet", read.Path);
            Assert.Equal(99, read.DeletionTimestamp);
            Assert.True(read.ExtendedFileMetadata);
            Assert.Equal("1", read.PartitionValues!["p"]);
            Assert.Equal(123, read.Size);
            Assert.Equal(remove.DeletionVector.UniqueId, read.DeletionVector!.UniqueId);
            Assert.Equal(40, read.DeletionVector.SizeInBytes);
            Assert.Equal(5, read.DeletionVector.Cardinality);
            Assert.Equal(11, read.BaseRowId);
            Assert.Equal(12, read.DefaultRowCommitVersion);
        }

        [Fact]
        public void RemovesHaveNoStatsOrTags()
        {
            var names = CheckpointSchema.Remove.Fields.Select(x => x.Name).ToList();
            Assert.DoesNotContain("stats", names);
            Assert.DoesNotContain("tags", names);
            Assert.Equal(52, CheckpointSchema.LeafCount);
        }

        [Fact]
        public async Task DataChangeIsFalseOnAddsAndRemoves()
        {
            var (path, _, _) = Write(Records(Protocol(), MetaData(),
                new DeltaAction() { Add = new DeltaAddAction() { Path = "a", DataChange = true, Size = 1 } },
                new DeltaAction() { Remove = new DeltaRemoveFileAction() { Path = "r", DataChange = true } }));

            var add = ReadColumn(path, "add").Single();
            var remove = ReadColumn(path, "remove").Single();
            var addDataChange = (BooleanArray)add.Fields[CheckpointSchema.Add.GetFieldIndex("dataChange")];
            var removeDataChange = (BooleanArray)remove.Fields[CheckpointSchema.Remove.GetFieldIndex("dataChange")];
            Assert.False(addDataChange.GetValue(2));
            Assert.False(removeDataChange.GetValue(3));
            Assert.True(addDataChange.IsNull(3));
        }

        [Fact]
        public async Task TxnAndDomainColumnsAreWritten()
        {
            var (path, result, _) = Write(Records(Protocol(), MetaData(),
                new DeltaAction() { Txn = new DeltaTransactionAction() { AppId = "app", Version = 7, LastUpdated = 8 } },
                new DeltaAction() { DomainMetadata = new DeltaDomainMetadataAction() { Domain = "delta.rowTracking", Configuration = "{\"rowIdHighWaterMark\":4}", Removed = false } }));

            var actions = await ReadActions(path);
            var txn = actions.Single(x => x.Txn != null).Txn!;
            var domain = actions.Single(x => x.DomainMetadata != null).DomainMetadata!;
            Assert.Equal(("app", 7L, (long?)8), (txn.AppId, txn.Version, txn.LastUpdated));
            Assert.Equal(("delta.rowTracking", "{\"rowIdHighWaterMark\":4}", false), (domain.Domain, domain.Configuration, domain.Removed));
            Assert.Equal(4, result.Actions);
            Assert.Equal(0, result.AddFiles);
        }

        [Fact]
        public void NullMapValuesAreWritten()
        {
            var add = new DeltaAddAction() { Path = "a", PartitionValues = new Dictionary<string, string>() { ["p"] = null!, ["q"] = "x" } };
            var (path, _, _) = Write(Records(Protocol(), MetaData(), new DeltaAction() { Add = add }));

            var map = (MapArray)ReadColumn(path, "add").Single().Fields[CheckpointSchema.Add.GetFieldIndex("partitionValues")];
            var keys = (StringArray)map.Keys;
            var values = (StringArray)map.Values;
            var start = map.ValueOffsets[2];
            Assert.Equal(2, map.ValueOffsets[3] - start);
            Assert.Equal("p", keys.GetString(start));
            Assert.True(values.IsNull(start));
            Assert.Equal("x", values.GetString(start + 1));
        }

        [Fact]
        public void MarkerRoundTrips()
        {
            var marker = "{\"format\":1,\"version\":3}";
            var (path, _, _) = Write(Records(Protocol(), MetaData()), marker);
            using var file = new ParquetFileReader(path);
            Assert.Equal(marker, file.FileMetaData.KeyValueMetadata[CheckpointSchema.CoverageMarkerKey]);
            Assert.Contains("ARROW:schema", file.FileMetaData.KeyValueMetadata.Keys);
        }

        [Fact]
        public void CheckpointOutputHasNoPageIndex()
        {
            var adds = Enumerable.Range(0, 20_000).Select(i => new DeltaAction() { Add = new DeltaAddAction() { Path = $"part-{i:D6}.parquet", Size = i, Statistics = "{\"numRecords\":" + i + "}" } });
            var (path, _, _) = Write(Records(Protocol(), MetaData()).Concat(adds.SelectMany(x => TestRecords.FromAction(x))));
            Assert.Equal(0, ParquetKit.BytesBeforeFooter(path));
        }

        private static IEnumerable<byte[]> WideAdds(int count, int statsBytes)
        {
            var stats = "{\"numRecords\":1,\"x\":\"" + new string('s', statsBytes) + "\"}";
            return Enumerable.Range(0, count).SelectMany(i => TestRecords.FromAction(new DeltaAction() { Add = new DeltaAddAction() { Path = $"part-{i:D7}.parquet", Size = i, Statistics = stats } }));
        }

        [Fact]
        public void RowGroupsCloseAtTheByteTarget()
        {
            var limits = new CheckpointWriterLimits() { GroupValueBytes = 2L << 20 };
            var (path, result, writer) = Write(Records(Protocol(), MetaData()).Concat(WideAdds(10_000, 1000)), limits: limits);

            Assert.True(result.GroupRows.Count >= 4, $"{result.GroupRows.Count} groups");
            Assert.All(writer.CloseReasons, x => Assert.Equal(GroupCloseReason.ValueBytes, x));
            using var file = new ParquetFileReader(path);
            Assert.Equal(result.GroupRows.Count, file.FileMetaData.NumRowGroups);
            Assert.Equal(10_002, result.GroupRows.Sum());
        }

        [Fact]
        public void GroupClosesBeforeTheProjectedDecodeBound()
        {
            // Each leaf's codec term is 1 MiB, so a cap just above the empty projection leaves room for little data
            var limits = new CheckpointWriterLimits() { DecodeCap = (8L << 20) + EmptyDecodeBound() + (20L << 20) };
            var (_, result, writer) = Write(Records(Protocol(), MetaData()).Concat(WideAdds(15_000, 1000)), limits: limits);

            Assert.True(result.GroupRows.Count >= 2);
            Assert.Contains(GroupCloseReason.DecodeBound, writer.CloseReasons);
            Assert.DoesNotContain(GroupCloseReason.ValueBytes, writer.CloseReasons);
        }

        private string TempPath() => Path.Combine(_directory, Guid.NewGuid().ToString("N") + ".parquet");

        private long EmptyDecodeBound()
        {
            using var writer = new CheckpointFileWriter(TempPath(), new DiskLedger(), new JobMemory(), Compression.Zstd, null);
            return writer.ProjectedDecode();
        }

        private long EmptyWriterBound()
        {
            using var writer = new CheckpointFileWriter(TempPath(), new DiskLedger(), new JobMemory(), Compression.Zstd, null);
            return writer.ProjectedWriter();
        }

        [Fact]
        public void GroupClosesBeforeTheProjectedLevelTerms()
        {
            // Many null-valued map entries: levels grow while value bytes stay small
            var tags = Enumerable.Range(0, 400).ToDictionary(i => "k" + i, _ => (string)null!);
            var adds = Enumerable.Range(0, 3_000).SelectMany(i => TestRecords.FromAction(new DeltaAction() { Add = new DeltaAddAction() { Path = $"p{i}", Tags = tags } }));
            // Room for a few batches per group: without the level terms the same reservation takes groups twice as large
            var limits = new CheckpointWriterLimits() { WriterReservation = EmptyWriterBound() + (24L << 20) };
            var (_, result, writer) = Write(Records(Protocol(), MetaData()).Concat(adds), limits: limits);

            Assert.Equal(new long[] { 656, 654, 654, 654, 384 }, result.GroupRows);
            Assert.All(writer.CloseReasons, x => Assert.Equal(GroupCloseReason.WriterMemory, x));
        }

        [Fact]
        public void GroupClosesBeforeTheProjectedSinkCapacity()
        {
            var limits = new CheckpointWriterLimits() { WriterReservation = EmptyWriterBound() + (12L << 20) };
            var (_, result, writer) = Write(Records(Protocol(), MetaData()).Concat(WideAdds(8_000, 1500)), compression: Compression.Uncompressed, limits: limits);

            // Exact sizes under the full projection; without the sink doubling the groups are larger
            Assert.Equal(new long[] { 1374, 1372, 686, 686, 686, 686, 686, 686, 1140 }, result.GroupRows);
            Assert.All(writer.CloseReasons, x => Assert.Equal(GroupCloseReason.WriterMemory, x));
        }

        [Fact]
        public void WriterProjectionIncludesGrowthOverlap()
        {
            // One protocol row: every leaf has one level and no values except the two versions, uncompressed
            using var writer = new CheckpointFileWriter(TempPath(), new DiskLedger(), new JobMemory(), Compression.Uncompressed, null);
            writer.Append(TestRecords.FromAction(new DeltaAction() { Protocol = new DeltaProtocolAction() { MinReaderVersion = 1, MinWriterVersion = 2 } })[0]);

            long expected = 0, sinks = 0, overlap = 0;
            for (int l = 0; l < CheckpointSchema.LeafCount; l++)
            {
                var leaf = (Leaf)l;
                var streams = CheckpointSchema.LevelStreams(leaf);
                long values = leaf is Leaf.ProtocolMinReader or Leaf.ProtocolMinWriter ? 4 : 0;
                long levels = 1;
                var rle = streams * (levels / 2 + 128);
                var page = values + rle;
                var sink = values + streams * levels / 2 + 1024 * (values > 0 ? 2 : 1);
                expected += 4 * streams * levels + rle + 3 * values + page;
                sinks += sink;
                overlap = Math.Max(overlap, Math.Max(Math.Max(2 * levels, rle), Math.Max(Math.Max(values, page), sink)));
            }
            // The path arrays of a one-row batch: 8 bytes per level of its widest leaf, plus one per row
            var path = 8 * 1 + 1;
            overlap = Math.Max(overlap, path);
            expected += 2 * sinks + overlap + path + Internal.Delta.ParquetFormat.Bounded.NativeConstants.WriterCodec + Internal.Delta.ParquetFormat.Bounded.NativeConstants.WriterFixed;

            Assert.Equal(expected, writer.ProjectedWriter());
            Assert.True(overlap > 1024);
        }

        [Fact]
        public void ReachingTheMetadataCapDeclines()
        {
            // Two groups' footer fits; opening the third passes the cap
            var limits = new CheckpointWriterLimits() { GroupValueBytes = 1L << 20, MetadataCap = CheckpointFileWriter.ProjectedMetadata(2) + (1L << 20) };
            var declined = Assert.Throws<CheckpointDeclinedException>(() => Write(Records(Protocol(), MetaData()).Concat(WideAdds(5_000, 1000)), limits: limits));
            Assert.Equal(MaintenanceReason.MetadataCap, declined.Reason);
            Assert.Contains("The footer of 3 row groups", declined.Message);
        }

        [Fact]
        public void ProductionArrowSettingsStartNoThreadsAndStoreTheSchema()
        {
            // The writer's own settings object, so dropping either call fails here and not only in a thread count
            using var properties = CheckpointWriterSettings.Arrow();
            Assert.False(properties.UseThreads);
            Assert.True(properties.StoreSchema);
        }

        [Fact]
        public void DiskCapIsNeverExceeded()
        {
            var ledger = new DiskLedger(64 << 10);
            var declined = Assert.Throws<CheckpointDeclinedException>(() => Write(Records(Protocol(), MetaData()).Concat(WideAdds(2_000, 1000)), compression: Compression.Uncompressed, ledger: ledger));
            Assert.Equal(MaintenanceReason.DiskCap, declined.Reason);
        }

        [Fact]
        public void FileStaysWithinItsChargeWhenAFlushDeclines()
        {
            // Room for a few flushed groups, so a group flush is the write that declines
            var ledger = new DiskLedger(3L << 20);
            var path = TempPath();
            var writer = new CheckpointFileWriter(path, ledger, new JobMemory(), Compression.Uncompressed, null, new CheckpointWriterLimits() { GroupValueBytes = 1L << 20 });
            try
            {
                var declined = Assert.Throws<CheckpointDeclinedException>(() =>
                {
                    foreach (var record in Records(Protocol(), MetaData()).Concat(WideAdds(5_000, 1000)))
                    {
                        writer.Append(record);
                    }
                });
                Assert.Equal(MaintenanceReason.DiskCap, declined.Reason);
                // Measured before the writer is disposed, which may write the rest of the file
                using var probe = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
                Assert.True(probe.Length <= writer.Charged, $"{probe.Length} bytes on disk with {writer.Charged} charged after {writer.RowGroups} groups");
            }
            finally
            {
                writer.Dispose();
            }
        }

        [Fact]
        public void AbortedWriterNeverWritesPastItsCharge()
        {
            // Room for opening the file only, so the first batch declines and disposing writes whatever the native writer holds
            var ledger = new DiskLedger(CheckpointFileWriter.OpenBytes + 1024);
            var path = TempPath();
            var writer = new CheckpointFileWriter(path, ledger, new JobMemory(), Compression.Uncompressed, null);
            var declined = Assert.Throws<CheckpointDeclinedException>(() =>
            {
                foreach (var record in Records(Protocol(), MetaData()).Concat(WideAdds(2_000, 1000)))
                {
                    writer.Append(record);
                }
                writer.Close();
            });
            Assert.Equal(MaintenanceReason.DiskCap, declined.Reason);
            writer.Dispose();

            Assert.True(new FileInfo(path).Length <= writer.Charged, $"{new FileInfo(path).Length} bytes on disk with {writer.Charged} charged");
        }

        [Fact]
        public void BatchesOfAnOpenGroupAreChargedBeforeTheNativeWriterTakesThem()
        {
            // Room for the opening, the footer and two batches of about 3 MiB, more than the footer term can absorb, so the third batch of the first group declines
            var ledger = new DiskLedger(CheckpointFileWriter.OpenBytes + (8L << 20));
            var path = TempPath();
            var writer = new CheckpointFileWriter(path, ledger, new JobMemory(), Compression.Uncompressed, null);
            var filler = new string('v', 1000);
            var stats = "{\"numRecords\":1,\"x\":\"" + filler + "\"}";
            var rows = Enumerable.Range(0, 5_000).SelectMany(i => TestRecords.FromAction(new DeltaAction()
            {
                Add = new DeltaAddAction() { Path = $"{filler}-{i:D7}.parquet", Size = i, Statistics = stats, Tags = new Dictionary<string, string>() { ["t"] = filler } }
            }));
            var declined = Assert.Throws<CheckpointDeclinedException>(() =>
            {
                foreach (var record in Records(Protocol(), MetaData()).Concat(rows))
                {
                    writer.Append(record);
                }
                writer.Close();
            });
            var groups = writer.RowGroups;
            writer.Dispose();

            Assert.Equal(MaintenanceReason.DiskCap, declined.Reason);
            Assert.True(new FileInfo(path).Length <= writer.Charged, $"{new FileInfo(path).Length} bytes on disk with {writer.Charged} charged");
            // The decline came from a batch of the first group, not from the next group's start
            Assert.Equal(1, groups);
        }

        [Fact]
        public void OneBatchOverTheWriterReservationDeclines()
        {
            // The first batch alone projects past the reservation, so no group can take it
            var limits = new CheckpointWriterLimits() { WriterReservation = EmptyWriterBound() + (1L << 20) };
            var declined = Assert.Throws<CheckpointDeclinedException>(() => Write(Records(Protocol(), MetaData()).Concat(WideAdds(2_000, 1000)), limits: limits));
            Assert.Equal(MaintenanceReason.NativeMemoryCap, declined.Reason);
            Assert.Contains("One batch projects", declined.Message);
        }

        [Fact]
        public void OneBatchOverTheDecodeReservationDeclines()
        {
            var limits = new CheckpointWriterLimits() { DecodeCap = (8L << 20) + EmptyDecodeBound() + (1L << 20) };
            var declined = Assert.Throws<CheckpointDeclinedException>(() => Write(Records(Protocol(), MetaData()).Concat(WideAdds(2_000, 1000)), limits: limits));
            Assert.Equal(MaintenanceReason.DecodeCap, declined.Reason);
            Assert.Contains("One batch projects", declined.Message);
        }

        [Fact]
        public void WriterDisposedBeforeItsFirstGroupStaysWithinTheOpeningCharge()
        {
            var ledger = new DiskLedger();
            var path = TempPath();
            var writer = new CheckpointFileWriter(path, ledger, new JobMemory(), Compression.Zstd, new string('m', CoverageMarker.MaxLength));
            writer.Dispose();

            Assert.True(new FileInfo(path).Length <= writer.Charged, $"{new FileInfo(path).Length} bytes on disk with {writer.Charged} charged");
        }

        [Fact]
        public void DeclinedOpenDoesNotDeleteAFileItNeverOpened()
        {
            var path = TempPath();
            File.WriteAllText(path, "not ours");
            // The opening charge declines before the native writer touches the path
            var declined = Assert.Throws<CheckpointDeclinedException>(() => new CheckpointFileWriter(path, new DiskLedger(1024), new JobMemory(), Compression.Zstd, null));
            Assert.Equal(MaintenanceReason.DiskCap, declined.Reason);
            Assert.True(File.Exists(path));
        }

        [Fact]
        public void FailedOpenKeepsNoDiskCharge()
        {
            var ledger = new DiskLedger();
            var memory = new JobMemory();
            var limits = new CheckpointWriterLimits() { PropertiesForTests = _ => throw new IOException("open failure") };
            Assert.Throws<IOException>(() => new CheckpointFileWriter(TempPath(), ledger, memory, Compression.Zstd, null, limits));

            Assert.Equal(0, memory.Native);
            Assert.Equal(0, ledger.Reserved);
        }

        [Fact]
        public void JobMemoryAllocatesOnlyWithinReservations()
        {
            var memory = new JobMemory(managedCap: 1 << 20);
            var first = memory.Allocate(600 << 10);
            Assert.Equal(600 << 10, memory.Managed);
            var declined = Assert.Throws<CheckpointDeclinedException>(() => memory.Allocate(500 << 10));
            Assert.Equal(MaintenanceReason.ManagedMemoryCap, declined.Reason);
            Assert.Equal(600 << 10, memory.Managed);
            memory.Free(first);
            Assert.Equal(0, memory.Managed);
            Assert.Equal(600 << 10, memory.ManagedPeak);
        }

        [Fact]
        public void BatchBuildersNeverGrow()
        {
            // A lone value at the action cap, then a full batch of realistic rows
            var lone = TestRecords.FromAction(new DeltaAction() { Add = new DeltaAddAction() { Path = "big", Statistics = new string('x', 4 << 20) } })[0];
            var memory = new JobMemory();
            var (path, result, _) = Write(Records(Protocol(), MetaData()).Append(lone).Concat(WideAdds(8_192, 200)), memory: memory);

            Assert.Equal(8_195, result.Actions);
            // Rows, row ends and the arena of the largest batch
            Assert.True(memory.ManagedPeak <= 14L << 20, $"peak {memory.ManagedPeak}");
            Assert.Equal(0, memory.Managed);
            Assert.Equal(0, memory.Native);
        }

        [Fact]
        public void NativeObjectLeftOpenFailsTheNextReservation()
        {
            var memory = new JobMemory();
            var writer = new CheckpointFileWriter(TempPath(), new DiskLedger(), memory, Compression.Zstd, null);
            Assert.Equal(96L << 20, memory.Native);
            var declined = Assert.Throws<CheckpointDeclinedException>(() => memory.ReserveNative(200L << 20));
            Assert.Equal(MaintenanceReason.NativeMemoryCap, declined.Reason);
            writer.Dispose();
            using var next = memory.ReserveNative(200L << 20);
        }

        [Fact]
        public async Task TestCheckpointKeepsAddFieldsAndLongCardinality()
        {
            var add = new DeltaAddAction()
            {
                Path = "a.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 1,
                ModificationTime = 1,
                DataChange = true,
                BaseRowId = 42,
                DefaultRowCommitVersion = 7,
                ClusteringProvider = "liquid",
                DeletionVector = new DeletionVector() { StorageType = "p", PathOrInlineDv = "dv.bin", Offset = 1, SizeInBytes = 10, Cardinality = 3_000_000_000 }
            };
            var (path, _, _) = Write(Records(Protocol(), MetaData(), new DeltaAction() { Add = add }));

            var read = (await ReadActions(path)).Single(x => x.Add != null).Add!;
            Assert.Equal(42, read.BaseRowId);
            Assert.Equal(7, read.DefaultRowCommitVersion);
            Assert.Equal("liquid", read.ClusteringProvider);
            Assert.Equal(3_000_000_000, read.DeletionVector!.Cardinality);
        }

        [Fact]
        public void IntFieldsOutsideTheIntRangeDecline()
        {
            var add = new DeltaAddAction() { Path = "a", DeletionVector = new DeletionVector() { StorageType = "p", PathOrInlineDv = "dv", Offset = 1L << 40, SizeInBytes = 1, Cardinality = 1 } };
            var declined = Assert.Throws<CheckpointDeclinedException>(() => Write(Records(Protocol(), MetaData(), new DeltaAction() { Add = add })));
            Assert.Equal(MaintenanceReason.ValueOutOfRange, declined.Reason);
        }

        [Fact]
        public void FailedWriterOpenDoesNotLeaveAPhantomDiskCharge()
        {
            var nonexistentDirectory = Path.Combine(AppContext.BaseDirectory, $"missing-{Guid.NewGuid():N}");
            var path = Path.Combine(nonexistentDirectory, "out.parquet");
            var ledger = new DiskLedger();
            var memory = new JobMemory();
            Assert.ThrowsAny<Exception>(() => new CheckpointFileWriter(path, ledger, memory, Compression.Uncompressed, null));
            Assert.False(File.Exists(path));
            Assert.Equal(0, memory.Native);
            Assert.Equal(0, ledger.Reserved);
        }

        [Fact]
        public void FailedDiskReservationDoesNotWriteOnDispose()
        {
            var path = Path.Combine(AppContext.BaseDirectory, $"disk-{Guid.NewGuid():N}.parquet");
            var ledger = new DiskLedger(CheckpointFileWriter.OpenBytes);
            var memory = new JobMemory();
            try
            {
                var writer = new CheckpointFileWriter(path, ledger, memory, Compression.Uncompressed, null);
                try
                {
                    var record = TestRecords.FromAction(new DeltaAction
                    {
                        Add = new DeltaAddAction { Path = "a.parquet", Statistics = new string('x', 1 << 20) }
                    })[0];
                    writer.Append(record);
                    var failure = Assert.Throws<CheckpointDeclinedException>(() => writer.Close());
                    Assert.Equal(MaintenanceReason.DiskCap, failure.Reason);
                }
                finally
                {
                    writer.Dispose();
                }
                var actualBytes = new FileInfo(path).Length;
                Assert.True(actualBytes <= ledger.Reserved,
                    $"Disposal wrote {actualBytes} bytes with only {ledger.Reserved} reserved (cap {ledger.Cap}).");
            }
            finally
            {
                File.Delete(path);
            }
        }

        [Fact]
        public void FailedDiskAdmissionDoesNotDeleteAnUnopenedExistingPath()
        {
            var path = Path.Combine(AppContext.BaseDirectory, "existing-" + Guid.NewGuid().ToString("N") + ".parquet");
            try
            {
                File.WriteAllText(path, "existing bytes");
                var error = Assert.Throws<CheckpointDeclinedException>(() => new CheckpointFileWriter(path, new DiskLedger(0), new JobMemory(), Compression.Uncompressed, null));
                Assert.Equal(MaintenanceReason.DiskCap, error.Reason);
                Assert.True(File.Exists(path), "Admission failed before FileWriter opened the path, but constructor cleanup deleted it.");
                Assert.Equal("existing bytes", File.ReadAllText(path));
            }
            finally
            {
                File.Delete(path);
            }
        }
    }
}
