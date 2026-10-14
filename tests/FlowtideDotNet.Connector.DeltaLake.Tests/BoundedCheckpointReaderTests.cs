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

using Apache.Arrow.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.CheckpointReading;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using ParquetSharp;
using Stowage;
using Xunit.Abstractions;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class BoundedCheckpointReaderTests : IDisposable
    {
        private readonly ITestOutputHelper _output;
        private readonly string _directory;

        public BoundedCheckpointReaderTests(ITestOutputHelper output)
        {
            _output = output;
            _directory = Path.Combine(Path.GetTempPath(), "flowtide-reader-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(_directory);
        }

        public void Dispose()
        {
            Directory.Delete(_directory, true);
        }

        private static List<byte[]> ReadBounded(string path, JobMemory? memory = null, int entryCap = TailRecordEncoder.DefaultEntryCap, Func<ScanResult, ScanResult>? doctor = null)
        {
            memory ??= new JobMemory();
            var scan = CheckpointPageScan.Scan(path, memory);
            scan = doctor?.Invoke(scan) ?? scan;
            var records = new List<byte[]>();
            using (var reader = new BoundedCheckpointReader(path, scan, memory, entryCap))
            {
                reader.Read(record => records.Add(record.ToArray()));
            }
            memory.Release(scan.ModelBytes);
            return records;
        }

        // The Arrow reader's actions as canonical records, with its known losses applied to both sides
        private static async Task<List<string>> ReadArrow(string path)
        {
            var storage = Files.Of.LocalDisk(Path.GetDirectoryName(path)!);
            var entry = (await storage.Ls("/")).Single(x => x.Name == Path.GetFileName(path));
            var records = new List<string>();
            await foreach (var action in new ParquetCheckpointReader().ReadCheckpointFile(storage, entry))
            {
                foreach (var record in TestRecords.FromAction(action))
                {
                    records.Add(Normalize(record));
                }
            }
            records.Sort(StringComparer.Ordinal);
            return records;
        }

        // The Arrow reader drops map entries with null values, reads a null map as empty, and keeps only the parsed record count
        private static string Normalize(ReadOnlySpan<byte> record)
        {
            var action = CheckpointRecord.ToAction(record);
            static Dictionary<string, string>? Drop(Dictionary<string, string>? map) => (map ?? new Dictionary<string, string>()).Where(x => x.Value != null).ToDictionary(x => x.Key, x => x.Value);
            if (action.Add != null)
            {
                action.Add.PartitionValues = Drop(action.Add.PartitionValues);
                action.Add.Tags = Drop(action.Add.Tags);
                action.Add.Statistics = action.Add.Statistics?.Replace(",\"tightBounds\":true", "").Replace(",\"tightBounds\":false", "");
            }
            if (action.Remove != null)
            {
                action.Remove.PartitionValues = Drop(action.Remove.PartitionValues);
            }
            if (action.MetaData != null)
            {
                action.MetaData.Configuration = Drop(action.MetaData.Configuration);
                if (action.MetaData.Format != null)
                {
                    action.MetaData.Format.Options = Drop(action.MetaData.Format.Options);
                }
            }
            return RandomActions.Canonical(action);
        }

        private static List<string> Canonical(IEnumerable<byte[]> records) => records.Select(x => Normalize(x)).Order(StringComparer.Ordinal).ToList();

        private string WriteOwn(IEnumerable<byte[]> records, CheckpointWriterLimits? limits = null, Compression compression = Compression.Zstd)
        {
            var path = Path.Combine(_directory, Guid.NewGuid().ToString("N") + ".parquet");
            using var writer = new CheckpointFileWriter(path, new DiskLedger(), new JobMemory(), compression, null, limits);
            foreach (var record in records)
            {
                writer.Append(record);
            }
            writer.Close();
            return path;
        }

        [Fact]
        public async Task BoundedReaderMatchesArrowReader()
        {
            var inputs = new List<string>();
            var storage = Files.Of.LocalDisk(_directory);
            await LegacyTableGenerator.Create(storage, "legacy", 200_000, 4, 0, true);
            inputs.Add(Path.Combine(_directory, "legacy", "_delta_log", $"{0:D20}.checkpoint.parquet"));
            inputs.AddRange(Directory.EnumerateFiles("../../../testdata", "*.checkpoint.parquet", SearchOption.AllDirectories).Select(Path.GetFullPath));
            inputs.Add(WriteOwn(CheckpointPageScanTests.RealisticRecords(20_000)));
            inputs.Add(WriteOwn(CheckpointPageScanTests.RealisticRecords(5_000), new CheckpointWriterLimits()
            {
                PropertiesForTests = c => DeltaTestKit.Properties(b => b.Compression(c).DataPageVersion(ParquetDataPageVersion.V2).EnableDictionary().DisableStatistics().DataPagesize(4096))
            }));
            inputs.Add(WriteOwn(CheckpointPageScanTests.RealisticRecords(5_000), new CheckpointWriterLimits()
            {
                PropertiesForTests = c => DeltaTestKit.Properties(b => b.Compression(c).DataPageVersion(ParquetDataPageVersion.V2).DisableDictionary().DisableStatistics().DataPagesize(1024))
            }, Compression.Snappy));

            foreach (var input in inputs)
            {
                var bounded = Canonical(ReadBounded(input));
                var arrow = await ReadArrow(input);
                _output.WriteLine($"{Path.GetFileName(Path.GetDirectoryName(Path.GetDirectoryName(input)))}/{Path.GetFileName(input)}: {bounded.Count} records");
                Assert.Equal(arrow.Count, bounded.Count);
                var differs = Enumerable.Range(0, arrow.Count).FirstOrDefault(i => arrow[i] != bounded[i], -1);
                Assert.True(differs < 0, differs < 0 ? "" : $"arrow:   {arrow[differs]} | bounded: {bounded[differs]}");
            }
        }

        [Fact]
        public void OwnOutputDigestMatchesTheWriter()
        {
            var records = CheckpointPageScanTests.RealisticRecords(10_000).ToList();
            var path = Path.Combine(_directory, "digest.parquet");
            CheckpointWriteResult written;
            using (var writer = new CheckpointFileWriter(path, new DiskLedger(), new JobMemory(), Compression.Zstd, null))
            {
                foreach (var record in records)
                {
                    writer.Append(record);
                }
                written = writer.Close();
            }
            using var hash = System.Security.Cryptography.IncrementalHash.CreateHash(System.Security.Cryptography.HashAlgorithmName.SHA256);
            var count = 0;
            var memory = new JobMemory();
            var scan = CheckpointPageScan.Scan(path, memory);
            using (var reader = new BoundedCheckpointReader(path, scan, memory))
            {
                reader.Read(record =>
                {
                    Span<byte> length = stackalloc byte[5];
                    var value = (uint)record.Length;
                    var i = 0;
                    while (value >= 0x80)
                    {
                        length[i++] = (byte)(value | 0x80);
                        value >>= 7;
                    }
                    length[i++] = (byte)value;
                    hash.AppendData(length.Slice(0, i));
                    hash.AppendData(record);
                    count++;
                });
            }
            Assert.Equal(written.Actions, count);
            Assert.Equal(written.Sha256, hash.GetHashAndReset());
        }

        [Fact]
        public void ViewsAreCopiedBeforeTheReaderAdvances()
        {
            // Tiny compressed pages: every row's string value lives in a decompression buffer the next batch overwrites
            var records = Enumerable.Range(0, 3000).SelectMany(i => TestRecords.FromAction(new DeltaAction() { Add = new DeltaAddAction() { Path = $"file-{i:D6}-{new string((char)('a' + i % 26), 40)}", Size = i } })).ToList();
            var path = WriteOwn(records, new CheckpointWriterLimits()
            {
                PropertiesForTests = c => DeltaTestKit.Properties(b => b.Compression(c).DisableDictionary().DisableStatistics().DataPagesize(512).WriteBatchSize(16))
            });
            var read = ReadBounded(path);
            Assert.Equal(records.Select(x => Convert.ToHexString(x)).Order(), read.Select(x => Convert.ToHexString(x)).Order());
        }

        [Theory]
        [InlineData(1023, false)]
        [InlineData(1024, false)]
        [InlineData(1025, false)]
        [InlineData(1025, true)]
        [InlineData(4097, true)]
        [InlineData(TailRecordEncoder.DefaultEntryCap, false)]
        public void RepeatedRowsCrossingLevelBatchesReadExactly(int entries, bool v2)
        {
            // One action's map spans several 1,024-level batches and tiny pages, with null values, between two short neighbours
            var map = Enumerable.Range(0, entries).ToDictionary(i => $"k{i:D6}", i => i % 7 == 3 ? null! : $"v{i}");
            var actions = new[]
            {
                new DeltaAction() { Add = new DeltaAddAction() { Path = "before", PartitionValues = new Dictionary<string, string>() { ["a"] = "1" } } },
                new DeltaAction() { Add = new DeltaAddAction() { Path = "wide", PartitionValues = map, Tags = entries < TailRecordEncoder.DefaultEntryCap ? new Dictionary<string, string>() { ["t"] = "x" } : null } },
                new DeltaAction() { MetaData = new DeltaMetadataAction() { Id = "m", PartitionColumns = Enumerable.Range(0, Math.Min(entries, 3000)).Select(i => $"c{i}").ToList() } },
                new DeltaAction() { Add = new DeltaAddAction() { Path = "after", PartitionValues = new Dictionary<string, string>() } }
            };
            var records = actions.SelectMany(x => TestRecords.FromAction(x)).ToList();
            var path = WriteOwn(records, new CheckpointWriterLimits()
            {
                PropertiesForTests = c => DeltaTestKit.Properties(b => b.Compression(c).DisableDictionary().DisableStatistics().DataPagesize(512).WriteBatchSize(16)
                    .DataPageVersion(v2 ? ParquetDataPageVersion.V2 : ParquetDataPageVersion.V1))
            });
            var read = ReadBounded(path);
            Assert.Equal(records.Select(x => Convert.ToHexString(x)), read.Select(x => Convert.ToHexString(x)));
        }

        [Fact]
        public async Task BitPackedV1LevelsAreAdmitted()
        {
            // Legacy BIT_PACKED definition levels 2, 0, 2 at two bits each, least significant first as the pinned reader decodes them
            var parquet = new SyntheticParquet() { Rows = 3 };
            parquet.Schema.Add(new SyntheticParquet.Element("schema", Repetition: 0, Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("add", Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("path", Type: 6, Converted: 0));
            var body = new byte[] { 0x22, 1, 0, 0, 0, (byte)'a', 1, 0, 0, 0, (byte)'b' };
            var page = SyntheticParquet.DataPage(3, body.Length, definitionEncoding: 4);
            body.CopyTo(page, page.Length - body.Length);
            parquet.RowGroups.Add(new List<SyntheticParquet.Chunk>
            {
                new() { Path = new[] { "add", "path" }, NumValues = 3, Encodings = new[] { 0, 4 }, Pages = { page } }
            });
            var path = Path.Combine(_directory, Guid.NewGuid().ToString("N") + ".parquet");
            File.WriteAllBytes(path, parquet.Build());

            var read = ReadBounded(path);
            Assert.Equal(new[] { "a", "b" }, read.Select(x => CheckpointRecord.ToAction(x).Add!.Path));
            Assert.Equal(await ReadArrow(path), Canonical(read));
        }

        [Fact]
        public void RowSpanningPagesBeyondTheCapDeclines()
        {
            // Two values together over the action cap, each under it
            var half = new string('x', BoundedCheckpointReader.ActionCap / 2 + 10);
            var path = WriteOwn(TestRecords.FromAction(new DeltaAction() { Add = new DeltaAddAction() { Path = half, Statistics = half } }));
            var memory = new JobMemory();
            var declined = Assert.Throws<CheckpointDeclinedException>(() => ReadBounded(path, memory));
            Assert.Equal(MaintenanceReason.RecordOverCap, declined.Reason);
            Assert.Equal(DeclineDependency.Root, declined.Dependency);
            // The row buffer holds the first value only: growing before the check would take it to the cap (7.5 MB measured, against 3.3 MB)
            Assert.True(memory.ManagedPeak < BoundedCheckpointReader.ActionCap + (1L << 20), $"Peak {memory.ManagedPeak}");
        }

        [Fact]
        public void RootRowOverTheEntryCapDeclines()
        {
            var tags = Enumerable.Range(0, 11).ToDictionary(i => "k" + i, i => "v" + i);
            var path = WriteOwn(TestRecords.FromAction(new DeltaAction() { Add = new DeltaAddAction() { Path = "a", Tags = tags } }));
            Assert.Single(ReadBounded(path, entryCap: 11));
            var declined = Assert.Throws<CheckpointDeclinedException>(() => ReadBounded(path, entryCap: 10));
            Assert.Equal(MaintenanceReason.EntriesOverCap, declined.Reason);
        }

        [Fact]
        public void RowTotalMismatchMakesTheRootUnsupported()
        {
            var path = WriteOwn(CheckpointPageScanTests.RealisticRecords(100));
            // The footer as admitted, then changed: one row more than the file holds
            var declined = Assert.Throws<CheckpointDeclinedException>(() => ReadBounded(path, doctor: scan => scan with
            {
                Footer = new ParquetFooterModel()
                {
                    Schema = scan.Footer.Schema,
                    RowGroupRows = scan.Footer.RowGroupRows.Select(x => x + 1).ToList(),
                    Chunks = scan.Footer.Chunks,
                    FooterLength = scan.Footer.FooterLength
                }
            }));
            Assert.Equal(MaintenanceReason.RootDecodeFailure, declined.Reason);
            Assert.Contains("ended before its row group", declined.Message);
        }

        [Fact]
        public void RowTotalBelowTheDataIsRefused()
        {
            var path = WriteOwn(CheckpointPageScanTests.RealisticRecords(100));
            // One row fewer than the file holds: the trailing levels of the last batch must not be dropped silently
            var declined = Assert.Throws<CheckpointDeclinedException>(() => ReadBounded(path, doctor: scan => scan with
            {
                Footer = new ParquetFooterModel()
                {
                    Schema = scan.Footer.Schema,
                    RowGroupRows = scan.Footer.RowGroupRows.Select(x => x - 1).ToList(),
                    Chunks = scan.Footer.Chunks,
                    FooterLength = scan.Footer.FooterLength
                }
            }));
            Assert.Equal(MaintenanceReason.RootDecodeFailure, declined.Reason);
            Assert.Contains("more or fewer levels", declined.Message);
        }

        [Fact]
        public void ColumnShorterThanItsChunkIsRefused()
        {
            var path = WriteOwn(CheckpointPageScanTests.RealisticRecords(100));
            // The chunk claims more levels than its pages hold, so the reader runs out early
            var declined = Assert.Throws<CheckpointDeclinedException>(() => ReadBounded(path, doctor: scan => scan with
            {
                Footer = new ParquetFooterModel()
                {
                    Schema = scan.Footer.Schema,
                    RowGroupRows = scan.Footer.RowGroupRows,
                    Chunks = scan.Footer.Chunks.Select(group => group.Select(chunk => chunk == null ? null : new FooterChunk()
                    {
                        Codec = chunk.Codec,
                        Encodings = chunk.Encodings,
                        NumValues = chunk.NumValues + 1,
                        TotalCompressedSize = chunk.TotalCompressedSize,
                        TotalUncompressedSize = chunk.TotalUncompressedSize,
                        DataPageOffset = chunk.DataPageOffset,
                        DictionaryPageOffset = chunk.DictionaryPageOffset
                    }).ToArray()).ToList(),
                    FooterLength = scan.Footer.FooterLength
                }
            }));
            Assert.Equal(MaintenanceReason.RootDecodeFailure, declined.Reason);
            Assert.Contains("more or fewer levels", declined.Message);
        }

        [Fact]
        public void ZeroProgressReadIsRefused()
        {
            var path = WriteOwn(CheckpointPageScanTests.RealisticRecords(100));
            var memory = new JobMemory();
            var scan = CheckpointPageScan.Scan(path, memory);
            using var reader = new BoundedCheckpointReader(path, scan, memory);
            var calls = 0;
            // The first batches report neither levels nor values while the column still has data; only the progress check refuses them
            reader.LevelsReadHookForTests = (ProjectedLeaf leaf, Span<short> definitions, ref long levels, ref long values) =>
            {
                if (calls++ < 100)
                {
                    levels = 0;
                    values = 0;
                }
            };
            var declined = Assert.Throws<CheckpointDeclinedException>(() => reader.Read(_ => { }));
            Assert.Equal(MaintenanceReason.RootDecodeFailure, declined.Reason);
            Assert.Contains("returned 0 levels and 0 values", declined.Message);
        }

        [Fact]
        public void LevelAboveSchemaMaximumIsRefused()
        {
            var path = WriteOwn(CheckpointPageScanTests.RealisticRecords(100));
            var memory = new JobMemory();
            var scan = CheckpointPageScan.Scan(path, memory);
            using var reader = new BoundedCheckpointReader(path, scan, memory);
            var raised = false;
            // A null level raised above the maximum leaves the defined count as it was, so only the row's level check refuses it
            reader.LevelsReadHookForTests = (ProjectedLeaf leaf, Span<short> definitions, ref long levels, ref long values) =>
            {
                if (!raised && leaf.Field == Leaf.AddPath)
                {
                    var i = definitions.Slice(0, (int)levels).IndexOfAnyExcept((short)leaf.MaxDefinition);
                    definitions[i] = (short)(leaf.MaxDefinition + 1);
                    raised = true;
                }
            };
            var declined = Assert.Throws<CheckpointDeclinedException>(() => reader.Read(_ => { }));
            Assert.Equal(MaintenanceReason.RootDecodeFailure, declined.Reason);
            Assert.Contains("above its schema maximum", declined.Message);
        }

        [Fact]
        public void InvalidDictionaryIndexMakesTheRootUnsupported()
        {
            var records = Enumerable.Range(0, 200).SelectMany(i => TestRecords.FromAction(new DeltaAction() { Add = new DeltaAddAction() { Path = "p" + (i % 5), Size = i } })).ToList();
            var path = WriteOwn(records, new CheckpointWriterLimits()
            {
                PropertiesForTests = c => DeltaTestKit.Properties(b => b.Compression(Compression.Uncompressed).EnableDictionary().DisableStatistics())
            }, Compression.Uncompressed);
            var scan = CheckpointPageScan.Scan(path, new JobMemory());
            var leaf = scan.Projection.Leaves.Single(x => x.Role == ProjectedRole.Field && x.Field == Leaf.AddPath);
            var chunk = scan.Footer.Chunks[0][leaf.Index]!;

            // Raise the bit width of the first data page's dictionary indices past the dictionary
            var bytes = File.ReadAllBytes(path);
            var reader = new ThriftCompactReader(bytes.AsSpan((int)chunk.DataPageOffset), new ThriftBudget(1 << 20));
            PageHeader.Read(ref reader);
            var body = (int)chunk.DataPageOffset + reader.Position;
            var levels = BitConverter.ToInt32(bytes, body);
            bytes[body + 4 + levels] = 20;
            File.WriteAllBytes(path, bytes);

            var declined = Assert.Throws<CheckpointDeclinedException>(() => ReadBounded(path));
            Assert.Equal(MaintenanceReason.RootDecodeFailure, declined.Reason);
        }

        [Fact]
        public void NullPartitionValuesSurviveTheRewrite()
        {
            var add = new DeltaAddAction() { Path = "a", PartitionValues = new Dictionary<string, string>() { ["p"] = null!, ["q"] = "x" } };
            var path = WriteOwn(TestRecords.FromAction(new DeltaAction() { Add = add }));
            var read = CheckpointRecord.ToAction(ReadBounded(path).Single()).Add!;
            Assert.True(read.PartitionValues!.ContainsKey("p"));
            Assert.Null(read.PartitionValues["p"]);
            Assert.Equal("x", read.PartitionValues["q"]);
        }

        [Fact]
        public void NullAndEmptyContainersKeepTheirDistinction()
        {
            var actions = new[]
            {
                new DeltaAction() { Add = new DeltaAddAction() { Path = "null", PartitionValues = null, Tags = null } },
                new DeltaAction() { Add = new DeltaAddAction() { Path = "empty", PartitionValues = new Dictionary<string, string>(), Tags = new Dictionary<string, string>() } },
                new DeltaAction() { MetaData = new DeltaMetadataAction() { Id = "m", PartitionColumns = new List<string>(), Configuration = null } },
                new DeltaAction() { Protocol = new DeltaProtocolAction() { MinReaderVersion = 1, MinWriterVersion = 2, ReaderFeatures = null, WriterFeatures = new List<string>() { null!, "x" } } }
            };
            var records = actions.SelectMany(x => TestRecords.FromAction(x)).ToList();
            var read = ReadBounded(WriteOwn(records));
            Assert.Equal(records.Select(Convert.ToHexString).Order(), read.Select(Convert.ToHexString).Order());
        }

        private async Task<string> ParsedRoot(params object?[]?[] statistics)
        {
            var storage = Files.Of.LocalDisk(_directory);
            var columns = new Dictionary<string, (string Name, IArrowType Type)[]>(DeltaLogReplayTests.AddWithParsedStatistics)
            {
                ["add"] = DeltaLogReplayTests.AddWithParsedStatistics["add"].Select(x => x.Name != "stats_parsed" ? x : ("stats_parsed", (IArrowType)new StructType(new List<Apache.Arrow.Field>()
                {
                    new Apache.Arrow.Field("numRecords", Int64Type.Default, true),
                    new Apache.Arrow.Field("tightBounds", BooleanType.Default, true),
                    new Apache.Arrow.Field("minValues", new StructType(new List<Apache.Arrow.Field>() { new Apache.Arrow.Field("userkey", Int64Type.Default, true) }), true),
                    new Apache.Arrow.Field("maxValues", new StructType(new List<Apache.Arrow.Field>() { new Apache.Arrow.Field("userkey", Int64Type.Default, true) }), true),
                    new Apache.Arrow.Field("nullCount", new StructType(new List<Apache.Arrow.Field>() { new Apache.Arrow.Field("userkey", Int64Type.Default, true) }), true)
                }))).ToArray()
            };
            var table = "parsed" + Guid.NewGuid().ToString("N");
            var rows = new List<(string, object?[])>()
            {
                ("protocol", new object?[] { 1, 2 }),
                ("metaData", new object?[] { "table-id", DeltaTestKit.UserSchema, 0L })
            };
            for (int i = 0; i < statistics.Length; i++)
            {
                rows.Add(("add", new object?[] { $"f{i}.parquet", 1L, 0L, true, null, null, statistics[i] }));
            }
            await DeltaLogReplayTests.WriteClassicCheckpoint(storage, table, 0, columns, rows.ToArray());
            return Path.Combine(_directory, table, "_delta_log", $"{0:D20}.checkpoint.parquet");
        }

        [Fact]
        public async Task ParsedOnlyFlagIsSetFromEveryPresenceLeaf()
        {
            var counted = await ParsedRoot(new object?[] { 42L, true, null, null, null }, new object?[] { 7L, null, null, null, null }, null);
            var adds = ReadBounded(counted).Where(x => CheckpointRecord.Kind(x) == RecordKind.Add).ToList();
            Assert.True(CheckpointRecord.ParsedOnly(adds[0]));
            Assert.Equal("{\"numRecords\":42,\"tightBounds\":true}", CheckpointRecord.ToAction(adds[0]).Add!.Statistics);
            Assert.Equal("{\"numRecords\":7}", CheckpointRecord.ToAction(adds[1]).Add!.Statistics);
            Assert.False(CheckpointRecord.ParsedOnly(adds[2]));
            Assert.Null(CheckpointRecord.ToAction(adds[2]).Add!.Statistics);

            foreach (var presence in new[] { new object?[] { 1L, null, new object?[] { 1L }, null, null }, new object?[] { 1L, null, null, new object?[] { 1L }, null }, new object?[] { 1L, null, null, null, new object?[] { 0L } } })
            {
                var declined = Assert.Throws<CheckpointDeclinedException>(() => ReadBounded(ParsedRoot(presence).Result));
                Assert.Equal(MaintenanceReason.ParsedOnlyRoot, declined.Reason);
            }
        }

        [Fact]
        public async Task UnknownNonNullActionColumnDeclines()
        {
            var storage = Files.Of.LocalDisk(_directory);
            var columns = new Dictionary<string, (string Name, IArrowType Type)[]>(DeltaLogReplayTests.CheckpointColumns) { ["rowIdHighWaterMark"] = new (string, IArrowType)[] { ("value", Int64Type.Default) } };
            await DeltaLogReplayTests.WriteClassicCheckpoint(storage, "unknown", 0, columns,
                ("protocol", new object?[] { 1, 2 }), ("metaData", new object?[] { "id", DeltaTestKit.UserSchema, 0L }), ("rowIdHighWaterMark", new object?[] { 5L }));
            var declined = Assert.Throws<CheckpointDeclinedException>(() => ReadBounded(Path.Combine(_directory, "unknown", "_delta_log", $"{0:D20}.checkpoint.parquet")));
            Assert.Equal(MaintenanceReason.UnknownColumn, declined.Reason);
        }

        [Fact]
        public void SinkErrorsAreNotTheRootsDecodeFailure()
        {
            var path = WriteOwn(CheckpointPageScanTests.RealisticRecords(100));
            var memory = new JobMemory();
            var scan = CheckpointPageScan.Scan(path, memory);
            try
            {
                using var reader = new BoundedCheckpointReader(path, scan, memory);
                // A probe-mode sink writes the output, whose errors belong to the writer, not to the root
                Assert.Throws<InvalidCastException>(() => reader.Read(_ => throw new InvalidCastException("writer fault")));
            }
            finally
            {
                memory.Release(scan.ModelBytes);
            }
        }

        [Fact]
        public void NativeSinkErrorsAreNotTheRootsDecodeFailure()
        {
            var path = WriteOwn(CheckpointPageScanTests.RealisticRecords(100));
            var notParquet = Path.Combine(Path.GetDirectoryName(path)!, $"not-parquet-{Guid.NewGuid():N}.bin");
            File.WriteAllBytes(notParquet, new byte[64]);
            var memory = new JobMemory();
            var scan = CheckpointPageScan.Scan(path, memory);
            try
            {
                using var reader = new BoundedCheckpointReader(path, scan, memory);
                // A native error raised by the sink, as the probe-mode writer raises it, belongs to the writer
                Assert.Throws<ParquetException>(() => reader.Read(_ =>
                {
                    using var native = new ParquetFileReader(notParquet);
                }));
            }
            finally
            {
                memory.Release(scan.ModelBytes);
            }
        }

        [Fact]
        public async Task UnknownColumnWithOnlyNullsIsIgnored()
        {
            var storage = Files.Of.LocalDisk(_directory);
            var columns = new Dictionary<string, (string Name, IArrowType Type)[]>(DeltaLogReplayTests.CheckpointColumns) { ["rowIdHighWaterMark"] = new (string, IArrowType)[] { ("value", Int64Type.Default) } };
            await DeltaLogReplayTests.WriteClassicCheckpoint(storage, "nulls", 0, columns,
                ("protocol", new object?[] { 1, 2 }), ("metaData", new object?[] { "id", DeltaTestKit.UserSchema, 0L }), ("rowIdHighWaterMark", new object?[] { 5L }));
            // A column that exists only as nulls in every row
            await DeltaLogReplayTests.WriteClassicCheckpoint(storage, "nulls", 1, columns,
                ("protocol", new object?[] { 1, 2 }), ("metaData", new object?[] { "id", DeltaTestKit.UserSchema, 0L }), ("rowIdHighWaterMark", null!));
            Assert.Equal(2, ReadBounded(Path.Combine(_directory, "nulls", "_delta_log", $"{1:D20}.checkpoint.parquet")).Count);
        }

        [Fact]
        public void NestedUnknownColumnContinuingItsOuterListDeclinesAsUnknown()
        {
            // [["x"], []]: the outer list continues with an empty inner list, below the inner list's entry level, as Dremel allows
            var inner = new ListType(StringType.Default);
            var extra = new Apache.Arrow.ListArray.Builder(inner);
            var innerLists = (Apache.Arrow.ListArray.Builder)extra.ValueBuilder;
            var strings = (Apache.Arrow.StringArray.Builder)innerLists.ValueBuilder;
            extra.Append();
            innerLists.Append();
            strings.Append("x");
            innerLists.Append();
            var addType = new StructType(new List<Apache.Arrow.Field>() { new Apache.Arrow.Field("path", StringType.Default, true) });
            var schema = new Apache.Arrow.Schema(new List<Apache.Arrow.Field>()
            {
                new Apache.Arrow.Field("add", addType, true),
                new Apache.Arrow.Field("extra", new ListType(inner), true)
            }, null);
            var path = Path.Combine(_directory, "nested-unknown-" + Guid.NewGuid().ToString("N") + ".parquet");
            using (var batch = new Apache.Arrow.RecordBatch(schema, new Apache.Arrow.IArrowArray[] { new Apache.Arrow.StructArray(addType, 1, new Apache.Arrow.IArrowArray[] { new Apache.Arrow.StringArray.Builder().Append("p").Build() }, Apache.Arrow.ArrowBuffer.Empty, 0), extra.Build() }, 1))
            using (var writer = new ParquetSharp.Arrow.FileWriter(path, schema))
            {
                writer.WriteRecordBatch(batch);
                writer.Close();
            }

            var declined = Assert.Throws<CheckpointDeclinedException>(() => ReadBounded(path));
            Assert.Equal(MaintenanceReason.UnknownColumn, declined.Reason);
        }

        [Fact]
        public async Task SidecarRootIsUnsupported()
        {
            var storage = Files.Of.LocalDisk(_directory);
            await DeltaLogReplayTests.WriteClassicCheckpoint(storage, "sidecar", 0, DeltaLogReplayTests.CheckpointColumns,
                ("protocol", new object?[] { 1, 2 }), ("metaData", new object?[] { "id", DeltaTestKit.UserSchema, 0L }), ("sidecar", new object?[] { "s.parquet", 10L, 0L }));
            var declined = Assert.Throws<CheckpointDeclinedException>(() => ReadBounded(Path.Combine(_directory, "sidecar", "_delta_log", $"{0:D20}.checkpoint.parquet")));
            Assert.Equal(MaintenanceReason.SidecarRoot, declined.Reason);
        }

        [Fact]
        public void RowGroupReadersAreDisposedBeforeTheNextGroup()
        {
            // Groups close per batch of at most 8,192 rows
            var path = WriteOwn(CheckpointPageScanTests.RealisticRecords(40_000), new CheckpointWriterLimits() { GroupValueBytes = 256 << 10 });
            var memory = new JobMemory();
            var scan = CheckpointPageScan.Scan(path, memory);
            Assert.True(scan.Footer.RowGroupRows.Count >= 4, $"{scan.Footer.RowGroupRows.Count} groups");
            using var reader = new BoundedCheckpointReader(path, scan, memory);
            var records = 0;
            reader.Read(_ => records++);
            Assert.Equal(40_005, records);
            Assert.True(reader.MaxOpenColumnReaders <= scan.Projection.Leaves.Count, $"{reader.MaxOpenColumnReaders} readers open at once");
        }

        [Fact]
        public void CalibrationMatchesThePinnedParquetSharp()
        {
            var version = typeof(ParquetFileReader).Assembly.GetCustomAttributes(typeof(System.Reflection.AssemblyInformationalVersionAttribute), false)
                .Cast<System.Reflection.AssemblyInformationalVersionAttribute>().Single().InformationalVersion;
            Assert.StartsWith(NativeConstants.PinnedParquetSharp, version);
        }

        [Fact]
        public void EveryNativeTermHasAnAuditRow()
        {
            var audit = File.ReadAllLines("../../../../../src/FlowtideDotNet.Connector.DeltaLake/Internal/Delta/ParquetFormat/Bounded/native-audit.md");
            var rows = audit.Where(x => x.StartsWith("| ") && !x.StartsWith("| Id") && !x.StartsWith("|---")).Select(x => x.Split('|')[1].Trim()).ToList();
            var constants = typeof(NativeConstants).GetFields().Where(x => x.IsLiteral && x.FieldType == typeof(long)).Select(x => x.Name).ToList();
            Assert.Equal(constants.Order(), rows.Order());
            Assert.Equal(constants.Order(), NativeConstants.Audit.Select(x => x.Id).Order());
            foreach (var term in NativeConstants.Audit)
            {
                Assert.Equal((long)typeof(NativeConstants).GetField(term.Id)!.GetValue(null)!, term.Reserved);
            }
        }

        [Fact]
        public void ReservedValuesAreAtLeastTheirDerivedBounds()
        {
            foreach (var term in NativeConstants.Audit)
            {
                if (term.Status == AuditStatus.Resolved)
                {
                    Assert.True(term.DerivedBound != null && term.Reserved >= term.DerivedBound, $"{term.Id} reserves {term.Reserved} under its derived bound {term.DerivedBound}");
                }
                else
                {
                    Assert.True(term.AcceptedAtGate, $"{term.Id} is unresolved and not accepted at the gate");
                }
                // A fit above the reservation falsifies the row, which then reserves at least 1.5 times the fit
                if (term.Fit != null)
                {
                    Assert.True(term.Reserved * 2 >= term.Fit * 3, $"{term.Id} reserves {term.Reserved} for a fit of {term.Fit}");
                }
            }
        }

        [Fact]
        public void DecodeBoundIncludesGrowthOverlap()
        {
            // A compressed chunk whose largest decompression buffer grows across pages: 1,000 then 5,000 uncompressed bytes
            var first = SyntheticParquet.Page(0, 600, 1000, h => h.I32(1, 5).I32(2, 0).I32(3, 3).I32(4, 3), 5);
            var second = SyntheticParquet.Page(0, 900, 5000, h => h.I32(1, 5).I32(2, 0).I32(3, 3).I32(4, 3), 5);
            var parquet = new SyntheticParquet();
            parquet.Schema.Add(new SyntheticParquet.Element("schema", Repetition: 0, Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("add", Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("path", Type: 6));
            var chunk = new SyntheticParquet.Chunk() { Path = new[] { "add", "path" }, NumValues = 10, Codec = 6, Pages = { first, second }, UncompressedExtra = (1000 - 600) + (5000 - 900) };
            parquet.RowGroups.Add(new List<SyntheticParquet.Chunk>() { chunk });
            var path = Path.Combine(_directory, "growth.parquet");
            File.WriteAllBytes(path, parquet.Build());
            var result = CheckpointPageScan.Scan(path, new JobMemory());

            var stream = first.Length + second.Length;
            var header = Math.Max(SyntheticParquet.HeaderLength(first, 600), SyntheticParquet.HeaderLength(second, 900));
            // S + U + 2H + codec, then the largest compressed body, then the overlap of the largest grow-only buffer
            var overlap = Math.Max(stream, 5000);
            var expected = stream + 5000 + 2 * header + NativeConstants.CodecPerLeaf + 900 + overlap + NativeConstants.ReaderFixed;
            Assert.Equal(expected, result.DecodeBound);
        }

        [Fact]
        public void OutOfEnvelopeRootIsRefusedWithoutANativeOpen()
        {
            var parquet = new SyntheticParquet();
            parquet.Schema.Add(new SyntheticParquet.Element("schema", Repetition: 0, Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("add", Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("path", Type: 6));
            parquet.RowGroups.Add(new List<SyntheticParquet.Chunk>() { new SyntheticParquet.Chunk() { Path = new[] { "add", "path" }, NumValues = 10, Codec = 4, Pages = { SyntheticParquet.DataPage(10, 10) } } });
            var path = Path.Combine(_directory, "brotli.parquet");
            File.WriteAllBytes(path, parquet.Build());

            var memory = new JobMemory();
            Assert.Throws<CheckpointDeclinedException>(() => ReadBounded(path, memory));
            Assert.Equal(0, memory.NativeOpens);
        }

        [Fact]
        public void AdmittedAdversarialFixturesDecodeCorrectly()
        {
            var longEntry = new string('d', 100_000);
            var fixtures = new List<List<byte[]>>()
            {
                // One long dictionary entry repeated on every row
                Enumerable.Range(0, 500).SelectMany(i => TestRecords.FromAction(new DeltaAction() { Add = new DeltaAddAction() { Path = "p" + i, Statistics = longEntry } })).ToList(),
                // One large value among small ones
                Enumerable.Range(0, 500).SelectMany(i => TestRecords.FromAction(new DeltaAction() { Add = new DeltaAddAction() { Path = "p" + i, Statistics = i == 250 ? new string('l', 3_000_000) : "{}" } })).ToList(),
                // Many tiny pages
                CheckpointPageScanTests.RealisticRecords(3_000).ToList()
            };
            var settings = new Func<Compression, WriterProperties>[]
            {
                c => DeltaTestKit.Properties(b => b.Compression(c).EnableDictionary().DisableStatistics()),
                c => DeltaTestKit.Properties(b => b.Compression(c).DisableDictionary().DisableStatistics()),
                c => DeltaTestKit.Properties(b => b.Compression(c).DisableDictionary().DisableStatistics().DataPagesize(128).WriteBatchSize(8))
            };
            for (int i = 0; i < fixtures.Count; i++)
            {
                var path = WriteOwn(fixtures[i], new CheckpointWriterLimits() { PropertiesForTests = settings[i] });
                var read = ReadBounded(path);
                Assert.Equal(fixtures[i].Count, read.Count);
                Assert.Equal(Canonical(fixtures[i].Select(Emit)), Canonical(read));
            }
        }

        // The checkpoint form of a record, as the writer emits it
        private static byte[] Emit(byte[] record)
        {
            var action = CheckpointRecord.ToAction(record);
            if (action.Add != null)
            {
                action.Add.DataChange = false;
            }
            if (action.Remove != null)
            {
                action.Remove.DataChange = false;
            }
            return TestRecords.FromAction(action)[0];
        }

        [Fact]
        public void RootEntryCapAppliesAcrossAllCollectionsInOneAction()
        {
            var path = Path.Combine(Path.GetTempPath(), "decode-" + Guid.NewGuid().ToString("N") + ".parquet");
            try
            {
                var action = new DeltaAction
                {
                    Add = new DeltaAddAction
                    {
                        Path = "a.parquet",
                        PartitionValues = Enumerable.Range(0, 6).ToDictionary(i => "p" + i, i => "v"),
                        Tags = Enumerable.Range(0, 6).ToDictionary(i => "t" + i, i => "v")
                    }
                };
                using (var writer = new CheckpointFileWriter(path, new DiskLedger(), new JobMemory(), Compression.Uncompressed, null))
                {
                    writer.Append(TestRecords.FromAction(action).Single());
                    writer.Close();
                }
                var memory = new JobMemory();
                var scan = CheckpointPageScan.Scan(path, memory);
                using var reader = new BoundedCheckpointReader(path, scan, memory, entryCap: 10);
                var error = Assert.Throws<CheckpointDeclinedException>(() => reader.Read(_ => { }));
                Assert.Equal(MaintenanceReason.EntriesOverCap, error.Reason);
            }
            finally
            {
                File.Delete(path);
            }
        }

        [Theory]
        [InlineData(0, 0, (int)MaintenanceReason.OutOfEnvelope)]
        [InlineData(1, 1, (int)MaintenanceReason.OutOfEnvelope)]
        public void V2PageRowCountMustAgreeWithDecodedLevels(int nulls, int rows, int reason)
        {
            var path = Path.Combine(Path.GetTempPath(), "v2-" + Guid.NewGuid().ToString("N") + ".parquet");
            try
            {
                // One defined string row, with a V2 page header claiming other row or null counts: the scan counts its levels
                byte[] body = { 2, 2, 1, 0, 0, 0, (byte)'a' };
                var page = SyntheticParquet.Page(3, body.Length, body.Length,
                    h => h.I32(1, 1).I32(2, nulls).I32(3, rows).I32(4, 0).I32(5, 2).I32(6, 0).Bool(7, false), 8);
                body.CopyTo(page, page.Length - body.Length);
                var parquet = new SyntheticParquet();
                parquet.Schema.Add(new SyntheticParquet.Element("schema", Repetition: 0, Children: 1));
                parquet.Schema.Add(new SyntheticParquet.Element("add", Children: 1));
                parquet.Schema.Add(new SyntheticParquet.Element("path", Type: 6));
                parquet.RowGroups.Add(new List<SyntheticParquet.Chunk>
                {
                    new() { Path = new[] { "add", "path" }, NumValues = 1, Pages = { page } }
                });
                File.WriteAllBytes(path, parquet.Build());
                var memory = new JobMemory();
                var error = Assert.Throws<CheckpointDeclinedException>(() =>
                {
                    var scan = CheckpointPageScan.Scan(path, memory);
                    using var reader = new BoundedCheckpointReader(path, scan, memory);
                    reader.Read(_ => { });
                });
                Assert.Equal((MaintenanceReason)reason, error.Reason);
            }
            finally
            {
                File.Delete(path);
            }
        }

        [Fact]
        public void KnownFieldInAnotherShapeIsNotReadAsNull()
        {
            static byte[] StringValue(string value)
            {
                var bytes = System.Text.Encoding.UTF8.GetBytes(value);
                return BitConverter.GetBytes(bytes.Length).Concat(bytes).ToArray();
            }
            static byte[] Page(byte definition, byte[] value, bool repeated = false)
            {
                var levels = repeated ? new byte[] { 2, 0, 2, definition } : new byte[] { 2, definition };
                var body = levels.Concat(value).ToArray();
                var page = SyntheticParquet.Page(3, body.Length, body.Length,
                    h => h.I32(1, 1).I32(2, 0).I32(3, 1).I32(4, 0).I32(5, 2).I32(6, repeated ? 2 : 0).Bool(7, false), 8);
                body.CopyTo(page, page.Length - body.Length);
                return page;
            }
            // partitionValues holds {"k":"v"} as a group of repeated key/value pairs without the MAP annotation
            var path = Path.Combine(_directory, "unannotated-map.parquet");
            var parquet = new SyntheticParquet();
            parquet.Schema.Add(new SyntheticParquet.Element("schema", Repetition: 0, Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("add", Children: 2));
            parquet.Schema.Add(new SyntheticParquet.Element("path", Type: 6));
            parquet.Schema.Add(new SyntheticParquet.Element("partitionValues", Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("key_value", Repetition: 2, Children: 2));
            parquet.Schema.Add(new SyntheticParquet.Element("key", Type: 6, Repetition: 0));
            parquet.Schema.Add(new SyntheticParquet.Element("value", Type: 6));
            parquet.RowGroups.Add(new List<SyntheticParquet.Chunk>
            {
                new() { Path = new[] { "add", "path" }, NumValues = 1, Pages = { Page(2, StringValue("p")) } },
                new() { Path = new[] { "add", "partitionValues", "key_value", "key" }, NumValues = 1, Pages = { Page(3, StringValue("k"), repeated: true) } },
                new() { Path = new[] { "add", "partitionValues", "key_value", "value" }, NumValues = 1, Pages = { Page(4, StringValue("v"), repeated: true) } }
            });
            File.WriteAllBytes(path, parquet.Build());

            var memory = new JobMemory();
            var emitted = new List<byte[]>();
            var error = Record.Exception(() =>
            {
                var scan = CheckpointPageScan.Scan(path, memory);
                using var reader = new BoundedCheckpointReader(path, scan, memory);
                reader.Read(record => emitted.Add(record.ToArray()));
            });
            // Declined, or read with its partition value; never an add whose partition values silently became null
            var add = error == null && emitted.Count == 1 ? CheckpointRecord.ToAction(emitted[0]).Add : null;
            Assert.True(error is CheckpointDeclinedException || add?.PartitionValues?.GetValueOrDefault("k") == "v",
                $"error {error?.GetType().Name ?? "none"}, emitted add path={add?.Path}, partitionValues={(add?.PartitionValues == null ? "null" : string.Join(",", add.PartitionValues))}");
        }

        // A one-row V2 page: repetition and definition sections, then the plain values
        private static byte[] OneRowV2(byte[] repetition, byte[] definition, byte[] values, int nulls = 0)
        {
            var body = repetition.Concat(definition).Concat(values).ToArray();
            var page = SyntheticParquet.Page(3, body.Length, body.Length,
                h => h.I32(1, 1).I32(2, nulls).I32(3, 1).I32(4, 0).I32(5, definition.Length).I32(6, repetition.Length).Bool(7, false), 8);
            body.CopyTo(page, page.Length - body.Length);
            return page;
        }

        private static byte[] PlainString(string value)
        {
            var bytes = System.Text.Encoding.UTF8.GetBytes(value);
            return BitConverter.GetBytes(bytes.Length).Concat(bytes).ToArray();
        }

        private (Exception? Error, List<byte[]> Emitted) ScanAndRead(SyntheticParquet parquet, string name)
        {
            var path = Path.Combine(_directory, name);
            File.WriteAllBytes(path, parquet.Build());
            var memory = new JobMemory();
            var emitted = new List<byte[]>();
            var error = Record.Exception(() =>
            {
                var scan = CheckpointPageScan.Scan(path, memory);
                using var reader = new BoundedCheckpointReader(path, scan, memory);
                reader.Read(record => emitted.Add(record.ToArray()));
            });
            return (error, emitted);
        }

        [Fact]
        public void BareRepeatedListIsNotReadAsNull()
        {
            // A repeated field outside a LIST group is a list of required elements by Parquet's compatibility rules; partitionColumns holds ["p"]
            var parquet = new SyntheticParquet();
            parquet.Schema.Add(new SyntheticParquet.Element("schema", Repetition: 0, Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("metaData", Children: 2));
            parquet.Schema.Add(new SyntheticParquet.Element("id", Type: 6));
            parquet.Schema.Add(new SyntheticParquet.Element("partitionColumns", Type: 6, Repetition: 2));
            parquet.RowGroups.Add(new List<SyntheticParquet.Chunk>
            {
                new() { Path = new[] { "metaData", "id" }, NumValues = 1, Pages = { OneRowV2(Array.Empty<byte>(), new byte[] { 2, 2 }, PlainString("x")) } },
                new() { Path = new[] { "metaData", "partitionColumns" }, NumValues = 1, Pages = { OneRowV2(new byte[] { 2, 0 }, new byte[] { 2, 2 }, PlainString("p")) } }
            });
            var (error, emitted) = ScanAndRead(parquet, "bare-list.parquet");
            var meta = error == null && emitted.Count == 1 ? CheckpointRecord.ToAction(emitted[0]).MetaData : null;
            Assert.True(error is CheckpointDeclinedException || meta?.PartitionColumns?.SequenceEqual(new[] { "p" }) == true,
                $"error {error?.GetType().Name ?? "none"}, emitted metaData id={meta?.Id}, partitionColumns={(meta?.PartitionColumns == null ? "null" : string.Join(",", meta.PartitionColumns))}");
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void ScalarWhereAKnownGroupIsExpectedIsNotReadAsNull(bool deletionVector)
        {
            // add.deletionVector or add.partitionValues written as a plain string column
            var name = deletionVector ? "deletionVector" : "partitionValues";
            var parquet = new SyntheticParquet();
            parquet.Schema.Add(new SyntheticParquet.Element("schema", Repetition: 0, Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("add", Children: 2));
            parquet.Schema.Add(new SyntheticParquet.Element("path", Type: 6));
            parquet.Schema.Add(new SyntheticParquet.Element(name, Type: 6));
            parquet.RowGroups.Add(new List<SyntheticParquet.Chunk>
            {
                new() { Path = new[] { "add", "path" }, NumValues = 1, Pages = { OneRowV2(Array.Empty<byte>(), new byte[] { 2, 2 }, PlainString("p")) } },
                new() { Path = new[] { "add", name }, NumValues = 1, Pages = { OneRowV2(Array.Empty<byte>(), new byte[] { 2, 2 }, PlainString("{\"k\":\"v\"}")) } }
            });
            var (error, emitted) = ScanAndRead(parquet, $"scalar-{name}.parquet");
            Assert.True(error is CheckpointDeclinedException, $"error {error?.GetType().Name ?? "none"}, emitted {emitted.Count} records with the {name} column dropped");
        }

        [Fact]
        public void KnownStructInAListShapeIsNotReadAsNull()
        {
            // add.deletionVector as a LIST of structs: its leaf is add.deletionVector.element.storageType, holding "u"
            var parquet = new SyntheticParquet();
            parquet.Schema.Add(new SyntheticParquet.Element("schema", Repetition: 0, Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("add", Children: 2));
            parquet.Schema.Add(new SyntheticParquet.Element("path", Type: 6));
            parquet.Schema.Add(new SyntheticParquet.Element("deletionVector", Children: 1, Converted: 3));
            parquet.Schema.Add(new SyntheticParquet.Element("list", Repetition: 2, Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("element", Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("storageType", Type: 6));
            parquet.RowGroups.Add(new List<SyntheticParquet.Chunk>
            {
                new() { Path = new[] { "add", "path" }, NumValues = 1, Pages = { OneRowV2(Array.Empty<byte>(), new byte[] { 2, 2 }, PlainString("p")) } },
                new() { Path = new[] { "add", "deletionVector", "list", "element", "storageType" }, NumValues = 1, Pages = { OneRowV2(new byte[] { 2, 0 }, new byte[] { 2, 5 }, PlainString("u")) } }
            });
            var (error, emitted) = ScanAndRead(parquet, "dv-as-list.parquet");
            var add = error == null && emitted.Count == 1 ? CheckpointRecord.ToAction(emitted[0]).Add : null;
            Assert.True(error is CheckpointDeclinedException || add?.DeletionVector != null,
                $"error {error?.GetType().Name ?? "none"}, emitted {emitted.Count} records, add path={add?.Path}, deletionVector={(add?.DeletionVector == null ? "null" : add.DeletionVector.StorageType)}");
        }

        [Fact]
        public void RepeatedKnownScalarIsRefused()
        {
            // add.path as a repeated string holding "a" and "b" in one row
            var parquet = new SyntheticParquet();
            parquet.Schema.Add(new SyntheticParquet.Element("schema", Repetition: 0, Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("add", Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("path", Type: 6, Repetition: 2));
            var page = TwoLevelsV2(new byte[] { 2, 0, 2, 1 }, new byte[] { 4, 2 }, PlainString("a").Concat(PlainString("b")).ToArray(), nulls: 0);
            parquet.RowGroups.Add(new List<SyntheticParquet.Chunk> { new() { Path = new[] { "add", "path" }, NumValues = 2, Pages = { page } } });
            var (error, emitted) = ScanAndRead(parquet, "repeated-path.parquet");
            var declined = Assert.IsType<CheckpointDeclinedException>(error);
            Assert.Contains("has repetition", declined.Message);
        }

        [Fact]
        public void ContinuationBelowTheEntryLevelIsRefused()
        {
            // A second partition entry whose definition level says the map is empty
            var parquet = new SyntheticParquet();
            parquet.Schema.Add(new SyntheticParquet.Element("schema", Repetition: 0, Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("add", Children: 2));
            parquet.Schema.Add(new SyntheticParquet.Element("path", Type: 6));
            parquet.Schema.Add(new SyntheticParquet.Element("partitionValues", Children: 1, Converted: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("key_value", Repetition: 2, Children: 2));
            parquet.Schema.Add(new SyntheticParquet.Element("key", Type: 6, Repetition: 0));
            parquet.Schema.Add(new SyntheticParquet.Element("value", Type: 6));
            parquet.RowGroups.Add(new List<SyntheticParquet.Chunk>
            {
                new() { Path = new[] { "add", "path" }, NumValues = 1, Pages = { OneRowV2(Array.Empty<byte>(), new byte[] { 2, 2 }, PlainString("p")) } },
                new() { Path = new[] { "add", "partitionValues", "key_value", "key" }, NumValues = 2, Pages = { TwoLevelsV2(new byte[] { 2, 0, 2, 1 }, new byte[] { 2, 3, 2, 2 }, PlainString("k"), nulls: 1) } },
                new() { Path = new[] { "add", "partitionValues", "key_value", "value" }, NumValues = 2, Pages = { TwoLevelsV2(new byte[] { 2, 0, 2, 1 }, new byte[] { 2, 4, 2, 2 }, PlainString("v"), nulls: 1) } }
            });
            var (error, emitted) = ScanAndRead(parquet, "continuation.parquet");
            var declined = Assert.IsType<CheckpointDeclinedException>(error);
            Assert.Contains("below its entry level", declined.Message);
            Assert.Empty(emitted);
        }

        [Fact]
        public void ListContinuationBelowTheEntryLevelIsRefused()
        {
            // partitionColumns ["a"] followed by a continuation whose definition level says the list is null
            var parquet = new SyntheticParquet();
            parquet.Schema.Add(new SyntheticParquet.Element("schema", Repetition: 0, Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("metaData", Children: 2));
            parquet.Schema.Add(new SyntheticParquet.Element("id", Type: 6));
            parquet.Schema.Add(new SyntheticParquet.Element("partitionColumns", Children: 1, Converted: 3));
            parquet.Schema.Add(new SyntheticParquet.Element("list", Repetition: 2, Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("element", Type: 6));
            parquet.RowGroups.Add(new List<SyntheticParquet.Chunk>
            {
                new() { Path = new[] { "metaData", "id" }, NumValues = 1, Pages = { OneRowV2(Array.Empty<byte>(), new byte[] { 2, 2 }, PlainString("x")) } },
                new() { Path = new[] { "metaData", "partitionColumns", "list", "element" }, NumValues = 2, Pages = { TwoLevelsV2(new byte[] { 2, 0, 2, 1 }, new byte[] { 2, 4, 2, 1 }, PlainString("a"), nulls: 1) } }
            });
            var (error, emitted) = ScanAndRead(parquet, "list-continuation.parquet");
            var meta = error == null && emitted.Count == 1 ? CheckpointRecord.ToAction(emitted[0]).MetaData : null;
            Assert.True(error is CheckpointDeclinedException, $"error {error?.GetType().Name ?? "none"}: {error?.Message}; partitionColumns={(meta?.PartitionColumns == null ? "null" : "[" + string.Join(",", meta.PartitionColumns.Select(x => x ?? "<null>")) + "]")}");
            Assert.Contains("below its entry level", error!.Message);
        }

        [Theory]
        [InlineData("empty run", "an empty level run")]
        [InlineData("empty bit-packed run", "an empty bit-packed run")]
        [InlineData("long header", "a level run header is too long")]
        [InlineData("short section", "ends before its levels")]
        [InlineData("repetition on a flat leaf", "repetition levels on a leaf without repetition")]
        [InlineData("definition on a required leaf", "definition levels on a required leaf")]
        public void MalformedV2LevelSectionsDecline(string shape, string message)
        {
            var required = shape == "definition on a required leaf";
            var (repetition, definition) = shape switch
            {
                "empty run" => (Array.Empty<byte>(), new byte[] { 0, 2 }),
                "empty bit-packed run" => (Array.Empty<byte>(), new byte[] { 1 }),
                "long header" => (Array.Empty<byte>(), new byte[] { 0x80, 0x80, 0x80, 0x80, 0x80, 0x01 }),
                "short section" => (Array.Empty<byte>(), Array.Empty<byte>()),
                "repetition on a flat leaf" => (new byte[] { 2, 0 }, new byte[] { 2, 2 }),
                _ => (Array.Empty<byte>(), new byte[] { 2, 1 })
            };
            var parquet = new SyntheticParquet();
            parquet.Schema.Add(new SyntheticParquet.Element("schema", Repetition: 0, Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("add", Repetition: required ? 0 : 1, Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("path", Type: 6, Repetition: required ? 0 : 1));
            parquet.RowGroups.Add(new List<SyntheticParquet.Chunk> { new() { Path = new[] { "add", "path" }, NumValues = 1, Pages = { OneRowV2(repetition, definition, PlainString("a")) } } });
            var (error, _) = ScanAndRead(parquet, "levels.parquet");
            var declined = Assert.IsType<CheckpointDeclinedException>(error);
            Assert.Equal(MaintenanceReason.OutOfEnvelope, declined.Reason);
            Assert.Contains(message, declined.Message);
        }

        // A one-row V2 page of two levels
        private static byte[] TwoLevelsV2(byte[] repetition, byte[] definition, byte[] values, int nulls)
        {
            var body = repetition.Concat(definition).Concat(values).ToArray();
            var page = SyntheticParquet.Page(3, body.Length, body.Length,
                h => h.I32(1, 2).I32(2, nulls).I32(3, 1).I32(4, 0).I32(5, definition.Length).I32(6, repetition.Length).Bool(7, false), 8);
            body.CopyTo(page, page.Length - body.Length);
            return page;
        }

        [Fact]
        public void UnknownFieldInsideAKnownStructIsIgnoredLikeOtherUnknownFields()
        {
            // Delta Spark's DeletionVectorDescriptor has maxRowIndex, so its checkpoints can hold add.deletionVector.maxRowIndex, all null
            var parquet = new SyntheticParquet();
            parquet.Schema.Add(new SyntheticParquet.Element("schema", Repetition: 0, Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("add", Children: 2));
            parquet.Schema.Add(new SyntheticParquet.Element("path", Type: 6));
            parquet.Schema.Add(new SyntheticParquet.Element("deletionVector", Children: 2));
            parquet.Schema.Add(new SyntheticParquet.Element("storageType", Type: 6));
            parquet.Schema.Add(new SyntheticParquet.Element("maxRowIndex", Type: 2));
            parquet.RowGroups.Add(new List<SyntheticParquet.Chunk>
            {
                new() { Path = new[] { "add", "path" }, NumValues = 1, Pages = { OneRowV2(Array.Empty<byte>(), new byte[] { 2, 2 }, PlainString("p")) } },
                new() { Path = new[] { "add", "deletionVector", "storageType" }, NumValues = 1, Pages = { OneRowV2(Array.Empty<byte>(), new byte[] { 2, 3 }, PlainString("u")) } },
                new() { Path = new[] { "add", "deletionVector", "maxRowIndex" }, Type = 2, NumValues = 1, Pages = { OneRowV2(Array.Empty<byte>(), new byte[] { 2, 2 }, Array.Empty<byte>(), nulls: 1) } }
            });
            var (error, emitted) = ScanAndRead(parquet, "dv-max-row-index.parquet");
            Assert.True(error == null, $"Declined: {error?.Message}");
            Assert.Equal("u", CheckpointRecord.ToAction(Assert.Single(emitted)).Add?.DeletionVector?.StorageType);
        }

        [Fact]
        public void LockedRootIsNotDeclinedAsUndecodable()
        {
            if (!OperatingSystem.IsWindows())
            {
                // Sharing modes are advisory elsewhere
                return;
            }
            var path = WriteOwn(CheckpointPageScanTests.RealisticRecords(10));
            var memory = new JobMemory();
            var scan = CheckpointPageScan.Scan(path, memory);
            // Another handle holds the admitted root exclusively, a transient I/O condition
            using var holder = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.None);
            var error = Record.Exception(() => new BoundedCheckpointReader(path, scan, memory).Dispose());
            Assert.NotNull(error);
            Assert.False(error is CheckpointDeclinedException { Reason: MaintenanceReason.RootDecodeFailure }, $"A locked root is declined as {error.GetType().Name}: {error.Message}");
        }

        [Fact]
        public void FooterTheNativeReaderRejectsIsDeclined()
        {
            // The scan admits a footer without its required version, which the native Thrift reader refuses on open
            var path = Path.Combine(_directory, "no-version.parquet");
            byte[] body = { 2, 2, 1, 0, 0, 0, (byte)'a' };
            var page = SyntheticParquet.Page(3, body.Length, body.Length,
                h => h.I32(1, 1).I32(2, 0).I32(3, 1).I32(4, 0).I32(5, 2).I32(6, 0).Bool(7, false), 8);
            body.CopyTo(page, page.Length - body.Length);
            var parquet = new SyntheticParquet() { OmitVersion = true };
            parquet.Schema.Add(new SyntheticParquet.Element("schema", Repetition: 0, Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("add", Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("path", Type: 6));
            parquet.RowGroups.Add(new List<SyntheticParquet.Chunk>
            {
                new() { Path = new[] { "add", "path" }, NumValues = 1, Pages = { page } }
            });
            File.WriteAllBytes(path, parquet.Build());

            var memory = new JobMemory();
            var error = Record.Exception(() =>
            {
                var scan = CheckpointPageScan.Scan(path, memory);
                using var reader = new BoundedCheckpointReader(path, scan, memory);
                reader.Read(_ => { });
            });
            Assert.True(error is CheckpointDeclinedException, $"Expected a typed decline, got {error?.GetType().Name ?? "no exception"}: {error?.Message}");
            Assert.Equal(0, memory.Native);
        }

        [Fact]
        public void FileRowTotalAboveTheRowGroupsIsRefused()
        {
            var path = Path.Combine(_directory, "file-rows.parquet");
            // One defined string row in a V2 page that agrees with its row group, under a file total of two rows
            byte[] body = { 2, 2, 1, 0, 0, 0, (byte)'a' };
            var page = SyntheticParquet.Page(3, body.Length, body.Length,
                h => h.I32(1, 1).I32(2, 0).I32(3, 1).I32(4, 0).I32(5, 2).I32(6, 0).Bool(7, false), 8);
            body.CopyTo(page, page.Length - body.Length);
            var parquet = new SyntheticParquet() { FileRows = 2 };
            parquet.Schema.Add(new SyntheticParquet.Element("schema", Repetition: 0, Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("add", Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("path", Type: 6));
            parquet.RowGroups.Add(new List<SyntheticParquet.Chunk>
            {
                new() { Path = new[] { "add", "path" }, NumValues = 1, Pages = { page } }
            });
            File.WriteAllBytes(path, parquet.Build());

            var memory = new JobMemory();
            var error = Assert.Throws<CheckpointDeclinedException>(() =>
            {
                var scan = CheckpointPageScan.Scan(path, memory);
                using var reader = new BoundedCheckpointReader(path, scan, memory);
                reader.Read(_ => { });
            });
            Assert.Equal(MaintenanceReason.RootDecodeFailure, error.Reason);
            Assert.Contains("the footer declares 2", error.Message);
        }

        [Fact]
        public void DottedUnknownTopLevelNameCannotBecomeAnAddAction()
        {
            var path = Path.Combine(Path.GetTempPath(), "dotted-" + Guid.NewGuid().ToString("N") + ".parquet");
            try
            {
                static byte[] StringValue(string value)
                {
                    var bytes = System.Text.Encoding.UTF8.GetBytes(value);
                    return BitConverter.GetBytes(bytes.Length).Concat(bytes).ToArray();
                }
                static byte[] Page(byte definition, byte[]? value, bool repeated = false)
                {
                    var levels = repeated ? new byte[] { 2, 0, 2, definition } : new byte[] { 2, definition };
                    var body = levels.Concat(value ?? Array.Empty<byte>()).ToArray();
                    var page = SyntheticParquet.Page(3, body.Length, body.Length,
                        h => h.I32(1, 1).I32(2, value == null ? 1 : 0).I32(3, 1).I32(4, 0).I32(5, 2).I32(6, repeated ? 2 : 0).Bool(7, false), 8);
                    body.CopyTo(page, page.Length - body.Length);
                    return page;
                }
                var parquet = new SyntheticParquet();
                parquet.Schema.Add(new SyntheticParquet.Element("schema", Repetition: 0, Children: 2));
                parquet.Schema.Add(new SyntheticParquet.Element("add", Children: 4));
                parquet.Schema.Add(new SyntheticParquet.Element("path", Type: 6));
                parquet.Schema.Add(new SyntheticParquet.Element("size", Type: 2));
                parquet.Schema.Add(new SyntheticParquet.Element("modificationTime", Type: 2));
                parquet.Schema.Add(new SyntheticParquet.Element("partitionValues", Children: 1, Converted: 1));
                parquet.Schema.Add(new SyntheticParquet.Element("key_value", Repetition: 2, Children: 2));
                parquet.Schema.Add(new SyntheticParquet.Element("key", Type: 6, Repetition: 0));
                parquet.Schema.Add(new SyntheticParquet.Element("value", Type: 6));
                parquet.Schema.Add(new SyntheticParquet.Element("add.path", Type: 6));
                parquet.RowGroups.Add(new List<SyntheticParquet.Chunk>
                {
                    new() { Path = new[] { "add", "path" }, NumValues = 1, Pages = { Page(2, StringValue("legitimate.parquet")) } },
                    new() { Path = new[] { "add", "size" }, Type = 2, NumValues = 1, Pages = { Page(2, BitConverter.GetBytes(123L)) } },
                    new() { Path = new[] { "add", "modificationTime" }, Type = 2, NumValues = 1, Pages = { Page(2, BitConverter.GetBytes(456L)) } },
                    new() { Path = new[] { "add", "partitionValues", "key_value", "key" }, NumValues = 1, Pages = { Page(2, null, repeated: true) } },
                    new() { Path = new[] { "add", "partitionValues", "key_value", "value" }, NumValues = 1, Pages = { Page(2, null, repeated: true) } },
                    new() { Path = new[] { "add.path" }, NumValues = 1, Pages = { Page(1, StringValue("ghost.parquet")) } }
                });
                File.WriteAllBytes(path, parquet.Build());
                var memory = new JobMemory();
                var scan = CheckpointPageScan.Scan(path, memory);
                using var reader = new BoundedCheckpointReader(path, scan, memory);
                var emitted = new List<byte[]>();
                var error = Record.Exception(() => reader.Read(record => emitted.Add(record.ToArray())));
                var add = emitted.Count == 1 ? CheckpointRecord.ToAction(emitted[0]).Add : null;
                Assert.True(error is CheckpointDeclinedException,
                    $"Expected UnknownColumn, got {error?.ToString() ?? "no exception"}; emitted add path={add?.Path}, size={add?.Size}, modificationTime={add?.ModificationTime}");
                Assert.Equal(MaintenanceReason.UnknownColumn, ((CheckpointDeclinedException)error!).Reason);
                Assert.Empty(emitted);
            }
            finally
            {
                File.Delete(path);
            }
        }

        [Fact]
        public void NativeOpenIoFailureIsNotClassifiedAsMalformedRoot()
        {
            var parquet = new SyntheticParquet();
            parquet.Schema.Add(new SyntheticParquet.Element("schema", Repetition: 0, Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("add", Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("path", Type: 6));
            parquet.RowGroups.Add(new List<SyntheticParquet.Chunk>
            {
                new() { Path = new[] { "add", "path" }, NumValues = 1, Pages = { OneStringPage(false) } }
            });
            var path = Path.Combine(AppContext.BaseDirectory, "native-open-io-" + Guid.NewGuid().ToString("N") + ".parquet");
            var memory = new JobMemory();
            ScanResult? scan = null;
            try
            {
                File.WriteAllBytes(path, parquet.Build());
                scan = CheckpointPageScan.Scan(path, memory);
                using (var reader = new BoundedCheckpointReader(path, scan, memory))
                {
                    Assert.Equal(1, reader.Read(_ => { }));
                }
                // A disappearing local source is an I/O failure, not evidence that its admitted footer is invalid.
                File.Delete(path);
                var error = Record.Exception(() =>
                {
                    using var reader = new BoundedCheckpointReader(path, scan, memory);
                });
                Assert.Equal(0, memory.Native);
                Assert.True(error != null && error is not CheckpointDeclinedException,
                    $"A local source-open failure must remain transient, got {error?.GetType().Name}: {error?.Message}");
            }
            finally
            {
                if (scan != null)
                {
                    memory.Release(scan.ModelBytes);
                }
                File.Delete(path);
            }
        }

        [Theory]
        [InlineData("add", "path", "partitionValues", false)]
        [InlineData("metaData", "id", "partitionColumns", true)]
        [InlineData("add", "path", "deletionVector", false)]
        [InlineData("metaData", "id", "format", false)]
        public void AScalarAtAKnownCollectionOrStructPathIsRefused(string action, string anchor, string field, bool repeated)
        {
            var parquet = new SyntheticParquet();
            parquet.Schema.Add(new SyntheticParquet.Element("schema", Repetition: 0, Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element(action, Children: 2));
            parquet.Schema.Add(new SyntheticParquet.Element(anchor, Type: 6));
            parquet.Schema.Add(new SyntheticParquet.Element(field, Type: 6, Repetition: repeated ? 2 : 1));
            parquet.RowGroups.Add(new List<SyntheticParquet.Chunk>
            {
                new() { Path = new[] { action, anchor }, NumValues = 1, Pages = { OneStringPage(false) } },
                new() { Path = new[] { action, field }, NumValues = 1, Pages = { OneStringPage(repeated) } }
            });
            var path = Path.Combine(AppContext.BaseDirectory, "scalar-shape-" + Guid.NewGuid().ToString("N") + ".parquet");
            try
            {
                File.WriteAllBytes(path, parquet.Build());
                var memory = new JobMemory();
                ScanResult? scan = null;
                var emitted = new List<byte[]>();
                var error = Record.Exception(() =>
                {
                    scan = CheckpointPageScan.Scan(path, memory);
                    using var reader = new BoundedCheckpointReader(path, scan, memory);
                    reader.Read(record => emitted.Add(record.ToArray()));
                });
                if (scan != null)
                {
                    memory.Release(scan.ModelBytes);
                }
                Assert.True(error is CheckpointDeclinedException { Reason: MaintenanceReason.OutOfEnvelope },
                    $"{action}.{field} in a scalar shape must decline; emitted={emitted.Count}, native opens={memory.NativeOpens}, error={error?.ToString() ?? "none"}");
            }
            finally
            {
                File.Delete(path);
            }
        }

        private static byte[] OneStringPage(bool repeated)
        {
            byte[] definitionAndValue = { 2, 0, 0, 0, 2, 2, 1, 0, 0, 0, (byte)'p' };
            byte[] repetition = { 2, 0, 0, 0, 2, 0 };
            var body = repeated ? repetition.Concat(definitionAndValue).ToArray() : definitionAndValue;
            var page = SyntheticParquet.DataPage(1, body.Length);
            body.CopyTo(page, page.Length - body.Length);
            return page;
        }

        [Theory]
        [InlineData(1)]
        [InlineData(6)]
        public void CorruptCompressedBodyRemainsDecodeFailure(int codec)
        {
            var parquet = new SyntheticParquet();
            parquet.Schema.Add(new SyntheticParquet.Element("schema", Repetition: 0, Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("add", Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("path", Type: 6));
            var page = SyntheticParquet.DataPage(1, 8);
            page.AsSpan(page.Length - 8).Fill(0xff);
            parquet.RowGroups.Add(new List<SyntheticParquet.Chunk>
            {
                new() { Path = new[] { "add", "path" }, Codec = codec, NumValues = 1, Pages = { page } }
            });
            var path = Path.Combine(AppContext.BaseDirectory, "corrupt-codec-" + Guid.NewGuid().ToString("N") + ".parquet");
            var memory = new JobMemory();
            ScanResult? scan = null;
            try
            {
                File.WriteAllBytes(path, parquet.Build());
                scan = CheckpointPageScan.Scan(path, memory);
                using (var reader = new BoundedCheckpointReader(path, scan, memory))
                {
                    var emitted = new List<byte[]>();
                    var error = Assert.Throws<CheckpointDeclinedException>(() => reader.Read(record => emitted.Add(record.ToArray())));
                    Assert.Equal(MaintenanceReason.RootDecodeFailure, error.Reason);
                    Assert.Contains("IOError:", error.Message);
                    Assert.Empty(emitted);
                }
                Assert.Equal(0, memory.Native);
            }
            finally
            {
                if (scan != null)
                {
                    memory.Release(scan.ModelBytes);
                }
                File.Delete(path);
            }
        }
    }

    internal static class CheckpointProjectionDoctor
    {
        // A projection with one leaf replaced, for doctored fixtures
        public static CheckpointProjection Replace(CheckpointProjection projection, ProjectedLeaf leaf, ProjectedLeaf replacement)
        {
            return projection.WithLeaves(projection.Leaves.Select(x => x == leaf ? replacement : x).ToList());
        }
    }
}
