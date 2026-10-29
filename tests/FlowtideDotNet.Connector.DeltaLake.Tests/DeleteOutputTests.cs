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

using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal.Catalog;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors.RoaringBitmap;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using FlowtideDotNet.Connector.DeltaLake.Internal.Deletes;
using ParquetSharp;
using ParquetSharp.Schema;
using Stowage;
using System.Text.Json;
using System.Text.Json.Nodes;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;
using Delta = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DeleteOutputTests
    {
        [Fact]
        public async Task AFullyDeletedFileIsOnlyRemoved()
        {
            var testName = nameof(AFullyDeletedFileIsOnlyRemoved);
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{testName}"));
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            await SettlePublications(stream);
            var path = Assert.Single((await DeltaTransactionReader.ReadVersionCommit(storage, "test", 0))!.AddedFiles).Path;

            storage.ClearRequests();
            foreach (var user in stream.Users.ToList())
            {
                stream.DeleteUser(user);
            }
            await WaitForVersion(storage, "test", stream, 1);

            var commit = (await DeltaTransactionReader.ReadVersionCommit(storage, "test", 1))!;
            Assert.Equal(path, Assert.Single(commit.RemovedFiles).Path);
            Assert.Empty(commit.AddedFiles);
            // Opened once to find the rows, not again to copy them
            Assert.Single(storage.Requests, x => x == $"OpenRead /test/{path}");
        }

        [Fact]
        public async Task AFileWhoseHitIsAlreadyDeletedGetsNoActions()
        {
            var testName = nameof(AFileWhoseHitIsAlreadyDeletedGetsNoActions);
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{testName}"));
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(20);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            var first = Assert.Single((await DeltaTransactionReader.ReadVersionCommit(storage, "test", 0))!.AddedFiles).Path;

            // The row leaves the first file through a vector, then comes back in a new file
            var user = stream.Users[0];
            stream.DeleteUser(user);
            await WaitForVersion(storage, "test", stream, 1);
            Assert.NotNull(Assert.Single((await DeltaTransactionReader.ReadVersionCommit(storage, "test", 1))!.AddedFiles).DeletionVector);
            stream.AddOrUpdateUser(user);
            await WaitForVersion(storage, "test", stream, 2);
            stream.DeleteUser(user);
            await WaitForVersion(storage, "test", stream, 3);

            var commit = (await DeltaTransactionReader.ReadVersionCommit(storage, "test", 3))!;
            Assert.Single(commit.RemovedFiles);
            Assert.DoesNotContain(commit.RemovedFiles, x => x.Path == first);
            Assert.DoesNotContain(commit.AddedFiles, x => x.Path == first);
        }

        [Theory]
        [InlineData("id")]
        [InlineData("name")]
        public async Task AnIdModeFileWithARenamedColumnStopsTheDelete(string mode)
        {
            var testName = nameof(AnIdModeFileWithARenamedColumnStopsTheDelete) + mode;
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{testName}"));
            var schema = "{\"type\":\"struct\",\"fields\":[" +
                "{\"name\":\"userkey\",\"type\":\"long\",\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":1,\"delta.columnMapping.physicalName\":\"col-userkey\"}}," +
                "{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{\"delta.columnMapping.id\":2,\"delta.columnMapping.physicalName\":\"col-name\"}}]}";
            await WriteCommit(storage, "test", 0, ProtocolWith("columnMapping"), Metadata(schema, new Dictionary<string, string>()
            {
                ["delta.columnMapping.mode"] = mode,
                ["delta.columnMapping.maxColumnId"] = "2",
                ["delta.enableDeletionVectors"] = "false"
            }));
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(10);
            await stream.StartStream(@"
                INSERT INTO test
                SELECT userKey AS userkey, firstName AS name FROM users
            ");
            await WaitForVersion(storage, "test", stream, 1);
            await SettlePublications(stream);

            // Another engine's copy of the rows, with the name column stored under its old name
            var own = Assert.Single((await DeltaTransactionReader.ReadVersionCommit(storage, "test", 1))!.AddedFiles);
            var keys = new Apache.Arrow.Int64Array.Builder().AppendRange(stream.Users.Select(x => (long)x.UserKey)).Build();
            var names = new Apache.Arrow.StringArray.Builder().AppendRange(stream.Users.Select(x => x.FirstName)).Build();
            var foreign = RewriteCopyTests.Write(new Apache.Arrow.Schema.Builder()
                .Field(new Apache.Arrow.Field("col-userkey", Apache.Arrow.Types.Int64Type.Default, true, new Dictionary<string, string>() { ["PARQUET:field_id"] = "1" }))
                .Field(new Apache.Arrow.Field("old_name", Apache.Arrow.Types.StringType.Default, true, new Dictionary<string, string>() { ["PARQUET:field_id"] = "2" }))
                .Build(), keys, names);
            using (var write = await storage.OpenWrite("/test/foreign.parquet"))
            {
                await write.WriteAsync(foreign);
            }
            await WriteCommit(storage, "test", 2,
                new DeltaAction() { Remove = new DeltaRemoveFileAction() { Path = own.Path, DataChange = false, DeletionTimestamp = 0 } },
                new DeltaAction() { Add = new DeltaAddAction() { Path = "foreign.parquet", Size = foreign.Length, DataChange = false, PartitionValues = new Dictionary<string, string>(), Statistics = own.Statistics } });

            stream.DeleteUser(stream.Users[0]);
            if (mode == "id")
            {
                var failure = await Assert.ThrowsAnyAsync<Exception>(() => WaitForVersion(storage, "test", stream, 3));
                Assert.Contains("column name has field id 2, which the file stores as old_name", failure.ToString());
                return;
            }
            // Name mode reads the renamed column as null, as other engines do, so no copy matches
            var unmatched = await Assert.ThrowsAnyAsync<Exception>(() => WaitForVersion(storage, "test", stream, 3));
            Assert.Contains("Could not find any data file that contains the row", unmatched.ToString());
            Assert.False(await storage.Exists(CommitPath("test", 3)));
            var statistics = stream.Sinks[0].DeleteStatisticsForTests!;
            // Only the deleted key passes the first column's test
            Assert.Equal((1, 10L, 1L, 0L), (statistics.Passes, statistics.RowsScanned, statistics.RowsHashed, statistics.Claims));
        }

        [Theory]
        [InlineData("null")]
        [InlineData("empty")]
        [InlineData("uncounted")]
        [InlineData("miscounted")]
        public async Task DeletionVectorReAddsCarryTheFooterCount(string statistics)
        {
            var testName = nameof(DeletionVectorReAddsCarryTheFooterCount) + statistics;
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{testName}"));
            await using var stream = new DeltaLakeSinkStream(testName, storage);
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(20);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            await SettlePublications(stream);

            // Another writer adds the file again with other statistics
            var add = Assert.Single((await DeltaTransactionReader.ReadVersionCommit(storage, "test", 0))!.AddedFiles);
            var original = JsonNode.Parse(add.Statistics!)!.AsObject();
            add.Statistics = statistics switch
            {
                "null" => null,
                "empty" => "{}",
                "uncounted" => Without(original, "numRecords"),
                _ => With(original, "numRecords", 7)
            };
            await WriteCommit(storage, "test", 1, new DeltaAction() { Remove = new DeltaRemoveFileAction() { Path = add.Path, DataChange = false, DeletionTimestamp = 0 } });
            await WriteCommit(storage, "test", 2, new DeltaAction() { Add = add });

            stream.DeleteUser(stream.Users[0]);
            await WaitForVersion(storage, "test", stream, 3);

            var reAdd = Assert.Single((await DeltaTransactionReader.ReadVersionCommit(storage, "test", 3))!.AddedFiles);
            Assert.NotNull(reAdd.DeletionVector);
            using var written = JsonDocument.Parse(reAdd.Statistics!);
            Assert.Equal(20, written.RootElement.GetProperty("numRecords").GetInt64());
            Assert.False(written.RootElement.GetProperty("tightBounds").GetBoolean());
        }

        [Fact]
        public async Task AFailedInsertFileIsDeleted()
        {
            var testName = nameof(AFailedInsertFileIsDeleted);
            // Owned, so the in-memory store deletes the attempt's files by name
            var storage = new OwnedHookFileStorage(Files.Of.InternalMemory($"./{testName}"));
            await using var stream = new DeltaLakeSinkStream(testName, storage, options => options.CheckpointInterval = 0);
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            await SettlePublications(stream);

            // The next insert file keeps a few bytes, then the write fails
            var torn = TearNextPartFile(storage);
            stream.Generate(5);
            var failure = await Assert.ThrowsAnyAsync<Exception>(() => WaitForVersion(storage, "test", stream, 1));

            Assert.Contains(nameof(CrashException), failure.ToString());
            Assert.False(await storage.Inner.Exists($"/test/{await torn}"));
        }

        [Fact]
        public async Task AFailedCleanupIsLoggedAndTheFailureStillSurfaces()
        {
            var testName = nameof(AFailedCleanupIsLoggedAndTheFailureStillSurfaces);
            var storage = new OwnedHookFileStorage(Files.Of.InternalMemory($"./{testName}"));
            var logs = new TestLogCollector();
            await using var stream = new DeltaLakeSinkStream(testName, storage, options => options.CheckpointInterval = 0);
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.AddLoggerProvider(logs);
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            await SettlePublications(stream);

            var torn = TearNextPartFile(storage);
            var before = storage.Before;
            storage.Before = async (verb, path) =>
            {
                if (verb == "Rm" && torn.IsCompleted && path.Full.EndsWith(torn.Result))
                {
                    throw new IOException("Simulated delete failure");
                }
                await before!(verb, path);
            };
            stream.Generate(5);
            var failure = await Assert.ThrowsAnyAsync<Exception>(() => WaitForVersion(storage, "test", stream, 1));

            Assert.Contains(nameof(CrashException), failure.ToString());
            Assert.Contains(logs.Warnings, x => x.Contains("could not delete 1 discarded files"));
        }

        [Fact]
        public async Task AFailedStageDeletesNothing()
        {
            var testName = nameof(AFailedStageDeletesNothing);
            // Owned, so a delete would go through
            var storage = new OwnedHookFileStorage(Files.Of.InternalMemory($"./{testName}"));
            await using var stream = new DeltaLakeSinkStream(testName, storage, options => options.CheckpointInterval = 0);
            stream.WaitForUpdateDoesNotRequireDataChange();
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            await SettlePublications(stream);

            var failed = 0;
            storage.Before = (verb, path) =>
            {
                if (verb == "OpenWrite" && path.Full.EndsWith(".tmp") && Interlocked.Exchange(ref failed, 1) == 0)
                {
                    throw new IOException("Simulated stage failure");
                }
                return Task.CompletedTask;
            };
            storage.ClearRequests();
            stream.Generate(5);
            var failure = await Assert.ThrowsAnyAsync<Exception>(() => WaitForVersion(storage, "test", stream, 1));

            // The stage may exist, so the attempt's files are kept
            Assert.Contains("Simulated stage failure", failure.ToString());
            Assert.Contains(storage.Requests, x => x.StartsWith("OpenWrite /test/part-"));
            Assert.DoesNotContain(storage.Requests, x => x.StartsWith("Rm /test/part-"));
        }

        [Theory]
        [InlineData("int96")]
        [InlineData("nanos")]
        public async Task AnInexactSurvivorTakesADeletionVector(string type)
        {
            var output = await Delete(InexactFile(type), Timestamps, deletionVectors: true, deleted: new long[] { 0, 1 });

            Assert.Equal(new[] { "remove data.parquet", "add data.parquet" }, output.Actions);
            Assert.Empty(output.Written);
            Assert.Equal(10, Assert.Single(output.Overlay.Updates).Record.NumRecords);
        }

        [Theory]
        [InlineData("int96")]
        [InlineData("nanos")]
        public async Task AnInexactSurvivorWithoutDeletionVectorsIsAnError(string type)
        {
            // Every batch fills a file, so a copy would write before the error
            var requests = new List<string>();
            var error = await Assert.ThrowsAsync<InvalidOperationException>(() => Delete(InexactFile(type), Timestamps, deletionVectors: false, deleted: new long[] { 0, 1 }, rollEveryBatch: true, requests: requests));

            Assert.Equal("Delta table t cannot rewrite data.parquet: a surviving row holds a timestamp finer than a microsecond. Enable deletion vectors on the table to delete from this file", error.Message);
            Assert.Contains(requests, x => x == "OpenRead /t/data.parquet");
            Assert.DoesNotContain(requests, x => x.StartsWith("OpenWrite"));
        }

        [Fact]
        public async Task AnInexactDeletedRowLetsTheRewriteGoAhead()
        {
            var output = await Delete(InexactFile("int96"), Timestamps, deletionVectors: false, deleted: new long[] { 0, 5 });

            Assert.Equal(new[] { "remove data.parquet" }, output.Actions);
            Assert.Equal(8, Assert.Single(output.Written).Rows);
            Assert.Equal(new[] { 0 }, output.Overlay.Removes);
        }

        [Theory]
        [InlineData(2)]
        [InlineData(0)]
        public async Task ANestedValueIsCheckedPastNullParents(long deletedRow)
        {
            // Null struct, struct with a null value, 1,500 ns past midnight
            using var ts = new PrimitiveNode("ts", Repetition.Optional, LogicalType.None(), PhysicalType.Int96);
            using var s = new GroupNode("s", Repetition.Optional, new Node[] { ts });
            using var root = new GroupNode("schema", Repetition.Required, new Node[] { s });
            var file = WriteColumn(root, writer => ((ColumnWriter<Int96>)writer).WriteBatch(3, new short[] { 0, 1, 2 }, null, new[] { new Int96(1_500, 0, 2_460_000) }));
            var table = new Delta.StructType(new[] { Field("s", new Delta.StructType(new[] { Field("ts", new Delta.TimestampType()) })) });

            if (deletedRow == 2)
            {
                var output = await Delete(file, table, deletionVectors: false, deleted: new long[] { deletedRow });
                Assert.Equal(2, Assert.Single(output.Written).Rows);
                return;
            }
            var error = await Assert.ThrowsAsync<InvalidOperationException>(() => Delete(file, table, deletionVectors: false, deleted: new long[] { deletedRow }));
            Assert.Contains("a surviving row holds a timestamp finer than a microsecond", error.Message);
        }

        [Fact]
        public async Task AVectorCountingRowsItDoesNotHoldIsADecodeError()
        {
            // The bitmap's header counts 4,999 rows, its bits hold 4,998
            using var bytes = new MemoryStream();
            using (var binary = new BinaryWriter(bytes, System.Text.Encoding.UTF8, leaveOpen: true))
            {
                RoaringBitmapArray.Create(Enumerable.Range(0, 4_999).Select(x => (long)x)).Serialize(binary);
            }
            var damaged = bytes.ToArray();
            // One outer entry and a bitmap container, its bits start at byte 28
            damaged[28 + 42 / 8] &= unchecked((byte)~(1 << (42 % 8)));
            using var damagedStream = new MemoryStream(damaged);
            using var damagedReader = new BinaryReader(damagedStream);
            var existing = RoaringBitmapArray.Deserialize(damagedReader);
            Assert.Equal((4_999, 4_998), (existing.Cardinality, existing.LongCount()));

            var error = await Assert.ThrowsAsync<InvalidDataException>(() => Delete(InexactFile("nanos", rows: 5_000, whole: true), Timestamps, deletionVectors: false, deleted: new long[] { 4_999 }, existingVector: existing));

            Assert.Equal("Delta file data.parquet cannot be decoded: its deletion vector counts 5000 rows and holds 4999", error.Message);
        }

        [Fact]
        public async Task ARepeatedPositionIsADecodeError()
        {
            // Counted three times, with row 3 still live the remove would lose it
            var existing = new ListVector(0, 1, 1);

            var error = await Assert.ThrowsAsync<InvalidDataException>(() => Delete(InexactFile("nanos", rows: 4, whole: true), Timestamps, deletionVectors: false, deleted: new long[] { 2 }, existingVector: existing));

            Assert.Equal("Delta file data.parquet cannot be decoded: deleted row 1 comes twice or out of order", error.Message);
        }

        [Fact]
        public async Task ARewriteRollsWithinOneSource()
        {
            // Three batches of 64k rows, each fills a file
            var source = RewriteCopyTests.Write(new Apache.Arrow.Schema.Builder().Field(new Apache.Arrow.Field("v", Apache.Arrow.Types.Int32Type.Default, true)).Build(),
                new Apache.Arrow.Int32Array.Builder().AppendRange(Enumerable.Range(0, 140_000)).Build());
            var table = new Delta.StructType(new[] { Field("v", new Delta.IntegerType()) });

            var output = await Delete(source, table, deletionVectors: false, deleted: new long[] { 0, 70_000 }, rollEveryBatch: true);

            Assert.True(output.Written.Count >= 2, $"{output.Written.Count} files");
            Assert.Equal(139_998, output.Written.Sum(x => x.Rows));
        }

        [Fact]
        public async Task ADeletedRowPastTheFooterIsADecodeError()
        {
            var error = await Assert.ThrowsAsync<InvalidDataException>(() => Delete(InexactFile("nanos"), Timestamps, deletionVectors: true, deleted: new long[] { 1, 10 }));

            Assert.Equal("Delta file data.parquet cannot be decoded: deleted row 10 is outside its 10 rows", error.Message);
        }

        [Fact]
        public async Task AnExistingVectorPastTheFooterIsADecodeError()
        {
            var error = await Assert.ThrowsAsync<InvalidDataException>(() => Delete(InexactFile("nanos"), Timestamps, deletionVectors: true, deleted: new long[] { 1 }, existing: new long[] { 12 }));

            Assert.Equal("Delta file data.parquet cannot be decoded: deleted row 12 is outside its 10 rows", error.Message);
        }

        [Fact]
        public async Task AFileWithEveryRowDeletedIsNotOpened()
        {
            var requests = new List<string>();
            var output = await Delete(InexactFile("int96"), Timestamps, deletionVectors: false, deleted: new long[] { 0, 1, 2, 3, 4 }, existing: new long[] { 5, 6, 7, 8, 9 }, requests: requests);

            Assert.Equal(new[] { "remove data.parquet" }, output.Actions);
            Assert.Empty(output.Written);
            Assert.Empty(requests);
        }

        [Fact]
        public async Task TheVectorShareUsesTheFooterCount()
        {
            // The record has no count, the footer says 20 rows
            var output = await Delete(InexactFile("nanos", rows: 20, whole: true), Timestamps, deletionVectors: true, deleted: new long[] { 3 });

            Assert.Equal(new[] { "remove data.parquet", "add data.parquet" }, output.Actions);
            Assert.Equal(20, Assert.Single(output.Overlay.Updates).Record.NumRecords);
        }

        [Fact]
        public async Task DuplicateStatisticPropertiesDoNotBlockADeletionVector()
        {
            const string statistics = "{\"x\":1,\"x\":2}";
            var options = new JsonSerializerOptions();
            options.Converters.Add(new Internal.Delta.Stats.DeltaStatisticsConverter(Timestamps));
            Assert.Equal(0, CatalogOverlay.ParseStatistics(statistics, options)!.NumRecords);

            var output = await Delete(InexactFile("nanos", rows: 20, whole: true), Timestamps,
                deletionVectors: true, deleted: new long[] { 3 }, statistics: statistics);

            Assert.Equal(new[] { "remove data.parquet", "add data.parquet" }, output.Actions);
            var update = Assert.Single(output.Overlay.Updates).Record;
            Assert.Equal(20, update.NumRecords);
            using var saved = JsonDocument.Parse(update.Statistics!);
            Assert.Equal(20, saved.RootElement.GetProperty("numRecords").GetInt64());
            Assert.False(saved.RootElement.GetProperty("tightBounds").GetBoolean());
        }

        private static readonly Delta.StructType Timestamps = new Delta.StructType(new[] { Field("ts", new Delta.TimestampType()) });

        private sealed record Output(List<string> Actions, List<(string Path, long Rows)> Written, CatalogOverlay Overlay);

        // Row 5 is 500 nanoseconds past a whole microsecond
        private static byte[] InexactFile(string type, int rows = 10, bool whole = false)
        {
            var nanos = Enumerable.Range(0, rows).Select(i => i * 1_000_000L + (i == 5 && !whole ? 500 : 0)).ToArray();
            return type == "int96"
                ? TimestampPrecisionTests.WriteInt96(nanos.Select(x => (Int96?)new Int96((int)x, (int)(x >> 32), 2_460_000)).ToArray())
                : TimestampPrecisionTests.WriteNanos(nanos.Select(x => (long?)x).ToArray());
        }

        // Runs the delete output on one file of the table
        private static async Task<Output> Delete(byte[] source, Delta.StructType table, bool deletionVectors, long[] deleted, bool rollEveryBatch = false, long[]? existing = null, List<string>? requests = null, IDeleteVector? existingVector = null, string? statistics = null)
        {
            var storage = new HookFileStorage(Files.Of.InternalMemory("./" + ScratchOwners.ShortId()));
            using (var write = await storage.OpenWrite("/t/data.parquet"))
            {
                await write.WriteAsync(source);
            }
            long rows;
            using (var file = await ParquetSharpReader.OpenDataFile(storage, "/t", "data.parquet", forDeletes: true))
            {
                rows = file.NumRows;
            }
            var vector = new ModifiableDeleteVector(existingVector ?? (existing == null ? EmptyDeleteVector.Instance : RoaringBitmapArray.Create(existing)));
            foreach (var position in deleted)
            {
                vector.Add(position);
            }
            var options = new JsonSerializerOptions();
            options.Converters.Add(new Internal.Delta.Stats.DeltaStatisticsConverter(table));
            var overlay = new CatalogOverlay(1, PruningLayout.Create(table, Array.Empty<string>(), 8), options, null);
            var actions = new List<DeltaAction>();
            var created = new List<string>();
            var written = new List<(string, long)>();
            var record = new DeltaFileRecord() { Path = "data.parquet", Size = source.Length, PartitionValues = new Dictionary<string, string>(), Statistics = statistics };

            storage.ClearRequests();
            var output = new DeleteOutput(storage, "/t", "t", table, false, deletionVectors, null, _ => rollEveryBatch, async writer =>
            {
                var name = $"part-{written.Count}.parquet";
                created.Add(name);
                written.Add((name, writer.WrittenCount));
                await writer.WriteData(storage, "/t", name);
                writer.NewBatch();
            });
            try
            {
                await output.WriteAsync(new Dictionary<int, ModifiableDeleteVector>() { [0] = vector }, new Dictionary<int, DeltaFileRecord>() { [0] = record }, new Dictionary<int, long>() { [0] = rows }, actions, 1000, overlay, created);
            }
            finally
            {
                requests?.AddRange(storage.Requests);
            }
            var described = actions.Select(x => x.Remove != null ? $"remove {x.Remove.Path}" : $"add {x.Add!.Path}").ToList();
            return new Output(described, written, overlay);
        }

        private static byte[] WriteColumn(GroupNode schema, Action<ColumnWriter> write)
        {
            using var stream = new MemoryStream();
            using var properties = DeltaTestKit.Properties(x => x);
            using (var writer = new ParquetFileWriter(stream, schema, properties, leaveOpen: true))
            {
                using (var rowGroup = writer.AppendRowGroup())
                using (var column = rowGroup.NextColumn())
                {
                    write(column);
                }
                writer.Close();
            }
            return stream.ToArray();
        }

        private static Delta.StructField Field(string name, FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.SchemaBaseType type)
        {
            return new Delta.StructField(name, type, true, new Dictionary<string, object>());
        }

        // An existing vector as listed, repeats included
        private sealed class ListVector : IDeleteVector
        {
            private readonly long[] _positions;

            public ListVector(params long[] positions)
            {
                _positions = positions;
            }

            public long Cardinality => _positions.Length;

            public bool Contains(long index) => _positions.Contains(index);

            public IEnumerator<long> GetEnumerator() => ((IEnumerable<long>)_positions).GetEnumerator();

            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
        }

        // The path of the next insert file, whose write is torn
        private static Task<string> TearNextPartFile(HookFileStorage storage)
        {
            var path = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
            storage.Before = (verb, target) =>
            {
                var name = target.Full.Substring(target.Full.LastIndexOf('/') + 1);
                if (verb == "OpenWrite" && name.StartsWith("part-") && path.TrySetResult(name))
                {
                    storage.TearAfterBytes = 8;
                    storage.TearWriteOf = name;
                }
                return Task.CompletedTask;
            };
            return path.Task;
        }

        private static string Without(JsonObject statistics, string property)
        {
            var copy = statistics.DeepClone().AsObject();
            copy.Remove(property);
            return copy.ToJsonString();
        }

        private static string With(JsonObject statistics, string property, long value)
        {
            var copy = statistics.DeepClone().AsObject();
            copy[property] = value;
            return copy.ToJsonString();
        }
    }
}
