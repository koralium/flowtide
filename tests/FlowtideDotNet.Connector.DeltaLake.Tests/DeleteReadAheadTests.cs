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

using FlowtideDotNet.AcceptanceTests.Entities;
using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal.Catalog;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Deletes;
using Stowage;
using System.Collections.Concurrent;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;
using Delta = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DeleteReadAheadTests
    {
        // Copies after the sink's file, each a candidate for every row
        private const int Copies = 10;

        // Users spread over enough files for several windows
        private const int SpreadUsers = 300;

        private const string AzureTable = "c/tables/t";

        private static User Fixed(int key)
        {
            return new User() { UserKey = key, FirstName = $"first name of {key * 7919}", LastName = "Last" };
        }

        private static void AddUsers(FlowtideTestStream stream, int count)
        {
            for (int i = 0; i < count; i++)
            {
                stream.AddOrUpdateUser(Fixed(i));
            }
        }

        private static async Task Until(FlowtideTestStream stream, Func<bool> condition, string what)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(2);
            while (!condition())
            {
                Assert.True(DateTime.UtcNow < deadline, $"Timed out waiting for {what}");
                await stream.SchedulerTick();
                await Task.Delay(20);
            }
        }

        // Long enough for a sink that did not wait to write
        private static async Task HoldOpen(FlowtideTestStream stream, ReadAheadStorage storage)
        {
            var until = DateTime.UtcNow + TimeSpan.FromMilliseconds(500);
            while (DateTime.UtcNow < until && !storage.Events.Any(x => x.StartsWith("held write ", StringComparison.Ordinal)))
            {
                await stream.SchedulerTick();
                await Task.Delay(20);
            }
        }

        private static async Task<Exception?> WaitForFailure(FlowtideTestStream stream, int failures)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(2);
            while (stream.FailureNotificationCount == failures)
            {
                Assert.True(DateTime.UtcNow < deadline, "The stream did not fail in time");
                if (await Record.ExceptionAsync(() => stream.SchedulerTick()) is { } error)
                {
                    return error;
                }
                await Task.Delay(50);
            }
            // The count can move before the error surfaces
            return await Record.ExceptionAsync(() => stream.SchedulerTick());
        }

        private static string Describe(Exception? error)
        {
            while (error is AggregateException { InnerException: { } inner })
            {
                error = inner;
            }
            return error == null ? "no exception" : $"{error.GetType().Name}: {error.Message}";
        }

        // The sink's file, then copies of it in a later commit
        private static async Task<(ReadAheadStorage Storage, DeltaLakeSinkStream Stream)> StartCopies(string name, int? readAhead, int sizelessCopy = -1)
        {
            var storage = new ReadAheadStorage(Files.Of.InternalMemory($"./{name}"));
            var stream = new DeltaLakeSinkStream(name, storage, options =>
            {
                options.CheckpointInterval = 0;
                options.DeleteLimitsOverride = new DeleteLimits() { ReadAhead = readAhead };
            });
            stream.WaitForUpdateDoesNotRequireDataChange();
            AddUsers(stream, 20);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            await SettlePublications(stream);
            await AddCopies(storage, "test", sizelessCopy);
            storage.Clear();
            return (storage, stream);
        }

        private static async Task AddCopies(IFileStorage storage, string table, int sizelessCopy = -1)
        {
            var own = Assert.Single((await DeltaTransactionReader.ReadTable(storage, table))!.AddFiles);
            var bytes = await ReadBytes(storage, $"/{table}/{own.Path}");
            var actions = new List<DeltaAction>();
            for (int i = 0; i < Copies; i++)
            {
                await using (var write = await storage.OpenWrite($"/{table}/copy{i}.parquet"))
                {
                    await write.WriteAsync(bytes);
                }
                actions.Add(new DeltaAction()
                {
                    Add = new DeltaAddAction() { Path = $"copy{i}.parquet", Size = i == sizelessCopy ? 0 : bytes.Length, DataChange = true, PartitionValues = new Dictionary<string, string>() }
                });
            }
            await WriteCommit(storage, table, 1, actions.ToArray());
        }

        // Small files of distinct users, each a candidate once statistics are gone
        private static async Task<(ReadAheadStorage Storage, DeltaLakeSinkStream Stream, List<string> Files)> StartSpread(string name, DeleteLimits limits,
            int sizeless = -1, bool duplicates = false)
        {
            var storage = new ReadAheadStorage(Files.Of.InternalMemory($"./{name}"));
            var stream = new DeltaLakeSinkStream(name, storage, options =>
            {
                options.CheckpointInterval = 0;
                options.MaxFileSizeBytes = 600;
                options.DeleteLimitsOverride = limits;
            });
            stream.WaitForUpdateDoesNotRequireDataChange();
            AddUsers(stream, SpreadUsers);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            await SettlePublications(stream);
            var adds = (await ReadCommitActions(storage, "test", 0)).Where(x => x.Add != null).Select(x => x.Add!).ToList();
            var actions = new List<DeltaAction>();
            for (int i = 0; i < adds.Count; i++)
            {
                actions.Add(new DeltaAction() { Remove = new DeltaRemoveFileAction() { Path = adds[i].Path, DataChange = false, DeletionTimestamp = 1 } });
                actions.Add(new DeltaAction() { Add = new DeltaAddAction() { Path = adds[i].Path, Size = i == sizeless ? 0 : adds[i].Size, DataChange = false, ModificationTime = 1, PartitionValues = new Dictionary<string, string>() } });
            }
            if (duplicates)
            {
                foreach (var (source, copy) in new[] { (1, "dupA.parquet"), (5, "dupB.parquet") })
                {
                    var bytes = await ReadBytes(storage, $"/test/{adds[source].Path}");
                    await using (var write = await storage.OpenWrite($"/test/{copy}"))
                    {
                        await write.WriteAsync(bytes);
                    }
                    actions.Add(new DeltaAction() { Add = new DeltaAddAction() { Path = copy, Size = bytes.Length, DataChange = true, ModificationTime = 1, PartitionValues = new Dictionary<string, string>() } });
                }
            }
            await WriteCommit(storage, "test", 1, actions.ToArray());
            storage.Clear();
            return (storage, stream, adds.Select(x => x.Path!).ToList());
        }

        private static List<long> LiveKeys(IEnumerable<(string Path, long Position, string Row)> rows)
        {
            return rows.Select(x => long.Parse(x.Row.Split(' ')[0])).Order().ToList();
        }

        [Theory]
        [InlineData(1, 1)]
        [InlineData(8, 8)]
        public async Task AnEarlyStopOpensOnlyItsWindow(int depth, int opens)
        {
            var (storage, stream) = await StartCopies(nameof(AnEarlyStopOpensOnlyItsWindow) + depth, depth);
            await using var _ = stream;
            var parked = storage.Gate();
            var held = storage.Gate();
            var parks = 0;
            var holds = 0;
            storage.Park = file => file == "copy3.parquet" && Interlocked.Increment(ref parks) == 1 ? parked.Task : null;
            storage.Hold = file => file == "copy5.parquet" && Interlocked.Increment(ref holds) == 1 ? held.Task : null;
            try
            {
                stream.DeleteUser(Fixed(5));
                if (depth > 1)
                {
                    await Until(stream, () => storage.Events.Contains("cancelled copy3.parquet"), "the window's disposal");
                    await HoldOpen(stream, storage);
                    held.SetResult();
                }
                await WaitForVersion(storage, "test", stream, 2);
            }
            finally
            {
                storage.Release();
            }

            Assert.Equal(opens, storage.DataOpens.Count);
            Assert.All(storage.Returned, x => Assert.True(x.Disposed, x.File));
            if (depth > 1)
            {
                var events = storage.Events.ToList();
                var commit = events.IndexOf($"write {2:D20}.json");
                Assert.True(events.IndexOf("cancelled copy3.parquet") is >= 0 and var cancelled && cancelled < commit);
                Assert.True(events.IndexOf("closed copy5.parquet") is >= 0 and var closed && closed < commit);
                Assert.All(events.Where(x => x.StartsWith("closed ", StringComparison.Ordinal)), x => Assert.True(events.IndexOf(x) < commit, x));
                Assert.DoesNotContain(events, x => x.StartsWith("held write ", StringComparison.Ordinal));
            }
            var keys = LiveKeys(await ForeignRewrite.LiveRows(storage, "test"));
            Assert.Equal(20 * (Copies + 1) - 1, keys.Count);
            Assert.Equal(Copies, keys.Count(x => x == 5));
        }

        [Fact]
        public async Task ARestartReadsAheadAgain()
        {
            var (storage, stream) = await StartCopies(nameof(ARestartReadsAheadAgain), 8);
            await using var _ = stream;
            await stream.Crash();
            storage.Clear();

            stream.DeleteUser(Fixed(5));
            await WaitForVersion(storage, "test", stream, 2);

            Assert.Equal(8, storage.DataOpens.Count);
            Assert.All(storage.Returned, x => Assert.True(x.Disposed, x.File));
        }

        [Theory]
        [InlineData(3)]
        [InlineData(1024)]
        public async Task ReadAheadDeletesExactlyAsInTurn(int recordGroup)
        {
            var sequential = await DeleteSpread($"{nameof(ReadAheadDeletesExactlyAsInTurn)}{recordGroup}one", recordGroup, 1);
            var ahead = await DeleteSpread($"{nameof(ReadAheadDeletesExactlyAsInTurn)}{recordGroup}eight", recordGroup, 8);

            Assert.True(sequential.Files > 3 * recordGroup || recordGroup > 3, $"{sequential.Files} files");
            Assert.True(sequential.VectorsRead > 0);
            Assert.Equal(sequential.Actions, ahead.Actions);
            Assert.Equal(sequential.Claims, ahead.Claims);
            Assert.Equal(sequential.Keys, ahead.Keys);
            Assert.Equal(sequential.Counts, ahead.Counts);
            Assert.Equal(1, sequential.MostOpen);
            Assert.True(ahead.MostOpen > 1);
        }

        private sealed record SpreadResult(List<string> Actions, List<string> Claims, List<long> Keys, string Counts, long VectorsRead, int MostOpen, int Files);

        // A setup delete leaves vectors, then a delete over them and the duplicates
        private static async Task<SpreadResult> DeleteSpread(string name, int recordGroup, int depth)
        {
            var (storage, stream, files) = await StartSpread(name, new DeleteLimits() { ReadAhead = depth, RecordGroup = recordGroup, RowCap = 4 }, duplicates: true);
            await using var _ = stream;
            var byFile = (await ForeignRewrite.LiveRows(storage, "test")).GroupBy(x => x.Path).ToDictionary(x => x.Key, x => LiveKeys(x));
            var random = new Random(11);
            var order = Enumerable.Range(0, SpreadUsers).Select(x => (long)x).OrderBy(x => random.Next()).ToList();
            var twice = new[] { byFile[files[1]][1], byFile[files[5]][2] };
            var setup = order.Take(10).Union(twice).ToList();
            var measured = order.Skip(10).Take(15).Except(twice).Concat(twice).ToList();
            stream.DeleteUsers(setup.Select(x => Fixed((int)x)).ToList());
            await WaitForVersion(storage, "test", stream, 2);
            foreach (var key in twice)
            {
                stream.AddOrUpdateUser(Fixed((int)key));
            }
            await WaitForVersion(storage, "test", stream, 3);
            storage.Clear();
            if (depth > 1)
            {
                var last = files[Math.Min(Math.Min(depth, recordGroup), files.Count) - 1];
                var held = storage.Gate();
                var first = 0;
                storage.Park = file => file == files[0] && Interlocked.Increment(ref first) == 1 ? held.Task : null;
                storage.Returning = file =>
                {
                    if (file == last)
                    {
                        held.TrySetResult();
                    }
                };
            }
            try
            {
                stream.DeleteUsers(measured.Select(x => Fixed((int)x)).ToList());
                await WaitForVersion(storage, "test", stream, 4);
            }
            finally
            {
                storage.Release();
            }

            var statistics = stream.Sinks[0].DeleteStatisticsForTests!;
            string Name(string path) => files.IndexOf(path) is var i and >= 0 ? $"f{i}" : path.StartsWith("dup", StringComparison.Ordinal) ? path : "new";
            var actions = new List<string>();
            foreach (var version in new[] { 2, 4 })
            {
                foreach (var action in await ReadCommitActions(storage, "test", version))
                {
                    if (action.Add != null)
                    {
                        actions.Add($"{version} add {Name(action.Add.Path!)} {action.Add.DeletionVector?.Cardinality}");
                    }
                    else if (action.Remove != null)
                    {
                        actions.Add($"{version} remove {Name(action.Remove.Path!)} {action.Remove.DeletionVector?.Cardinality}");
                    }
                }
            }
            actions.Sort(StringComparer.Ordinal);
            var head = await DeltaTransactionReader.ReadTable(storage, "test");
            var claims = new List<string>();
            foreach (var add in head!.AddFiles)
            {
                var positions = add.DeletionVector == null ? Array.Empty<long>() : (await new DeletionVectorReader(storage, "/test").Read(add.DeletionVector)).ToArray();
                claims.Add($"{Name(add.Path!)}: {string.Join(",", positions)}");
            }
            claims.Sort(StringComparer.Ordinal);
            var keys = LiveKeys(await ForeignRewrite.LiveRows(storage, "test"));
            Assert.All(storage.Returned, x => Assert.True(x.Disposed, x.File));
            var counts = string.Join(" ", typeof(DeleteStatistics).GetProperties()
                .Where(x => !x.Name.EndsWith("Ticks", StringComparison.Ordinal) && (x.PropertyType == typeof(long) || x.PropertyType == typeof(int)))
                .Select(x => $"{x.Name}={x.GetValue(statistics)}"));
            return new SpreadResult(actions, claims, keys, counts, statistics.VectorsRead, storage.MostOpen, files.Count);
        }

        [Theory]
        [InlineData("throw")]
        [InlineData("missing")]
        public async Task AnUnreachedFailingOpenDoesNotFailTheDelete(string fault)
        {
            var (storage, stream) = await StartCopies(nameof(AnUnreachedFailingOpenDoesNotFailTheDelete) + fault, 8);
            await using var _ = stream;
            storage.Fault = file => file == "copy4.parquet" ? fault : null;
            var failures = stream.FailureNotificationCount;

            stream.DeleteUser(Fixed(5));
            await WaitForVersion(storage, "test", stream, 2);

            Assert.Contains("open copy4.parquet", storage.DataOpens);
            Assert.Equal(failures, stream.FailureNotificationCount);
            Assert.All(storage.Returned, x => Assert.True(x.Disposed, x.File));
            storage.Fault = null;
            Assert.Equal(20 * (Copies + 1) - 1, (await ForeignRewrite.LiveRows(storage, "test")).Count);
        }

        [Theory]
        [InlineData("throw")]
        [InlineData("missing")]
        [InlineData("corrupt")]
        public async Task AFailingOpenAtItsTurnFailsAsInTurn(string fault)
        {
            var sequential = await FailAtTurn($"{nameof(AFailingOpenAtItsTurnFailsAsInTurn)}{fault}one", 1, fault);
            var ahead = await FailAtTurn($"{nameof(AFailingOpenAtItsTurnFailsAsInTurn)}{fault}eight", 8, fault);

            Assert.Equal(sequential.Error, ahead.Error);
            Assert.Equal(0, sequential.Cancelled);
            Assert.Equal(5, ahead.Cancelled);
            Assert.Equal(new[] { 0, 1 }.Concat(fault == "corrupt" ? new[] { 2 } : Array.Empty<int>()), sequential.Returned);
            Assert.Equal(new[] { 0, 1 }.Concat(fault == "corrupt" ? new[] { 2 } : Array.Empty<int>()).Concat(new[] { 8, 9 }), ahead.Returned);
        }

        private sealed record Failure(string Error, int Cancelled, List<int> Returned);

        // Two scans succeed, the third file fails at its turn
        private static async Task<Failure> FailAtTurn(string name, int depth, string fault)
        {
            var (storage, stream, files) = await StartSpread(name, new DeleteLimits() { ReadAhead = depth });
            await using var _ = stream;
            // A restart scans again, only the failing attempt counts
            storage.Attempt = () => stream.FailureNotificationCount;
            var failing = files[2];
            if (fault == "corrupt")
            {
                await using var write = await storage.OpenWrite($"/test/{failing}");
                await write.WriteAsync(new byte[64]);
            }
            else
            {
                storage.Fault = file => file == failing ? fault : null;
            }
            var parked = storage.Gate();
            var parks = 0;
            storage.Park = file => files.IndexOf(file) is >= 3 and <= 7 && Interlocked.Increment(ref parks) <= 5 ? parked.Task : null;
            var failures = stream.FailureNotificationCount;
            var retryReturned = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            storage.Returning = file =>
            {
                if (file == files[3] && stream.FailureNotificationCount > failures)
                {
                    retryReturned.TrySetResult();
                }
            };
            Exception? error;
            try
            {
                stream.DeleteUser(Fixed(SpreadUsers - 1));
                error = await WaitForFailure(stream, failures);
            }
            finally
            {
                storage.Release();
            }

            // The retry's opens must be in, so the filter is tested
            if (depth > 1)
            {
                await retryReturned.Task.WaitAsync(TimeSpan.FromMinutes(2));
            }

            Assert.NotNull(error);
            Assert.False(await storage.Exists(CommitPath("test", 2)));
            var description = Describe(error);
            if (fault != "corrupt")
            {
                Assert.Contains(failing, description);
            }
            var first = storage.Returned.Where(x => x.Attempt == failures).ToList();
            Assert.All(first, x => Assert.True(x.Disposed, x.File));
            var cancelled = storage.Cancelled.Count(x => x == failures);
            return new Failure(description.Replace(failing, "<failing>"), cancelled, first.Select(x => files.IndexOf(x.File)).Order().ToList());
        }

        [Fact]
        public async Task AStopEndsSpeculationButNotTheTurns()
        {
            var (storage, stream, files) = await StartSpread(nameof(AStopEndsSpeculationButNotTheTurns), new DeleteLimits() { ReadAhead = 8 });
            await using var _ = stream;
            Assert.True(files.Count >= 13, $"{files.Count} files");
            var release = storage.Gate();
            var parks = 0;
            storage.Park = file => file == files[3] && Interlocked.Increment(ref parks) == 1 ? release.Task : null;
            var stopped = 0;
            var speculated = new ConcurrentQueue<string>();
            storage.Opening = file =>
            {
                if (Volatile.Read(ref stopped) == 1 && storage.Returned.Any(x => !x.Disposed))
                {
                    speculated.Enqueue(file);
                }
            };

            try
            {
                stream.DeleteUser(Fixed(SpreadUsers - 1));
                await Until(stream, () => storage.Returned.Any(x => x.File == files[10]), "the window to fill at the parked turn");
                Assert.Contains($"open {files[8]}", storage.DataOpens);
                var sink = stream.Sinks[0];
                var cancellation = sink.GetType().GetInterfaces().Single(x => x.Name == "IStreamVertexCancellation");
                await (Task)cancellation.GetMethod("CancelPendingOperations")!.Invoke(sink, null)!;
                Volatile.Write(ref stopped, 1);
                release.SetResult();
                await Until(stream, () => storage.Returned.Any(x => x.File == files[^1]) && storage.Returned.All(x => x.Disposed), "the last turn");
            }
            finally
            {
                storage.Release();
            }

            Assert.Empty(speculated);
            await stream.Crash();
            await WaitForVersion(storage, "test", stream, 2);
            var keys = LiveKeys(await ForeignRewrite.LiveRows(storage, "test"));
            Assert.Equal(Enumerable.Range(0, SpreadUsers - 1).Select(x => (long)x), keys);
        }

        [Fact]
        public async Task AMissingRecordFailsAtItsOwnTurn()
        {
            var storage = new ReadAheadStorage(Files.Of.InternalMemory($"./{nameof(AMissingRecordFailsAtItsOwnTurn)}"));
            var arrow = new Apache.Arrow.Schema.Builder().Field(new Apache.Arrow.Field("userkey", Apache.Arrow.Types.Int64Type.Default, true)).Build();
            var bytes = ForeignRewrite.Write(arrow, new Apache.Arrow.IArrowArray[] { new Apache.Arrow.Int64Array.Builder().Append(1).Build() });
            foreach (var file in new[] { "a.parquet", "c.parquet" })
            {
                await using var write = await storage.OpenWrite($"/t/{file}");
                await write.WriteAsync(bytes);
            }
            var run = new DeleteRun(storage, "/t");
            run.Records[0] = new DeltaFileRecord() { Path = "a.parquet", Size = bytes.Length };
            run.Records[2] = new DeltaFileRecord() { Path = "c.parquet", Size = bytes.Length };
            var schema = new Delta.StructType(new List<Delta.StructField>() { new Delta.StructField("userkey", new Delta.LongType(), true, new Dictionary<string, object>()) });
            var matcher = new FileMatcher(storage, "/t", "t", schema, false);
            await using var window = CandidateReadAhead.Create(storage, "/t", new List<int>() { 0, 1, 2 }, 0, 3, run.Records, 8, CancellationToken.None);

            using (var first = await window.NextAsync())
            {
                await first.Stream!.DisposeAsync();
            }
            await Assert.ThrowsAsync<KeyNotFoundException>(() => matcher.Scan(1, null!, run, window));

            Assert.Equal(new[] { "open a.parquet" }, storage.DataOpens);
        }

        [Fact]
        public async Task ASizelessFileWaitsForItsTurn()
        {
            var (storage, stream, files) = await StartSpread(nameof(ASizelessFileWaitsForItsTurn), new DeleteLimits() { ReadAhead = 8 }, sizeless: 2);
            await using var _ = stream;

            stream.DeleteUser(Fixed(SpreadUsers - 1));
            await WaitForVersion(storage, "test", stream, 2);

            var events = storage.Events.ToList();
            int At(string what)
            {
                var at = events.IndexOf(what);
                Assert.True(at >= 0, what);
                return at;
            }
            Assert.True(At($"open {files[1]}") < At($"closed {files[0]}"));
            Assert.True(At($"open {files[2]}") > At($"closed {files[1]}"));
            Assert.True(At($"open {files[4]}") < At($"closed {files[3]}"));
            Assert.Equal(SpreadUsers - 1, (await ForeignRewrite.LiveRows(storage, "test")).Count);
        }

        [Fact]
        public async Task TheDefaultDepthFollowsTheStore()
        {
            await using var azure = new LoopbackObjectStore(StoreFlavor.Azure);
            await using var s3 = new LoopbackObjectStore(StoreFlavor.S3);
            using var azureStorage = azure.CreateStorage();
            using var s3Storage = s3.CreateStorage();
            var limits = new DeleteLimits();

            Assert.Equal(8, limits.ReadAheadOn(azureStorage));
            Assert.Equal(8, limits.ReadAheadOn(OwnedHookFileStorage.Over(azureStorage)));
            Assert.Equal(1, limits.ReadAheadOn(new HookFileStorage(azureStorage)));
            Assert.Equal(1, limits.ReadAheadOn(s3Storage));
            Assert.Equal(1, limits.ReadAheadOn(Files.Of.InternalMemory($"./{nameof(TheDefaultDepthFollowsTheStore)}")));
            Assert.Equal(1, limits.ReadAheadOn(Files.Of.LocalDisk(Path.GetTempPath())));
            Assert.Equal(3, new DeleteLimits() { ReadAhead = 3 }.ReadAheadOn(s3Storage));
        }

        private static async Task<(DeltaLakeSinkStream Stream, DeltaLakeOptions Options)> StartOnAzure(string name, IFileStorage storage)
        {
            DeltaLakeOptions? captured = null;
            var stream = new DeltaLakeSinkStream(name, storage, options =>
            {
                options.CheckpointInterval = 0;
                options.MaintenanceInitFailsForTests = true;
                captured = options;
            });
            stream.WaitForUpdateDoesNotRequireDataChange();
            AddUsers(stream, 20);
            await stream.StartStream("CREATE TABLE c.tables.t (userkey INT, name STRING); INSERT INTO c.tables.t SELECT userKey AS userkey, firstName AS name FROM users");
            await WaitForVersion(storage, AzureTable, stream, 0);
            await SettlePublications(stream);
            await AddCopies(storage, AzureTable);
            return (stream, captured!);
        }

        private static int AzureDataOpens(HookFileStorage storage)
        {
            return storage.Requests.Count(x => x.StartsWith("OpenRead /c/tables/t/", StringComparison.Ordinal) && x.EndsWith(".parquet", StringComparison.Ordinal) && !x.Contains("/_delta_log/", StringComparison.Ordinal));
        }

        [Fact]
        public async Task AnAzureSinkReadsAheadByDefault()
        {
            await using var server = new LoopbackObjectStore(StoreFlavor.Azure);
            using var raw = server.CreateStorage();
            var storage = OwnedHookFileStorage.Over(raw);
            var (stream, _) = await StartOnAzure(nameof(AnAzureSinkReadsAheadByDefault), storage);
            await using var __ = stream;
            storage.ClearRequests();

            stream.DeleteUser(Fixed(5));
            await WaitForVersion(storage, AzureTable, stream, 2);

            Assert.Equal(8, AzureDataOpens(storage));
            Assert.Equal(20 * (Copies + 1) - 1, (await ForeignRewrite.LiveRows(storage, AzureTable)).Count);
        }

        [Fact]
        public async Task AReplacedStorageIsClassifiedAgain()
        {
            await using var server = new LoopbackObjectStore(StoreFlavor.Azure);
            using var raw = server.CreateStorage();
            var (stream, options) = await StartOnAzure(nameof(AReplacedStorageIsClassifiedAgain), OwnedHookFileStorage.Over(raw));
            await using var _ = stream;
            var replaced = new HookFileStorage(raw);
            options.StorageLocation = replaced;

            stream.DeleteUser(Fixed(5));
            await WaitForVersion(replaced, AzureTable, stream, 2);

            Assert.Equal(1, AzureDataOpens(replaced));
            Assert.Equal(20 * (Copies + 1) - 1, (await ForeignRewrite.LiveRows(replaced, AzureTable)).Count);
        }
    }

    // Parks chosen data opens and tracks every data stream it returns
    internal sealed class ReadAheadStorage : HookFileStorage, IFileStorage
    {
        private readonly ConcurrentQueue<string> _events = new ConcurrentQueue<string>();
        private readonly ConcurrentQueue<TrackedData> _returned = new ConcurrentQueue<TrackedData>();
        private int _mostOpen;
        private int _holding;

        public ReadAheadStorage(IFileStorage inner) : base(inner)
        {
        }

        // The data open waits on the task, the window may cancel it
        public Func<string, Task?>? Park { get; set; }

        // The data open waits on the task, whatever the window does
        public Func<string, Task?>? Hold { get; set; }

        // "throw" or "missing" for a data open
        public Func<string, string?>? Fault { get; set; }

        public Action<string>? Opening { get; set; }

        public Action<string>? Returning { get; set; }

        public IReadOnlyList<string> Events => _events.ToArray();

        public IReadOnlyList<string> DataOpens => Events.Where(x => x.StartsWith("open ", StringComparison.Ordinal)).ToList();

        public IReadOnlyList<TrackedData> Returned => _returned.ToArray();

        public int MostOpen => Volatile.Read(ref _mostOpen);

        // Tags each data open, a test passes the stream's failure count
        public Func<int>? Attempt { get; set; }

        private readonly ConcurrentQueue<int> _cancelled = new ConcurrentQueue<int>();

        // The attempt of each cancelled park
        public IReadOnlyList<int> Cancelled => _cancelled.ToArray();

        private readonly ConcurrentQueue<TaskCompletionSource> _gates = new ConcurrentQueue<TaskCompletionSource>();

        // A park the test releases, or the cleanup does
        public TaskCompletionSource Gate()
        {
            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _gates.Enqueue(gate);
            return gate;
        }

        // Lets every parked open go, so the stream can close
        public void Release()
        {
            Park = null;
            Hold = null;
            foreach (var gate in _gates)
            {
                gate.TrySetResult();
            }
        }

        public void Clear()
        {
            _events.Clear();
            _returned.Clear();
            _cancelled.Clear();
            Volatile.Write(ref _mostOpen, 0);
            ClearRequests();
        }

        private static bool IsData(IOPath path)
        {
            return path.Full.EndsWith(".parquet", StringComparison.Ordinal) && !path.Full.Contains("/_delta_log/", StringComparison.Ordinal);
        }

        public new async Task<Stream?> OpenRead(IOPath path, CancellationToken cancellationToken = default)
        {
            if (!IsData(path))
            {
                return await base.OpenRead(path, cancellationToken);
            }
            var file = System.IO.Path.GetFileName(path.Full);
            var attempt = Attempt?.Invoke() ?? -1;
            _events.Enqueue($"open {file}");
            Opening?.Invoke(file);
            if (Park?.Invoke(file) is { } parked)
            {
                try
                {
                    await parked.WaitAsync(cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    _events.Enqueue($"cancelled {file}");
                    _cancelled.Enqueue(attempt);
                    throw;
                }
            }
            if (Hold?.Invoke(file) is { } held)
            {
                Interlocked.Increment(ref _holding);
                await held;
                Interlocked.Decrement(ref _holding);
                cancellationToken = CancellationToken.None;
            }
            switch (Fault?.Invoke(file))
            {
                case "throw":
                    throw new IOException($"The open of {file} failed");
                case "missing":
                    return null;
            }
            var stream = await base.OpenRead(path, cancellationToken);
            if (stream == null)
            {
                return null;
            }
            var tracked = new TrackedData(file, attempt, stream, () => _events.Enqueue($"closed {file}"));
            _returned.Enqueue(tracked);
            var open = _returned.Count(x => !x.Disposed);
            int most;
            while (open > (most = Volatile.Read(ref _mostOpen)) && Interlocked.CompareExchange(ref _mostOpen, open, most) != most)
            {
            }
            Returning?.Invoke(file);
            return tracked;
        }

        public new async Task<Stream> OpenWrite(IOPath path, CancellationToken cancellationToken = default)
        {
            // The sink writing while a held open is still out
            if (Volatile.Read(ref _holding) > 0)
            {
                _events.Enqueue($"held write {System.IO.Path.GetFileName(path.Full)}");
            }
            if (path.Full.Contains("/_delta_log/", StringComparison.Ordinal) && path.Full.EndsWith(".json", StringComparison.Ordinal))
            {
                _events.Enqueue($"write {System.IO.Path.GetFileName(path.Full)}");
            }
            return await base.OpenWrite(path, cancellationToken);
        }
    }

    internal sealed class TrackedData : Stream
    {
        private readonly Stream _inner;
        private readonly Action _closed;
        private int _disposed;

        public TrackedData(string file, int attempt, Stream inner, Action closed)
        {
            File = file;
            Attempt = attempt;
            _inner = inner;
            _closed = closed;
        }

        public string File { get; }

        // The stream's failure count when it was opened
        public int Attempt { get; }

        public bool Disposed => Volatile.Read(ref _disposed) == 1;

        public override bool CanRead => _inner.CanRead;

        public override bool CanSeek => _inner.CanSeek;

        public override bool CanWrite => false;

        public override long Length => _inner.Length;

        public override long Position
        {
            get => _inner.Position;
            set => _inner.Position = value;
        }

        public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);

        public override int Read(Span<byte> buffer) => _inner.Read(buffer);

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) => _inner.ReadAsync(buffer, cancellationToken);

        public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);

        public override void Flush()
        {
        }

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing && Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                _inner.Dispose();
                _closed();
            }
            base.Dispose(disposing);
        }
    }
}
