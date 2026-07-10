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
using FlowtideDotNet.Base.Engine.Internal.StateMachine;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Engine.Distributed;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Sql;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.AcceptanceTests.Distributed
{
    // Assigns the process-wide StreamContext startup hook, must not run in parallel with
    // other classes assigning the same statics.
    [Collection("StreamContext test hooks")]
    public class OrphanedStartRecoveryTests : IAsyncLifetime
    {
        private const string TestName = "e2e_orphaned_start";
        private const string JoinSql = @"
            INSERT INTO output
            SELECT u.userkey FROM users u
            INNER JOIN orders o ON u.userkey = o.userkey;
            ";

        private readonly MockDatabase _db = new MockDatabase();
        private DistributedFlowtideStream? _stream;

        public OrphanedStartRecoveryTests()
        {
            FastEngineTimings.Apply();
        }

        public Task InitializeAsync() => Task.CompletedTask;

        public async Task DisposeAsync()
        {
            if (_stream != null)
            {
                // Bounded: a stream wedged in the orphaned-start failure loop cannot be
                // disposed, and an unbounded wait here hangs the whole test host.
                await Task.WhenAny(_stream.DisposeAsync().AsTask(), Task.Delay(TimeSpan.FromSeconds(20)));
            }
        }

        /// <summary>
        /// Storage whose initialization can be held on a chosen call, so a test can park a
        /// restart inside the state manager restore - before the start has created any
        /// blocks - and land a failure in exactly that window.
        /// </summary>
        private sealed class HoldingStorage : IPersistentStorage
        {
            private readonly IPersistentStorage _inner;
            private readonly int _holdOnCall;
            private int _calls;

            public readonly TaskCompletionSource Held = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            public readonly TaskCompletionSource Release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            public HoldingStorage(IPersistentStorage inner, int holdOnCall)
            {
                _inner = inner;
                _holdOnCall = holdOnCall;
            }

            public long CurrentVersion => _inner.CurrentVersion;

            public async Task InitializeAsync(StorageInitializationMetadata metadata)
            {
                if (Interlocked.Increment(ref _calls) == _holdOnCall)
                {
                    Held.TrySetResult();
                    await Release.Task;
                }
                await _inner.InitializeAsync(metadata);
            }

            public IPersistentStorageSession CreateSession() => _inner.CreateSession();
            public ValueTask CheckpointAsync(byte[] metadata, bool includeIndex) => _inner.CheckpointAsync(metadata, includeIndex);
            public ValueTask CompactAsync(ulong changesSinceLastCompact, ulong pageCount) => _inner.CompactAsync(changesSinceLastCompact, pageCount);
            public ValueTask ResetAsync() => _inner.ResetAsync();
            public ValueTask RecoverAsync(long checkpointVersion) => _inner.RecoverAsync(checkpointVersion);
            public bool TryGetValue(long key, [NotNullWhen(true)] out ReadOnlyMemory<byte>? value) => _inner.TryGetValue(key, out value);
            public ValueTask Write(long key, byte[] value) => _inner.Write(key, value);
            public void ClearForRestore() => _inner.ClearForRestore();
            public void Dispose() => _inner.Dispose();
        }

        /// <summary>
        /// A failure reported while a restart is mid flight - before the start has created
        /// the blocks - must not orphan the running start. The failure teardown sees no
        /// blocks and correctly skips, but unobserved the orphaned start then continues,
        /// creates blocks and starts source and fetch tasks that nothing ever faults or
        /// awaits; every following restart dies on "Initialize while there are running
        /// tasks", once per transition delay, forever, and the stream never recovers.
        /// Captured live from the clean handoff lost-state test's recovery cascade.
        /// </summary>
        [Fact]
        public async Task FailureDuringRestartDoesNotOrphanTheStart()
        {
            var generator = new DatasetGenerator(_db);
            generator.Generate(200);
            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();

            // Held on the second initialization: the first is the initial start, the second
            // is the recovery restart the failure must land into.
            var holdingStorage = new HoldingStorage(
                new ReservoirPersistentStorage(new Storage.Persistence.Reservoir.ReservoirStorageOptions()
                {
                    FileProvider = new MemoryFileProvider()
                }),
                holdOnCall: 2);

            _stream = new DistributedStreamBuilder(TestName)
                .AddPlan(BuildPlan)
                .WithStateOptionsFactory((_, substreamName) => CreateStateOptions(substreamName, substreamName == "substream_0" ? holdingStorage : null))
                .ConfigureSubstream((substreamName, substreamBuilder) =>
                {
                    var connectorManager = new ConnectorManager();
                    connectorManager.AddSource(new MockSourceFactory("*", _db, true));
                    connectorManager.AddSink(new MockSinkFactory("*", data => latestData[substreamName] = data, 0, _ => { }));
                    substreamBuilder.AddConnectorManager(connectorManager);
                    substreamBuilder.WithFailureListener(e => failures.Add((substreamName, e)));
                })
                .DistributeAutomatically(2)
                .Build();
            await _stream.StartAsync();
            await WaitForCount(latestData, "substream_0", ExpectedCount(generator), failures);

            var substream0 = _stream.Substreams["substream_0"];

            // Drive a recovery whose restart parks inside the state manager restore.
            await substream0.InjectFailureForTests(new Exception("Forces the recovery restart"));
            var held = await Task.WhenAny(holdingStorage.Held.Task, Task.Delay(TimeSpan.FromSeconds(30)));
            Assert.True(held == holdingStorage.Held.Task, "The recovery restart never reached the storage initialization");

            // The late report: a failure lands while the restart is mid flight and has not
            // created any blocks yet. The failure teardown correctly finds nothing to tear
            // down; the held start must observe that it was superseded.
            await substream0.InjectFailureForTests(new Exception("Late failure during the restart"));
            await Task.Delay(100);
            holdingStorage.Release.TrySetResult();

            // The stream must converge; with an orphaned start every restart dies on
            // "Initialize while there are running tasks" and the data never returns.
            generator.Generate(100);
            await WaitForCount(latestData, "substream_0", ExpectedCount(generator), failures);
        }

        /// <summary>
        /// An abandoned start may only tear down blocks it created itself. When the abandon
        /// runs late - here parked in the startup hook - the failure teardown has already
        /// cleaned that start's blocks and the recovery has begun a successor start whose
        /// blocks now own the shared created flag. Claiming that flag faulted the successor's
        /// blocks mid start: when nothing had observed the fault yet the successor's
        /// initialization events were dropped on the faulted blocks and the stream wedged
        /// silently forever; at best a spurious extra recovery ran. Captured live from the
        /// clean handoff lost-state test under full-suite load.
        /// </summary>
        [Fact]
        public async Task AbandonedStartDoesNotTearDownTheSuccessorsBlocks()
        {
            var generator = new DatasetGenerator(_db);
            generator.Generate(200);
            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();

            int substream0Starts = 0;
            var firstRestartHeld = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var releaseFirstRestart = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var successorHeld = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var releaseSuccessor = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            StreamContext.StartupBeforeInitTrackingHookForTests = async (streamName) =>
            {
                if (!streamName.Contains(TestName) || !streamName.Contains("substream_0"))
                {
                    return;
                }
                int call = Interlocked.Increment(ref substream0Starts);
                if (call == 2)
                {
                    // The restart after the first failure: parked here, past its block
                    // creation, until the successor exists.
                    firstRestartHeld.TrySetResult();
                    await releaseFirstRestart.Task;
                }
                else if (call == 3)
                {
                    // The successor: parked past its own block creation so the abandoned
                    // start's late cleanup runs while these blocks own the created flag.
                    successorHeld.TrySetResult();
                    await releaseSuccessor.Task;
                }
            };

            try
            {
                _stream = new DistributedStreamBuilder(TestName)
                    .AddPlan(BuildPlan)
                    .WithStateOptionsFactory((_, substreamName) => CreateStateOptions(substreamName, null))
                    .ConfigureSubstream((substreamName, substreamBuilder) =>
                    {
                        var connectorManager = new ConnectorManager();
                        connectorManager.AddSource(new MockSourceFactory("*", _db, true));
                        connectorManager.AddSink(new MockSinkFactory("*", data => latestData[substreamName] = data, 0, _ => { }));
                        substreamBuilder.AddConnectorManager(connectorManager);
                        substreamBuilder.WithFailureListener(e => failures.Add((substreamName, e)));
                    })
                    .DistributeAutomatically(2)
                    .Build();
                await _stream.StartAsync();
                await WaitForCount(latestData, "substream_0", ExpectedCount(generator), failures);

                var substream0 = _stream.Substreams["substream_0"];

                // First failure: the recovery restart parks in the startup hook, after it
                // created its blocks.
                await substream0.InjectFailureForTests(new Exception("Forces the first recovery restart"));
                var held = await Task.WhenAny(firstRestartHeld.Task, Task.Delay(TimeSpan.FromSeconds(30)));
                Assert.True(held == firstRestartHeld.Task, "The recovery restart never reached the startup hook");

                // Second failure supersedes the parked start; its teardown cleans that
                // start's blocks and the recovery begins the successor.
                await substream0.InjectFailureForTests(new Exception("Supersedes the parked restart"));
                held = await Task.WhenAny(successorHeld.Task, Task.Delay(TimeSpan.FromSeconds(30)));
                Assert.True(held == successorHeld.Task, "The successor restart never reached the startup hook");

                // The superseded start wakes with the successor's blocks owning the created
                // flag; its abandon must leave them alone.
                releaseFirstRestart.TrySetResult();
                await Task.Delay(200);
                releaseSuccessor.TrySetResult();

                generator.Generate(100);
                await WaitForCount(latestData, "substream_0", ExpectedCount(generator), failures);

                var supersededFault = failures.FirstOrDefault(f => f.Exception?.ToString().Contains("The start was superseded by a failure.") == true);
                Assert.True(supersededFault.Exception == null,
                    $"The abandoned start tore down its successor's blocks: a block fault from the abandon surfaced as a stream failure on {supersededFault.Substream}: {supersededFault.Exception}");
            }
            finally
            {
                StreamContext.StartupBeforeInitTrackingHookForTests = null;
                releaseFirstRestart.TrySetResult();
                releaseSuccessor.TrySetResult();
            }
        }

        private int ExpectedCount(DatasetGenerator generator)
        {
            return generator.Orders
                .Join(generator.Users, o => o.UserKey, u => u.UserKey, (o, u) => u.UserKey)
                .Count();
        }

        private Plan BuildPlan()
        {
            var sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.AddTableProvider(new DatasetTableProvider(_db));
            sqlPlanBuilder.Sql(JoinSql);
            return sqlPlanBuilder.GetPlan();
        }

        private static StateManagerOptions CreateStateOptions(string substreamName, IPersistentStorage? storage)
        {
            return new StateManagerOptions()
            {
                CachePageCount = 100_000,
                PersistentStorage = storage ?? new ReservoirPersistentStorage(new Storage.Persistence.Reservoir.ReservoirStorageOptions()
                {
                    FileProvider = new MemoryFileProvider()
                }),
                TemporaryStorageOptions = new FileCacheOptions()
                {
                    DirectoryPath = $"./data/tempFiles/{TestName}/{substreamName}/tmp/{Guid.NewGuid():N}"
                }
            };
        }

        private static async Task WaitForCount(
            ConcurrentDictionary<string, EventBatchData> latestData,
            string key,
            int expectedCount,
            ConcurrentBag<(string Substream, Exception? Exception)> failures)
        {
            var stopwatch = Stopwatch.StartNew();
            while (true)
            {
                if (latestData.TryGetValue(key, out var batch) && batch.Count == expectedCount)
                {
                    return;
                }
                if (stopwatch.Elapsed > TimeSpan.FromSeconds(60))
                {
                    var runningTasks = failures.Count(f => f.Exception?.ToString().Contains("Initialize while there are running tasks") == true);
                    throw new TimeoutException(
                        $"The result did not reach {expectedCount} rows, last {(latestData.TryGetValue(key, out var last) ? last.Count : 0)}. " +
                        $"Failures containing 'Initialize while there are running tasks': {runningTasks} - the failure during the restart orphaned the running start.");
                }
                await Task.Delay(20);
            }
        }
    }
}
