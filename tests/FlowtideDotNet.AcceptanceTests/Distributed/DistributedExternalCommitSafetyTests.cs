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

using FlowtideDotNet.Core.Operators.Exchange;
using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Base.Engine.Internal.StateMachine;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Engine.Distributed;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Sql;
using System.Collections.Concurrent;
using System.Diagnostics;

namespace FlowtideDotNet.AcceptanceTests.Distributed
{
    [Collection("StreamContext test hooks")]
    public class DistributedExternalCommitSafetyTests : IAsyncLifetime
    {
        private const string ChainSql = @"
            SUBSTREAM sub1;

            CREATE VIEW v1 WITH (DISTRIBUTED = true, SCATTER_BY = userkey, PARTITION_COUNT = 1) AS
            SELECT userkey FROM users;

            SUBSTREAM sub2;

            CREATE VIEW v2 WITH (DISTRIBUTED = true, SCATTER_BY = userkey, PARTITION_COUNT = 1) AS
            SELECT userkey FROM v1 WITH (PARTITION_ID = 0);

            SUBSTREAM sub3;

            INSERT INTO output SELECT userkey FROM v2 WITH (PARTITION_ID = 0);
            ";

        private const string TwoSubstreamSql = @"
            SUBSTREAM sub1;

            CREATE VIEW v1 WITH (DISTRIBUTED = true, SCATTER_BY = userkey, PARTITION_COUNT = 1) AS
            SELECT userkey FROM users;

            SUBSTREAM sub2;

            INSERT INTO output SELECT userkey FROM v1 WITH (PARTITION_ID = 0);
            ";

        private const string TwoSinksSql = @"
            SUBSTREAM sub1;

            CREATE VIEW read_users WITH (DISTRIBUTED = true, SCATTER_BY = userkey, PARTITION_COUNT = 2) AS
            SELECT userkey FROM users;

            INSERT INTO output SELECT userkey FROM read_users WITH (PARTITION_ID = 0);

            SUBSTREAM sub2;

            INSERT INTO output SELECT userkey FROM read_users WITH (PARTITION_ID = 1);
            ";

        private readonly MockDatabase _db;
        private readonly DatasetGenerator _generator;
        private readonly List<Base.Engine.DataflowStream> _streams = new List<Base.Engine.DataflowStream>();
        private readonly CancellationTokenSource _tickCancellation = new CancellationTokenSource();
        private Task? _tickLoop;

        public DistributedExternalCommitSafetyTests()
        {
            FastEngineTimings.Apply();
            _db = new MockDatabase();
            _generator = new DatasetGenerator(_db);
        }

        public Task InitializeAsync()
        {
            // StartAsync does not tick the schedulers, the sources poll on ticks.
            _tickLoop = Task.Run(async () =>
            {
                using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(10));
                var inflightTicks = new Dictionary<Base.Engine.DataflowStream, Task>();
                try
                {
                    while (await timer.WaitForNextTickAsync(_tickCancellation.Token))
                    {
                        List<Base.Engine.DataflowStream> snapshot;
                        lock (_streams)
                        {
                            snapshot = new List<Base.Engine.DataflowStream>(_streams);
                        }
                        foreach (var stream in snapshot)
                        {
                            if (stream.Scheduler is not DefaultStreamScheduler scheduler)
                            {
                                continue;
                            }
                            if (inflightTicks.TryGetValue(stream, out var previous) && !previous.IsCompleted)
                            {
                                continue;
                            }
                            inflightTicks[stream] = Task.Run(async () =>
                            {
                                try
                                {
                                    await scheduler.Tick();
                                }
                                catch
                                {
                                    // A stream mid stop or dispose may reject the tick.
                                }
                            });
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                }
            });
            return Task.CompletedTask;
        }

        public async Task DisposeAsync()
        {
            StreamContext.CheckpointCommitHookForTests = null;
            StreamContext.CheckpointPostCommitGapHookForTests = null;
            StreamContext.CompactionHookForTests = null;
            StreamContext.RestoreVersionForTests = null;
            StreamContext.StartupBeforeRestoreHookForTests = null;
            _tickCancellation.Cancel();
            if (_tickLoop != null)
            {
                await _tickLoop;
            }
            _tickCancellation.Dispose();
            List<Base.Engine.DataflowStream> streams;
            lock (_streams)
            {
                streams = new List<Base.Engine.DataflowStream>(_streams);
            }
            foreach (var stream in streams)
            {
                try
                {
                    await stream.DisposeAsync();
                }
                catch
                {
                    // Teardown of a stream a test left mid failure.
                }
            }
        }

        /// <summary>
        /// A sink that commits externally at compaction must not get there for a version a
        /// substream further up the chain has not made durable.
        /// </summary>
        [Fact]
        public async Task ChainTailDoesNotCompactBeforeTheHeadIsDurable()
        {
            const string testName = "commit_safety_chain";
            _generator.Generate(300);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var hub = new LocalSubstreamCommunicationHub();
            var failures = new ConcurrentBag<string>();
            var started = new ConcurrentDictionary<string, long>();
            var compactions = new ConcurrentDictionary<string, int>();

            int armed = 0;
            int headHeld = 0;
            long heldVersion = -1;
            var headHoldStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var releaseHead = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var tailCompactedHeldVersion = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);
            var tailCompactedAfterRelease = new TaskCompletionSource<long>(TaskCreationOptions.RunContinuationsAsynchronously);

            StreamContext.CheckpointCommitHookForTests = async (streamName, lastVersion) =>
            {
                var substream = SubstreamOf(streamName, testName);
                if (substream == null)
                {
                    return;
                }
                started.AddOrUpdate(substream, lastVersion + 1, (_, current) => Math.Max(current, lastVersion + 1));
                if (substream == "sub1" && Volatile.Read(ref armed) == 1 && Interlocked.CompareExchange(ref headHeld, 1, 0) == 0)
                {
                    Interlocked.Exchange(ref heldVersion, lastVersion + 1);
                    headHoldStarted.TrySetResult();
                    await releaseHead.Task;
                    Interlocked.Exchange(ref headHeld, 2);
                }
            };
            StreamContext.CompactionHookForTests = (streamName) =>
            {
                var substream = SubstreamOf(streamName, testName);
                if (substream == null)
                {
                    return Task.CompletedTask;
                }
                compactions.AddOrUpdate(substream, 1, (_, count) => count + 1);
                if (substream == "sub3" &&
                    started.TryGetValue(substream, out var version) &&
                    version >= Interlocked.Read(ref heldVersion))
                {
                    var held = Volatile.Read(ref headHeld);
                    if (held == 1)
                    {
                        tailCompactedHeldVersion.TrySetResult(version);
                    }
                    else if (held == 2)
                    {
                        tailCompactedAfterRelease.TrySetResult(version);
                    }
                }
                return Task.CompletedTask;
            };

            try
            {
                // Built first, started together, as the hosts do: a handshake needs the peer's handler.
                var substreams = new[] { "sub1", "sub2", "sub3" }
                    .Select(substream => BuildSubstream(testName, ChainSql, substream, hub, latestData, failures))
                    .ToList();
                await Task.WhenAll(substreams.Select(substream => substream.StartAsync()));

                await WaitUntil(() => RowCount(latestData, "sub3") == _generator.Users.Count, "initial data in the tail sink");
                await WaitUntil(() => new[] { "sub1", "sub2", "sub3" }.All(s => compactions.GetValueOrDefault(s) > 0), "a first checkpoint on all three substreams");

                var durable = TrackDurable(testName, started);
                Volatile.Write(ref armed, 1);
                _generator.Generate(200);
                await WaitForTask(headHoldStarted.Task, "the head commit to be held");
                await WaitForTailBlockedOnlyByTheGroup(durable, Interlocked.Read(ref heldVersion));

                var first = await Task.WhenAny(tailCompactedHeldVersion.Task, Task.Delay(TimeSpan.FromSeconds(5)));
                Assert.False(
                    first == tailCompactedHeldVersion.Task,
                    $"sub3 reached compaction for version {(first == tailCompactedHeldVersion.Task ? tailCompactedHeldVersion.Task.Result : -1)} while sub1 had not made version {Interlocked.Read(ref heldVersion)} durable.");

                // Liveness: the held version completes on the tail once the head is durable.
                releaseHead.TrySetResult();
                await WaitForTask(tailCompactedAfterRelease.Task, "the tail to compact the held version after the head became durable");
                await WaitUntil(() => RowCount(latestData, "sub3") == _generator.Users.Count, "the new rows in the tail sink");

                // Progress: the held cycle and the one after it complete on every substream.
                await WaitForCompletedCycles(new[] { "sub1", "sub2", "sub3" }, started, Interlocked.Read(ref heldVersion) + 1);
                await WaitUntil(() => RowCount(latestData, "sub3") == _generator.Users.Count, "the later rows in the tail sink");
                Assert.Empty(failures);
            }
            finally
            {
                releaseHead.TrySetResult();
            }
        }

        /// <summary>
        /// A substream must not finish starting while a peer it exchanges data with has not
        /// initialized, that peer can still come up at a lower version.
        /// </summary>
        [Fact]
        public async Task SubstreamDoesNotReachRunningBeforeItsPeerInitialized()
        {
            const string testName = "commit_safety_late_peer";
            _generator.Generate(100);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var hub = new LocalSubstreamCommunicationHub();
            var failures = new ConcurrentBag<string>();
            var started = new ConcurrentDictionary<string, long>();

            StreamContext.CheckpointCommitHookForTests = (streamName, lastVersion) =>
            {
                var substream = SubstreamOf(streamName, testName);
                if (substream != null)
                {
                    started.AddOrUpdate(substream, lastVersion + 1, (_, current) => Math.Max(current, lastVersion + 1));
                }
                return Task.CompletedTask;
            };

            // Both are built, so the peer's communication handler exists, only sub1 starts.
            var writer = BuildSubstream(testName, TwoSubstreamSql, "sub1", hub, latestData, failures);
            var peer = BuildSubstream(testName, TwoSubstreamSql, "sub2", hub, latestData, failures);

            await writer.StartAsync();

            var sw = Stopwatch.StartNew();
            while (sw.Elapsed < TimeSpan.FromSeconds(5))
            {
                Assert.False(
                    writer.State == StreamStateValue.Running,
                    $"sub1 reached running after {sw.ElapsedMilliseconds} ms while sub2 was never started.");
                await Task.Delay(25);
            }

            // Liveness: both reach running once the peer starts.
            await peer.StartAsync();
            await WaitUntil(() => writer.State == StreamStateValue.Running && peer.State == StreamStateValue.Running, "both substreams to reach running");
            await WaitUntil(() => RowCount(latestData, "sub2") == _generator.Users.Count, "data in the peer's sink");

            // Progress: two more cycles complete on both after the gated start.
            var baseline = started.Values.DefaultIfEmpty(0).Max();
            await WaitForCompletedCycles(new[] { "sub1", "sub2" }, started, baseline + 1);
            await WaitUntil(() => RowCount(latestData, "sub2") == _generator.Users.Count, "the later rows in the peer's sink");
            Assert.Empty(failures);
        }

        /// <summary>
        /// The rule itself: no sink is told to commit a version a substream is not durable at.
        /// </summary>
        [Fact]
        public async Task CommitVersionWaitsForEverySubstreamToBeDurable()
        {
            const string testName = "commit_safety_rule";
            _generator.Generate(200);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var hub = new LocalSubstreamCommunicationHub();
            var failures = new ConcurrentBag<string>();
            var started = new ConcurrentDictionary<string, long>();
            var commits = new ConcurrentQueue<(string Substream, long Version)>();
            var names = new[] { "sub1", "sub2", "sub3" };

            int armed = 0;
            int headHeld = 0;
            long heldVersion = -1;
            var headHoldStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var releaseHead = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            StreamContext.CheckpointCommitHookForTests = async (streamName, lastVersion) =>
            {
                var substream = SubstreamOf(streamName, testName);
                if (substream == null)
                {
                    return;
                }
                started.AddOrUpdate(substream, lastVersion + 1, (_, current) => Math.Max(current, lastVersion + 1));
                if (substream == "sub1" && Volatile.Read(ref armed) == 1 && Interlocked.CompareExchange(ref headHeld, 1, 0) == 0)
                {
                    Interlocked.Exchange(ref heldVersion, lastVersion + 1);
                    headHoldStarted.TrySetResult();
                    await releaseHead.Task;
                    Interlocked.Exchange(ref headHeld, 2);
                }
            };

            try
            {
                var durable = TrackDurable(testName, started);
                var substreams = names
                    .Select(name => BuildSubstream(testName, ChainSql, name, hub, latestData, failures,
                        onCommitVersion: version => commits.Enqueue((name, version))))
                    .ToList();
                await Task.WhenAll(substreams.Select(substream => substream.StartAsync()));

                await WaitUntil(() => RowCount(latestData, "sub3") == _generator.Users.Count, "initial data in the tail sink");
                var baseline = started.Values.DefaultIfEmpty(0).Max();
                await WaitForCompletedCycles(names, started, baseline);

                // The start commits the restore version, a fresh stream restores version 0.
                Assert.Equal(0, commits.First(c => c.Substream == "sub3").Version);

                Volatile.Write(ref armed, 1);
                _generator.Generate(200);
                await WaitForTask(headHoldStarted.Task, "the head commit to be held");
                var held = Interlocked.Read(ref heldVersion);

                // The tail and its neighbour are durable at the held version, the head is not.
                await WaitForTailBlockedOnlyByTheGroup(durable, held);
                await Task.Delay(TimeSpan.FromSeconds(3));
                var early = commits.Where(c => c.Version >= held).ToList();
                Assert.True(
                    early.Count == 0,
                    $"told to commit while sub1 had not made version {held} durable: {string.Join(", ", early.Select(c => $"{c.Substream} version {c.Version}"))}");

                releaseHead.TrySetResult();
                await WaitUntil(() => commits.Any(c => c.Substream == "sub3" && c.Version == held), "the tail to be told to commit the held version");
                await WaitForCompletedCycles(names, started, held + 1);

                var tail = commits.Where(c => c.Substream == "sub3").Select(c => c.Version).ToList();
                Assert.Equal(tail.OrderBy(v => v).ToList(), tail);
                Assert.Equal(tail.Distinct().Count(), tail.Count);
                Assert.Empty(failures);
            }
            finally
            {
                releaseHead.TrySetResult();
            }
        }

        /// <summary>
        /// The proven gap end to end: no substream is rolled back below a version a sink was told to commit.
        /// </summary>
        [Fact]
        public async Task NothingIsRolledBackBelowACommittedVersion()
        {
            const string testName = "commit_safety_rollback";
            _generator.Generate(300);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var hub = new LocalSubstreamCommunicationHub();
            var failures = new ConcurrentBag<string>();
            var started = new ConcurrentDictionary<string, long>();
            var commits = new ConcurrentQueue<(string Substream, long Version)>();
            var restores = new ConcurrentQueue<(string Substream, long Version)>();
            var violations = new ConcurrentQueue<string>();
            var names = new[] { "sub1", "sub2", "sub3" };

            int armed = 0;
            int headHeld = 0;
            long heldVersion = -1;
            var headHoldStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var failHead = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            StreamContext.CheckpointCommitHookForTests = async (streamName, lastVersion) =>
            {
                var substream = SubstreamOf(streamName, testName);
                if (substream == null)
                {
                    return;
                }
                started.AddOrUpdate(substream, lastVersion + 1, (_, current) => Math.Max(current, lastVersion + 1));
                if (substream == "sub1" && Volatile.Read(ref armed) == 1 && Interlocked.CompareExchange(ref headHeld, 1, 0) == 0)
                {
                    Interlocked.Exchange(ref heldVersion, lastVersion + 1);
                    headHoldStarted.TrySetResult();
                    await failHead.Task;
                    throw new InvalidOperationException("simulated state commit failure in the head substream");
                }
            };
            StreamContext.RestoreVersionForTests = (streamName, version) =>
            {
                var substream = SubstreamOf(streamName, testName);
                if (substream == null)
                {
                    return;
                }
                restores.Enqueue((substream, version));
                foreach (var commit in commits)
                {
                    if (commit.Version > version)
                    {
                        violations.Enqueue($"{substream} rolls back to version {version} after {commit.Substream} was told to commit version {commit.Version}");
                    }
                }
            };

            try
            {
                var durable = TrackDurable(testName, started);
                var substreams = names
                    .Select(name => BuildSubstream(testName, ChainSql, name, hub, latestData, failures,
                        onCommitVersion: version => commits.Enqueue((name, version))))
                    .ToList();
                await Task.WhenAll(substreams.Select(substream => substream.StartAsync()));

                await WaitUntil(() => RowCount(latestData, "sub3") == _generator.Users.Count, "initial data in the tail sink");
                var baseline = started.Values.DefaultIfEmpty(0).Max();
                await WaitForCompletedCycles(names, started, baseline);

                Volatile.Write(ref armed, 1);
                _generator.Generate(200);
                await WaitForTask(headHoldStarted.Task, "the head commit to be held");

                // The tail is durable at the held version and its neighbour acknowledged it.
                var held = Interlocked.Read(ref heldVersion);
                await WaitForTailBlockedOnlyByTheGroup(durable, held);
                await Task.Delay(1000);

                failHead.TrySetResult();
                await WaitUntil(() => names.All(name => restores.Any(r => r.Substream == name)), "every substream to roll back");

                // Checked at each rollback against what was committed before it, a commit of the
                // replayed version afterwards is what should happen.
                Assert.True(violations.IsEmpty, string.Join("; ", violations));

                // Recovers and keeps going, the held version is committed after the replay.
                await WaitForCompletedCycles(names, started, held + 1);
                await WaitUntil(() => RowCount(latestData, "sub3") == _generator.Users.Count, "all rows in the tail sink after the recovery");
                await WaitUntil(() => commits.Any(c => c.Substream == "sub3" && c.Version >= held), "the tail to be told to commit the replayed version");
                Assert.True(violations.IsEmpty, string.Join("; ", violations));
            }
            finally
            {
                failHead.TrySetResult();
            }
        }

        /// <summary>
        /// A substream whose exchanges are all local has nobody to agree with and must not wait.
        /// </summary>
        [Fact]
        public async Task ASubstreamWithOnlyLocalExchangesKeepsCheckpointing()
        {
            const string testName = "commit_safety_single";
            _generator.Generate(100);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var hub = new LocalSubstreamCommunicationHub();
            var failures = new ConcurrentBag<string>();
            var started = new ConcurrentDictionary<string, long>();
            var commits = new ConcurrentQueue<long>();

            StreamContext.CheckpointCommitHookForTests = (streamName, lastVersion) =>
            {
                var substream = SubstreamOf(streamName, testName);
                if (substream != null)
                {
                    started.AddOrUpdate(substream, lastVersion + 1, (_, current) => Math.Max(current, lastVersion + 1));
                }
                return Task.CompletedTask;
            };

            var stream = BuildSubstream(testName, @"
                SUBSTREAM sub1;

                CREATE VIEW read_users WITH (DISTRIBUTED = true, SCATTER_BY = userkey, PARTITION_COUNT = 2) AS
                SELECT userkey FROM users;

                INSERT INTO output SELECT userkey FROM read_users WITH (PARTITION_ID = 0);

                INSERT INTO output2 SELECT userkey FROM read_users WITH (PARTITION_ID = 1);
                ", "sub1", hub, latestData, failures, onCommitVersion: version => commits.Enqueue(version));
            await stream.StartAsync();

            await WaitUntil(() => stream.State == StreamStateValue.Running, "the substream to reach running");
            await WaitForCompletedCycles(new[] { "sub1" }, started, 2);

            Assert.Empty(failures);
            Assert.Contains(commits, version => version >= 2);
        }

        /// <summary>
        /// A recover notification from long ago reaches a substream after the group committed further.
        /// </summary>
        [Fact]
        public async Task AnOldRecoverNotificationBelowACommittedVersionIsRefused()
        {
            const string testName = "commit_safety_old_notification";
            _generator.Generate(100);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var hub = new LocalSubstreamCommunicationHub();
            var failures = new ConcurrentBag<string>();
            var started = new ConcurrentDictionary<string, long>();
            var restores = new ConcurrentQueue<(string Substream, long Version)>();
            var commits = new ConcurrentQueue<(string Substream, long Version)>();
            var names = new[] { "sub1", "sub2" };

            StreamContext.CheckpointCommitHookForTests = (streamName, lastVersion) =>
            {
                var substream = SubstreamOf(streamName, testName);
                if (substream != null)
                {
                    started.AddOrUpdate(substream, lastVersion + 1, (_, current) => Math.Max(current, lastVersion + 1));
                }
                return Task.CompletedTask;
            };
            StreamContext.RestoreVersionForTests = (streamName, version) =>
            {
                var substream = SubstreamOf(streamName, testName);
                if (substream != null)
                {
                    restores.Enqueue((substream, version));
                }
            };

            // The handler sub1 really sends through, a second one for the same pair would take its place in the hub.
            var sub1Factory = new RecordingFactory(hub.CreateFactory("sub1"));
            var substreams = new[]
            {
                BuildSubstream(testName, TwoSubstreamSql, "sub1", hub, latestData, failures, onCommitVersion: version => commits.Enqueue(("sub1", version)), communicationFactory: sub1Factory),
                BuildSubstream(testName, TwoSubstreamSql, "sub2", hub, latestData, failures, onCommitVersion: version => commits.Enqueue(("sub2", version))),
            };
            await Task.WhenAll(substreams.Select(substream => substream.StartAsync()));

            await WaitUntil(() => RowCount(latestData, "sub2") == _generator.Users.Count, "initial data in the sink");
            await WaitForCompletedCycles(names, started, 3);
            var committed = commits.Where(c => c.Substream == "sub2").Select(c => c.Version).DefaultIfEmpty(-1).Max();
            Assert.True(committed >= 2, $"sub2 only committed version {committed}");

            // What a failure of sub1 long ago told sub2, delivered now.
            Assert.True(sub1Factory.Handlers.TryGetValue("sub2", out var toSub2));
            await toSub2!.SendFailAndRecover(committed - 1);

            await Task.Delay(TimeSpan.FromSeconds(2));
            Assert.Empty(restores);
            Assert.Empty(failures);

            // Both keep going.
            var reached = started.Values.Max();
            await WaitForCompletedCycles(names, started, reached + 1);
            await WaitUntil(() => RowCount(latestData, "sub2") == _generator.Users.Count, "rows generated after the old notification");
        }

        /// <summary>
        /// A stopped substream accepted an old request for the version before its last, then learns the group is durable at the last one while it starts.
        /// </summary>
        [Fact]
        public async Task AStartDoesNotRestoreBelowAVersionItLetAPeerCommit()
        {
            const string testName = "commit_safety_start_window";
            _generator.Generate(100);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var hub = new LocalSubstreamCommunicationHub();
            var failures = new ConcurrentBag<string>();
            var started = new ConcurrentDictionary<string, long>();
            var commits = new ConcurrentQueue<(string Substream, long Version)>();
            var names = new[] { "sub1", "sub2" };

            StreamContext.CheckpointCommitHookForTests = (streamName, lastVersion) =>
            {
                var substream = SubstreamOf(streamName, testName);
                if (substream != null)
                {
                    started.AddOrUpdate(substream, lastVersion + 1, (_, current) => Math.Max(current, lastVersion + 1));
                }
                return Task.CompletedTask;
            };
            var durable = TrackDurable(testName, started);
            // Compaction runs once the cycle's agreement is in.
            var compacted = new ConcurrentDictionary<string, long>();
            StreamContext.CompactionHookForTests = streamName =>
            {
                var substream = SubstreamOf(streamName, testName);
                if (substream != null && started.TryGetValue(substream, out var version))
                {
                    compacted[substream] = version;
                }
                return Task.CompletedTask;
            };

            // Everything sub2 tells sub1 about durability goes through the gate.
            var gate = new ClaimGate();
            var sub2Factory = new RecordingFactory(hub.CreateFactory("sub2"), handler => new ClaimGatedHandler(handler, gate));
            var sub1 = BuildSubstream(testName, TwoSubstreamSql, "sub1", hub, latestData, failures);
            var sub2 = BuildSubstream(testName, TwoSubstreamSql, "sub2", hub, latestData, failures, onCommitVersion: version => commits.Enqueue(("sub2", version)), communicationFactory: sub2Factory);
            await Task.WhenAll(sub1.StartAsync(), sub2.StartAsync());

            await WaitUntil(() => RowCount(latestData, "sub2") == _generator.Users.Count, "initial data in the sink");
            await WaitForCompletedCycles(names, started, 2);
            // No more data: the last cycle completes on both and nothing follows it.
            long last;
            while (true)
            {
                await WaitUntil(() => names.All(name => compacted.GetValueOrDefault(name, -1) == started["sub2"] && started[name] == started["sub2"]), "the last cycle to complete on both");
                last = started["sub2"];
                await Task.Delay(500);
                if (names.All(name => started[name] == last))
                {
                    break;
                }
            }
            Assert.Contains(commits, c => c.Version == last);

            // sub1 stops at the next version without hearing that sub2 is durable at it.
            gate.Drop = true;
            await WaitForTask(sub1.StopAsync(), "sub1 to stop");
            var stopVersion = last + 1;
            await WaitUntil(() => durable.GetValueOrDefault("sub2", -1) >= stopVersion, "sub2 to be durable at the stop version");
            Assert.DoesNotContain(commits, c => c.Version >= stopVersion);

            // What a failure of sub2 long ago told sub1, delivered while it is stopped.
            Assert.True(sub2Factory.Handlers.TryGetValue("sub1", out var toSub1));
            await toSub1!.SendFailAndRecover(last);

            long? restored = null;
            var parked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            StreamContext.StartupBeforeRestoreHookForTests = async (streamName, restoreVersion) =>
            {
                if (SubstreamOf(streamName, testName) != "sub1")
                {
                    return;
                }
                restored = restoreVersion;
                // sub1 hears it now, between choosing the version and restoring it.
                gate.Drop = false;
                await WaitUntil(() => gate.DeliveredAtOrAbove(stopVersion) >= 2, "the held claims to reach sub1");
                await Task.Delay(TimeSpan.FromSeconds(1));
                parked.TrySetResult();
            };
            _ = sub1.StartAsync();
            await WaitForTask(parked.Task, "sub1 to restore");

            var committed = commits.Select(c => c.Version).DefaultIfEmpty(-1).Max();
            Assert.True(!restored.HasValue || restored.Value >= committed, $"sub1 restores version {restored} after sub2 was told to commit version {committed}");
        }

        private sealed class ClaimGate
        {
            private readonly ConcurrentQueue<long> _delivered = new ConcurrentQueue<long>();

            public volatile bool Drop;

            public void Delivered(long version) => _delivered.Enqueue(version);

            public int DeliveredAtOrAbove(long version) => _delivered.Count(v => v >= version);
        }

        private sealed class ClaimGatedHandler : ISubstreamCommunicationHandler
        {
            private readonly ISubstreamCommunicationHandler _inner;
            private readonly ClaimGate _gate;

            public ClaimGatedHandler(ISubstreamCommunicationHandler inner, ClaimGate gate)
            {
                _inner = inner;
                _gate = gate;
            }

            public void SetReceiveAllocatorResolver(Func<int, Storage.Memory.IMemoryAllocator> allocatorResolver) => _inner.SetReceiveAllocatorResolver(allocatorResolver);

            public void OnStreamFailure() => _inner.OnStreamFailure();

            public void Initialize(
                Func<IReadOnlySet<int>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>> getDataFunction,
                Func<long, Task> callFailAndRecover,
                Func<long, long, bool, Task<SubstreamInitializeResponse>> initializeFromTarget,
                Func<long, long, bool, Task> callRecieveCheckpointDone)
            {
                _inner.Initialize(getDataFunction, callFailAndRecover, initializeFromTarget, callRecieveCheckpointDone);
            }

            public Task<IReadOnlyList<SubstreamEventData>> FetchData(IReadOnlySet<int> targetIds, int numberOfEvents, CancellationToken cancellationToken) => _inner.FetchData(targetIds, numberOfEvents, cancellationToken);

            public Task SendFailAndRecover(long restoreVersion) => _inner.SendFailAndRecover(restoreVersion);

            public Task<SubstreamInitializeResponse> SendInitializeRequest(long restoreVersion, long checkpointEpoch, bool cleanHandoff, CancellationToken cancellationToken) => _inner.SendInitializeRequest(restoreVersion, checkpointEpoch, cleanHandoff, cancellationToken);

            public Task SendCheckpointDone(long checkpointVersion, long targetCheckpointEpoch, bool coversPeerStopBarrier) => _inner.SendCheckpointDone(checkpointVersion, targetCheckpointEpoch, coversPeerStopBarrier);

            public void InitializeDurabilityClaims(Func<long, int, long, long, bool, Task> callReceiveDurabilityClaim) => _inner.InitializeDurabilityClaims(callReceiveDurabilityClaim);

            public async Task SendDurabilityClaim(long version, int radius, long senderCheckpointEpoch, long targetCheckpointEpoch, bool requestReply, CancellationToken cancellationToken)
            {
                if (_gate.Drop)
                {
                    return;
                }
                await _inner.SendDurabilityClaim(version, radius, senderCheckpointEpoch, targetCheckpointEpoch, requestReply, cancellationToken);
                _gate.Delivered(version);
            }
        }

        private sealed class RecordingFactory : ISubstreamCommunicationHandlerFactory
        {
            private readonly ISubstreamCommunicationHandlerFactory _inner;
            private readonly Func<ISubstreamCommunicationHandler, ISubstreamCommunicationHandler>? _wrap;

            public RecordingFactory(ISubstreamCommunicationHandlerFactory inner, Func<ISubstreamCommunicationHandler, ISubstreamCommunicationHandler>? wrap = null)
            {
                _inner = inner;
                _wrap = wrap;
            }

            public ConcurrentDictionary<string, ISubstreamCommunicationHandler> Handlers { get; } = new ConcurrentDictionary<string, ISubstreamCommunicationHandler>();

            public ISubstreamCommunicationHandler GetCommunicationHandler(string targetSubstreamName, string selfSubstreamName)
            {
                return Handlers.GetOrAdd(targetSubstreamName, _ =>
                {
                    var handler = _inner.GetCommunicationHandler(targetSubstreamName, selfSubstreamName);
                    return _wrap != null ? _wrap(handler) : handler;
                });
            }
        }

        /// <summary>
        /// Fires after a substream committed and sent its acknowledgements, the version is the one its commit started.
        /// </summary>
        private static ConcurrentDictionary<string, long> TrackDurable(string testName, ConcurrentDictionary<string, long> started)
        {
            var durable = new ConcurrentDictionary<string, long>();
            StreamContext.CheckpointPostCommitGapHookForTests = streamName =>
            {
                var substream = SubstreamOf(streamName, testName);
                if (substream != null && started.TryGetValue(substream, out var version))
                {
                    durable.AddOrUpdate(substream, version, (_, current) => Math.Max(current, version));
                }
                return Task.CompletedTask;
            };
            return durable;
        }

        /// <summary>
        /// The tail and its only direct peer are durable and acknowledged, nothing but the group agreement holds the tail.
        /// </summary>
        private static Task WaitForTailBlockedOnlyByTheGroup(ConcurrentDictionary<string, long> durable, long held)
        {
            return WaitUntil(
                () => durable.GetValueOrDefault("sub2", -1) >= held && durable.GetValueOrDefault("sub3", -1) >= held,
                "the middle and the tail to be durable at the held version");
        }

        private Base.Engine.DataflowStream BuildSubstream(
            string testName,
            string sql,
            string substreamName,
            LocalSubstreamCommunicationHub hub,
            ConcurrentDictionary<string, EventBatchData> latestData,
            ConcurrentBag<string> failures,
            Action<long>? onCommitVersion = null,
            ISubstreamCommunicationHandlerFactory? communicationFactory = null)
        {
            var connectorManager = new ConnectorManager();
            connectorManager.AddSource(new MockSourceFactory("*", _db, false));
            connectorManager.AddSink(new MockSinkFactory("*", data => latestData[substreamName] = data, 0, _ => { }, onCommitVersion: onCommitVersion));

            var builder = new FlowtideBuilder($"{testName.Length}_{testName}_{substreamName}")
                .AddPlan(CreatePlan(sql), false)
                .WithStateOptions(new Storage.StateManager.StateManagerOptions()
                {
                    CachePageCount = 100_000,
                    PersistentStorage = new ReservoirPersistentStorage(new Storage.Persistence.Reservoir.ReservoirStorageOptions()
                    {
                        FileProvider = new MemoryFileProvider()
                    }),
                    DefaultBPlusTreePageSize = 1024,
                    DefaultBPlusTreePageSizeBytes = 32 * 1024,
                    TemporaryStorageOptions = new Storage.FileCacheOptions()
                    {
                        DirectoryPath = $"./data/tempFiles/{testName}/{substreamName}/tmp"
                    }
                })
                .AddConnectorManager(connectorManager);
            builder.SetDistributedOptions(new DistributedOptions(substreamName, default, communicationFactory ?? hub.CreateFactory(substreamName)));
            builder.SetStopDrainTimeout(FastEngineTimings.StopDrainTimeout);
            builder.WithFailureListener(e => failures.Add($"{substreamName}: {e?.Message}"));

            var stream = builder.Build();
            lock (_streams)
            {
                _streams.Add(stream);
            }
            return stream;
        }

        private Plan CreatePlan(string sql)
        {
            // Building a stream mutates the plan, every substream gets its own instance.
            var sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.AddTableProvider(new DatasetTableProvider(_db));
            sqlPlanBuilder.Sql(sql);
            return sqlPlanBuilder.GetPlan();
        }

        /// <summary>
        /// Feeds data until every substream started a checkpoint above the given version.
        /// A checkpoint cannot start before the previous cycle completed, compaction included,
        /// so this proves every cycle up to that version completed.
        /// </summary>
        private async Task WaitForCompletedCycles(string[] substreams, ConcurrentDictionary<string, long> started, long throughVersion)
        {
            var sw = Stopwatch.StartNew();
            while (!substreams.All(s => started.GetValueOrDefault(s, -1) > throughVersion))
            {
                if (sw.Elapsed > TimeSpan.FromSeconds(60))
                {
                    var seen = string.Join(", ", substreams.Select(s => $"{s}={started.GetValueOrDefault(s, -1)}"));
                    throw new TimeoutException($"Checkpoints stalled, no cycle above version {throughVersion} started everywhere: {seen}.");
                }
                _generator.Generate(20);
                await Task.Delay(250);
            }
        }

        private static string? SubstreamOf(string streamName, string testName)
        {
            if (!streamName.Contains(testName))
            {
                return null;
            }
            var index = streamName.LastIndexOf('_');
            return index < 0 ? null : streamName.Substring(index + 1);
        }

        private static int RowCount(ConcurrentDictionary<string, EventBatchData> latestData, string substreamName)
        {
            try
            {
                return latestData.TryGetValue(substreamName, out var data) ? data.Count : -1;
            }
            catch (Exception e) when (e is NullReferenceException || e is ObjectDisposedException)
            {
                // The sink disposed the batch while it was read, the next poll sees its successor.
                return -1;
            }
        }

        private static async Task WaitUntil(Func<bool> condition, string what)
        {
            var sw = Stopwatch.StartNew();
            while (!condition())
            {
                if (sw.Elapsed > TimeSpan.FromSeconds(60))
                {
                    throw new TimeoutException($"Timed out waiting for {what}.");
                }
                await Task.Delay(25);
            }
        }

        private static async Task WaitForTask(Task task, string what)
        {
            if (await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(60))) != task)
            {
                throw new TimeoutException($"Timed out waiting for {what}.");
            }
        }
    }
}
