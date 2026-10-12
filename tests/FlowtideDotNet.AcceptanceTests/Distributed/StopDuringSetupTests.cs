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
using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Base.Engine.Internal.StateMachine;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Engine.Distributed;
using FlowtideDotNet.Core.Operators.Exchange;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.AcceptanceTests.Distributed
{
    [Collection("StreamContext test hooks")]
    public class StopDuringSetupTests : IAsyncLifetime
    {
        // Both substreams have a sink, so both are told to commit.
        private const string TwoSinksSql = @"
            SUBSTREAM sub1;

            CREATE VIEW read_users WITH (DISTRIBUTED = true, SCATTER_BY = userkey, PARTITION_COUNT = 2) AS
            SELECT userkey FROM users;

            INSERT INTO output SELECT userkey FROM read_users WITH (PARTITION_ID = 0);

            SUBSTREAM sub2;

            INSERT INTO output SELECT userkey FROM read_users WITH (PARTITION_ID = 1);
            ";

        private static readonly string[] Names = new[] { "sub1", "sub2" };

        private readonly MockDatabase _db;
        private readonly DatasetGenerator _generator;
        private readonly List<Base.Engine.DataflowStream> _streams = new List<Base.Engine.DataflowStream>();
        private readonly ConcurrentDictionary<string, RingBufferLoggerProvider> _logBuffers = new ConcurrentDictionary<string, RingBufferLoggerProvider>();
        private readonly CancellationTokenSource _tickCancellation = new CancellationTokenSource();
        private Task? _tickLoop;

        public StopDuringSetupTests()
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
            StreamContext.RestoreVersionForTests = null;
            StreamContext.StartupDuringBlockInitHookForTests = null;
            StreamContext.StartupBeforeInitTrackingHookForTests = null;
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
        /// A stop while sub1 starts ends that run: its next start handshakes again on a new epoch and the group commits together.
        /// </summary>
        [Theory]
        [InlineData(HoldPoint.SourceInitialize)]
        [InlineData(HoldPoint.BeforeBlockInitialize)]
        [InlineData(HoldPoint.BeforeInitEvents)]
        public async Task StopDuringTheStartHandshakesAgainOnTheNextStart(HoldPoint holdPoint)
        {
            var testName = holdPoint switch
            {
                HoldPoint.SourceInitialize => "stop_setup_source",
                HoldPoint.BeforeBlockInitialize => "stop_setup_blocks",
                _ => "stop_setup_init_events",
            };
            _generator.Generate(100);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var hub = new LocalSubstreamCommunicationHub();
            var failures = new ConcurrentBag<string>();
            var started = new ConcurrentDictionary<string, long>();
            var commits = new ConcurrentQueue<(string Substream, long Version, int Phase)>();
            var restores = new ConcurrentQueue<(string Substream, long Version)>();
            var violations = new ConcurrentQueue<string>();
            var sourceFailures = new ConcurrentQueue<long>();
            var phase = new PhaseMarker();

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

            var sub1Factory = new RecordingFactory(hub.CreateFactory("sub1"), phase);
            var sub2Factory = new RecordingFactory(hub.CreateFactory("sub2"), phase);
            var sub1Source = new HeldSourceFactory(_db, version => sourceFailures.Enqueue(version));
            Action<long> OnCommit(string substream) => version => commits.Enqueue((substream, version, phase.Value));
            var sub1Transitions = new FailureTransitionGate();
            var sub1 = BuildSubstream(testName, "sub1", sub1Factory, sub1Source, latestData, failures, OnCommit("sub1"), sub1Transitions);
            var sub2 = BuildSubstream(testName, "sub2", sub2Factory, new MockSourceFactory("*", _db, false), latestData, failures, OnCommit("sub2"));

            string outcome = "not run";
            var hold = new Hold();
            try
            {
                // Phase 0: the group runs and commits.
                await AwaitBounded(Task.WhenAll(sub1.StartAsync(), sub2.StartAsync()), "the first start");
                await WaitUntil(() => TotalRows(latestData) == _generator.Users.Count, "initial data in the sinks");
                await WaitForCompletedCycles(started, 2);
                await WaitUntil(() => Names.All(n => commits.Any(c => c.Substream == n && c.Version >= 2)), "both sinks to commit version 2");
                var committedBefore = commits.Max(c => c.Version);

                await CheckpointSettle.WaitForCheckpointsToSettle(sub1, sub2);
                await AwaitBounded(Task.WhenAll(sub1.StopAsync(), sub2.StopAsync()), "the coordinated stop");
                var epochBefore = sub1Factory.Handshakes.Where(h => h.Target == "sub2").Max(h => h.Epoch);

                // Phase 1: both start, sub1 is stopped at the hold point.
                phase.Value = 1;
                Func<string, Task> holdHook = async streamName =>
                {
                    if (SubstreamOf(streamName, testName) == "sub1" && Interlocked.Exchange(ref hold.Taken, 1) == 0)
                    {
                        hold.Reached.TrySetResult();
                        await hold.Release.Task;
                    }
                };
                switch (holdPoint)
                {
                    case HoldPoint.SourceInitialize:
                        sub1Source.Hold = hold;
                        break;
                    case HoldPoint.BeforeBlockInitialize:
                        StreamContext.StartupDuringBlockInitHookForTests = holdHook;
                        break;
                    default:
                        StreamContext.StartupBeforeInitTrackingHookForTests = holdHook;
                        break;
                }
                var sub2Start = sub2.StartAsync();
                var abortedStart = sub1.StartAsync();
                await WaitForTask(hold.Reached.Task, "sub1's setup to reach the hold");
                if (holdPoint == HoldPoint.BeforeBlockInitialize)
                {
                    Assert.DoesNotContain(sub1Factory.Handshakes, h => h.Phase == 1);
                }
                else
                {
                    // The exchange initializes first, its handshake is through.
                    Assert.Contains(sub1Factory.Handshakes, h => h.Phase == 1 && h.Target == "sub2" && h.Success);
                }
                if (holdPoint == HoldPoint.BeforeInitEvents)
                {
                    // Announced initialized, the run claims its version to sub2.
                    await WaitUntil(() => sub1Factory.Claims.Any(c => c.Phase == 1), "sub1's stopped run to send a durability claim");
                }
                var abortedEpoch = sub1Factory.Handshakes.Where(h => h.Target == "sub2").Max(h => h.Epoch);

                if (holdPoint == HoldPoint.BeforeInitEvents)
                {
                    // The teardown is held before it starts, so the aborted start reaches its blocks first.
                    sub1Transitions.Armed = 1;
                }
                var stop = sub1.StopAsync();
                if (holdPoint == HoldPoint.BeforeInitEvents)
                {
                    await WaitForTask(sub1Transitions.Blocked.Task, "sub1's transition into the failure teardown");
                    hold.Release.TrySetResult();
                    await AwaitBounded(abortedStart, "the aborted start to return");
                    sub1Transitions.Release.Set();
                }
                else
                {
                    await WaitUntil(() => sub1.State == StreamStateValue.Failure, "sub1 to leave the start for the failure teardown");
                    hold.Release.TrySetResult();
                }
                await AwaitBounded(stop, "the stop during setup");
                await AwaitBounded(abortedStart, "the aborted start to return");
                await WaitUntil(() => sub1.State == StreamStateValue.NotStarted, "sub1 to rest in not started");
                Assert.NotEmpty(_logBuffers["sub1"].LinesContaining("was superseded by a failure, abandoning it"));
                var claimsOfAbortedRun = sub1Factory.Claims.Where(c => c.Phase == 1).ToList();
                if (holdPoint != HoldPoint.BeforeInitEvents)
                {
                    // Claims start at AnnounceInitialized, no late claim of this run can reach sub2.
                    Assert.Empty(claimsOfAbortedRun);
                }
                StreamContext.StartupDuringBlockInitHookForTests = null;
                StreamContext.StartupBeforeInitTrackingHookForTests = null;

                // Phase 2: the same stream object starts again.
                phase.Value = 2;
                var restart = sub1.StartAsync();
                string Describe() =>
                    $"holdPoint={holdPoint}; states sub1={sub1.State} sub2={sub2.State}; epoch before stop={epochBefore}; aborted run epoch={abortedEpoch}; " +
                    $"sub1 handshakes=[{string.Join(", ", sub1Factory.Handshakes.Select(h => h.ToString()))}]; " +
                    $"sub1 handler OnStreamFailure=[{string.Join(",", sub1Factory.StreamFailures)}]; source OnFailure=[{string.Join(",", sourceFailures)}]; " +
                    $"claims sent by the aborted run=[{string.Join(", ", claimsOfAbortedRun.Select(c => c.ToString()))}]; " +
                    $"restores=[{string.Join(",", restores.Select(r => $"{r.Substream}:{r.Version}"))}]; commits=[{string.Join(",", commits.Select(c => $"{c.Substream}:{c.Version}@{c.Phase}"))}]; " +
                    $"failures=[{string.Join(" | ", failures)}]";

                var rehandshake = await WaitFor(() => sub1Factory.Handshakes.Any(h => h.Phase == 2 && h.Target == "sub2" && h.Success), TimeSpan.FromSeconds(15));
                // Collected, the checks below still run and tell what the missing handshake costs.
                var problems = new List<string>();
                if (!rehandshake)
                {
                    problems.Add("sub1's restart sent no initialize handshake to sub2");
                }
                else
                {
                    var restartEpoch = sub1Factory.Handshakes.Where(h => h.Phase == 2 && h.Target == "sub2").Min(h => h.Epoch);
                    if (restartEpoch <= abortedEpoch)
                    {
                        problems.Add($"sub1's restart announced epoch {restartEpoch}, not above the aborted run's {abortedEpoch}");
                    }
                }

                // The group converges and both sinks are told to commit the same version again.
                await AwaitBounded(restart, "the restart");
                await AwaitBounded(sub2Start, "sub2's start");
                await WaitUntil(() => sub1.State == StreamStateValue.Running && sub2.State == StreamStateValue.Running, "both substreams to run");
                _generator.Generate(50);
                await WaitUntil(() => TotalRows(latestData) == _generator.Users.Count, "the new rows in the sinks");
                var baseline = started.Values.Max();
                await WaitForCompletedCycles(started, baseline + 1);
                await WaitUntil(() => CommonVersionAfterRestart(commits) >= committedBefore, "both sinks to commit one version after the restart");
                outcome = Describe();
                Assert.True(violations.IsEmpty, $"{string.Join("; ", violations)}: {outcome}");

                if (holdPoint == HoldPoint.BeforeInitEvents)
                {
                    // A claim of the stopped run delivered late, after the restart.
                    var stale = claimsOfAbortedRun.Last();
                    var dropLine = $"its epochs {stale.SenderEpoch}/{stale.TargetEpoch} do not match";
                    var dropsBefore = _logBuffers["sub2"].LinesContaining(dropLine).Count;
                    await ((RecordingHandler)sub1Factory.Handlers["sub2"]).Replay(stale);
                    var dropped = await WaitFor(() => _logBuffers["sub2"].LinesContaining(dropLine).Count > dropsBefore, TimeSpan.FromSeconds(5));
                    if (!dropped)
                    {
                        problems.Add($"sub2 accepted the durability claim {stale} of sub1's stopped run after the restart");
                    }
                }
                outcome = Describe();
                Assert.True(problems.Count == 0, $"{string.Join("; ", problems)}: {outcome}");

                await CheckpointSettle.WaitForCheckpointsToSettle(sub1, sub2);
                await AwaitBounded(Task.WhenAll(sub1.StopAsync(), sub2.StopAsync()), "the final stop");
            }
            catch (Exception e)
            {
                outcome = $"{e.Message}; {outcome}";
                DumpLogBuffers(testName, outcome);
                throw;
            }
            finally
            {
                hold.Release.TrySetResult();
                sub1Transitions.Release.Set();
                StreamContext.StartupDuringBlockInitHookForTests = null;
                StreamContext.StartupBeforeInitTrackingHookForTests = null;
            }
        }

        public enum HoldPoint
        {
            // Inside the source's initialize, after the exchange handshook the peer.
            SourceInitialize,
            // After the blocks are created, before any of them initializes.
            BeforeBlockInitialize,
            // Blocks initialized and announced, before the init watermarks are sent.
            BeforeInitEvents,
        }

        private static long CommonVersionAfterRestart(ConcurrentQueue<(string Substream, long Version, int Phase)> commits)
        {
            var sub1 = commits.Where(c => c.Substream == "sub1" && c.Phase == 2).Select(c => c.Version).ToHashSet();
            return commits.Where(c => c.Substream == "sub2" && c.Phase == 2 && sub1.Contains(c.Version)).Select(c => c.Version).DefaultIfEmpty(-1).Max();
        }

        private void DumpLogBuffers(string testName, string outcome)
        {
            var directory = Path.GetFullPath($"./debugwrite/{testName}_{DateTime.UtcNow:HHmmssfff}");
            Directory.CreateDirectory(directory);
            File.WriteAllText(Path.Combine(directory, "outcome.txt"), outcome);
            foreach (var buffer in _logBuffers)
            {
                buffer.Value.WriteToFile(Path.Combine(directory, $"{buffer.Key}.log"));
            }
            Console.WriteLine($"Logs written to {directory}: {outcome}");
        }

        private Base.Engine.DataflowStream BuildSubstream(
            string testName,
            string substreamName,
            ISubstreamCommunicationHandlerFactory communicationFactory,
            MockSourceFactory sourceFactory,
            ConcurrentDictionary<string, EventBatchData> latestData,
            ConcurrentBag<string> failures,
            Action<long> onCommitVersion,
            IStreamStateChangeListener? stateChangeListener = null)
        {
            var connectorManager = new ConnectorManager();
            connectorManager.AddSource(sourceFactory);
            connectorManager.AddSink(new MockSinkFactory("*", data => latestData[substreamName] = data, 0, _ => { }, onCommitVersion: onCommitVersion));

            var logProvider = _logBuffers.GetOrAdd(substreamName, _ => new RingBufferLoggerProvider());
            var builder = new FlowtideBuilder($"{testName.Length}_{testName}_{substreamName}")
                .AddPlan(CreatePlan(), false)
                .WithStateOptions(new StateManagerOptions()
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
            builder.WithLoggerFactory(LoggerFactory.Create(b =>
            {
                b.SetMinimumLevel(LogLevel.Debug);
                b.AddProvider(logProvider);
            }));
            builder.SetDistributedOptions(new DistributedOptions(substreamName, default, communicationFactory));
            builder.SetStopDrainTimeout(FastEngineTimings.StopDrainTimeout);
            builder.WithFailureListener(e => failures.Add($"{substreamName}: {e?.Message}"));
            if (stateChangeListener != null)
            {
                builder.WithStateChangeListener(stateChangeListener);
            }

            var stream = builder.Build();
            lock (_streams)
            {
                _streams.Add(stream);
            }
            return stream;
        }

        private Plan CreatePlan()
        {
            var sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.AddTableProvider(new DatasetTableProvider(_db));
            sqlPlanBuilder.Sql(TwoSinksSql);
            return sqlPlanBuilder.GetPlan();
        }

        private async Task WaitForCompletedCycles(ConcurrentDictionary<string, long> started, long throughVersion)
        {
            var sw = Stopwatch.StartNew();
            while (!Names.All(s => started.GetValueOrDefault(s, -1) > throughVersion))
            {
                if (sw.Elapsed > TimeSpan.FromSeconds(60))
                {
                    var seen = string.Join(", ", Names.Select(s => $"{s}={started.GetValueOrDefault(s, -1)}"));
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

        private static int TotalRows(ConcurrentDictionary<string, EventBatchData> latestData)
        {
            try
            {
                return Names.Sum(n => latestData.TryGetValue(n, out var data) ? data.Count : 0);
            }
            catch (Exception e) when (e is NullReferenceException || e is ObjectDisposedException)
            {
                // The sink disposed the batch while it was read, the next poll sees its successor.
                return -1;
            }
        }

        private static async Task<bool> WaitFor(Func<bool> condition, TimeSpan timeout)
        {
            var sw = Stopwatch.StartNew();
            while (!condition())
            {
                if (sw.Elapsed > timeout)
                {
                    return false;
                }
                await Task.Delay(25);
            }
            return true;
        }

        private static async Task WaitUntil(Func<bool> condition, string what)
        {
            if (!await WaitFor(condition, TimeSpan.FromSeconds(60)))
            {
                throw new TimeoutException($"Timed out waiting for {what}.");
            }
        }

        private static async Task WaitForTask(Task task, string what)
        {
            if (await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(60))) != task)
            {
                throw new TimeoutException($"Timed out waiting for {what}.");
            }
        }

        private static async Task AwaitBounded(Task task, string what)
        {
            await WaitForTask(task, what);
            await task;
        }

        private sealed class PhaseMarker
        {
            public volatile int Value;
        }

        private sealed class Hold
        {
            public int Taken;
            public TaskCompletionSource Reached { get; } = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            public TaskCompletionSource Release { get; } = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        private sealed class FailureTransitionGate : IStreamStateChangeListener
        {
            public int Armed;
            public TaskCompletionSource Blocked { get; } = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            public ManualResetEventSlim Release { get; } = new ManualResetEventSlim(false);

            public void OnStreamStateChange(StreamStateChangeNotification notification)
            {
                if (notification.State == StreamStateValue.Failure && Interlocked.Exchange(ref Armed, 0) == 1)
                {
                    // Called before the failure state starts its teardown.
                    Blocked.TrySetResult();
                    Release.Wait(TimeSpan.FromSeconds(60));
                }
            }
        }

        private sealed class HeldSourceFactory : MockSourceFactory
        {
            private readonly MockDatabase _db;
            private readonly Action<long> _onFailure;

            public HeldSourceFactory(MockDatabase db, Action<long> onFailure) : base("*", db, false)
            {
                _db = db;
                _onFailure = onFailure;
            }

            public Hold? Hold { get; set; }

            public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
            {
                return new HeldSourceOperator(this, readRelation, _db, dataflowBlockOptions, _onFailure);
            }

            internal Hold? TakeHold()
            {
                var hold = Hold;
                if (hold == null || Interlocked.Exchange(ref hold.Taken, 1) != 0)
                {
                    return null;
                }
                return hold;
            }
        }

        private sealed class HeldSourceOperator : MockDataSourceOperator
        {
            private readonly HeldSourceFactory _factory;

            public HeldSourceOperator(HeldSourceFactory factory, ReadRelation readRelation, MockDatabase db, DataflowBlockOptions options, Action<long> onFailure)
                : base(readRelation, db, options, onFailure: onFailure)
            {
                _factory = factory;
            }

            protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
            {
                var hold = _factory.TakeHold();
                if (hold != null)
                {
                    hold.Reached.TrySetResult();
                    await hold.Release.Task;
                }
                await base.InitializeOrRestore(restoreTime, stateManagerClient);
            }
        }

        private sealed record HandshakeRecord(int Phase, string Target, long Epoch, RecoveryWave Wave, bool Success)
        {
            public override string ToString() => $"{Target}@phase{Phase}/epoch={Epoch}/wave={Wave}/success={Success}";
        }

        private sealed record ClaimRecord(int Phase, string Target, long Version, int Radius, long InitVersion, RecoveryWave Wave, long SenderEpoch, long TargetEpoch)
        {
            public override string ToString() => $"{Target}@phase{Phase}/v={Version}/r={Radius}/init={InitVersion}/epochs={SenderEpoch}->{TargetEpoch}";
        }

        private sealed class RecordingFactory : ISubstreamCommunicationHandlerFactory
        {
            private readonly ISubstreamCommunicationHandlerFactory _inner;

            public RecordingFactory(ISubstreamCommunicationHandlerFactory inner, PhaseMarker phase)
            {
                _inner = inner;
                Phase = phase;
            }

            public PhaseMarker Phase { get; }

            public ConcurrentDictionary<string, ISubstreamCommunicationHandler> Handlers { get; } = new ConcurrentDictionary<string, ISubstreamCommunicationHandler>();

            public ConcurrentQueue<HandshakeRecord> Handshakes { get; } = new ConcurrentQueue<HandshakeRecord>();

            public ConcurrentQueue<ClaimRecord> Claims { get; } = new ConcurrentQueue<ClaimRecord>();

            public ConcurrentQueue<string> StreamFailures { get; } = new ConcurrentQueue<string>();

            public ISubstreamCommunicationHandler GetCommunicationHandler(string targetSubstreamName, string selfSubstreamName)
            {
                return Handlers.GetOrAdd(targetSubstreamName, _ => new RecordingHandler(this, _inner.GetCommunicationHandler(targetSubstreamName, selfSubstreamName), targetSubstreamName));
            }
        }

        private sealed class RecordingHandler : ISubstreamCommunicationHandler
        {
            private readonly RecordingFactory _factory;
            private readonly ISubstreamCommunicationHandler _inner;
            private readonly string _target;

            public RecordingHandler(RecordingFactory factory, ISubstreamCommunicationHandler inner, string target)
            {
                _factory = factory;
                _inner = inner;
                _target = target;
            }

            public void SetReceiveAllocatorResolver(Func<int, IMemoryAllocator> allocatorResolver)
            {
                _inner.SetReceiveAllocatorResolver(allocatorResolver);
            }

            public void OnStreamFailure()
            {
                _factory.StreamFailures.Enqueue($"{_target}@phase{_factory.Phase.Value}");
                _inner.OnStreamFailure();
            }

            public void Initialize(
                Func<IReadOnlySet<int>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>> getDataFunction,
                Func<RecoveryWave, Task> callFailAndRecover,
                Func<long, long, bool, RecoveryWave, Task<SubstreamInitializeResponse>> initializeFromTarget,
                Func<long, long, bool, Task> callRecieveCheckpointDone)
            {
                _inner.Initialize(getDataFunction, callFailAndRecover, initializeFromTarget, callRecieveCheckpointDone);
            }

            public Task<IReadOnlyList<SubstreamEventData>> FetchData(IReadOnlySet<int> targetIds, int numberOfEvents, CancellationToken cancellationToken)
            {
                return _inner.FetchData(targetIds, numberOfEvents, cancellationToken);
            }

            public Task SendFailAndRecover(RecoveryWave wave)
            {
                return _inner.SendFailAndRecover(wave);
            }

            public async Task<SubstreamInitializeResponse> SendInitializeRequest(long restoreVersion, long checkpointEpoch, bool cleanHandoff, RecoveryWave wave, CancellationToken cancellationToken)
            {
                var phase = _factory.Phase.Value;
                var response = await _inner.SendInitializeRequest(restoreVersion, checkpointEpoch, cleanHandoff, wave, cancellationToken);
                _factory.Handshakes.Enqueue(new HandshakeRecord(phase, _target, checkpointEpoch, wave, response.Success));
                return response;
            }

            public Task SendCheckpointDone(long checkpointVersion, long targetCheckpointEpoch, bool coversPeerStopBarrier)
            {
                return _inner.SendCheckpointDone(checkpointVersion, targetCheckpointEpoch, coversPeerStopBarrier);
            }

            public void InitializeDurabilityClaims(Func<long, int, long, RecoveryWave, long, long, bool, Task> callReceiveDurabilityClaim)
            {
                _inner.InitializeDurabilityClaims(callReceiveDurabilityClaim);
            }

            public Task SendDurabilityClaim(long version, int radius, long initVersion, RecoveryWave wave, long senderCheckpointEpoch, long targetCheckpointEpoch, bool requestReply, CancellationToken cancellationToken)
            {
                _factory.Claims.Enqueue(new ClaimRecord(_factory.Phase.Value, _target, version, radius, initVersion, wave, senderCheckpointEpoch, targetCheckpointEpoch));
                return _inner.SendDurabilityClaim(version, radius, initVersion, wave, senderCheckpointEpoch, targetCheckpointEpoch, requestReply, cancellationToken);
            }

            public Task Replay(ClaimRecord claim)
            {
                return _inner.SendDurabilityClaim(claim.Version, claim.Radius, claim.InitVersion, claim.Wave, claim.SenderEpoch, claim.TargetEpoch, false, CancellationToken.None);
            }
        }
    }
}
