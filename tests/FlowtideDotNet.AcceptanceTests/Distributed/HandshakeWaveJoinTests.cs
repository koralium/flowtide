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
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Engine.Distributed;
using FlowtideDotNet.Core.Operators.Exchange;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Sql;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Diagnostics;
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests.Distributed
{
    [Collection("StreamContext test hooks")]
    public class HandshakeWaveJoinTests : IAsyncLifetime
    {
        // Every substream has a sink so each one's CommitVersion calls can be observed.
        private const string ChainSql = @"
            SUBSTREAM sub1;

            CREATE VIEW v1 WITH (DISTRIBUTED = true, SCATTER_BY = userkey, PARTITION_COUNT = 1) AS
            SELECT userkey FROM users;

            INSERT INTO output1 SELECT userkey FROM users;

            SUBSTREAM sub2;

            CREATE VIEW v2 WITH (DISTRIBUTED = true, SCATTER_BY = userkey, PARTITION_COUNT = 1) AS
            SELECT userkey FROM v1 WITH (PARTITION_ID = 0);

            INSERT INTO output2 SELECT userkey FROM users;

            SUBSTREAM sub3;

            INSERT INTO output SELECT userkey FROM v2 WITH (PARTITION_ID = 0);
            ";

        private const string TwoSubstreamSql = @"
            SUBSTREAM sub1;

            CREATE VIEW v1 WITH (DISTRIBUTED = true, SCATTER_BY = userkey, PARTITION_COUNT = 1) AS
            SELECT userkey FROM users;

            INSERT INTO output1 SELECT userkey FROM users;

            SUBSTREAM sub2;

            INSERT INTO output SELECT userkey FROM v1 WITH (PARTITION_ID = 0);
            ";

        private readonly ITestOutputHelper _output;
        private readonly MockDatabase _db;
        private readonly DatasetGenerator _generator;
        private readonly List<Base.Engine.DataflowStream> _streams = new List<Base.Engine.DataflowStream>();
        private readonly ConcurrentDictionary<string, RingBufferLoggerProvider> _logBuffers = new ConcurrentDictionary<string, RingBufferLoggerProvider>();
        private readonly CancellationTokenSource _tickCancellation = new CancellationTokenSource();
        private Task? _tickLoop;

        public HandshakeWaveJoinTests(ITestOutputHelper output)
        {
            _output = output;
            FastEngineTimings.Apply();
            _db = new MockDatabase();
            _generator = new DatasetGenerator(_db);
        }

        public Task InitializeAsync()
        {
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
        /// sub2 joins sub1's later wave through its handshake, sub3 must be told to follow it.
        /// </summary>
        [Fact]
        public async Task WaveJoinedThroughTheHandshakeReachesTheOtherPeers()
        {
            await RunWaveJoin("wave_join_chain", ChainSql, new[] { "sub1", "sub2", "sub3" }, announceJoinedWave: false, expectTeardownAnnouncesWave: true);
        }

        /// <summary>
        /// Control: the same interleaving converges once sub3 hears of the wave the way a failure teardown tells it.
        /// </summary>
        [Fact]
        public async Task WaveJoinConvergesWhenTheWaveIsAnnouncedToTheOtherPeers()
        {
            await RunWaveJoin("wave_join_announced", ChainSql, new[] { "sub1", "sub2", "sub3" }, announceJoinedWave: true, expectTeardownAnnouncesWave: false);
        }

        /// <summary>
        /// Two substreams: the joining one has no other peer that could be left behind.
        /// </summary>
        [Fact]
        public async Task WaveJoinBetweenTwoSubstreamsConverges()
        {
            await RunWaveJoin("wave_join_pair", TwoSubstreamSql, new[] { "sub1", "sub2" }, announceJoinedWave: false, expectTeardownAnnouncesWave: false);
        }

        private async Task RunWaveJoin(string testName, string sql, string[] names, bool announceJoinedWave, bool expectTeardownAnnouncesWave)
        {
            _generator.Generate(100);
            var tail = names.Last();
            bool chain = names.Length == 3;

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var hub = new LocalSubstreamCommunicationHub();
            var failures = new ConcurrentBag<string>();
            var started = new ConcurrentDictionary<string, long>();
            var commits = new ConcurrentQueue<(string Substream, long Version)>();

            StreamContext.CheckpointCommitHookForTests = (streamName, lastVersion) =>
            {
                var substream = SubstreamOf(streamName, testName);
                if (substream != null)
                {
                    started.AddOrUpdate(substream, lastVersion + 1, (_, current) => Math.Max(current, lastVersion + 1));
                }
                return Task.CompletedTask;
            };

            var factories = names.ToDictionary(n => n, n => new ScriptedFactory(hub.CreateFactory(n)));
            var sub1Factory = factories["sub1"];
            var sub2Factory = factories["sub2"];
            // sub1's first handshake is lost, sub1 fails in its start and restarts in a new wave.
            sub1Factory.ThrowOnFirstHandshake = true;
            // sub1's handshake towards sub2 is held until sub2 handshook sub1 itself.
            sub1Factory.HoldHandshakes = true;

            var streams = new List<Base.Engine.DataflowStream>();
            RecoveryWave? sub1Wave = null;
            bool running = false;
            bool passed = false;
            var sw = Stopwatch.StartNew();
            string Outcome() =>
                $"running={running} after {sw.ElapsedMilliseconds} ms; states {string.Join(" ", names.Zip(streams, (n, s) => $"{n}={s.State}"))}; " +
                $"{tail} rows={RowCount(latestData, tail)}/{_generator.Users.Count}; sub1 wave={sub1Wave}; " +
                $"sub2 handshakes={string.Join(",", sub2Factory.Requests.Select(Describe))}; " +
                $"sub2 handler OnStreamFailure calls=[{string.Join(",", sub2Factory.StreamFailures)}]; " +
                (chain ? $"sub3 handshakes={string.Join(",", factories["sub3"].Requests.Select(Describe))}; fail-and-recover received by sub3=[{string.Join(",", factories["sub3"].ReceivedFailAndRecover)}]; " : "") +
                $"started={string.Join(",", started.Select(s => $"{s.Key}={s.Value}"))}; commits={string.Join(",", commits.Select(c => $"{c.Substream}:{c.Version}"))}; " +
                $"failures={string.Join(" | ", failures)}";
            try
            {
                var sub1 = BuildSubstream(testName, sql, "sub1", sub1Factory, latestData, failures, commits);
                streams.Add(sub1);
                await sub1.StartAsync();
                await WaitUntil(() => sub1Factory.Requests.Any(r => r.Target == "sub2" && r.Wave.Counter > 0), "sub1 to handshake again in a new wave");

                var sub2 = BuildSubstream(testName, sql, "sub2", sub2Factory, latestData, failures, commits);
                streams.Add(sub2);
                if (chain)
                {
                    // sub3 handshakes sub2 while both are in the first wave.
                    var sub3 = BuildSubstream(testName, sql, "sub3", factories["sub3"], latestData, failures, commits);
                    streams.Add(sub3);
                    await sub3.StartAsync();
                    await WaitUntil(() => factories["sub3"].Requests.Any(r => r.Target == "sub2" && r.Response != null && r.Response.Success), "sub3 to handshake sub2");
                }

                await sub2.StartAsync();
                await WaitUntil(() => _logBuffers["sub2"].LinesContaining("Substream sub1 starts in wave").Count > 0, "sub2 to join sub1's wave through the handshake");
                var joinedWave = sub1Factory.Requests.Last(r => r.Target == "sub2").Wave;
                sub1Wave = joinedWave;
                if (announceJoinedWave)
                {
                    // What sub2's failure teardown would tell its other peer.
                    await sub2Factory.Handlers["sub3"].SendFailAndRecover(joinedWave);
                }
                if (expectTeardownAnnouncesWave)
                {
                    // The joining run's teardown fails its handlers and tells sub3 of the wave, while sub1 is still held.
                    await WaitUntil(() => sub2Factory.StreamFailures.Contains("sub1") && sub2Factory.StreamFailures.Contains("sub3"), "sub2's handlers to get OnStreamFailure", TimeSpan.FromSeconds(15), Outcome);
                    await WaitUntil(() => factories["sub3"].ReceivedFailAndRecover.Contains($"sub2:{joinedWave}"), "sub3 to receive fail-and-recover in sub1's wave", TimeSpan.FromSeconds(15), Outcome);
                }

                // sub2's restart passes its exchange initialize before sub1's handshake arrives.
                await Task.Delay(1000);
                sub1Factory.HoldHandshakes = false;

                sw.Restart();
                while (sw.Elapsed < TimeSpan.FromSeconds(30))
                {
                    if (streams.All(s => s.State == StreamStateValue.Running) && RowCount(latestData, tail) == _generator.Users.Count)
                    {
                        running = true;
                        break;
                    }
                    await Task.Delay(50);
                }
                Assert.True(running, Outcome());

                // Progress after the join: cycles complete on every substream.
                var baseline = started.Values.DefaultIfEmpty(0).Max();
                await WaitForCompletedCycles(names, started, baseline + 1);
                await WaitUntil(() => RowCount(latestData, tail) == _generator.Users.Count, "the later rows in the tail sink", TimeSpan.FromSeconds(60), Outcome);

                // Every substream's sink gets CommitVersion for one same version.
                await WaitUntil(() => CommonCommittedVersion(commits, names).HasValue, "one CommitVersion on every substream's sink", TimeSpan.FromSeconds(60), Outcome);
                Assert.True(CommonCommittedVersion(commits, names) >= 1, Outcome());
                passed = true;
                _output.WriteLine($"Common CommitVersion {CommonCommittedVersion(commits, names)}; {Outcome()}");
            }
            finally
            {
                sub1Factory.HoldHandshakes = false;
                if (!passed)
                {
                    DumpLogBuffers(testName, Outcome());
                }
            }
        }

        private static long? CommonCommittedVersion(ConcurrentQueue<(string Substream, long Version)> commits, string[] names)
        {
            var snapshot = commits.ToList();
            var common = names
                .Select(n => snapshot.Where(c => c.Substream == n).Select(c => c.Version).ToHashSet())
                .Aggregate((a, b) => { a.IntersectWith(b); return a; });
            return common.Count == 0 ? null : common.Max();
        }

        private static string Describe(HandshakeRecord record)
        {
            return $"{record.Target}@{record.Wave}->{(record.Response == null ? "none" : $"{record.Response.Wave}/inInit={record.Response.PeerInInit}/success={record.Response.Success}")}";
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
            string sql,
            string substreamName,
            ISubstreamCommunicationHandlerFactory communicationFactory,
            ConcurrentDictionary<string, EventBatchData> latestData,
            ConcurrentBag<string> failures,
            ConcurrentQueue<(string Substream, long Version)> commits)
        {
            var connectorManager = new ConnectorManager();
            connectorManager.AddSource(new MockSourceFactory("*", _db, false));
            connectorManager.AddSink(new MockSinkFactory("*", data => latestData[substreamName] = data, 0, _ => { }, onCommitVersion: version => commits.Enqueue((substreamName, version))));

            var logProvider = _logBuffers.GetOrAdd(substreamName, _ => new RingBufferLoggerProvider());
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
            builder.WithLoggerFactory(LoggerFactory.Create(b =>
            {
                b.SetMinimumLevel(LogLevel.Debug);
                b.AddProvider(logProvider);
            }));
            builder.SetDistributedOptions(new DistributedOptions(substreamName, default, communicationFactory));
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
            var sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.AddTableProvider(new DatasetTableProvider(_db));
            sqlPlanBuilder.Sql(sql);
            return sqlPlanBuilder.GetPlan();
        }

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
                return -1;
            }
        }

        private static async Task WaitUntil(Func<bool> condition, string what, TimeSpan? timeout = null, Func<string>? describe = null)
        {
            var sw = Stopwatch.StartNew();
            while (!condition())
            {
                if (sw.Elapsed > (timeout ?? TimeSpan.FromSeconds(60)))
                {
                    throw new TimeoutException($"Timed out waiting for {what}.{(describe == null ? "" : $" {describe()}")}");
                }
                await Task.Delay(25);
            }
        }

        private sealed record HandshakeRecord(string Target, RecoveryWave Wave, long Epoch, SubstreamInitializeResponse? Response);

        private sealed class ScriptedFactory : ISubstreamCommunicationHandlerFactory
        {
            private readonly ISubstreamCommunicationHandlerFactory _inner;
            private int _thrown;

            public ScriptedFactory(ISubstreamCommunicationHandlerFactory inner)
            {
                _inner = inner;
            }

            public bool ThrowOnFirstHandshake { get; set; }

            public volatile bool HoldHandshakes;

            public ConcurrentDictionary<string, ISubstreamCommunicationHandler> Handlers { get; } = new ConcurrentDictionary<string, ISubstreamCommunicationHandler>();

            public ConcurrentQueue<HandshakeRecord> Requests { get; } = new ConcurrentQueue<HandshakeRecord>();

            public ConcurrentQueue<string> StreamFailures { get; } = new ConcurrentQueue<string>();

            public ConcurrentQueue<string> ReceivedFailAndRecover { get; } = new ConcurrentQueue<string>();

            public ISubstreamCommunicationHandler GetCommunicationHandler(string targetSubstreamName, string selfSubstreamName)
            {
                return Handlers.GetOrAdd(targetSubstreamName, _ => new ScriptedHandler(this, _inner.GetCommunicationHandler(targetSubstreamName, selfSubstreamName), targetSubstreamName));
            }

            internal bool ShouldThrow()
            {
                return ThrowOnFirstHandshake && Interlocked.Exchange(ref _thrown, 1) == 0;
            }
        }

        private sealed class ScriptedHandler : ISubstreamCommunicationHandler
        {
            private readonly ScriptedFactory _factory;
            private readonly ISubstreamCommunicationHandler _inner;
            private readonly string _target;

            public ScriptedHandler(ScriptedFactory factory, ISubstreamCommunicationHandler inner, string target)
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
                _factory.StreamFailures.Enqueue(_target);
                _inner.OnStreamFailure();
            }

            public void Initialize(
                Func<IReadOnlySet<int>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>> getDataFunction,
                Func<RecoveryWave, Task> callFailAndRecover,
                Func<long, long, bool, RecoveryWave, Task<SubstreamInitializeResponse>> initializeFromTarget,
                Func<long, long, bool, Task> callRecieveCheckpointDone)
            {
                _inner.Initialize(getDataFunction, wave =>
                {
                    _factory.ReceivedFailAndRecover.Enqueue($"{_target}:{wave}");
                    return callFailAndRecover(wave);
                }, initializeFromTarget, callRecieveCheckpointDone);
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
                if (_factory.ShouldThrow())
                {
                    _factory.Requests.Enqueue(new HandshakeRecord(_target, wave, checkpointEpoch, null));
                    throw new InvalidOperationException("simulated lost initialize request");
                }
                if (_factory.HoldHandshakes)
                {
                    // Answered as draining: the requester waits without spending its retry budget.
                    _factory.Requests.Enqueue(new HandshakeRecord(_target, wave, checkpointEpoch, null));
                    return new SubstreamInitializeResponse(true, false, restoreVersion, peerDraining: true);
                }
                var response = await _inner.SendInitializeRequest(restoreVersion, checkpointEpoch, cleanHandoff, wave, cancellationToken);
                _factory.Requests.Enqueue(new HandshakeRecord(_target, wave, checkpointEpoch, response));
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
                return _inner.SendDurabilityClaim(version, radius, initVersion, wave, senderCheckpointEpoch, targetCheckpointEpoch, requestReply, cancellationToken);
            }
        }
    }
}
