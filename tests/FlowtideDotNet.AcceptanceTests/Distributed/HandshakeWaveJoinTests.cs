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
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Sql;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
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

        // sub2 reads v2 first: the target of the exchange that initializes second has the lower id.
        private const string TwoExchangeSql = @"
            SUBSTREAM sub1;

            CREATE VIEW v1 WITH (DISTRIBUTED = true, SCATTER_BY = userkey, PARTITION_COUNT = 1) AS
            SELECT userkey FROM users;

            CREATE VIEW v2 WITH (DISTRIBUTED = true, SCATTER_BY = userkey, PARTITION_COUNT = 1) AS
            SELECT userkey FROM users;

            INSERT INTO output1 SELECT userkey FROM users;

            SUBSTREAM sub2;

            INSERT INTO output
            SELECT userkey FROM v2 WITH (PARTITION_ID = 0)
            UNION ALL
            SELECT userkey FROM v1 WITH (PARTITION_ID = 0);
            ";

        // One exchange, target 0 to sub2 and target 1 to sub3.
        private const string TwoTargetSql = @"
            SUBSTREAM sub1;

            CREATE VIEW v1 WITH (DISTRIBUTED = true, SCATTER_BY = userkey, PARTITION_COUNT = 2) AS
            SELECT userkey FROM users;

            INSERT INTO output1 SELECT userkey FROM users;

            SUBSTREAM sub2;

            INSERT INTO output SELECT userkey FROM v1 WITH (PARTITION_ID = 0);

            SUBSTREAM sub3;

            INSERT INTO output SELECT userkey FROM v1 WITH (PARTITION_ID = 1);
            ";

        // Above every wave the runs mint themselves.
        private static readonly RecoveryWave PeerWave = new RecoveryWave(1000, Guid.NewGuid());

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

        /// <summary>
        /// sub2 and sub3 come back fresh beside a running sub1: the wave sub2 mints on sub1's answer must reach sub3 too.
        /// </summary>
        [Fact]
        public async Task WaveMintedBesideARunningPeerReachesTheOtherPeers()
        {
            const string testName = "mint_beside_running";
            _generator.Generate(100);
            var names = new[] { "sub1", "sub2", "sub3" };
            var hub = new LocalSubstreamCommunicationHub();
            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<string>();
            var commits = new ConcurrentQueue<(string Substream, long Version)>();
            var factories = names.ToDictionary(n => n, n => new ScriptedFactory(hub.CreateFactory(n)));
            var streams = names.ToDictionary(n => n, n => BuildSubstream(testName, ChainSql, n, factories[n], latestData, failures, commits));
            int commitsAtEvent = 0;
            string Outcome() =>
                $"states {string.Join(" ", streams.Select(s => $"{s.Key}={s.Value.State}"))}; " +
                string.Join("; ", factories.Select(f => $"{f.Key} handshakes={string.Join(",", f.Value.Requests.Select(Describe))} claims sent={string.Join(",", f.Value.ClaimWaves.Keys)} fail-and-recover received=[{string.Join(",", f.Value.ReceivedFailAndRecover)}]")) +
                $"; commits after the event={string.Join(",", commits.Skip(commitsAtEvent).Select(c => $"{c.Substream}:{c.Version}"))}; failures={string.Join(" | ", failures)}";
            bool Running() => streams.Values.All(s => s.State == StreamStateValue.Running) && RowCount(latestData, "sub2") + RowCount(latestData, "sub3") == 2 * _generator.Users.Count;
            bool Handshook(string self, string target) => factories[self].Requests.Any(r => r.Target == target && r.Response is { Success: true });
            try
            {
                foreach (var stream in streams.Values)
                {
                    await stream.StartAsync();
                }
                await WaitUntil(Running, "the group's first run", TimeSpan.FromSeconds(30), Outcome);
                commitsAtEvent = commits.Count;

                // sub2 and sub3 come back as fresh stream objects in no wave, sub1 keeps running.
                foreach (var name in new[] { "sub2", "sub3" })
                {
                    await streams[name].DisposeAsync();
                    lock (_streams)
                    {
                        _streams.Remove(streams[name]);
                    }
                    latestData.TryRemove(name, out _);
                    factories[name] = new ScriptedFactory(hub.CreateFactory(name));
                    streams[name] = BuildSubstream(testName, ChainSql, name, factories[name], latestData, failures, commits);
                }
                // sub2's handshake to sub1 is held until sub2 and sub3 handshook each other.
                factories["sub2"].HeldTarget = "sub1";
                try
                {
                    // Not awaited here, a start returns once its blocks initialized.
                    var restarts = Task.WhenAll(streams["sub3"].StartAsync(), streams["sub2"].StartAsync());
                    await WaitUntil(() => Handshook("sub2", "sub3") && Handshook("sub3", "sub2"), "sub2 and sub3 to handshake each other", TimeSpan.FromSeconds(30), Outcome);
                    factories["sub2"].HeldTarget = null;
                    await restarts.WaitAsync(TimeSpan.FromSeconds(30));
                }
                finally
                {
                    factories["sub2"].HeldTarget = null;
                }

                // The split's precondition: sub2 handshook sub3 in no wave, then found sub1 running in no wave.
                Assert.Contains(factories["sub2"].Requests, r => r.Target == "sub3" && r.Wave == RecoveryWave.None && r.Response is { Success: true });
                Assert.Contains(factories["sub2"].Requests, r => r.Target == "sub1" && r.Wave == RecoveryWave.None && r.Response is { NotStarted: false, PeerInInit: false });
                await WaitUntil(() => factories["sub2"].Requests.Any(r => r.Target == "sub1" && r.Wave > RecoveryWave.None), "sub2 to handshake sub1 in the wave it minted", TimeSpan.FromSeconds(15), Outcome);
                var minted = factories["sub2"].Requests.Where(r => r.Target == "sub1").Max(r => r.Wave);
                await WaitUntil(() => factories["sub3"].ReceivedFailAndRecover.Contains($"sub2:{minted}"), $"sub3 to be told of wave {minted}", TimeSpan.FromSeconds(30), Outcome);
                await WaitUntil(() => factories["sub3"].Requests.Any(r => r.Wave >= minted && r.Response?.Success == true), $"sub3 to handshake in wave {minted}", TimeSpan.FromSeconds(30), Outcome);
                await WaitUntil(Running, "the group to run again", TimeSpan.FromSeconds(30), Outcome);
                await WaitUntil(() => CommonCommittedVersion(commits, names, commitsAtEvent).HasValue, "one CommitVersion on every sink after the event", TimeSpan.FromSeconds(60), Outcome);
                _output.WriteLine(Outcome());
            }
            catch
            {
                DumpLogBuffers(testName, Outcome());
                throw;
            }
        }

        /// <summary>
        /// The peer read the group's version, the dying substream dies before its come-down and returns without its storage: the group starts over.
        /// </summary>
        [Fact]
        public async Task APeerReturningWithoutItsStorageAfterTheAgreementReadStartsTheGroupOver()
        {
            const string testName = "storage_lost_rejoin";
            // sub1 does not read from sub2, nothing else notices the rejoin.
            const string dying = "sub2";
            _generator.Generate(100);
            var names = new[] { "sub1", "sub2" };
            var hub = new LocalSubstreamCommunicationHub();
            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<string>();
            var commits = new ConcurrentQueue<(string Substream, long Version)>();
            var factories = names.ToDictionary(n => n, n => new ScriptedFactory(hub.CreateFactory(n)));
            // The dying substream restores above its peer and has to come down.
            var seeded = names.ToDictionary(n => n, n => n == dying ? 2 : 1);
            var streams = new Dictionary<string, Base.Engine.DataflowStream>();
            foreach (var name in names)
            {
                streams[name] = BuildSubstream(testName, TwoSubstreamSql, name, factories[name], latestData, failures, commits, fileProvider: await Seeded($"{testName.Length}_{testName}_{name}", seeded[name]));
            }
            string Outcome() =>
                $"states {string.Join(" ", streams.Select(s => $"{s.Key}={s.Value.State}"))}; rows {string.Join(" ", names.Select(n => $"{n}={RowCount(latestData, n)}"))}; " +
                string.Join("; ", factories.Select(f => $"{f.Key} handshakes={string.Join(",", f.Value.Requests.Select(Describe))} claims={string.Join(",", f.Value.Claims.Keys)}")) +
                $"; commits={string.Join(",", commits.Select(c => $"{c.Substream}:{c.Version}"))}; failures={string.Join(" | ", failures)}";
            var comingDown = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var peer = names.Single(n => n != dying);
            var peerRead = new TaskCompletionSource<long?>(TaskCreationOptions.RunContinuationsAsynchronously);
            StreamContext.GroupVersionReadHookForTests = (streamName, version) =>
            {
                if (streamName == $"{testName.Length}_{testName}_{peer}") peerRead.TrySetResult(version);
            };
            using var dead = new ManualResetEventSlim();
            StreamContext.BeforeFailureDisposeForTests = streamName =>
            {
                // Its come-down teardown, before its operators tell the peer: the process dies here.
                if (streamName == $"{testName.Length}_{testName}_{dying}" && comingDown.TrySetResult())
                {
                    factories[dying].Dead = true;
                    dead.Wait(TimeSpan.FromMinutes(5));
                }
            };
            try
            {
                foreach (var stream in streams.Values)
                {
                    await stream.StartAsync();
                }
                await comingDown.Task.WaitAsync(TimeSpan.FromSeconds(30));
                // The peer read the group's version before the rejoin and parks in its settle wait.
                Assert.Equal(1, await peerRead.Task.WaitAsync(TimeSpan.FromSeconds(30)));
                await WaitUntil(() => streams[peer].IsWaitingForConnectedStreams, "the peer's settle wait", TimeSpan.FromSeconds(30), Outcome);
                Assert.Empty(_logBuffers[peer].LinesContaining("restarting at that one"));
                Assert.Equal(StreamStateValue.Starting, streams[peer].State);

                // A fresh object without its storage, under its own temp directory beside the dead one; both outputs come from the new run.
                latestData.Clear();
                factories[dying] = new ScriptedFactory(hub.CreateFactory(dying));
                streams[dying] = BuildSubstream(testName + "_fresh", TwoSubstreamSql, dying, factories[dying], latestData, failures, commits);
                await streams[dying].StartAsync();

                await WaitUntil(() => streams.Values.All(s => s.State == StreamStateValue.Running) && names.All(n => RowCount(latestData, n) == _generator.Users.Count),
                    "the group to start over", TimeSpan.FromSeconds(60), Outcome);
                await WaitUntil(() => names.All(n => commits.Any(c => c.Substream == n && c.Version == 1)), "the first checkpoint", TimeSpan.FromSeconds(60), Outcome);
                _output.WriteLine(Outcome());

                // sub1 came down from its settled wait, not through a failure, and nobody committed above the group.
                Assert.Single(_logBuffers[peer].LinesContaining("came back below it, restarting at 0"));
                Assert.All(failures.Where(f => f.StartsWith($"{peer}:")), f => Assert.Equal($"{peer}: ", f));
                Assert.All(names, n => Assert.Equal(new long[] { 0, 1 }, commits.Where(c => c.Substream == n).Select(c => c.Version).Take(2)));
            }
            catch
            {
                DumpLogBuffers(testName, Outcome());
                throw;
            }
            finally
            {
                StreamContext.BeforeFailureDisposeForTests = null;
                StreamContext.GroupVersionReadHookForTests = null;
                dead.Set();
            }
        }

        private static async Task<MemoryFileProvider> Seeded(string streamName, int checkpoints)
        {
            var provider = new MemoryFileProvider();
            // Not disposed, that would clear the provider.
            var storage = new ReservoirPersistentStorage(new Storage.Persistence.Reservoir.ReservoirStorageOptions { FileProvider = provider });
            var manager = new StateManagerSync<StreamState>(new StateManagerOptions { PersistentStorage = storage },
                NullLoggerFactory.Instance, new Meter(streamName), streamName, GlobalMemoryManager.Instance);
            await manager.InitializeAsync();
            for (int i = 0; i < checkpoints; i++)
            {
                await manager.CheckpointAsync();
            }
            return provider;
        }

        /// <summary>
        /// A peer's wave reaches a restarting substream after its agreement reset, before its exchange initialized again: the start joins it.
        /// </summary>
        [Fact]
        public async Task PeerWaveBeforeTheExchangeReinitializesJoinsTheStart()
        {
            var sub1Log = _logBuffers.GetOrAdd("sub1", _ => new RingBufferLoggerProvider());
            await RunPeerWave("restart_stale_target", TwoSubstreamSql, new[] { "sub1", "sub2" }, 1, async run =>
            {
                // sub1's restart is held after its agreement reset, before its exchange initializes.
                var held = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                var release = new ManualResetEventSlim();
                sub1Log.OnLine = line =>
                {
                    if (line.Contains("Initializing egress blocks", StringComparison.Ordinal) && held.TrySetResult())
                    {
                        release.Wait(TimeSpan.FromSeconds(30));
                    }
                };
                try
                {
                    await run.Streams["sub1"].InjectFailureForTests(new InvalidOperationException("restart sub1"));
                    await held.Task.WaitAsync(TimeSpan.FromSeconds(30));
                    // What sub2's failure teardown tells sub1.
                    await run.Factories["sub2"].Handlers["sub1"].SendFailAndRecover(PeerWave).WaitAsync(TimeSpan.FromSeconds(10));
                }
                finally
                {
                    sub1Log.OnLine = null;
                    release.Set();
                }
            });
            Assert.Single(sub1Log.LinesContaining($"recovers in wave {PeerWave}, restarting into it"));
            // No operator of the dead run takes the wave, the start continues in it.
            Assert.Single(sub1Log.LinesContaining($"before any exchange operator is initialized, the start continues in wave {PeerWave}"));
        }

        /// <summary>
        /// sub1 joins a peer's wave in its handshake while its other exchange's target is still wired to the ended run.
        /// </summary>
        [Fact]
        public async Task HandshakeWaveJoinRestartsPastAnotherExchangesEndedRunTarget()
        {
            var sub1Log = _logBuffers.GetOrAdd("sub1", _ => new RingBufferLoggerProvider());
            await RunPeerWave("join_second_exchange", TwoExchangeSql, new[] { "sub1", "sub2" }, 2, run =>
            {
                // sub2's answer to sub1's next handshake: it starts in a wave above sub1's.
                run.Factories["sub1"].OnNextResponse = response => Task.FromResult(
                    new SubstreamInitializeResponse(false, response.Success, response.RestoreVersion, response.CheckpointEpoch, response.RecordedCheckpointEpoch, wave: PeerWave, peerInInit: true));
                return run.Streams["sub1"].InjectFailureForTests(new InvalidOperationException("restart sub1"));
            });
            Assert.Single(sub1Log.LinesContaining($"starts in wave {PeerWave}, this stream restarts into it"));
            // The point has a wired target, the fallback must not run.
            Assert.Empty(sub1Log.LinesContaining("before any exchange operator is initialized"));
        }

        /// <summary>
        /// A peer's wave reaches sub1 while its exchange wires its targets, after one target handshook the other peer: that peer must be told.
        /// </summary>
        [Fact]
        public async Task PeerWaveWhileTheExchangeWiresItsTargetsReachesTheOtherPeer()
        {
            await RunPeerWave("wave_mid_wiring", TwoTargetSql, new[] { "sub1", "sub2", "sub3" }, 1, async run =>
            {
                var sub2Before = run.Factories["sub2"].Requests.Count;
                var answered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                // sub2 has answered target 0 in the start wave, target 1 initializes once that answer is back.
                run.Factories["sub1"].OnNextResponse = async response =>
                {
                    answered.TrySetResult();
                    await release.Task;
                    return response;
                };
                try
                {
                    await run.Streams["sub1"].InjectFailureForTests(new InvalidOperationException("restart sub1"));
                    await answered.Task.WaitAsync(TimeSpan.FromSeconds(30));
                    await WaitUntil(() => run.Factories["sub2"].Requests.Skip(sub2Before).Any(r => r.Response?.Success == true), "sub2 to handshake sub1 in the start wave", TimeSpan.FromSeconds(30), run.Outcome);
                    // What sub3's failure teardown tells sub1.
                    await run.Factories["sub3"].Handlers["sub1"].SendFailAndRecover(PeerWave).WaitAsync(TimeSpan.FromSeconds(10));
                }
                finally
                {
                    release.TrySetResult();
                }
            });
        }

        /// <summary>
        /// A peer's wave reaches sub2's point to sub1 before its reader is wired, after its target handshook sub3: sub3 must be told.
        /// </summary>
        [Fact]
        public async Task PeerWaveAtAnUnwiredPointReachesThePeerAnotherPointHandshook()
        {
            var sub2Log = _logBuffers.GetOrAdd("sub2", _ => new RingBufferLoggerProvider());
            PeerWaveRun? delivered = null;
            await RunPeerWave("wave_unwired_point", ChainSql, new[] { "sub1", "sub2", "sub3" }, 2, async run =>
            {
                delivered = run;
                var sub2Before = run.Factories["sub2"].Requests.Count;
                var sub3Before = run.Factories["sub3"].Requests.Count;
                // sub2's restart is held after its exchange handshook sub3, before its reader initializes.
                var held = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                var release = new ManualResetEventSlim();
                sub2Log.OnLine = line =>
                {
                    if (line.Contains("Initializing ingress blocks", StringComparison.Ordinal) && held.TrySetResult())
                    {
                        release.Wait(TimeSpan.FromSeconds(30));
                    }
                };
                try
                {
                    await run.Streams["sub2"].InjectFailureForTests(new InvalidOperationException("restart sub2"));
                    await held.Task.WaitAsync(TimeSpan.FromSeconds(30));
                    Assert.Contains(run.Factories["sub2"].Requests.Skip(sub2Before), r => r.Target == "sub3" && r.Wave < PeerWave && r.Response?.Success == true);
                    // Both directions of the pair stand in sub2's restart wave.
                    await WaitUntil(() => run.Factories["sub3"].Requests.Skip(sub3Before).Any(r => r.Target == "sub2" && r.Wave < PeerWave && r.Response?.Success == true), "sub3 to handshake sub2 in its restart wave", TimeSpan.FromSeconds(30), run.Outcome);
                    // What sub1's failure teardown tells sub2.
                    await run.Factories["sub1"].Handlers["sub2"].SendFailAndRecover(PeerWave).WaitAsync(TimeSpan.FromSeconds(10));
                }
                finally
                {
                    sub2Log.OnLine = null;
                    release.Set();
                }
            }, traced: "sub2");
            Assert.Single(sub2Log.LinesContaining($"recovers in wave {PeerWave}, restarting into it"));
            Assert.Empty(sub2Log.LinesContaining($"the start continues in wave {PeerWave}"));
            Assert.Contains($"sub2:{PeerWave}", delivered!.Factories["sub3"].ReceivedFailAndRecover);
        }

        private sealed record PeerWaveRun(Dictionary<string, Base.Engine.DataflowStream> Streams, Dictionary<string, ScriptedFactory> Factories, Func<string> Outcome);

        /// <summary>
        /// Runs the group, lets the test bring <see cref="PeerWave"/> to a restart, then requires the group to run and commit in it.
        /// </summary>
        private async Task RunPeerWave(string testName, string sql, string[] names, int copies, Func<PeerWaveRun, Task> deliver, string traced = "sub1")
        {
            _generator.Generate(100);
            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var hub = new LocalSubstreamCommunicationHub();
            var failures = new ConcurrentBag<string>();
            var commits = new ConcurrentQueue<(string Substream, long Version)>();
            var factories = names.ToDictionary(n => n, n => new ScriptedFactory(hub.CreateFactory(n)));
            // The traced substream's initialize lines are hold points.
            var streams = names.ToDictionary(n => n, n => BuildSubstream(testName, sql, n, factories[n], latestData, failures, commits, n == traced ? LogLevel.Trace : LogLevel.Debug));
            int commitsAtDelivery = 0;
            string Outcome() =>
                $"states {string.Join(" ", streams.Select(s => $"{s.Key}={s.Value.State}/waitingForGroup={s.Value.IsWaitingForConnectedStreams}"))}; wave {PeerWave}; " +
                $"commits after delivery={string.Join(",", commits.Skip(commitsAtDelivery).Select(c => $"{c.Substream}:{c.Version}"))}; " +
                $"handshakes {string.Join("; ", factories.Select(f => $"{f.Key}: {string.Join(",", f.Value.Requests.Select(Describe))}"))}; failures={string.Join(" | ", failures)}";
            bool Running() => streams.Values.All(s => s.State == StreamStateValue.Running) && names.Skip(1).Sum(n => RowCount(latestData, n)) == copies * _generator.Users.Count;
            try
            {
                foreach (var stream in streams.Values)
                {
                    await stream.StartAsync();
                }
                await WaitUntil(() => Running() && CommonCommittedVersion(commits, names).HasValue, "the first run", TimeSpan.FromSeconds(30), Outcome);
                commitsAtDelivery = commits.Count;
                await deliver(new PeerWaveRun(streams, factories, Outcome));
                // Every substream handshakes in the wave: each one was told.
                await WaitUntil(() => Running() && names.All(n => factories[n].Requests.Any(r => r.Wave >= PeerWave && r.Response?.Success == true)), "the group to run again in the wave", TimeSpan.FromSeconds(30), Outcome);
                await WaitUntil(() => CommonCommittedVersion(commits, names, commitsAtDelivery).HasValue, "one CommitVersion on every sink after the delivery", TimeSpan.FromSeconds(60), Outcome);
                _output.WriteLine(Outcome());
            }
            catch
            {
                DumpLogBuffers(testName, Outcome());
                throw;
            }
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

        private static long? CommonCommittedVersion(ConcurrentQueue<(string Substream, long Version)> commits, string[] names, int skip = 0)
        {
            var snapshot = commits.Skip(skip).ToList();
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
            ConcurrentQueue<(string Substream, long Version)> commits,
            LogLevel minimumLevel = LogLevel.Debug,
            MemoryFileProvider? fileProvider = null)
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
                        FileProvider = fileProvider ?? new MemoryFileProvider()
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
                b.SetMinimumLevel(minimumLevel);
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

            public volatile string? HeldTarget;

            // The process is gone: nothing it sends or fetches reaches the peers.
            public volatile bool Dead;

            // Target and wave of every durability claim sent.
            public ConcurrentDictionary<string, byte> ClaimWaves { get; } = new ConcurrentDictionary<string, byte>();

            // Every distinct durability claim sent.
            public ConcurrentDictionary<string, byte> Claims { get; } = new ConcurrentDictionary<string, byte>();

            // Applied once, to the next handshake answer from a started peer.
            public Func<SubstreamInitializeResponse, Task<SubstreamInitializeResponse>>? OnNextResponse;

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
                if (!_factory.Dead) _inner.OnStreamFailure();
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
                if (_factory.Dead) return Task.FromResult<IReadOnlyList<SubstreamEventData>>(Array.Empty<SubstreamEventData>());
                return _inner.FetchData(targetIds, numberOfEvents, cancellationToken);
            }

            public Task SendFailAndRecover(RecoveryWave wave)
            {
                if (_factory.Dead) return Task.CompletedTask;
                return _inner.SendFailAndRecover(wave);
            }

            public async Task<SubstreamInitializeResponse> SendInitializeRequest(long restoreVersion, long checkpointEpoch, bool cleanHandoff, RecoveryWave wave, CancellationToken cancellationToken)
            {
                if (_factory.Dead) throw new OperationCanceledException("the process is gone");
                if (_factory.ShouldThrow())
                {
                    _factory.Requests.Enqueue(new HandshakeRecord(_target, wave, checkpointEpoch, null));
                    throw new InvalidOperationException("simulated lost initialize request");
                }
                if (_factory.HoldHandshakes || _factory.HeldTarget == _target)
                {
                    // Answered as draining: the requester waits without spending its retry budget.
                    _factory.Requests.Enqueue(new HandshakeRecord(_target, wave, checkpointEpoch, null));
                    return new SubstreamInitializeResponse(true, false, restoreVersion, peerDraining: true);
                }
                var response = await _inner.SendInitializeRequest(restoreVersion, checkpointEpoch, cleanHandoff, wave, cancellationToken);
                if (!response.NotStarted && Interlocked.Exchange(ref _factory.OnNextResponse, null) is { } onResponse)
                {
                    response = await onResponse(response);
                }
                _factory.Requests.Enqueue(new HandshakeRecord(_target, wave, checkpointEpoch, response));
                return response;
            }

            public Task SendCheckpointDone(long checkpointVersion, long targetCheckpointEpoch, bool coversPeerStopBarrier)
            {
                if (_factory.Dead) return Task.CompletedTask;
                return _inner.SendCheckpointDone(checkpointVersion, targetCheckpointEpoch, coversPeerStopBarrier);
            }

            public void InitializeDurabilityClaims(Func<long, int, long, RecoveryWave, long, long, bool, Task> callReceiveDurabilityClaim)
            {
                _inner.InitializeDurabilityClaims(callReceiveDurabilityClaim);
            }

            public Task SendDurabilityClaim(long version, int radius, long initVersion, RecoveryWave wave, long senderCheckpointEpoch, long targetCheckpointEpoch, bool requestReply, CancellationToken cancellationToken)
            {
                if (_factory.Dead) return Task.CompletedTask;
                _factory.ClaimWaves.TryAdd($"{_target}@{wave}", 0);
                _factory.Claims.TryAdd($"{_target}@{wave} r{radius} v{version} init {initVersion}", 0);
                return _inner.SendDurabilityClaim(version, radius, initVersion, wave, senderCheckpointEpoch, targetCheckpointEpoch, requestReply, cancellationToken);
            }
        }
    }
}
