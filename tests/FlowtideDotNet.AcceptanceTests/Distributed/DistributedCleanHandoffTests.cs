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
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Engine.Distributed;
using FlowtideDotNet.Core.Operators.Exchange;
using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Core.Optimizer.DistributedMode;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Sql;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Diagnostics;

namespace FlowtideDotNet.AcceptanceTests.Distributed
{
    /// <summary>
    /// Protocol tests for the clean handoff a planned migration uses. Runs the real streams
    /// over the local hub without an Orleans cluster; rebuilding a substream over the same hub
    /// and storage is what a grain activation moving to another silo does.
    /// </summary>
    [Collection("StreamContext test hooks")]
    public class DistributedCleanHandoffTests : IAsyncLifetime
    {
        private record UserKeyRow(long UserKey);

        /// <summary>
        /// A memory file provider whose contents survive the owning storage being disposed,
        /// so a rebuilt substream restores what its predecessor persisted, like durable
        /// storage. Relisting the interface remaps its Dispose to the no-op here.
        /// </summary>
        private sealed class KeepAliveMemoryFileProvider : MemoryFileProvider, Storage.Persistence.Reservoir.IReservoirStorageProvider
        {
            public new void Dispose()
            {
            }
        }

        private const string JoinSql = @"
            INSERT INTO output
            SELECT u.userkey FROM users u
            INNER JOIN orders o ON u.userkey = o.userkey;
            ";

        private readonly MockDatabase _db;
        private readonly DatasetGenerator _generator;
        private readonly List<Base.Engine.DataflowStream> _streams = new List<Base.Engine.DataflowStream>();
        private readonly CancellationTokenSource _tickCancellation = new CancellationTokenSource();
        private Task? _tickLoop;

        public DistributedCleanHandoffTests()
        {
            FastEngineTimings.Apply();
            _db = new MockDatabase();
            _generator = new DatasetGenerator(_db);
        }

        public Task InitializeAsync()
        {
            // Drives the substream schedulers so the sources poll for new data, the same loop
            // the distributed host and the Orleans grains run; StartAsync does not tick them.
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
                            if (stream.Scheduler is not Base.Engine.DefaultStreamScheduler scheduler)
                            {
                                continue;
                            }
                            // One dispatch in flight per stream, a parked dispatch into a
                            // stopping stream must not stall the others.
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
                                    // A stream mid stop or dispose may reject the tick, the
                                    // next tick reaches it again if it is still running.
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
                await stream.DisposeAsync();
            }
        }

        /// <summary>
        /// A substream restarted through a clean handoff resumes against its running peer
        /// without any substream being failed or rolled back, and data keeps flowing.
        /// </summary>
        [Fact]
        public async Task SubstreamRestartedThroughACleanHandoffResumesWithoutAnyRollback()
        {
            var testName = "e2e_clean_handoff";
            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var fileProviders = new ConcurrentDictionary<string, KeepAliveMemoryFileProvider>();
            var hub = new LocalSubstreamCommunicationHub();

            var substream0 = BuildSubstream(testName, "substream_0", hub, fileProviders, latestData, failures, announceCleanHandoff: false);
            var substream1 = BuildSubstream(testName, "substream_1", hub, fileProviders, latestData, failures, announceCleanHandoff: false);
            await substream0.StartAsync();
            await substream1.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());

            // The handoff a migrating grain runs: drain consumption from the peer, stop at a
            // final checkpoint the peer acknowledges, dispose. The peer keeps running.
            await substream1.PrepareHandoffAsync();
            await AwaitBounded(substream1.StopAsync(), "handoff stop");
            await substream1.DisposeAsync();
            lock (_streams)
            {
                _streams.Remove(substream1);
            }

            // The "new activation": a fresh stream instance restores the final checkpoint
            // from the same storage and announces the clean handoff at its reconnect.
            substream1 = BuildSubstream(testName, "substream_1", hub, fileProviders, latestData, failures, announceCleanHandoff: true);
            await substream1.StartAsync();

            // Data added after the handoff must flow through both substreams again.
            _generator.Generate(250);
            try
            {
                await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());
            }
            catch
            {
                DumpLogBuffers("resume");
                throw;
            }

            // The clean handoff must not have failed or rolled back anything - a coordinated
            // rollback reports a null exception, so the whole bag must stay empty.
            Assert.Empty(failures);

            await AwaitBounded(Task.WhenAll(substream0.StopAsync(), substream1.StopAsync()), "coordinated stop");
        }

        /// <summary>
        /// A handoff that begins while checkpoints are in flight must neither wedge the stop
        /// nor roll anything back: a checkpoint stored when the drain unsubscribes the readers
        /// has no peer event left to pair with and must be self-forwarded, else it defers the
        /// stop until the watchdog fails the stream. Runs several rounds to widen the window.
        /// </summary>
        [Fact]
        public async Task HandoffWithCheckpointsInFlightResumesWithoutAnyRollback()
        {
            var testName = "e2e_handoff_ckpt_inflight";
            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var fileProviders = new ConcurrentDictionary<string, KeepAliveMemoryFileProvider>();
            var hub = new LocalSubstreamCommunicationHub();

            var substream0 = BuildSubstream(testName, "substream_0", hub, fileProviders, latestData, failures, announceCleanHandoff: false);
            var substream1 = BuildSubstream(testName, "substream_1", hub, fileProviders, latestData, failures, announceCleanHandoff: false);
            await substream0.StartAsync();
            await substream1.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());

            for (int round = 0; round < 3; round++)
            {
                // Checkpoints racing the drain on both sides: the triggers are not awaited so
                // the barriers are in flight when the drain begins.
                _ = substream0.TriggerCheckpoint();
                _ = substream1.TriggerCheckpoint();

                await substream1.PrepareHandoffAsync();
                await AwaitBounded(substream1.StopAsync(), $"handoff stop (round {round})");
                await substream1.DisposeAsync();
                lock (_streams)
                {
                    _streams.Remove(substream1);
                }

                substream1 = BuildSubstream(testName, "substream_1", hub, fileProviders, latestData, failures, announceCleanHandoff: true);
                await substream1.StartAsync();

                _generator.Generate(100);
                try
                {
                    await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());
                }
                catch
                {
                    DumpLogBuffers($"ckpt_inflight_round{round}");
                    throw;
                }
            }

            Assert.Empty(failures);

            await AwaitBounded(Task.WhenAll(substream0.StopAsync(), substream1.StopAsync()), "coordinated stop");
        }

        /// <summary>
        /// The safety fence: a reconnect that announces the handoff but restored older (here:
        /// no) state must be refused and fall back to coordinated recovery, so the result
        /// stays complete. Only reachable with state loss, not with the durable Orleans tests.
        /// </summary>
        [Fact]
        public async Task CleanHandoffAnnouncedWithLostStateFallsBackToRecovery()
        {
            var testName = "e2e_handoff_lost_state";
            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var fileProviders = new ConcurrentDictionary<string, KeepAliveMemoryFileProvider>();
            var hub = new LocalSubstreamCommunicationHub();

            var substream0 = BuildSubstream(testName, "substream_0", hub, fileProviders, latestData, failures, announceCleanHandoff: false);
            var substream1 = BuildSubstream(testName, "substream_1", hub, fileProviders, latestData, failures, announceCleanHandoff: false);
            await substream0.StartAsync();
            await substream1.StartAsync();

            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());

            await substream1.PrepareHandoffAsync();
            await AwaitBounded(substream1.StopAsync(), "handoff stop");
            await substream1.DisposeAsync();
            lock (_streams)
            {
                _streams.Remove(substream1);
            }

            // The restarted instance lost its state: an empty provider replaces the one the
            // handoff persisted into, so it restores nothing and announces the clean handoff
            // at a restore point below the commits the peer already acknowledged.
            fileProviders["substream_1"] = new KeepAliveMemoryFileProvider();
            substream1 = BuildSubstream(testName, "substream_1", hub, fileProviders, latestData, failures, announceCleanHandoff: true);
            await substream1.StartAsync();

            try
            {
                _generator.Generate(250);
                await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);

                // The peer must have refused the clean claim and gone through the coordinated
                // recovery instead of resuming over data the returned substream cannot know.
                Assert.NotEmpty(failures);

                await AwaitBounded(Task.WhenAll(substream0.StopAsync(), substream1.StopAsync()), "coordinated stop");
            }
            catch
            {
                // This has flaked under full-suite load (net10, recovery cascade missing the
                // wait deadline); the buffers hold both generations of substream_1.
                DumpLogBuffers("lost_state");
                throw;
            }
        }

        /// <summary>
        /// A clean reconnect resumes startup from the restored watermark names, the running
        /// peer never resends its init watermarks event. When the restored state holds no
        /// watermark names there is nothing to resume from and no init will ever arrive; the
        /// stream must fail over loudly so the recovery reconciles the substreams, not wait
        /// forever. The accept fence makes this state unreachable through honest paths (a
        /// clean reconnect needs an acked commit, and every commit follows the init), so the
        /// peer here is hand driven and answers the handshake with a dishonest clean
        /// reconnect accept.
        /// </summary>
        [Fact]
        public async Task CleanReconnectWithoutRestoredWatermarksFailsOverInsteadOfHanging()
        {
            var testName = "e2e_reconnect_no_watermarks";
            _generator.Generate(100);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var fileProviders = new ConcurrentDictionary<string, KeepAliveMemoryFileProvider>();
            var hub = new LocalSubstreamCommunicationHub();

            // The hand driven peer: serves no events and accepts every handshake as a clean
            // reconnect, the state a zombie accept or corrupted restore would produce.
            var peerHandler = hub.CreateFactory("substream_0").GetCommunicationHandler("substream_1", "substream_0");
            peerHandler.Initialize(
                (targets, count, ct) => Task.FromResult<IReadOnlyList<SubstreamEventData>>(Array.Empty<SubstreamEventData>()),
                _ => Task.CompletedTask,
                (restoreVersion, checkpointEpoch, cleanHandoff) => Task.FromResult(
                    new SubstreamInitializeResponse(notStarted: false, success: true, restoreVersion: restoreVersion, checkpointEpoch: 1, recordedCheckpointEpoch: 0, cleanReconnect: true)),
                (_, _, _) => Task.CompletedTask);

            // A fresh substream: nothing restored, so its read operators hold no watermark
            // names to resume the dishonestly accepted reconnect from.
            var substream1 = BuildSubstream(testName, "substream_1", hub, fileProviders, latestData, failures, announceCleanHandoff: false);
            await substream1.StartAsync();

            var deadline = Stopwatch.StartNew();
            while (failures.IsEmpty && deadline.Elapsed < TimeSpan.FromSeconds(20))
            {
                await Task.Delay(50);
            }

            Assert.False(failures.IsEmpty,
                "The clean reconnect had no restored watermark names to resume from and no failure was reported: the substream is hanging in startup waiting for an init watermarks event that never comes.");
        }

        private readonly ConcurrentDictionary<string, RingBufferLoggerProvider> _logBuffers = new ConcurrentDictionary<string, RingBufferLoggerProvider>();

        /// <summary>
        /// Rule 1: the barrier pairing keeps every substream on the same version.
        /// </summary>
        [Fact]
        public async Task AllSubstreamsCommitTheSameCheckpointVersionsWhileRunning()
        {
            var testName = "e2e_rule1_running";
            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var fileProviders = new ConcurrentDictionary<string, KeepAliveMemoryFileProvider>();
            var hub = new LocalSubstreamCommunicationHub();
            var committed = new ConcurrentDictionary<string, long>();

            Base.Engine.Internal.StateMachine.StreamContext.CheckpointCommitHookForTests = (streamName, lastVersion) =>
            {
                if (streamName.Contains(testName, StringComparison.Ordinal))
                {
                    committed.AddOrUpdate(streamName, lastVersion, (_, existing) => Math.Max(existing, lastVersion));
                }
                return Task.CompletedTask;
            };
            try
            {
                var substream0 = BuildSubstream(testName, "substream_0", hub, fileProviders, latestData, failures, announceCleanHandoff: false);
                var substream1 = BuildSubstream(testName, "substream_1", hub, fileProviders, latestData, failures, announceCleanHandoff: false);
                await substream0.StartAsync();
                await substream1.StartAsync();

                await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());

                // One checkpoint proves nothing, both sides start on the same version.
                for (int i = 0; i < 3; i++)
                {
                    _generator.Generate(150);
                    await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());
                }

                await AssertCommittedVersionsConverge(committed, "while running", minVersion: 2);

                await AwaitBounded(Task.WhenAll(substream0.StopAsync(), substream1.StopAsync()), "coordinated stop");
            }
            finally
            {
                Base.Engine.Internal.StateMachine.StreamContext.CheckpointCommitHookForTests = null;
            }
        }

        /// <summary>
        /// Rule 1: a rebuilt substream comes back on its peer's version.
        /// </summary>
        [Fact]
        public async Task AllSubstreamsCommitTheSameCheckpointVersionsAfterACleanHandoff()
        {
            var testName = "e2e_rule1_handoff";
            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var fileProviders = new ConcurrentDictionary<string, KeepAliveMemoryFileProvider>();
            var hub = new LocalSubstreamCommunicationHub();
            var committed = new ConcurrentDictionary<string, long>();

            Base.Engine.Internal.StateMachine.StreamContext.CheckpointCommitHookForTests = (streamName, lastVersion) =>
            {
                if (streamName.Contains(testName, StringComparison.Ordinal))
                {
                    committed.AddOrUpdate(streamName, lastVersion, (_, existing) => Math.Max(existing, lastVersion));
                }
                return Task.CompletedTask;
            };
            try
            {
                var substream0 = BuildSubstream(testName, "substream_0", hub, fileProviders, latestData, failures, announceCleanHandoff: false);
                var substream1 = BuildSubstream(testName, "substream_1", hub, fileProviders, latestData, failures, announceCleanHandoff: false);
                await substream0.StartAsync();
                await substream1.StartAsync();

                await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());

                // One checkpoint proves nothing, both sides start on the same version.
                for (int i = 0; i < 3; i++)
                {
                    _generator.Generate(150);
                    await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());
                }

                await AssertCommittedVersionsConverge(committed, "before the handoff", minVersion: 2);

                await substream1.PrepareHandoffAsync();
                await AwaitBounded(substream1.StopAsync(), "handoff stop");
                await substream1.DisposeAsync();
                lock (_streams)
                {
                    _streams.Remove(substream1);
                }

                // Only the versions committed after the handoff say anything about it.
                long beforeHandoff = committed.Values.DefaultIfEmpty(0).Max();
                committed.Clear();

                substream1 = BuildSubstream(testName, "substream_1", hub, fileProviders, latestData, failures, announceCleanHandoff: true);
                await substream1.StartAsync();

                _generator.Generate(250);
                await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());

                await AssertCommittedVersionsConverge(committed, "after the handoff", minVersion: beforeHandoff);

                await AwaitBounded(Task.WhenAll(substream0.StopAsync(), substream1.StopAsync()), "coordinated stop");
            }
            finally
            {
                Base.Engine.Internal.StateMachine.StreamContext.CheckpointCommitHookForTests = null;
            }
        }

        /// <summary>
        /// Lets the commits settle, then requires one version everywhere.
        /// </summary>
        private static Task AssertCommittedVersionsConverge(
            ConcurrentDictionary<string, long> committed,
            string phase,
            int expectedSubstreams = 2,
            long minVersion = 0)
        {
            return AssertCommittedVersionsConverge(
                () => committed.ToArray().ToDictionary(x => x.Key, x => x.Value),
                phase,
                expectedSubstreams,
                minVersion);
        }

        /// <summary>
        /// Re-snapshots every poll, so hand in a live view.
        /// The minimum version stops everyone sitting at version one counting as converged.
        /// </summary>
        private static async Task AssertCommittedVersionsConverge(
            Func<Dictionary<string, long>> takeSnapshot,
            string phase,
            int expectedSubstreams = 2,
            long minVersion = 0)
        {
            var deadline = DateTime.UtcNow.AddSeconds(20);
            string snapshot = string.Empty;
            while (DateTime.UtcNow < deadline)
            {
                await Task.Delay(250);
                var pairs = takeSnapshot().OrderBy(x => x.Key).ToList();
                snapshot = string.Join(", ", pairs.Select(x => $"{x.Key}={x.Value}"));
                if (pairs.Count >= expectedSubstreams
                    && pairs.All(x => x.Value >= minVersion)
                    && pairs.Select(x => x.Value).Distinct().Count() == 1)
                {
                    return;
                }
            }
            Assert.Fail($"Substream checkpoint versions did not converge {phase}: {snapshot}");
        }

        private static Dictionary<string, long> HighestVersionPerStream(ConcurrentQueue<(string Stream, long Version)> commits)
        {
            var result = new Dictionary<string, long>();
            foreach (var (stream, version) in commits.ToArray())
            {
                result[stream] = result.TryGetValue(stream, out var existing) ? Math.Max(existing, version) : version;
            }
            return result;
        }

        /// <summary>
        /// Rule 3: a paired checkpoint carries the same version on both substreams.
        /// Asserted where the pairing happens, comparing sink sequences proves nothing,
        /// every substream counts 1,2,3 on its own whether or not it is coordinating.
        /// </summary>
        [Fact]
        public async Task PairedCheckpointsCarryTheSameVersionOnBothSubstreams()
        {
            var testName = "e2e_rule3_pairing";
            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var fileProviders = new ConcurrentDictionary<string, KeepAliveMemoryFileProvider>();
            var hub = new LocalSubstreamCommunicationHub();
            var pairings = new ConcurrentQueue<(string Stream, long Peer, long Local)>();
            var discarded = new ConcurrentQueue<(string Stream, long Version, long Floor)>();

            SubstreamReadOperator.PairedCheckpointHookForTests = (streamName, peerVersion, localVersion) =>
            {
                if (streamName.Contains(testName, StringComparison.Ordinal))
                {
                    pairings.Enqueue((streamName, peerVersion, localVersion));
                }
            };
            SubstreamReadOperator.CoveredPeerBarrierHookForTests = (streamName, version, floor) =>
            {
                if (streamName.Contains(testName, StringComparison.Ordinal))
                {
                    discarded.Enqueue((streamName, version, floor));
                }
            };
            try
            {
                var substream0 = BuildSubstream(testName, "substream_0", hub, fileProviders, latestData, failures, announceCleanHandoff: false);
                var substream1 = BuildSubstream(testName, "substream_1", hub, fileProviders, latestData, failures, announceCleanHandoff: false);
                await substream0.StartAsync();
                await substream1.StartAsync();

                await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());
                for (int i = 0; i < 3; i++)
                {
                    _generator.Generate(150);
                    await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());
                }
                AssertPairedVersionsMatch(pairings, "while running");

                // A handoff must not change what a paired checkpoint carries.
                await substream1.PrepareHandoffAsync();
                // Tight bound, AwaitBounded would hide a reintroduced stop drain timeout.
                var handoffStop = Stopwatch.StartNew();
                await AwaitBounded(substream1.StopAsync(), "handoff stop");
                handoffStop.Stop();
                Assert.True(
                    handoffStop.Elapsed < TimeSpan.FromSeconds(10),
                    $"The handoff stop took {handoffStop.Elapsed}, a drain timeout is being burned.");
                await substream1.DisposeAsync();
                lock (_streams)
                {
                    _streams.Remove(substream1);
                }

                pairings.Clear();
                substream1 = BuildSubstream(testName, "substream_1", hub, fileProviders, latestData, failures, announceCleanHandoff: true);
                await substream1.StartAsync();

                _generator.Generate(250);
                await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());
                for (int i = 0; i < 3; i++)
                {
                    _generator.Generate(150);
                    await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());
                }
                AssertPairedVersionsMatch(pairings, "after the handoff");

                // Green pairings alone cannot tell the leftover apart from one never arriving.
                var leftovers = discarded.ToArray();
                Assert.True(
                    leftovers.Length > 0,
                    "The returning substream never discarded a leftover peer barrier, so the pairings prove nothing about the handoff.");
                Assert.All(leftovers, x => Assert.True(
                    x.Version <= x.Floor,
                    $"Discarded a peer barrier with version {x.Version} above the restore floor {x.Floor} on {x.Stream}."));

                await AwaitBounded(Task.WhenAll(substream0.StopAsync(), substream1.StopAsync()), "coordinated stop");
            }
            finally
            {
                SubstreamReadOperator.PairedCheckpointHookForTests = null;
                SubstreamReadOperator.CoveredPeerBarrierHookForTests = null;
            }
        }

        /// <summary>
        /// Every pairing must have matched the two versions, and there must be enough of them
        /// that the substreams were really exchanging.
        /// </summary>
        private static void AssertPairedVersionsMatch(ConcurrentQueue<(string Stream, long Peer, long Local)> pairings, string phase)
        {
            var observed = pairings.ToArray();
            Assert.True(observed.Length >= 4, $"Too few paired checkpoints {phase} to prove anything, saw {observed.Length}.");
            Assert.True(
                observed.Select(x => x.Stream).Distinct().Count() > 1,
                $"Only one substream paired {phase}, the plan is not exchanging: {string.Join("; ", observed.Select(x => x.Stream).Distinct())}");

            var mismatched = observed.Where(x => x.Peer != x.Local).ToList();
            Assert.True(
                mismatched.Count == 0,
                $"Paired checkpoints carried different versions {phase}: {string.Join("; ", mismatched.Select(x => $"{x.Stream} peer={x.Peer} local={x.Local}"))}");
        }

        /// <summary>
        /// Rule 4: a failure after a handoff still lands everyone on one version.
        /// Asserts the data too, the same number can name a different cut on each side.
        /// </summary>
        [Fact]
        public async Task AllSubstreamsRollBackToTheSameVersionOnFailure()
        {
            var testName = "e2e_rule4_rollback";
            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var fileProviders = new ConcurrentDictionary<string, KeepAliveMemoryFileProvider>();
            var hub = new LocalSubstreamCommunicationHub();
            var commits = new ConcurrentQueue<(string Stream, long Version)>();

            Base.Engine.Internal.StateMachine.StreamContext.CheckpointCommitHookForTests = (streamName, lastVersion) =>
            {
                if (streamName.Contains(testName, StringComparison.Ordinal))
                {
                    commits.Enqueue((streamName, lastVersion));
                }
                return Task.CompletedTask;
            };
            try
            {
                var substream0 = BuildSubstream(testName, "substream_0", hub, fileProviders, latestData, failures, announceCleanHandoff: false);
                var substream1 = BuildSubstream(testName, "substream_1", hub, fileProviders, latestData, failures, announceCleanHandoff: false);
                await substream0.StartAsync();
                await substream1.StartAsync();

                await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());

                // A clean handoff first, this is what makes the substream versions differ.
                await substream1.PrepareHandoffAsync();
                await AwaitBounded(substream1.StopAsync(), "handoff stop");
                await substream1.DisposeAsync();
                lock (_streams)
                {
                    _streams.Remove(substream1);
                }
                substream1 = BuildSubstream(testName, "substream_1", hub, fileProviders, latestData, failures, announceCleanHandoff: true);
                await substream1.StartAsync();

                _generator.Generate(250);
                await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());

                // Now fail the substream that owns the sink, so the rollback has to reach both.
                await substream0.StopAsync();
                await substream0.DisposeAsync();
                lock (_streams)
                {
                    _streams.Remove(substream0);
                }
                substream0 = BuildSubstream(testName, "substream_0", hub, fileProviders, latestData, failures, announceCleanHandoff: false, egressCrashOnCheckpointCount: 1);
                await substream0.StartAsync();

                _generator.Generate(250);

                // The data has to survive the rollback, the numbers alone would not show it.
                await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);

                commits.Clear();
                _generator.Generate(100);
                await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);

                await AssertCommittedVersionsConverge(
                    () => HighestVersionPerStream(commits),
                    "after the failure",
                    expectedSubstreams: 2,
                    minVersion: 2);

                await AwaitBounded(Task.WhenAll(substream0.StopAsync(), substream1.StopAsync()), "coordinated stop");
            }
            finally
            {
                Base.Engine.Internal.StateMachine.StreamContext.CheckpointCommitHookForTests = null;
            }
        }

        /// <summary>
        /// Rule 4: a failover restart must not reset either side to the start version.
        /// The peer answers with _selfInitializeVersion, set at its own start and never refreshed.
        /// </summary>
        [Fact]
        public async Task AFailoverRestartDoesNotDragTheSubstreamsBackToTheStartVersion()
        {
            var testName = "e2e_rule4_stale";
            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var fileProviders = new ConcurrentDictionary<string, KeepAliveMemoryFileProvider>();
            var hub = new LocalSubstreamCommunicationHub();
            var commits = new ConcurrentQueue<(string Stream, long Version)>();

            Base.Engine.Internal.StateMachine.StreamContext.CheckpointCommitHookForTests = (streamName, lastVersion) =>
            {
                if (streamName.Contains(testName, StringComparison.Ordinal))
                {
                    commits.Enqueue((streamName, lastVersion));
                }
                return Task.CompletedTask;
            };
            try
            {
                var substream0 = BuildSubstream(testName, "substream_0", hub, fileProviders, latestData, failures, announceCleanHandoff: false);
                var substream1 = BuildSubstream(testName, "substream_1", hub, fileProviders, latestData, failures, announceCleanHandoff: false);
                await substream0.StartAsync();
                await substream1.StartAsync();

                // Run until the versions are far above the start, that is the whole point.
                await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());
                for (int i = 0; i < 6; i++)
                {
                    _generator.Generate(200);
                    await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());
                }

                long HighestFor(string name) => commits.ToArray()
                    .Where(c => c.Stream.EndsWith(name, StringComparison.Ordinal))
                    .Select(c => c.Version)
                    .DefaultIfEmpty(0)
                    .Max();

                long peerBeforeRestart = HighestFor("substream_0");
                long restartingBeforeRestart = HighestFor("substream_1");
                Assert.True(restartingBeforeRestart > 5, $"Not enough checkpoints to be meaningful, substream_1 was at {restartingBeforeRestart}.");

                // A failover restart, no clean handoff announced, so the handshake reconciliation runs.
                await substream1.StopAsync();
                await substream1.DisposeAsync();
                lock (_streams)
                {
                    _streams.Remove(substream1);
                }
                commits.Clear();
                substream1 = BuildSubstream(testName, "substream_1", hub, fileProviders, latestData, failures, announceCleanHandoff: false);
                await substream1.StartAsync();

                _generator.Generate(200);
                await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);

                long LowestAfterRestartFor(string name) => commits.ToArray()
                    .Where(c => c.Stream.EndsWith(name, StringComparison.Ordinal))
                    .Select(c => c.Version)
                    .DefaultIfEmpty(-1)
                    .Min();

                // The restarting substream must resume on its own last version. The peer answers the
                // handshake with the version it started on, and Math.Min of that takes it to zero.
                long restartingAfter = LowestAfterRestartFor("substream_1");
                Assert.True(
                    restartingAfter >= restartingBeforeRestart - 2,
                    $"The restarting substream was reset: it was at {restartingBeforeRestart} and resumed at {restartingAfter}.");

                long peerAfter = LowestAfterRestartFor("substream_0");
                Assert.True(
                    peerAfter >= peerBeforeRestart - 2,
                    $"The running peer was dragged back: it was at {peerBeforeRestart} and resumed at {peerAfter}.");

                await AwaitBounded(Task.WhenAll(substream0.StopAsync(), substream1.StopAsync()), "coordinated stop");
            }
            finally
            {
                Base.Engine.Internal.StateMachine.StreamContext.CheckpointCommitHookForTests = null;
            }
        }

        /// <summary>
        /// Rule 4 with three substreams: a failure in one has to bring every substream to the same
        /// version. The negotiation is pairwise, so a substream with no direct link to the failing
        /// one only learns about the rollback second hand.
        /// </summary>
        [Fact]
        public async Task AllThreeSubstreamsRollBackToTheSameVersionOnFailure()
        {
            var testName = "e2e_rule4_three";
            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var fileProviders = new ConcurrentDictionary<string, KeepAliveMemoryFileProvider>();
            var hub = new LocalSubstreamCommunicationHub();
            var commits = new ConcurrentQueue<(string Stream, long Version)>();

            Base.Engine.Internal.StateMachine.StreamContext.CheckpointCommitHookForTests = (streamName, lastVersion) =>
            {
                if (streamName.Contains(testName, StringComparison.Ordinal))
                {
                    commits.Enqueue((streamName, lastVersion));
                }
                return Task.CompletedTask;
            };
            try
            {
                // The sink lands on substream_0, so that is the one whose crash fails the stream.
                var substream0 = BuildSubstream(testName, "substream_0", hub, fileProviders, latestData, failures, announceCleanHandoff: false, egressCrashOnCheckpointCount: 1, substreamCount: 3);
                var substream1 = BuildSubstream(testName, "substream_1", hub, fileProviders, latestData, failures, announceCleanHandoff: false, substreamCount: 3);
                var substream2 = BuildSubstream(testName, "substream_2", hub, fileProviders, latestData, failures, announceCleanHandoff: false, substreamCount: 3);
                await substream0.StartAsync();
                await substream1.StartAsync();
                await substream2.StartAsync();

                _generator.Generate(250);
                await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);

                Assert.True(failures.Any(f => f.Exception != null), "No substream failed, the sink crash did not fire.");

                commits.Clear();
                _generator.Generate(100);
                await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);

                await AssertCommittedVersionsConverge(
                    () => HighestVersionPerStream(commits),
                    "after the failure with three substreams",
                    expectedSubstreams: 3,
                    minVersion: 2);

                await AwaitBounded(Task.WhenAll(substream0.StopAsync(), substream1.StopAsync(), substream2.StopAsync()), "coordinated stop");
            }
            finally
            {
                Base.Engine.Internal.StateMachine.StreamContext.CheckpointCommitHookForTests = null;
            }
        }

        /// <summary>
        /// Rule 2: every substream has the negotiated version, so the rollback is clean.
        /// A missing one shows up as a recovery failure and a stream that never resumes.
        /// </summary>
        [Fact]
        public async Task TheNegotiatedRollbackVersionIsAvailableOnEverySubstream()
        {
            var testName = "e2e_rule2_available";
            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var fileProviders = new ConcurrentDictionary<string, KeepAliveMemoryFileProvider>();
            var hub = new LocalSubstreamCommunicationHub();

            var substream0 = BuildSubstream(testName, "substream_0", hub, fileProviders, latestData, failures, announceCleanHandoff: false, egressCrashOnCheckpointCount: 1);
            var substream1 = BuildSubstream(testName, "substream_1", hub, fileProviders, latestData, failures, announceCleanHandoff: false);
            await substream0.StartAsync();
            await substream1.StartAsync();

            _generator.Generate(250);
            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);

            Assert.True(failures.Any(f => f.Exception != null), "No substream failed, the sink crash did not fire.");

            // A version that is missing where it is applied throws out of the recovery and the
            // stream retries the same version forever.
            var recoveryFailures = failures
                .Where(f => f.Exception != null && f.Exception.ToString().Contains("not found for recovery", StringComparison.OrdinalIgnoreCase))
                .ToList();
            Assert.True(
                recoveryFailures.Count == 0,
                $"A negotiated rollback version was not available: {string.Join(" | ", recoveryFailures.Select(f => f.Substream + ": " + f.Exception!.Message))}");

            // The rollback must actually settle, a stream stuck retrying never produces again.
            _generator.Generate(100);
            await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);

            await AwaitBounded(Task.WhenAll(substream0.StopAsync(), substream1.StopAsync()), "coordinated stop");
        }

        /// <summary>
        /// A handoff closes the peer's cycle without the departing substream consuming its
        /// answering barrier, so that version counts rows the departing side never received.
        /// They live only in the target queue, which is never checkpointed, so a failure while
        /// the substream is away rolls both back onto that version and loses them.
        /// </summary>
        [Fact]
        public async Task AFailureDuringACleanHandoffKeepsTheRowsInFlight()
        {
            await RunFailureUnderLoad("e2e_handoff_loss", HandoffFailureMode.WhilePeerIsAway);
        }

        /// <summary>
        /// Control: the same failure, no handoff at all. Green, or a red result above says
        /// nothing more than that crashing loses data.
        /// </summary>
        [Fact]
        public async Task AFailureWithoutACleanHandoffKeepsTheRowsInFlight()
        {
            await RunFailureUnderLoad("e2e_nohandoff_loss", HandoffFailureMode.NoHandoff);
        }

        /// <summary>
        /// Control: the same handoff, dispose and rebuild, but the pair is let past the handoff
        /// version before the failure. Green, so a red result above is the rollback landing on
        /// that version and not the rebuild.
        /// </summary>
        [Fact]
        public async Task AFailureAfterAHandoffHasSettledKeepsTheRowsInFlight()
        {
            await RunFailureUnderLoad("e2e_settled_loss", HandoffFailureMode.AfterHandoffSettled);
        }

        private enum HandoffFailureMode
        {
            // Fail with the peer gone, pinned on the handoff version.
            WhilePeerIsAway,
            // Fail with no handoff anywhere in the run.
            NoHandoff,
            // Hand off, bring it back, let the pair commit on before failing.
            AfterHandoffSettled,
        }

        // How long the departing substream is held after it stops fetching from its peer.
        private static readonly TimeSpan HandoffStrandingWindow = TimeSpan.FromSeconds(1);

        private const string InjectedFailureMessage = "Injected failure for the handoff loss test";

        /// <summary>
        /// Loads continuously, optionally hands substream_1 off, then fails the substream that
        /// stayed while its peer is away. That peer cannot commit past the handoff version
        /// without an ack, so this is the version both roll back to.
        /// </summary>
        private async Task RunFailureUnderLoad(string testName, HandoffFailureMode mode)
        {
            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var fileProviders = new ConcurrentDictionary<string, KeepAliveMemoryFileProvider>();
            var hub = new LocalSubstreamCommunicationHub();
            var commits = new ConcurrentQueue<(string Stream, long Version)>();
            var restores = new ConcurrentQueue<(string Stream, long Version)>();
            bool withHandoff = mode != HandoffFailureMode.NoHandoff;

            Base.Engine.Internal.StateMachine.StreamContext.CheckpointCommitHookForTests = (streamName, lastVersion) =>
            {
                if (streamName.Contains(testName, StringComparison.Ordinal))
                {
                    commits.Enqueue((streamName, lastVersion));
                }
                return Task.CompletedTask;
            };
            Base.Engine.Internal.StateMachine.StreamContext.RestoreVersionForTests = (streamName, version) =>
            {
                if (streamName.Contains(testName, StringComparison.Ordinal))
                {
                    restores.Enqueue((streamName, version));
                }
            };
            try
            {

                var substream0 = BuildSubstream(testName, "substream_0", hub, fileProviders, latestData, failures, announceCleanHandoff: false);
                var substream1 = BuildSubstream(testName, "substream_1", hub, fileProviders, latestData, failures, announceCleanHandoff: false);
                await substream0.StartAsync();
                await substream1.StartAsync();

                await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());

                // The stranded rows are whatever is produced while the peer is not consuming, so
                // the load has to run right through the handoff.
                using var loadDone = new CancellationTokenSource();
                var load = Task.Run(async () =>
                {
                    while (!loadDone.IsCancellationRequested)
                    {
                        _generator.Generate(25);
                        await Task.Delay(25);
                    }
                });

                if (withHandoff)
                {
                    // Unsubscribes, so from here the peer produces for a substream that no longer
                    // fetches. Held open so the stranded span covers many source polls: the poll
                    // interval is 50 ms and the span is about that, so it is otherwise a coin flip.
                    await substream1.PrepareHandoffAsync();
                    await Task.Delay(HandoffStrandingWindow);
                    await AwaitBounded(substream1.StopAsync(), "handoff stop");
                    await substream1.DisposeAsync();
                    lock (_streams)
                    {
                        _streams.Remove(substream1);
                    }
                }
                else
                {
                    // Same shape as the handoff run, so only the handoff differs between them.
                    await Task.Delay(HandoffStrandingWindow);
                }

                int StayingCommits() => commits.Count(x => x.Stream.Contains("substream_0", StringComparison.Ordinal));
                var commitsAtHandoff = StayingCommits();

                if (mode == HandoffFailureMode.AfterHandoffSettled)
                {
                    // Back first, and past the handoff version, so the rollback cannot land on it.
                    substream1 = BuildSubstream(testName, "substream_1", hub, fileProviders, latestData, failures, announceCleanHandoff: true);
                    await substream1.StartAsync();
                    await WaitForCommitsToAdvance(commits, "substream_0", 3);
                }

                var commitsAtFailure = StayingCommits();

                await substream0.InjectFailureForTests(new CrashException(InjectedFailureMessage));

                if (mode == HandoffFailureMode.WhilePeerIsAway)
                {
                    substream1 = BuildSubstream(testName, "substream_1", hub, fileProviders, latestData, failures, announceCleanHandoff: true);
                    await substream1.StartAsync();
                }

                loadDone.Cancel();
                await load;

                // Nothing below means anything unless the injected failure is what landed.
                var failureDeadline = DateTime.UtcNow.AddSeconds(30);
                while (!failures.Any(f => f.Exception != null && f.Exception.ToString().Contains(InjectedFailureMessage, StringComparison.Ordinal))
                    && DateTime.UtcNow < failureDeadline)
                {
                    await Task.Delay(100);
                }
                Assert.True(
                    failures.Any(f => f.Exception != null && f.Exception.ToString().Contains(InjectedFailureMessage, StringComparison.Ordinal)),
                    $"The injected failure was never reported, so nothing rolled back and the run proves nothing. Reported: {string.Join(" | ", failures.Where(f => f.Exception != null).Select(f => f.Substream + ": " + f.Exception!.Message))}");

                // The substream that failed has to have actually rolled back. Only it takes the
                // failure path, a rebuilt peer restores through its own start instead.
                var restoreDeadline = DateTime.UtcNow.AddSeconds(30);
                while (!restores.Any(r => r.Stream.Contains("substream_0", StringComparison.Ordinal)) && DateTime.UtcNow < restoreDeadline)
                {
                    await Task.Delay(100);
                }
                var restored = restores.ToArray();
                Assert.True(
                    restored.Any(r => r.Stream.Contains("substream_0", StringComparison.Ordinal)),
                    $"The staying substream never rolled back, so nothing was restored and the run proves nothing. Saw: {string.Join(", ", restored.Select(r => r.Stream + "=" + r.Version))}");

                if (mode == HandoffFailureMode.WhilePeerIsAway)
                {
                    // Where the rollback lands is the whole point. With its partner gone the
                    // peer cannot commit, so it is pinned on the handoff version; a commit here
                    // means the run never entered the state the loss needs.
                    Assert.True(
                        commitsAtFailure == commitsAtHandoff,
                        $"The staying substream committed {commitsAtFailure - commitsAtHandoff} times while its peer was away, so the rollback did not land on the handoff version and this run proves nothing.");
                }
                else if (mode == HandoffFailureMode.AfterHandoffSettled)
                {
                    // The counterpart: this one only means something past the handoff version.
                    Assert.True(
                        commitsAtFailure > commitsAtHandoff,
                        "The staying substream never committed after the handoff, so the rollback landed on the handoff version and this is not the settled case.");
                }

                // Every generated row has to survive the rollback, the versions alone would agree
                // either way. A shortfall here is the stranded rows never coming back.
                await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);

                await AwaitBounded(Task.WhenAll(substream0.StopAsync(), substream1.StopAsync()), "coordinated stop");
            }
            finally
            {
                Base.Engine.Internal.StateMachine.StreamContext.CheckpointCommitHookForTests = null;
                Base.Engine.Internal.StateMachine.StreamContext.RestoreVersionForTests = null;
            }
        }

        /// <summary>
        /// Waits for a substream to commit a number of further cycles, so a test can put the
        /// pair past a version before it does anything else.
        /// </summary>
        private static async Task WaitForCommitsToAdvance(ConcurrentQueue<(string Stream, long Version)> commits, string substreamName, int count)
        {
            int Seen() => commits.Count(x => x.Stream.Contains(substreamName, StringComparison.Ordinal));
            var target = Seen() + count;
            var deadline = DateTime.UtcNow.AddSeconds(30);
            while (Seen() < target && DateTime.UtcNow < deadline)
            {
                await Task.Delay(50);
            }
            Assert.True(Seen() >= target, $"{substreamName} did not commit {count} more cycles within the deadline.");
        }

        private void DumpLogBuffers(string phase)
        {
            foreach (var buffer in _logBuffers)
            {
                buffer.Value.WriteToFile($"./debugwrite/clean_handoff_{phase}_{buffer.Key}.log");
            }
        }

        private Base.Engine.DataflowStream BuildSubstream(
            string testName,
            string substreamName,
            LocalSubstreamCommunicationHub hub,
            ConcurrentDictionary<string, KeepAliveMemoryFileProvider> fileProviders,
            ConcurrentDictionary<string, EventBatchData> latestData,
            ConcurrentBag<(string Substream, Exception? Exception)> failures,
            bool announceCleanHandoff,
            int egressCrashOnCheckpointCount = 0,
            int substreamCount = 2)
        {
            var connectorManager = new ConnectorManager();
            connectorManager.AddSource(new MockSourceFactory("*", _db, false));
            connectorManager.AddSink(new MockSinkFactory(
                "*",
                data => latestData[substreamName] = data,
                egressCrashOnCheckpointCount,
                _ => { }));

            var logProvider = _logBuffers.GetOrAdd(substreamName, _ => new RingBufferLoggerProvider());
            var builder = new FlowtideBuilder($"{testName.Length}_{testName}_{substreamName}")
                .AddPlan(CreateDistributedPlan(JoinSql, substreamCount), false)
                .WithStateOptions(CreateStateOptions(testName, substreamName, fileProviders))
                .AddConnectorManager(connectorManager);
            builder.WithLoggerFactory(Microsoft.Extensions.Logging.LoggerFactory.Create(b =>
            {
                b.SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Debug);
                b.AddProvider(logProvider);
            }));
            builder.WithFailureListener(e => failures.Add((substreamName, e)));
            builder.SetDistributedOptions(new DistributedOptions(
                substreamName,
                default,
                hub.CreateFactory(substreamName))
            {
                AnnounceCleanHandoff = announceCleanHandoff
            });

            var stream = builder.Build();
            lock (_streams)
            {
                _streams.Add(stream);
            }
            return stream;
        }

        /// <summary>
        /// Builds the distributed plan the same way for every substream instance; building a
        /// stream mutates the plan in place, so each build needs its own identical instance.
        /// </summary>
        private Plan CreateDistributedPlan(string sql, int substreamCount = 2)
        {
            var sqlPlanBuilder = new SqlPlanBuilder();
            sqlPlanBuilder.AddTableProvider(new DatasetTableProvider(_db));
            sqlPlanBuilder.Sql(sql);
            return PlanOptimizer.Optimize(sqlPlanBuilder.GetPlan(), new PlanOptimizerSettings()
            {
                DistributedPlanOptions = new DistributedPlanOptions()
                {
                    SubstreamCount = substreamCount
                }
            });
        }

        private static Storage.StateManager.StateManagerOptions CreateStateOptions(
            string testName,
            string substreamName,
            ConcurrentDictionary<string, KeepAliveMemoryFileProvider> fileProviders)
        {
            return new Storage.StateManager.StateManagerOptions()
            {
                CachePageCount = 100_000,
                PersistentStorage = new ReservoirPersistentStorage(new Storage.Persistence.Reservoir.ReservoirStorageOptions()
                {
                    // Shared between the instances of a substream so a rebuilt instance
                    // restores the state its predecessor persisted, like durable storage.
                    FileProvider = fileProviders.GetOrAdd(substreamName, _ => new KeepAliveMemoryFileProvider())
                }),
                DefaultBPlusTreePageSize = 1024,
                DefaultBPlusTreePageSizeBytes = 32 * 1024,
                TemporaryStorageOptions = new Storage.FileCacheOptions()
                {
                    DirectoryPath = $"./data/tempFiles/{testName}/{substreamName}/tmp/{Guid.NewGuid():N}"
                }
            };
        }

        private List<UserKeyRow> GetExpectedJoinResult()
        {
            return _generator.Orders
                .Join(_generator.Users, o => o.UserKey, u => u.UserKey, (o, u) => new UserKeyRow(u.UserKey))
                .ToList();
        }

        private static async Task AwaitBounded(Task task, string operation)
        {
            var finished = await Task.WhenAny(task, Task.Delay(TimeSpan.FromSeconds(60)));
            Assert.True(finished == task, $"The {operation} timed out");
            await task;
        }

        private static async Task WaitForSinkData<T>(
            ConcurrentDictionary<string, EventBatchData> latestData,
            ConcurrentBag<(string Substream, Exception? Exception)> failures,
            string substreamName,
            List<T> expected,
            bool allowFailures = false)
        {
            var expectedBatch = BatchConverter.ConvertToBatchSorted(expected, GlobalMemoryManager.Instance);

            var stopwatch = Stopwatch.StartNew();
            while (true)
            {
                if (!allowFailures)
                {
                    var failure = failures.FirstOrDefault(x => x.Exception != null);
                    if (failure.Exception != null)
                    {
                        throw new Exception($"Substream {failure.Substream} failed", failure.Exception);
                    }
                }

                if (latestData.TryGetValue(substreamName, out var actual))
                {
                    try
                    {
                        EventBatchAssertion.Equal(expectedBatch, actual);
                        return;
                    }
                    catch when (stopwatch.Elapsed < TimeSpan.FromSeconds(60))
                    {
                        // Not the expected data yet, retry until the deadline.
                    }
                }
                if (stopwatch.Elapsed >= TimeSpan.FromSeconds(60))
                {
                    Assert.Fail($"Substream {substreamName} did not produce the expected data within the deadline.");
                }
                await Task.Delay(100);
            }
        }
    }
}
