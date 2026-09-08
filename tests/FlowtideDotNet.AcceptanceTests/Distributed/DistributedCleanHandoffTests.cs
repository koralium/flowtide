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

            // The handoff a migrating grain runs: stop at a final checkpoint the peer
            // acknowledges, dispose. The peer keeps running.
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
        /// nor roll anything back: the stop barrier lands behind a running cycle on both sides
        /// and still has to pair with the peer's answer. Runs several rounds to widen the window.
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
                // Checkpoints racing the stop on both sides: the triggers are not awaited so
                // the barriers are in flight when the stop begins.
                _ = substream0.TriggerCheckpoint();
                _ = substream1.TriggerCheckpoint();

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

            SubstreamReadOperator.PairedCheckpointHookForTests = (streamName, peerVersion, localVersion) =>
            {
                if (streamName.Contains(testName, StringComparison.Ordinal))
                {
                    pairings.Enqueue((streamName, peerVersion, localVersion));
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
                // Tight bound, AwaitBounded would hide a reintroduced stop drain timeout.
                var handoffStop = Stopwatch.StartNew();
                await AwaitBounded(substream1.StopAsync(), "handoff stop");
                handoffStop.Stop();
                Assert.True(
                    handoffStop.Elapsed < FastEngineTimings.StopDrainTimeout,
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

                await AwaitBounded(Task.WhenAll(substream0.StopAsync(), substream1.StopAsync()), "coordinated stop");
            }
            finally
            {
                SubstreamReadOperator.PairedCheckpointHookForTests = null;
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
                    // Held open by the peer's own commits, not a sleep: each committed cycle is
                    // a barrier and a batch it produced for us, so waiting for two of them puts
                    // real rows in flight at the stop instead of hoping they land there.
                    await WaitForCommitsToAdvance(commits, "substream_0", 2);
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
                    await WaitForCommitsToAdvance(commits, "substream_0", 2);
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

                // The sink only publishes on a watermark, and an idle stream sends none, so it
                // would sit on whatever it last published. Nudge it so the final state comes out.
                _generator.Generate(50);

                // Nothing below means anything unless the injected failure is what landed.
                bool InjectedFailureSeen() => failures.Any(f => f.Exception != null && f.Exception.ToString().Contains(InjectedFailureMessage, StringComparison.Ordinal));
                await WaitUntil(
                    InjectedFailureSeen,
                    () => $"the injected failure to be reported, so nothing rolled back and the run proves nothing. Reported: {string.Join(" | ", failures.Where(f => f.Exception != null).Select(f => f.Substream + ": " + f.Exception!.Message))}");

                // The substream that failed has to have actually rolled back. Only it takes the
                // failure path, a rebuilt peer restores through its own start instead.
                await WaitUntil(
                    () => restores.Any(r => r.Stream.Contains("substream_0", StringComparison.Ordinal)),
                    () => $"the staying substream to roll back. Saw: {string.Join(", ", restores.Select(r => r.Stream + "=" + r.Version))}");

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
                try
                {
                    await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);
                }
                catch (Exception sinkFailure)
                {
                    // Rare and load dependent, so the logs have to come from the run that failed.
                    DumpLogBuffers($"rows_in_flight_{mode}");
                    if (latestData.TryGetValue("substream_0", out var actualRows))
                    {
                        using var expectedRows = BatchConverter.ConvertToBatchSorted(GetExpectedJoinResult(), GlobalMemoryManager.Instance);
                        throw new Exception($"{sinkFailure.Message} | {DescribeRowDifference(expectedRows, actualRows)}", sinkFailure);
                    }
                    throw;
                }

                await AwaitBounded(Task.WhenAll(substream0.StopAsync(), substream1.StopAsync()), "coordinated stop");
            }
            finally
            {
                Base.Engine.Internal.StateMachine.StreamContext.CheckpointCommitHookForTests = null;
                Base.Engine.Internal.StateMachine.StreamContext.RestoreVersionForTests = null;
            }
        }

        // Past the alignment escape, inside the drain timeout.
        private static readonly TimeSpan LatePeerInterval = TimeSpan.FromMilliseconds(3200);
        // Long enough that a stop lands while a peer barrier is parked.
        private static readonly TimeSpan ParkedBarrierInterval = TimeSpan.FromSeconds(3);

        private sealed record HandoffObservation(
            ConcurrentDictionary<string, EventBatchData> LatestData,
            ConcurrentBag<(string Substream, Exception? Exception)> Failures,
            List<(string Stream, long Peer, long Local)> PairingsDuringStop,
            TimeSpan StopDuration,
            RingBufferLoggerProvider LeavingLog);

        /// <summary>
        /// Finding: a stop that gave up waiting forwarded its barrier alone and committed, and
        /// a later drain cycle then paired the peer's late barrier at a version the stream
        /// never commits. The stop must wait for the answer and pair at the shared version.
        /// </summary>
        [Fact]
        public async Task StopDrainCycleMustNotPairAtAVersionItNeverCommits()
        {
            var run = await RunHandoffUnderLoad("e2e_drain_attest", LatePeerInterval, null, TimeSpan.FromMilliseconds(100));

            Assert.True(
                run.StopDuration > FastEngineTimings.StopDrainTimeout / 2,
                $"The peer answered within {run.StopDuration}, the late answer was not exercised.");
            Assert.True(run.PairingsDuringStop.Count > 0, "The stop never paired the late peer barrier.");
            var mismatched = run.PairingsDuringStop.Where(p => p.Peer != p.Local).ToList();
            Assert.True(
                mismatched.Count == 0,
                $"A stop drain cycle paired the peer's barrier at a version it never commits and attested it as covered: {string.Join("; ", mismatched.Select(p => $"peer={p.Peer} local={p.Local}"))}");
        }

        /// <summary>
        /// Finding: giving up on the peer's barrier by forwarding the stop without it committed
        /// a cut the peer never took and kept fetching past it. A peer that never answers must
        /// fail the stop instead, so both substreams recover to the checkpoint they share.
        /// </summary>
        [Fact]
        public async Task StopWithoutAPeerAnswerMustNotCommitAnUnmatchedCut()
        {
            var testName = "e2e_unanswered_stop";
            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var fileProviders = new ConcurrentDictionary<string, KeepAliveMemoryFileProvider>();
            var hub = new LocalSubstreamCommunicationHub();
            var commits = new ConcurrentQueue<(string Stream, long Version)>();
            var pairings = new ConcurrentQueue<(string Stream, long Peer, long Local)>();
            var restores = new ConcurrentQueue<(string Stream, long Version)>();
            var peerGate = new FetchGate();

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
            SubstreamReadOperator.PairedCheckpointHookForTests = (streamName, peerVersion, localVersion) =>
            {
                if (streamName.Contains(testName, StringComparison.Ordinal))
                {
                    pairings.Enqueue((streamName, peerVersion, localVersion));
                }
            };
            try
            {
                var substream0 = BuildSubstream(testName, "substream_0", hub, fileProviders, latestData, failures, announceCleanHandoff: false,
                    communicationFactory: new GatedCommunicationFactory(hub.CreateFactory("substream_0"), peerGate));
                var substream1 = BuildSubstream(testName, "substream_1", hub, fileProviders, latestData, failures, announceCleanHandoff: false);
                await substream0.StartAsync();
                await substream1.StartAsync();
                await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());
                // Quiet, then the peer's fetch is held: it never sees the stop barrier and never answers.
                await Task.Delay(500);
                peerGate.Close();

                var pairingsBefore = pairings.ToArray().Length;
                var commitsBefore = commits.ToArray().Length;
                var stopwatch = Stopwatch.StartNew();
                await AwaitBounded(substream1.StopAsync(), "stop against a peer that never answers");
                stopwatch.Stop();
                var pairingsDuringStop = pairings.ToArray().Skip(pairingsBefore)
                    .Where(p => p.Stream.Contains("substream_1", StringComparison.Ordinal))
                    .ToList();
                var cyclesDuringStop = commits.ToArray().Skip(commitsBefore)
                    .Where(c => c.Stream.Contains("substream_1", StringComparison.Ordinal))
                    .ToList();
                // Reopen before the restart below needs the peer, and before the asserts so a
                // failing one cannot leave the peer's fetch parked in the gate. Idempotent.
                peerGate.Open();

                Assert.True(
                    stopwatch.Elapsed >= FastEngineTimings.StopDrainTimeout,
                    $"The stop finished after {stopwatch.Elapsed} without the peer's barrier, before the drain timeout.");
                Assert.True(pairingsDuringStop.Count == 0, "The stop paired a barrier the peer never sent.");
                Assert.True(
                    cyclesDuringStop.Count == 0,
                    $"The stop committed {cyclesDuringStop.Count} cycle(s) without the peer's barrier, a cut the peer never took.");
                Assert.NotEmpty(_logBuffers["substream_1"].LinesContaining("failing the stop so both substreams recover"));

                // Not a clean handoff, the peer never took the cut. Both must continue from one
                // version, with every row.
                await substream1.DisposeAsync();
                lock (_streams)
                {
                    _streams.Remove(substream1);
                }
                pairings.Clear();
                substream1 = BuildSubstream(testName, "substream_1", hub, fileProviders, latestData, failures, announceCleanHandoff: false);
                await substream1.StartAsync();
                _generator.Generate(250);
                await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);

                var rollbackVersions = restores.Select(r => r.Version).Distinct().ToList();
                Assert.True(
                    rollbackVersions.Count <= 1,
                    $"The substreams rolled back to different versions: {string.Join(", ", restores.Select(r => r.Stream + "=" + r.Version))}");
                var afterRestart = pairings.ToArray();
                Assert.True(afterRestart.Length > 0, "The substreams never paired a checkpoint after the recovery.");
                var mismatched = afterRestart.Where(p => p.Peer != p.Local).ToList();
                Assert.True(
                    mismatched.Count == 0,
                    $"The substreams continue on different versions after the failed stop: {string.Join("; ", mismatched.Select(p => $"{p.Stream} peer={p.Peer} local={p.Local}"))}");
            }
            finally
            {
                // Belt and braces if the stop itself timed out before the reopen above.
                peerGate.Open();
                Base.Engine.Internal.StateMachine.StreamContext.CheckpointCommitHookForTests = null;
                Base.Engine.Internal.StateMachine.StreamContext.RestoreVersionForTests = null;
                SubstreamReadOperator.PairedCheckpointHookForTests = null;
            }
        }

        /// <summary>
        /// Finding: a stop drain cycle that skips the commit still notifies checkpoint
        /// complete and re-delivers checkpoint done. Every notification must be backed by a
        /// version the stream actually wrote.
        /// </summary>
        [Fact]
        public async Task StopDrainCyclesWithoutACommitMustNotNotifyCheckpointComplete()
        {
            var testName = "e2e_drain_notify";
            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var fileProviders = new ConcurrentDictionary<string, KeepAliveMemoryFileProvider>();
            var hub = new LocalSubstreamCommunicationHub();
            var commits = new ConcurrentQueue<(string Stream, long Version)>();
            int notifications = 0;
            var peerGate = new FetchGate();

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
                var substream0 = BuildSubstream(testName, "substream_0", hub, fileProviders, latestData, failures, announceCleanHandoff: false,
                    communicationFactory: new GatedCommunicationFactory(hub.CreateFactory("substream_0"), peerGate));
                var substream1 = BuildSubstream(testName, "substream_1", hub, fileProviders, latestData, failures, announceCleanHandoff: false,
                    configure: builder =>
                    {
                        // A parked peer barrier pairs the stop at once, only the target waits.
                        builder.SetMinimumTimeBetweenCheckpoint(ParkedBarrierInterval);
                        builder.WithCheckpointListener(new NotificationReciever(() => Interlocked.Increment(ref notifications)));
                    });
                await substream0.StartAsync();
                await substream1.StartAsync();
                await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());

                using var loadDone = new CancellationTokenSource();
                var load = Task.Run(async () =>
                {
                    while (!loadDone.IsCancellationRequested)
                    {
                        _generator.Generate(25);
                        await Task.Delay(25);
                    }
                });
                await WaitForCommitsToAdvance(commits, "substream_1", 1);
                await Task.Delay(500);

                // The peer never fetches the stop barrier, so the drain polls until its timeout.
                peerGate.Close();
                var commitsBefore = commits.ToArray().Length;
                var notificationsBefore = Volatile.Read(ref notifications);
                await AwaitBounded(substream1.StopAsync(), "stop against a peer that never fetches");
                // The commit hook runs once per stop cycle, committed or not.
                var cyclesDuringStop = commits.ToArray().Skip(commitsBefore)
                    .Where(c => c.Stream.Contains("substream_1", StringComparison.Ordinal))
                    .Select(c => c.Version)
                    .ToList();
                var versionsSeen = cyclesDuringStop.Distinct().Count();
                var notificationsDuringStop = Volatile.Read(ref notifications) - notificationsBefore;
                peerGate.Open();
                loadDone.Cancel();
                await load;

                Assert.True(cyclesDuringStop.Count > 1, "The stop finished in a single cycle, no drain cycle ran.");
                Assert.True(
                    notificationsDuringStop <= versionsSeen,
                    $"The stop raised {notificationsDuringStop} checkpoint complete notifications while the commit hook reported only {versionsSeen} distinct versions, drain cycles that wrote nothing notified as if they had committed.");
            }
            finally
            {
                Base.Engine.Internal.StateMachine.StreamContext.CheckpointCommitHookForTests = null;
            }
        }

        /// <summary>
        /// Finding: the fetch cut is decided when a peer barrier is written to the channel,
        /// against a stop that may not be parked yet. A barrier already parked when the stop
        /// lands is paired by it, and everything fetched behind it is forwarded past the stop
        /// barrier into cycles that never commit.
        /// </summary>
        [Fact]
        public async Task StopMustNotForwardRowsFetchedBehindAParkedPeerBarrier()
        {
            var run = await RunHandoffUnderLoad("e2e_parked_cut", null, ParkedBarrierInterval, TimeSpan.FromMilliseconds(500));

            Assert.Empty(run.LeavingLog.LinesContaining("failing the stop so both substreams recover"));
            Assert.True(run.PairingsDuringStop.Count > 0, "The stop did not pair a parked peer barrier, the window was missed.");
            var leaked = run.LeavingLog.LinesContaining("rows after the stop barrier");
            Assert.True(
                leaked.Count == 0,
                $"Rows fetched behind the barrier the stop paired against were forwarded past the stop barrier and are committed by nobody: {string.Join(" | ", leaked.Take(3))}");
        }

        /// <summary>
        /// Finding: the sender only holds its queue for a stopping peer once its own read
        /// operator consumed that peer's stop barrier, which is after the peer already
        /// fetched past the barrier it pairs against. Those rows are handed out, never
        /// committed on the stopping side and never held for its return.
        /// </summary>
        [Fact]
        public async Task RowsBehindTheBarrierAStopPairedAgainstMustSurviveTheHandoff()
        {
            var run = await RunHandoffUnderLoad("e2e_parked_rows", null, ParkedBarrierInterval, TimeSpan.FromMilliseconds(500));

            Assert.True(run.PairingsDuringStop.Count > 0, "The stop did not pair a parked peer barrier, the window was missed.");
            await AssertRowsSurvived(run, "parked_rows");
            Assert.Empty(run.Failures);
        }

        /// <summary>
        /// Finding: a substream that is itself draining accepts a returning peer's clean
        /// handoff. The accept resets its reader's stop tracking and re-subscribes it, so the
        /// drain fetches the returned peer's rows into cycles that never commit, and the peer
        /// is told nothing needs resending. A stopping substream must answer retry instead.
        /// </summary>
        [Fact]
        public async Task StoppingSubstreamMustNotAcceptACleanHandoff()
        {
            var testName = "e2e_handoff_into_drain";
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

            // substream_1 hands off cleanly, the peer consumes its stop barrier. Quiet, so
            // the peer has no cycle left waiting on a gone substream when it is told to stop.
            await AwaitBounded(substream1.StopAsync(), "handoff stop");
            await substream1.DisposeAsync();
            lock (_streams)
            {
                _streams.Remove(substream1);
            }

            // The peer starts stopping with nobody left to fetch its stop barrier, so it
            // drains until its target gives up; the handoff returns into that drain.
            var peerStop = substream0.StopAsync();
            await WaitUntil(
                () => _logBuffers["substream_0"].LinesContaining("Stopping stream:").Count > 0,
                () => "the peer to enter its stop");
            substream1 = BuildSubstream(testName, "substream_1", hub, fileProviders, latestData, failures, announceCleanHandoff: true);
            await substream1.StartAsync();

            // Rows produced while the peer drains, what a resumed reader would dequeue.
            using var loadDone = new CancellationTokenSource();
            var load = Task.Run(async () =>
            {
                while (!loadDone.IsCancellationRequested)
                {
                    _generator.Generate(25);
                    await Task.Delay(25);
                }
            });
            await AwaitBounded(peerStop, "peer stop during the handoff return");
            loadDone.Cancel();
            await load;

            Assert.NotEmpty(_logBuffers["substream_0"].LinesContaining("while this stream is stopping, answering retry"));
            Assert.Empty(_logBuffers["substream_0"].LinesContaining("reconnected from a clean handoff"));

            // The peer comes back cleanly too. Every row must be there, without a failure
            // having regenerated them.
            await substream0.DisposeAsync();
            lock (_streams)
            {
                _streams.Remove(substream0);
            }
            substream0 = BuildSubstream(testName, "substream_0", hub, fileProviders, latestData, failures, announceCleanHandoff: true);
            await substream0.StartAsync();
            _generator.Generate(50);
            try
            {
                await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult(), allowFailures: true);
            }
            catch (Exception sinkFailure)
            {
                DumpLogBuffers("handoff_into_drain");
                if (latestData.TryGetValue("substream_0", out var actualRows))
                {
                    using var expectedRows = BatchConverter.ConvertToBatchSorted(GetExpectedJoinResult(), GlobalMemoryManager.Instance);
                    throw new Exception($"{sinkFailure.Message} | {DescribeRowDifference(expectedRows, actualRows)}", sinkFailure);
                }
                throw;
            }
        }

        /// <summary>
        /// Loads continuously, hands substream_1 off right after a commit on the clamped
        /// side, brings it back through a clean handoff and records what the stop did. A
        /// clamp on the staying peer delays its answer past the alignment escape, one on the
        /// leaving side leaves a peer barrier parked at its read operator when the stop lands.
        /// </summary>
        private async Task<HandoffObservation> RunHandoffUnderLoad(
            string testName,
            TimeSpan? stayingInterval,
            TimeSpan? leavingInterval,
            TimeSpan settleBeforeStop)
        {
            _generator.Generate(500);

            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();
            var fileProviders = new ConcurrentDictionary<string, KeepAliveMemoryFileProvider>();
            var hub = new LocalSubstreamCommunicationHub();
            var commits = new ConcurrentQueue<(string Stream, long Version)>();
            var pairings = new ConcurrentQueue<(string Stream, long Peer, long Local)>();

            Base.Engine.Internal.StateMachine.StreamContext.CheckpointCommitHookForTests = (streamName, lastVersion) =>
            {
                if (streamName.Contains(testName, StringComparison.Ordinal))
                {
                    commits.Enqueue((streamName, lastVersion));
                }
                return Task.CompletedTask;
            };
            SubstreamReadOperator.PairedCheckpointHookForTests = (streamName, peerVersion, localVersion) =>
            {
                if (streamName.Contains(testName, StringComparison.Ordinal))
                {
                    pairings.Enqueue((streamName, peerVersion, localVersion));
                }
            };
            try
            {
                Action<FlowtideBuilder> configureStaying = builder =>
                {
                    if (stayingInterval.HasValue)
                    {
                        builder.SetMinimumTimeBetweenCheckpoint(stayingInterval.Value);
                    }
                };
                Action<FlowtideBuilder> configureLeaving = builder =>
                {
                    if (leavingInterval.HasValue)
                    {
                        builder.SetMinimumTimeBetweenCheckpoint(leavingInterval.Value);
                    }
                };
                var substream0 = BuildSubstream(testName, "substream_0", hub, fileProviders, latestData, failures, announceCleanHandoff: false, configure: configureStaying);
                var substream1 = BuildSubstream(testName, "substream_1", hub, fileProviders, latestData, failures, announceCleanHandoff: false, configure: configureLeaving);
                await substream0.StartAsync();
                await substream1.StartAsync();

                await WaitForSinkData(latestData, failures, "substream_0", GetExpectedJoinResult());

                using var loadDone = new CancellationTokenSource();
                var load = Task.Run(async () =>
                {
                    while (!loadDone.IsCancellationRequested)
                    {
                        _generator.Generate(25);
                        await Task.Delay(25);
                    }
                });

                // Right after a commit on the clamped side its next cycle is a full interval away.
                var clampedSide = stayingInterval.HasValue ? "substream_0" : "substream_1";
                await WaitForCommitsToAdvance(commits, clampedSide, 1);
                await Task.Delay(settleBeforeStop);

                var pairingsBefore = pairings.ToArray().Length;

                var stopwatch = Stopwatch.StartNew();
                await AwaitBounded(substream1.StopAsync(), "handoff stop");
                stopwatch.Stop();

                var pairingsDuringStop = pairings.ToArray().Skip(pairingsBefore)
                    .Where(p => p.Stream.Contains("substream_1", StringComparison.Ordinal))
                    .ToList();

                await substream1.DisposeAsync();
                lock (_streams)
                {
                    _streams.Remove(substream1);
                }

                substream1 = BuildSubstream(testName, "substream_1", hub, fileProviders, latestData, failures, announceCleanHandoff: true, configure: configureLeaving);
                await substream1.StartAsync();

                loadDone.Cancel();
                await load;
                // The sink publishes on a watermark, nudge the final state out.
                _generator.Generate(50);

                return new HandoffObservation(latestData, failures, pairingsDuringStop, stopwatch.Elapsed, _logBuffers["substream_1"]);
            }
            finally
            {
                Base.Engine.Internal.StateMachine.StreamContext.CheckpointCommitHookForTests = null;
                SubstreamReadOperator.PairedCheckpointHookForTests = null;
            }
        }

        /// <summary>
        /// Every generated row must be in the sink after the handoff, with nothing failed.
        /// </summary>
        private async Task AssertRowsSurvived(HandoffObservation run, string phase)
        {
            try
            {
                await WaitForSinkData(run.LatestData, run.Failures, "substream_0", GetExpectedJoinResult());
            }
            catch (Exception sinkFailure)
            {
                DumpLogBuffers(phase);
                if (run.LatestData.TryGetValue("substream_0", out var actualRows))
                {
                    using var expectedRows = BatchConverter.ConvertToBatchSorted(GetExpectedJoinResult(), GlobalMemoryManager.Instance);
                    throw new Exception($"{sinkFailure.Message} | {DescribeRowDifference(expectedRows, actualRows)}", sinkFailure);
                }
                throw;
            }
        }

        /// <summary>
        /// Holds a substream's fetches while closed, so a test can order the peer's commit
        /// before the dequeue of the barrier that commit snapshots.
        /// </summary>
        private sealed class FetchGate
        {
            private volatile TaskCompletionSource _open = Completed();

            private static TaskCompletionSource Completed()
            {
                var source = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                source.SetResult();
                return source;
            }

            public void Close()
            {
                _open = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            public void Open()
            {
                _open.TrySetResult();
            }

            public Task WaitAsync(CancellationToken cancellationToken)
            {
                return _open.Task.WaitAsync(cancellationToken);
            }
        }

        private sealed class GatedCommunicationFactory : ISubstreamCommunicationHandlerFactory
        {
            private readonly ISubstreamCommunicationHandlerFactory _inner;
            private readonly FetchGate _gate;

            public GatedCommunicationFactory(ISubstreamCommunicationHandlerFactory inner, FetchGate gate)
            {
                _inner = inner;
                _gate = gate;
            }

            public ISubstreamCommunicationHandler GetCommunicationHandler(string targetSubstreamName, string selfSubstreamName)
            {
                return new GatedHandler(_inner.GetCommunicationHandler(targetSubstreamName, selfSubstreamName), _gate);
            }
        }

        private sealed class GatedHandler : ISubstreamCommunicationHandler
        {
            private readonly ISubstreamCommunicationHandler _inner;
            private readonly FetchGate _gate;

            public GatedHandler(ISubstreamCommunicationHandler inner, FetchGate gate)
            {
                _inner = inner;
                _gate = gate;
            }

            public void SetReceiveAllocatorResolver(Func<int, IMemoryAllocator> allocatorResolver)
            {
                _inner.SetReceiveAllocatorResolver(allocatorResolver);
            }

            public void OnStreamFailure()
            {
                _inner.OnStreamFailure();
            }

            public void Initialize(
                Func<IReadOnlySet<int>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>> getDataFunction,
                Func<long, Task> callFailAndRecover,
                Func<long, long, bool, Task<SubstreamInitializeResponse>> initializeFromTarget,
                Func<long, long, bool, Task> callRecieveCheckpointDone)
            {
                _inner.Initialize(getDataFunction, callFailAndRecover, initializeFromTarget, callRecieveCheckpointDone);
            }

            public async Task<IReadOnlyList<SubstreamEventData>> FetchData(IReadOnlySet<int> targetIds, int numberOfEvents, CancellationToken cancellationToken)
            {
                await _gate.WaitAsync(cancellationToken);
                return await _inner.FetchData(targetIds, numberOfEvents, cancellationToken);
            }

            public Task SendFailAndRecover(long restoreVersion)
            {
                return _inner.SendFailAndRecover(restoreVersion);
            }

            public Task<SubstreamInitializeResponse> SendInitializeRequest(long restoreVersion, long checkpointEpoch, bool cleanHandoff, CancellationToken cancellationToken)
            {
                return _inner.SendInitializeRequest(restoreVersion, checkpointEpoch, cleanHandoff, cancellationToken);
            }

            public Task SendCheckpointDone(long checkpointVersion, long targetCheckpointEpoch, bool coversPeerStopBarrier)
            {
                return _inner.SendCheckpointDone(checkpointVersion, targetCheckpointEpoch, coversPeerStopBarrier);
            }
        }

        /// <summary>
        /// Waits for a substream to commit a number of further cycles, so a test can put the
        /// pair past a version before it does anything else.
        /// </summary>
        private static Task WaitForCommitsToAdvance(ConcurrentQueue<(string Stream, long Version)> commits, string substreamName, int count)
        {
            int Seen() => commits.Count(x => x.Stream.Contains(substreamName, StringComparison.Ordinal));
            var target = Seen() + count;
            return WaitUntil(() => Seen() >= target, () => $"{substreamName} to commit {count} more cycles, it committed {Seen() - (target - count)}");
        }

        /// <summary>
        /// Waits for something to be observed rather than for a duration. The deadline is only a
        /// hang guard: reaching it means the thing never happened, not that the wait was too short.
        /// </summary>
        private static async Task WaitUntil(Func<bool> condition, Func<string> describeWhat)
        {
            var hangGuard = DateTime.UtcNow.AddSeconds(60);
            while (!condition())
            {
                if (DateTime.UtcNow >= hangGuard)
                {
                    Assert.Fail($"Hung waiting for {describeWhat()}.");
                }
                await Task.Delay(25);
            }
        }

        /// <summary>
        /// Names which rows are wrong, not just how many. A cut the two substreams disagree on
        /// leaves gaps and duplicates at the same time, so the direction of each difference is
        /// what tells them apart. Both batches are sorted, so one merge walk finds both.
        /// </summary>
        private string DescribeRowDifference(EventBatchData expected, EventBatchData actual)
        {
            var comparer = new FlowtideDotNet.Core.ColumnStore.Comparers.DataValueComparer();
            var missing = new List<string>();
            var extra = new List<string>();
            int i = 0;
            int j = 0;
            while (i < expected.Count && j < actual.Count)
            {
                var order = comparer.Compare(expected.Columns[0].GetValueAt(i, default), actual.Columns[0].GetValueAt(j, default));
                if (order == 0)
                {
                    i++;
                    j++;
                }
                else if (order < 0)
                {
                    missing.Add(expected.Columns[0].GetValueAt(i, default).ToString() ?? "?");
                    i++;
                }
                else
                {
                    extra.Add(actual.Columns[0].GetValueAt(j, default).ToString() ?? "?");
                    j++;
                }
            }
            for (; i < expected.Count; i++)
            {
                missing.Add(expected.Columns[0].GetValueAt(i, default).ToString() ?? "?");
            }
            for (; j < actual.Count; j++)
            {
                extra.Add(actual.Columns[0].GetValueAt(j, default).ToString() ?? "?");
            }
            // The key alone says nothing, its position in the generated order says which
            // source fetch window it came from and so which side of the handoff lost it.
            var origins = missing.Distinct().Select(k => $"{k}@{_generator.Users.FindIndex(u => u.UserKey.ToString() == k)}");
            return $"missing {missing.Count} [{string.Join(",", missing.Take(25))}] gen [{string.Join(",", origins.Take(25))}] extra {extra.Count} [{string.Join(",", extra.Take(25))}]";
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
            int substreamCount = 2,
            Action<FlowtideBuilder>? configure = null,
            ISubstreamCommunicationHandlerFactory? communicationFactory = null)
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
                communicationFactory ?? hub.CreateFactory(substreamName))
            {
                AnnounceCleanHandoff = announceCleanHandoff
            });
            configure?.Invoke(builder);

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
