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

using FlowtideDotNet.Core.Engine.Distributed;
using FlowtideDotNet.Core.Operators.Exchange;
using Microsoft.Extensions.Logging.Abstractions;

namespace FlowtideDotNet.Core.Tests.Exchange
{
    /// <summary>
    /// Send-side half of the checkpoint-done ack fencing. A failure must bump the substream's
    /// checkpoint epoch and the following handshake must announce the new value, so a peer's ack
    /// tagged with the epoch from before the failure is recognisable as stale afterwards. Combined
    /// with the receive-side drop (DistributedTests.StaleEpochCheckpointDoneAcksDoNotCompleteCheckpoints)
    /// this covers the full fence without driving a real crash recovery.
    /// </summary>
    public class SubstreamCheckpointEpochTests
    {
        private sealed class RecordingHandler : ISubstreamCommunicationHandler
        {
            public List<long> AnnouncedCheckpointEpochs { get; } = new List<long>();

            public void Initialize(
                Func<IReadOnlySet<int>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>> getDataFunction,
                Func<long, Task> callFailAndRecover,
                Func<long, long, Task<SubstreamInitializeResponse>> initializeFromTarget,
                Func<long, long, Task> callRecieveCheckpointDone)
            {
            }

            public Task<SubstreamInitializeResponse> SendInitializeRequest(long restoreVersion, long checkpointEpoch, CancellationToken cancellationToken)
            {
                AnnouncedCheckpointEpochs.Add(checkpointEpoch);
                return Task.FromResult(new SubstreamInitializeResponse(false, true, restoreVersion));
            }

            public Task SendCheckpointDone(long checkpointVersion, long targetCheckpointEpoch) => Task.CompletedTask;

            public Task SendFailAndRecover(long restoreVersion) => Task.CompletedTask;

            public Task<IReadOnlyList<SubstreamEventData>> FetchData(IReadOnlySet<int> targetIds, int numberOfEvents, CancellationToken cancellationToken)
                => throw new NotSupportedException();
        }

        [Fact]
        public async Task FailureBumpsAndReannouncesTheCheckpointEpoch()
        {
            var handler = new RecordingHandler();
            var point = new SubstreamCommunicationPoint(NullLogger.Instance, "self", "target", handler);

            // First handshake announces the initial epoch.
            await point.InitializeOperator(0);

            // A real failure bumps the epoch; the point survives, so the counter is monotonic.
            point.OnStreamFailure();

            // The next handshake (as after a restart) announces the new epoch.
            await point.InitializeOperator(0);

            Assert.Equal(2, handler.AnnouncedCheckpointEpochs.Count);
            Assert.True(
                handler.AnnouncedCheckpointEpochs[1] > handler.AnnouncedCheckpointEpochs[0],
                $"a failure must bump the announced checkpoint epoch: {handler.AnnouncedCheckpointEpochs[0]} -> {handler.AnnouncedCheckpointEpochs[1]}");
        }

        /// <summary>
        /// Both halves of the fence, exercised through two real communication points wired to each
        /// other by the real in process hub, nothing injected. It reproduces the actual timing: when
        /// this stream (A) fails it bumps its epoch and then only best effort notifies the peer (B),
        /// so there is a window where B still holds A's old epoch and completes a checkpoint. The ack
        /// B sends in that window carries the epoch it genuinely learned through the handshake, and
        /// arrives at A after A already moved on. Crediting it would complete a checkpoint dependency
        /// the current generation never acked, so the fence must drop it. After B re handshakes and
        /// learns the new epoch its next ack must be credited again, proving the fence discriminates
        /// by epoch rather than blocking everything.
        ///
        /// The recording target stands in for the exchange target the ack drives in production, whose
        /// TargetSubstreamCheckpointDone calls SetDependenciesDone; a credited ack fires it, a dropped
        /// one does not.
        /// </summary>
        [Fact]
        public async Task StaleAckFromABeforeBRehandshakesIsDroppedThroughTheRealHub()
        {
            var hub = new LocalSubstreamCommunicationHub();
            var handlerA = hub.CreateFactory("subA").GetCommunicationHandler("subB", "subA");
            var handlerB = hub.CreateFactory("subB").GetCommunicationHandler("subA", "subB");

            var pointA = new SubstreamCommunicationPoint(NullLogger.Instance, "subA", "subB", handlerA);
            var pointB = new SubstreamCommunicationPoint(NullLogger.Instance, "subB", "subA", handlerB);

            // The exchange target A drives when B's checkpoint done arrives; the action is what
            // credits the cross substream dependency in production.
            int credited = 0;
            _ = new SubstreamTarget(1, 1, pointA, () => Interlocked.Increment(ref credited));

            // Real handshake: B learns A's current epoch (the value it will tag its acks with).
            await pointA.InitializeOperator(0);
            await pointB.InitializeOperator(0);

            // A fails and restarts. This bumps A's epoch synchronously; B is only notified best
            // effort and has not re handshaked, so it still holds A's pre failure epoch.
            pointA.OnStreamFailure();

            // B completes a checkpoint in that window and acks it, tagged with the stale epoch.
            await pointB.SendCheckpointDone(5);
            Assert.Equal(0, Volatile.Read(ref credited));

            // B re handshakes with the restarted A and learns the new epoch.
            await pointA.InitializeOperator(0);

            // Its next ack now carries the current epoch and must be credited.
            await pointB.SendCheckpointDone(6);
            Assert.Equal(1, Volatile.Read(ref credited));
        }

        /// <summary>
        /// A handshake announcement from an aborted generation must not regress the peer epoch a
        /// newer handshake already installed. The pre-failure SendInitializeRequest retry loop
        /// captures the self epoch once before the loop and survives OnStreamFailure (nothing
        /// cancels it, it retries with sleeps for up to ~55s), so a late retry re-announces the
        /// aborted generation's epoch after the restarted generation already announced the
        /// current one. Installing it regresses the peer's record: every ack the peer sends
        /// afterwards is tagged with the aborted epoch and dropped by the receive fence, so no
        /// checkpoint dependency completes again while data keeps flowing - the fetch loop stays
        /// healthy, so no watchdog ever fires and the stall is permanent and silent.
        /// </summary>
        [Fact]
        public async Task LateHandshakeFromAbortedGenerationDoesNotRegressThePeerEpoch()
        {
            var hub = new LocalSubstreamCommunicationHub();
            var handlerA = hub.CreateFactory("subA").GetCommunicationHandler("subB", "subA");
            var handlerB = hub.CreateFactory("subB").GetCommunicationHandler("subA", "subB");

            var pointA = new SubstreamCommunicationPoint(NullLogger.Instance, "subA", "subB", handlerA);
            var pointB = new SubstreamCommunicationPoint(NullLogger.Instance, "subB", "subA", handlerB);

            int credited = 0;
            _ = new SubstreamTarget(1, 1, pointA, () => Interlocked.Increment(ref credited));

            await pointA.InitializeOperator(0);
            await pointB.InitializeOperator(0);

            // A fails and restarts; the restarted generation re-handshakes, so B now holds A's
            // current (bumped) epoch.
            pointA.OnStreamFailure();
            await pointA.InitializeOperator(0);

            // The aborted generation's handshake retry captured its self epoch (the initial 0)
            // before the failure and lands late, re-announcing it after the fresh handshake.
            await handlerA.SendInitializeRequest(0, 0, default);

            // B completes a checkpoint. Its ack must carry A's current epoch and be credited; a
            // regressed record tags it with the aborted epoch and the fence drops it.
            await pointB.SendCheckpointDone(5);
            Assert.Equal(1, Volatile.Read(ref credited));
        }

        /// <summary>
        /// The fence must also cover hard restarts - the cross-generation case is its whole
        /// purpose. A hard crash (process kill, silo loss, grain reactivation) never runs
        /// OnStreamFailure; the rebuilt stream constructs a brand new communication point whose
        /// self epoch starts at the same initial value the dead generation announced. An ack
        /// from the peer that was in flight across the crash carries the dead generation's
        /// epoch, matches the fresh point's initial epoch, and is credited into the new
        /// generation - completing a post-restart checkpoint dependency the peer never acked
        /// for this generation, exactly what RecieveCheckpointDone's drop comment says must not
        /// happen. The fetch epoch is seeded from the clock so fresh instances never collide;
        /// the checkpoint epoch needs the same property (or an equivalent guard).
        /// </summary>
        [Fact]
        public async Task AckInFlightAcrossAHardRestartIsNotCreditedToTheNewGeneration()
        {
            var hub = new LocalSubstreamCommunicationHub();
            var handlerA = hub.CreateFactory("subA").GetCommunicationHandler("subB", "subA");
            var handlerB = hub.CreateFactory("subB").GetCommunicationHandler("subA", "subB");

            var pointA = new SubstreamCommunicationPoint(NullLogger.Instance, "subA", "subB", handlerA);
            var pointB = new SubstreamCommunicationPoint(NullLogger.Instance, "subB", "subA", handlerB);

            // Real handshake: B learns the first generation's epoch, the value it tags acks with.
            await pointA.InitializeOperator(0);
            await pointB.InitializeOperator(0);

            // A hard-crashes: no OnStreamFailure runs in a killed process. The host rebuilds the
            // stream from scratch - a brand new point on the same substream name, as a grain
            // reactivation does - and messages route to the new instance by name.
            int credited = 0;
            var handlerA2 = hub.CreateFactory("subA").GetCommunicationHandler("subB", "subA");
            var pointA2 = new SubstreamCommunicationPoint(NullLogger.Instance, "subA", "subB", handlerA2);
            _ = new SubstreamTarget(1, 1, pointA2, () => Interlocked.Increment(ref credited));

            // B has not learned of the crash; its in-flight ack carries the dead generation's
            // epoch and is delivered to the new activation.
            await pointB.SendCheckpointDone(5);

            // The pre-crash ack belongs to a generation whose state was rolled back; crediting it
            // completes the new generation's first checkpoint dependency without a real ack.
            Assert.Equal(0, Volatile.Read(ref credited));

            // After B re-handshakes with the new generation its acks must be credited again, the
            // fence discriminates by generation rather than dropping everything.
            await pointA2.InitializeOperator(0);
            await pointB.SendCheckpointDone(6);
            Assert.Equal(1, Volatile.Read(ref credited));
        }

        /// <summary>
        /// The highest-wins guard on the peer epoch record assumes a peer's generations only
        /// ever announce higher epochs. Generations are clock-seeded per process, so after a
        /// hard fail over onto a process whose clock seed is behind the dead generation's,
        /// the LIVE peer announces a LOWER epoch: the guard keeps the dead generation's
        /// record, every ack gets tagged with it, and the live peer drops them all - a
        /// silent, permanent checkpoint stall, data keeps flowing so no watchdog fires. Like
        /// the fetch epoch's failover escape, the handshake response must let the announcer
        /// detect that a higher epoch is recorded for it and re-seed above it, so its acks
        /// are credited again.
        /// </summary>
        [Fact]
        public async Task FailedOverPeerWithLowerEpochIsNotPermanentlyFencedByTheHighestWinsGuard()
        {
            var hub = new LocalSubstreamCommunicationHub();
            var handlerA = hub.CreateFactory("subA").GetCommunicationHandler("subB", "subA");
            var handlerB = hub.CreateFactory("subB").GetCommunicationHandler("subA", "subB");

            var pointB = new SubstreamCommunicationPoint(NullLogger.Instance, "subB", "subA", handlerB);

            // The dead generation of A ran on a process whose clock seeded its epochs a day
            // ahead; its announcement is what B has recorded when A fails over.
            await handlerA.SendInitializeRequest(0, DateTime.UtcNow.Ticks + TimeSpan.FromDays(1).Ticks, default);

            // The failed-over live A: a fresh point whose clock-seeded epoch is far below
            // the dead generation's announcement.
            int credited = 0;
            var pointA = new SubstreamCommunicationPoint(NullLogger.Instance, "subA", "subB", handlerA);
            _ = new SubstreamTarget(1, 1, pointA, () => Interlocked.Increment(ref credited));
            await pointA.InitializeOperator(0);

            // B completes a checkpoint and acks it, tagged with the epoch it has recorded
            // for A. If the dead generation's record still stands the ack is dropped, and
            // since A re-announces the same lower epoch on every recovery, it would never be
            // credited again.
            await pointB.SendCheckpointDone(5);
            Assert.Equal(1, Volatile.Read(ref credited));
        }
    }
}
