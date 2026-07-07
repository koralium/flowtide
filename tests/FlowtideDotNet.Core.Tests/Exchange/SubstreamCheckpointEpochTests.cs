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
    }
}
