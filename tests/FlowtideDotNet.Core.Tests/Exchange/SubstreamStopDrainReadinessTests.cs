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

using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Core.Engine.Distributed;
using FlowtideDotNet.Core.Operators.Exchange;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Core.Tests.Exchange
{
    /// <summary>
    /// A substream target may only report ready to stop once the events up to and including
    /// its stop barrier have actually reached the other substream. Fetching removes events
    /// from the transient queue, but the fetch response can still be lost after the dequeue:
    /// the serving grain's wire serializer can throw, or the cross-silo call can fail in
    /// transit, and both release the already-dequeued events. Latching readiness at the
    /// dequeue would then let the stop complete claiming a full drain while the peer never
    /// received the pre-barrier events. The real confirmation is the peer's checkpoint done
    /// acknowledgement arriving AFTER the barrier was handed out: the peer only acks once it
    /// committed a checkpoint cycle, and the cycle that paired with the fetched stop barrier
    /// covers everything up to it.
    /// </summary>
    public class SubstreamStopDrainReadinessTests
    {
        private static async Task<IStateManagerClient> CreateStateClient()
        {
            var stateManager = new StateManagerSync<StreamState>(
                new StateManagerOptions(),
                NullLoggerFactory.Instance,
                new Meter("statemanager"),
                "stream",
                GlobalMemoryManager.Instance);
            await stateManager.InitializeAsync();
            return stateManager.GetOrCreateClient("target");
        }

        [Fact]
        public async Task FetchLostOnTheWireDoesNotMakeTheTargetReadyToStop()
        {
            var hub = new LocalSubstreamCommunicationHub();
            var handlerA = hub.CreateFactory("subA").GetCommunicationHandler("subB", "subA");
            var handlerB = hub.CreateFactory("subB").GetCommunicationHandler("subA", "subB");

            var pointA = new SubstreamCommunicationPoint(NullLogger.Instance, "subA", "subB", handlerA);
            var pointB = new SubstreamCommunicationPoint(NullLogger.Instance, "subB", "subA", handlerB);

            var target = new SubstreamTarget(1, 1, pointA, () => { });
            await target.Initialize(0, 1, await CreateStateClient(), new ExchangeOperatorState(), GlobalMemoryManager.Instance, _ => Task.CompletedTask);
            await pointB.InitializeOperator(0);

            // The stream is stopping: the stop barrier is stored, the target must hold the
            // stop until the other substream received it.
            await target.OnLockingEvent(new StopStreamCheckpoint(1, 2));
            Assert.False(target.ReadyToStop);

            // The peer's fetch dequeues the barrier, but the response is lost after the
            // dequeue: the serving side releases the events exactly like the grain's wire
            // serializer failure path does, and nothing reaches the peer.
            var fetched = new List<SubstreamEventData>();
            await target.ReadData(fetched, 100);
            Assert.Contains(fetched, e => e.StreamEvent is StopStreamCheckpoint);
            SubstreamEventWireSerializer.ReturnEvents(fetched);

            // Nothing has confirmed the delivery, finishing the stop now would dispose the
            // queue while the peer never received the pre-barrier events - they would only
            // come back after this stream is manually started again.
            Assert.False(target.ReadyToStop, "The target reported ready to stop on the dequeue alone, before anything confirmed the fetch response reached the peer");

            // The peer completes the checkpoint cycle that paired with the stop barrier and
            // acknowledges it - that is the delivery confirmation, now the stop may finish.
            await pointB.SendCheckpointDone(5);
            Assert.True(target.ReadyToStop, "The peer acknowledged a checkpoint after fetching the stop barrier, the target must be ready to stop");
        }

        [Fact]
        public async Task AckFromBeforeTheBarrierFetchDoesNotConfirmTheDrain()
        {
            var hub = new LocalSubstreamCommunicationHub();
            var handlerA = hub.CreateFactory("subA").GetCommunicationHandler("subB", "subA");
            var handlerB = hub.CreateFactory("subB").GetCommunicationHandler("subA", "subB");

            var pointA = new SubstreamCommunicationPoint(NullLogger.Instance, "subA", "subB", handlerA);
            var pointB = new SubstreamCommunicationPoint(NullLogger.Instance, "subB", "subA", handlerB);

            var target = new SubstreamTarget(1, 1, pointA, () => { });
            await target.Initialize(0, 1, await CreateStateClient(), new ExchangeOperatorState(), GlobalMemoryManager.Instance, _ => Task.CompletedTask);
            await pointB.InitializeOperator(0);

            // An acknowledgement for an earlier cycle arrives before the stop barrier is
            // even stored, it cannot say anything about the drain.
            await pointB.SendCheckpointDone(4);

            await target.OnLockingEvent(new StopStreamCheckpoint(1, 2));
            var fetched = new List<SubstreamEventData>();
            await target.ReadData(fetched, 100);
            SubstreamEventWireSerializer.ReturnEvents(fetched);

            Assert.False(target.ReadyToStop, "An acknowledgement from before the barrier fetch confirmed the drain - only an ack arriving after the barrier was handed out covers it");

            // The ack for the covering cycle arrives after the fetch and confirms it.
            await pointB.SendCheckpointDone(5);
            Assert.True(target.ReadyToStop);
        }
    }
}
