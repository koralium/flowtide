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
    /// A checkpoint done acknowledgement is epoch fenced so an ack from before a failure
    /// cannot credit a cycle of the restarted generation. The fence runs when the ack
    /// arrives, but the crediting is dispatched to the thread pool - a failure landing in
    /// that gap bumps the generation and resets the credit ledgers, and the deferred
    /// dispatch would then credit the fresh ledgers with the aborted generation's ack. A
    /// later cycle completes its cross-substream dependency without a real acknowledgement
    /// and compaction can run past a version the peer never committed, losing the lowest
    /// common rollback point. The dispatch must therefore re-check the epoch.
    /// </summary>
    public class SubstreamCheckpointDoneFencingTests
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

        private static async Task<(SubstreamCommunicationPoint pointA, SubstreamCommunicationPoint pointB, Func<int> credits)> CreateConnectedPoints(string nameA, string nameB)
        {
            var hub = new LocalSubstreamCommunicationHub();
            var handlerA = hub.CreateFactory(nameA).GetCommunicationHandler(nameB, nameA);
            var handlerB = hub.CreateFactory(nameB).GetCommunicationHandler(nameA, nameB);

            var pointA = new SubstreamCommunicationPoint(NullLogger.Instance, nameA, nameB, handlerA);
            var pointB = new SubstreamCommunicationPoint(NullLogger.Instance, nameB, nameA, handlerB);

            int credits = 0;
            var target = new SubstreamTarget(1, 1, pointA, () => Interlocked.Increment(ref credits));
            await target.Initialize(0, 1, await CreateStateClient(), new ExchangeOperatorState(), GlobalMemoryManager.Instance, _ => Task.CompletedTask);
            await pointB.InitializeOperator(0);

            return (pointA, pointB, () => Volatile.Read(ref credits));
        }

        [Fact]
        public async Task AckDeferredAcrossAFailureDoesNotCreditTheNewGeneration()
        {
            var (pointA, pointB, credits) = await CreateConnectedPoints("cdfenceA", "cdfenceB");

            // Holds the deferred crediting dispatch, modeling thread pool scheduling delay.
            var hold = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            SubstreamCommunicationPoint.ReceiveCheckpointDoneDispatchHookForTests = name =>
                name == "cdfenceA" ? hold.Task : Task.CompletedTask;
            try
            {
                // The ack passes the arrival fence in the current generation, then the local
                // stream fails before the crediting runs: the ack now belongs to the aborted
                // generation and must not credit the ledgers the failure reset.
                var ackTask = pointB.SendCheckpointDone(5);
                pointA.OnStreamFailure();
                hold.TrySetResult();
                await ackTask;

                Assert.Equal(0, credits());
            }
            finally
            {
                SubstreamCommunicationPoint.ReceiveCheckpointDoneDispatchHookForTests = null;
            }
        }

        [Fact]
        public async Task AckDeferredWithoutAFailureStillCredits()
        {
            var (pointA, pointB, credits) = await CreateConnectedPoints("cdpassA", "cdpassB");

            var hold = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            SubstreamCommunicationPoint.ReceiveCheckpointDoneDispatchHookForTests = name =>
                name == "cdpassA" ? hold.Task : Task.CompletedTask;
            try
            {
                // A delayed dispatch with no failure in between is a real ack for the current
                // generation, the re-check must let it through.
                var ackTask = pointB.SendCheckpointDone(5);
                hold.TrySetResult();
                await ackTask;

                Assert.Equal(1, credits());
            }
            finally
            {
                SubstreamCommunicationPoint.ReceiveCheckpointDoneDispatchHookForTests = null;
            }
        }
    }
}
