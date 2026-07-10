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
    /// A fail and recover from the other substream rolls this whole stream back, any single
    /// operator wired to the stream failure path carries it. Targets register themselves at
    /// construction but wire the rollback first at their initialize, and an unwired target
    /// silently no-ops. The routing must therefore skip unwired targets: picking one
    /// arbitrarily can skip a required rollback entirely, leaving the two substreams running
    /// at divergent restore versions.
    /// </summary>
    public class SubstreamFailAndRecoverRoutingTests
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
        public async Task RollbackReachesAWiredTargetPastAnUninitializedOne()
        {
            var hub = new LocalSubstreamCommunicationHub();
            var handlerA = hub.CreateFactory("subA").GetCommunicationHandler("subB", "subA");
            var handlerB = hub.CreateFactory("subB").GetCommunicationHandler("subA", "subB");

            var pointA = new SubstreamCommunicationPoint(NullLogger.Instance, "subA", "subB", handlerA);
            var pointB = new SubstreamCommunicationPoint(NullLogger.Instance, "subB", "subA", handlerB);

            // Registered by its constructor but never initialized, its rollback is not wired.
            // This is the state of every further target while the first one runs the
            // initialize handshake that can itself request a rollback on a version mismatch.
            _ = new SubstreamTarget(1, 1, pointA, () => { });

            var recovered = new List<long>();
            var wired = new SubstreamTarget(2, 1, pointA, () => { });
            await wired.Initialize(0, 1, await CreateStateClient(), new ExchangeOperatorState(), GlobalMemoryManager.Instance, point =>
            {
                lock (recovered)
                {
                    recovered.Add(point);
                }
                return Task.CompletedTask;
            });

            await pointB.SendFailAndRecover(5);

            Assert.Equal(new[] { 5L }, recovered);
        }

        /// <summary>
        /// Read operators also register at construction but wire the stream failure path
        /// first at their initialize. A fail and recover that arrives in that window - for
        /// example a peer failing over while this substream is still starting or restarting -
        /// must take the nothing-wired path and let the initialize handshake reconcile the
        /// versions. Dispatching into the unwired operator threw, the fire and forget caller
        /// swallowed the exception, and the stream kept running without the rollback, wedged
        /// against the peer forever.
        /// </summary>
        [Fact]
        public async Task RollbackWithOnlyUnwiredReadOperatorsTakesTheGracefulPath()
        {
            var hub = new LocalSubstreamCommunicationHub();
            var handlerA = hub.CreateFactory("subA").GetCommunicationHandler("subB", "subA");
            var handlerB = hub.CreateFactory("subB").GetCommunicationHandler("subA", "subB");

            var pointA = new SubstreamCommunicationPoint(NullLogger.Instance, "subA", "subB", handlerA);
            var pointB = new SubstreamCommunicationPoint(NullLogger.Instance, "subB", "subA", handlerB);

            // Registered by its constructor but never initialized: the state of every read
            // operator while the substream is still starting or restarting.
            _ = new SubstreamReadOperator(pointA, new Substrait.Relations.SubstreamExchangeReferenceRelation()
            {
                SubStreamName = "subB",
                ExchangeTargetId = 1
            }, new System.Threading.Tasks.Dataflow.DataflowBlockOptions());

            // Must complete without dispatching into the unwired operator, the initialize
            // handshake reconciles the versions once the operator initializes.
            await pointB.SendFailAndRecover(5);
        }

        [Fact]
        public async Task RollbackIsDispatchedOnce()
        {
            var hub = new LocalSubstreamCommunicationHub();
            var handlerA = hub.CreateFactory("subA").GetCommunicationHandler("subB", "subA");
            var handlerB = hub.CreateFactory("subB").GetCommunicationHandler("subA", "subB");

            var pointA = new SubstreamCommunicationPoint(NullLogger.Instance, "subA", "subB", handlerA);
            var pointB = new SubstreamCommunicationPoint(NullLogger.Instance, "subB", "subA", handlerB);

            // One rollback fails the whole stream over, a second dispatch for the same
            // request would race the failure handling it triggered.
            var recovered = new List<long>();
            Func<long, Task> record = point =>
            {
                lock (recovered)
                {
                    recovered.Add(point);
                }
                return Task.CompletedTask;
            };
            var first = new SubstreamTarget(1, 1, pointA, () => { });
            await first.Initialize(0, 1, await CreateStateClient(), new ExchangeOperatorState(), GlobalMemoryManager.Instance, record);
            var second = new SubstreamTarget(2, 1, pointA, () => { });
            await second.Initialize(0, 1, await CreateStateClient(), new ExchangeOperatorState(), GlobalMemoryManager.Instance, record);

            await pointB.SendFailAndRecover(5);

            Assert.Equal(new[] { 5L }, recovered);
        }
    }
}
