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

using FlowtideDotNet.Core.Operators.Exchange;
using Microsoft.Extensions.Logging.Abstractions;

namespace FlowtideDotNet.Core.Tests.Exchange
{
    public class SubstreamDurabilityCoordinatorTests
    {
        /// <summary>
        /// The claim passed the epoch fence, then the peer restarted and can have come back lower.
        /// </summary>
        [Fact]
        public void AClaimFencedBeforeThePeersEpochChangedIsDropped()
        {
            var coordinator = new SubstreamDurabilityCoordinator(NullLogger.Instance, "subA", new[] { "subB" }, 1);
            coordinator.LocalDurable(5);

            var fenced = coordinator.Generation;
            coordinator.PeerEpochChanged("subB");
            coordinator.PeerClaim("subB", new SubstreamDurabilityClaim(0, 5, 5), default, fenced, requestReply: false);

            Assert.False(coordinator.IsAgreed(5));

            // The same claim fenced after the change counts.
            coordinator.PeerClaim("subB", new SubstreamDurabilityClaim(0, 5, 5), default, coordinator.Generation, requestReply: false);
            Assert.True(coordinator.IsAgreed(5));
        }

        [Fact]
        public void AClaimFencedBeforeAFailureIsDropped()
        {
            var coordinator = new SubstreamDurabilityCoordinator(NullLogger.Instance, "subA", new[] { "subB" }, 1);
            coordinator.LocalDurable(5);

            var fenced = coordinator.Generation;
            coordinator.Invalidate();
            coordinator.EnterWave(default);
            coordinator.LocalDurable(4);
            coordinator.PeerClaim("subB", new SubstreamDurabilityClaim(0, 5, 5), default, fenced, requestReply: false);

            Assert.False(coordinator.IsAgreed(4));
        }
    }
}
