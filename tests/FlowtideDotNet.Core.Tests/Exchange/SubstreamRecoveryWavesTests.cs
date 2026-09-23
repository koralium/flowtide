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
    public class SubstreamRecoveryWavesTests
    {
        /// <summary>
        /// A peer's handshake can land before the start marked the substream, it must not read as running.
        /// </summary>
        [Fact]
        public void AFreshSubstreamIsInInitUntilItsFirstStartCompleted()
        {
            var waves = new SubstreamRecoveryWaves();

            Assert.True(waves.InInit);
            waves.ForStart();
            Assert.True(waves.InInit);
            waves.StartCompleted();
            Assert.False(waves.InInit);
            waves.Stopped();
            Assert.True(waves.InInit);
        }

        /// <summary>
        /// Nothing is wired to restart the stream yet, so the start under way continues in the peer's wave: its claims must be in it.
        /// </summary>
        [Fact]
        public async Task AWaveEnteredBeforeAnyOperatorIsWiredCarriesTheStart()
        {
            var hub = new LocalSubstreamCommunicationHub();
            var handlerA = hub.CreateFactory("subA").GetCommunicationHandler("subB", "subA");
            var handlerB = hub.CreateFactory("subB").GetCommunicationHandler("subA", "subB");
            var wavesA = new SubstreamRecoveryWaves();
            var wavesB = new SubstreamRecoveryWaves();
            var durabilityB = new SubstreamDurabilityCoordinator(NullLogger.Instance, "subB", new[] { "subA" }, 1);
            var pointA = new SubstreamCommunicationPoint(NullLogger.Instance, "subA", "subB", handlerA, waves: wavesA);
            var pointB = new SubstreamCommunicationPoint(NullLogger.Instance, "subB", "subA", handlerB, false, durabilityB, wavesB);

            // B's start reset its table in wave None, its operators are not initialized yet.
            durabilityB.EnterWave(wavesB.ForStart());
            var peers = new RecoveryWave(3, Guid.NewGuid());
            Assert.True(wavesA.TryEnter(peers));
            wavesA.ForStart();

            await pointA.InitializeOperator(0);

            Assert.Equal(peers, wavesB.Current);
            Assert.Equal(peers, durabilityB.Wave);
        }

        [Fact]
        public void AFailureWhileRunningMintsANewWave()
        {
            var waves = new SubstreamRecoveryWaves();
            var started = waves.ForStart();
            waves.StartCompleted();

            var failure = waves.ForFailure();

            Assert.True(failure > started);
            Assert.Equal(failure, waves.ForFailure());
        }

        /// <summary>
        /// The run began in the wave and may have fetched events in it, the peers must send them again.
        /// </summary>
        [Fact]
        public void AFailureOfARunStartedInTheWaveMintsANewWave()
        {
            var waves = new SubstreamRecoveryWaves();
            waves.ForFailure();
            var started = waves.ForStart();

            var failure = waves.ForFailure();

            Assert.True(failure > started);
        }

        /// <summary>
        /// The run was ended by entering the peer's wave, it restarts in that one.
        /// </summary>
        [Fact]
        public void AFailureAfterEnteringAWaveKeepsIt()
        {
            var waves = new SubstreamRecoveryWaves();
            waves.ForStart();
            waves.StartCompleted();
            var peers = new RecoveryWave(5, Guid.NewGuid());
            Assert.True(waves.TryEnter(peers));

            Assert.Equal(peers, waves.ForFailure());
            Assert.Equal(peers, waves.ForStart());
        }

        [Fact]
        public void AFailureAfterAComeDownKeepsTheLoweringWave()
        {
            var waves = new SubstreamRecoveryWaves();
            waves.ForStart();
            var lowering = waves.MintForLowering(3);

            Assert.Equal(lowering, waves.ForFailure());
        }

        [Fact]
        public void ALowerWaveIsNotEntered()
        {
            var waves = new SubstreamRecoveryWaves();
            waves.ForFailure();
            var current = waves.Current;

            Assert.False(waves.TryEnter(RecoveryWave.None));
            Assert.False(waves.TryEnter(current));
            Assert.Equal(current, waves.Current);
        }

        [Fact]
        public void TheLoweringWaveIsTheSameForEveryMemberOfTheStart()
        {
            var startWave = new RecoveryWave(2, Guid.NewGuid());
            var one = new SubstreamRecoveryWaves();
            var two = new SubstreamRecoveryWaves();
            Assert.True(one.TryEnter(startWave));
            Assert.True(two.TryEnter(startWave));
            one.ForStart();
            two.ForStart();

            Assert.Equal(one.MintForLowering(7), two.MintForLowering(7));
        }
    }
}
