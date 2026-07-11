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

using FlowtideDotNet.Base.Engine.Internal.StateMachine;
using FlowtideDotNet.Core.Operators.Exchange;

namespace FlowtideDotNet.AcceptanceTests.Internal
{
    /// <summary>
    /// Shortens the engine's real-time delays for tests. The delays stay real (no fake
    /// clock: advancing fake time deterministically against the streams' live threads is a
    /// flake factory), only shorter: every recovery hop pays the restart settle delay and
    /// every distributed start pays a handshake retry slice for the substream that loses the
    /// startup race. Not applied by the throughput probe, its measurements should reflect
    /// production timings.
    /// </summary>
    internal static class FastEngineTimings
    {
        private static int _applied;

        public static void Apply()
        {
            if (Interlocked.Exchange(ref _applied, 1) == 1)
            {
                return;
            }
            FailureStreamState.RecoveryRestartDelay = TimeSpan.FromMilliseconds(50);
            SubstreamCommunicationPoint.NotStartedRetrySliceMs = 50;
            RunningStreamState.DeferredWishWatchdogPollInterval = TimeSpan.FromMilliseconds(100);
        }
    }
}
