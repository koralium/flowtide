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

using System.Diagnostics;

namespace FlowtideDotNet.AcceptanceTests.Internal
{
    internal static class CheckpointSettle
    {
        public static Task WaitForCheckpointsToSettle(params Base.Engine.DataflowStream[] streams)
        {
            return WaitForCheckpointsToSettle((IEnumerable<Base.Engine.DataflowStream>)streams);
        }

        /// <summary>
        /// Waits until no stream has a checkpoint running, queued or scheduled.
        /// </summary>
        public static async Task WaitForCheckpointsToSettle(IEnumerable<Base.Engine.DataflowStream> streams)
        {
            var hangGuard = DateTime.UtcNow.AddSeconds(30);
            var idleSince = Stopwatch.StartNew();
            while (DateTime.UtcNow < hangGuard)
            {
                if (!streams.All(s => s.CheckpointSchedulingIdleForTests))
                {
                    idleSince.Restart();
                }
                // Longer than the substream readers covering checkpoint delay.
                else if (idleSince.Elapsed > TimeSpan.FromMilliseconds(500))
                {
                    return;
                }
                await Task.Delay(20);
            }
            Assert.Fail("The checkpoint wave did not settle within 30 seconds.");
        }
    }
}
