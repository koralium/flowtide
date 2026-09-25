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
using System.Diagnostics;
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests
{
    [Collection("StreamContext test hooks")]
    public class CheckpointScheduleTimerTests : FlowtideAcceptanceBase
    {
        private const string Token = "CheckpointScheduleTimerTests";

        public CheckpointScheduleTimerTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper, true)
        {
        }

        /// <summary>
        /// A schedule timer replaced by a newer one must not trigger.
        /// </summary>
        [Fact]
        public async Task SupersededScheduleTimerDoesNotTriggerACheckpoint()
        {
            var pendingTimerDelay = TimeSpan.FromMilliseconds(500);
            var commitHeld = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var releaseCommit = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            int commits = 0;

            try
            {
                GenerateData();
                await StartStream("INSERT INTO output SELECT userkey, firstName FROM users");
                await WaitForUpdate();
                await WaitForCheckpointsToSettle();

                // Armed after the initial checkpoint, holding that one would deadlock.
                StreamContext.CheckpointCommitHookForTests = async (streamName, lastVersion) =>
                {
                    if (!streamName.Contains(Token))
                    {
                        return;
                    }
                    if (Interlocked.Increment(ref commits) == 1)
                    {
                        commitHeld.TrySetResult();
                        await releaseCommit.Task;
                    }
                };

                // Pending timer, then a cycle it did not start.
                var pendingTimerCreated = Stopwatch.StartNew();
                TryScheduleCheckpoint(pendingTimerDelay);
                await TriggerCheckpoint();
                var held = await Task.WhenAny(commitHeld.Task, Task.Delay(TimeSpan.FromSeconds(30)));
                Assert.True(held == commitHeld.Task, "No checkpoint commit was held by the hook");

                // Queued behind the cycle, its promotion replaces the pending timer.
                TryScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
                releaseCommit.TrySetResult();

                var deadline = DateTime.UtcNow.AddSeconds(30);
                while (Volatile.Read(ref commits) < 2 && DateTime.UtcNow < deadline)
                {
                    await Task.Delay(10);
                }
                Assert.Equal(2, Volatile.Read(ref commits));

                // Past the replaced timers due time.
                var remaining = pendingTimerDelay + TimeSpan.FromMilliseconds(500) - pendingTimerCreated.Elapsed;
                if (remaining > TimeSpan.Zero)
                {
                    await Task.Delay(remaining);
                }
                await WaitForCheckpointsToSettle();

                Assert.Equal(2, Volatile.Read(ref commits));
            }
            finally
            {
                releaseCommit.TrySetResult();
                StreamContext.CheckpointCommitHookForTests = null;
            }
        }

        /// <summary>
        /// A timer replaced after it fired, before it triggers, must not trigger.
        /// </summary>
        [Fact]
        public async Task ScheduleTimerSupersededWhileFiringDoesNotTriggerACheckpoint()
        {
            var commitHeld = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var releaseCommit = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var timerParked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            using var releaseTimer = new ManualResetEventSlim(false);
            int commits = 0;
            int fired = 0;

            try
            {
                GenerateData();
                await StartStream("INSERT INTO output SELECT userkey, firstName FROM users");
                await WaitForUpdate();
                await WaitForCheckpointsToSettle();

                // Armed after the initial checkpoint, holding that one would deadlock.
                StreamContext.CheckpointCommitHookForTests = async (streamName, lastVersion) =>
                {
                    if (!streamName.Contains(Token))
                    {
                        return;
                    }
                    if (Interlocked.Increment(ref commits) == 1)
                    {
                        commitHeld.TrySetResult();
                        await releaseCommit.Task;
                    }
                };
                // Only the first fired timer parks.
                StreamContext.ScheduledCheckpointFiredHookForTests = streamName =>
                {
                    if (!streamName.Contains(Token))
                    {
                        return;
                    }
                    if (Interlocked.Increment(ref fired) == 1)
                    {
                        timerParked.TrySetResult();
                        releaseTimer.Wait(TimeSpan.FromSeconds(60));
                    }
                };

                // The timer fires into a cycle it did not start.
                TryScheduleCheckpoint(TimeSpan.FromMilliseconds(50));
                await TriggerCheckpoint();
                var held = await Task.WhenAny(commitHeld.Task, Task.Delay(TimeSpan.FromSeconds(30)));
                Assert.True(held == commitHeld.Task, "No checkpoint commit was held by the hook");
                var parked = await Task.WhenAny(timerParked.Task, Task.Delay(TimeSpan.FromSeconds(30)));
                Assert.True(parked == timerParked.Task, "The schedule timer never fired");

                // Promotion replaces the parked timer, the replacement runs its cycle.
                TryScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
                releaseCommit.TrySetResult();
                var deadline = DateTime.UtcNow.AddSeconds(30);
                while (Volatile.Read(ref commits) < 2 && DateTime.UtcNow < deadline)
                {
                    await Task.Delay(10);
                }
                Assert.Equal(2, Volatile.Read(ref commits));
                await WaitForCheckpointsToSettle();

                releaseTimer.Set();
                await Task.Delay(500);
                await WaitForCheckpointsToSettle();

                Assert.Equal(2, Volatile.Read(ref commits));
            }
            finally
            {
                releaseCommit.TrySetResult();
                releaseTimer.Set();
                StreamContext.CheckpointCommitHookForTests = null;
                StreamContext.ScheduledCheckpointFiredHookForTests = null;
            }
        }
    }
}
