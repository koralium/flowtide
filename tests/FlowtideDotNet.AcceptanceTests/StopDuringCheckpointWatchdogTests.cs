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

using FlowtideDotNet.AcceptanceTests.Entities;
using FlowtideDotNet.Base.Engine.Internal.StateMachine;
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests
{
    // The StreamContext test hooks are process-wide statics, classes assigning them must not
    // run in parallel: one class's hook assignment or null-out clobbers the other's mid-test.
    [Collection("StreamContext test hooks")]
    public class StopDuringCheckpointWatchdogTests : FlowtideAcceptanceBase
    {
        private const string Token = "StopDuringCheckpointWatchdogTests";

        public StopDuringCheckpointWatchdogTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper, true)
        {
        }

        /// <summary>
        /// A stop deferred behind an in-progress checkpoint is bounded by a watchdog that
        /// fails the stream when the checkpoint hangs past the drain timeout. A checkpoint
        /// that finishes right at that boundary is not hanging: its commit is done and its
        /// completion only has to cross a thread scheduling gap before it honors the stop.
        /// The watchdog previously read the released write count in that gap as an idle hang
        /// and failed the stream, turning a cleanly completing stop into a rollback.
        /// </summary>
        [Fact]
        public async Task StopCompletingAtTheDrainTimeoutBoundaryStopsCleanly()
        {
            var commitHeld = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var releaseCommit = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var gapHeld = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var releaseGap = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            bool failureObserved = false;

            StopDrainTimeout = TimeSpan.FromMilliseconds(200);
            try
            {
                GenerateData();
                await StartStream("INSERT INTO output SELECT userkey, firstName FROM users");
                await WaitForUpdate();

                // Armed only after the initial checkpoint, holding the first one would
                // deadlock the initial update the stream startup waits for.
                StreamContext.CheckpointCommitHookForTests = async (streamName, lastVersion) =>
                {
                    if (!streamName.Contains(Token))
                    {
                        return;
                    }
                    commitHeld.TrySetResult();
                    await releaseCommit.Task;
                };
                StreamContext.CheckpointPostCommitGapHookForTests = async (streamName) =>
                {
                    if (!streamName.Contains(Token))
                    {
                        return;
                    }
                    gapHeld.TrySetResult();
                    await releaseGap.Task;
                };
                StreamContext.BeforeFailureDisposeForTests = (streamName) =>
                {
                    if (!streamName.Contains(Token))
                    {
                        return;
                    }
                    failureObserved = true;
                };

                // Trigger a checkpoint whose commit blocks on the hook.
                AddOrUpdateUser(new User() { UserKey = 999999, FirstName = "watchdogtrigger" });
                var deadline = DateTime.UtcNow.AddSeconds(30);
                while (!commitHeld.Task.IsCompleted && DateTime.UtcNow < deadline)
                {
                    await SchedulerTick();
                    await Task.Delay(10);
                }
                Assert.True(commitHeld.Task.IsCompleted, "No checkpoint commit was held by the hook");

                // Stop while the checkpoint is in progress: the stop defers behind it and
                // arms the drain timeout watchdog.
                var stopTask = StopStream();

                // The watchdog passes its initial delay and polls while the commit still
                // writes; it must defer on the in-flight write.
                await Task.Delay(600);
                Assert.False(failureObserved, "The watchdog failed the stream while the checkpoint commit was still writing");

                // The commit finishes right at the timeout boundary, and the completion is
                // held in the scheduling gap between the commit task and its continuation -
                // long enough for several watchdog polls to land inside the gap.
                releaseCommit.TrySetResult();
                var gapDeadline = DateTime.UtcNow.AddSeconds(10);
                while (!gapHeld.Task.IsCompleted && DateTime.UtcNow < gapDeadline)
                {
                    await Task.Delay(10);
                }
                Assert.True(gapHeld.Task.IsCompleted, "The checkpoint completion never reached the post-commit gap");
                // Several watchdog polls land in the held gap.
                await Task.Delay(500);
                releaseGap.TrySetResult();

                var stopDeadline = DateTime.UtcNow.AddSeconds(60);
                while (!stopTask.IsCompleted && DateTime.UtcNow < stopDeadline)
                {
                    await SchedulerTick();
                    await Task.Delay(10);
                }
                Assert.True(stopTask.IsCompleted, "The stop did not complete");
                await stopTask;

                Assert.False(failureObserved, "The watchdog failed a checkpoint that was completing cleanly, turning the stop into a rollback");
            }
            finally
            {
                releaseCommit.TrySetResult();
                releaseGap.TrySetResult();
                StreamContext.CheckpointCommitHookForTests = null;
                StreamContext.CheckpointPostCommitGapHookForTests = null;
                StreamContext.BeforeFailureDisposeForTests = null;
            }
        }
    }
}
