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
using FlowtideDotNet.Base.Engine.Internal.StateMachine;
using System;
using System.Linq;
using System.Threading.Tasks;
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests
{
    public class DeleteStreamTests : FlowtideAcceptanceBase
    {
        public DeleteStreamTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper, true)
        {
        }

        [Fact]
        public async Task DeleteStreamAfterCheckpoint()
        {
            GenerateData();

            await StartStream("INSERT INTO output SELECT userkey, firstName FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { x.UserKey, x.FirstName }));

            await DeleteStream();

            CancellationTokenSource tokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            
            while (!tokenSource.IsCancellationRequested)
            {
                if (State == StreamStateValue.Deleted)
                {
                    break;
                }
                if (State != StreamStateValue.Deleting)
                {
                    throw new InvalidOperationException("Not in the deleting state");
                }
                
                await Task.Delay(100);
            }

            Assert.Equal(StreamStateValue.Deleted, State);
        }

        /// <summary>
        /// A delete that lands while the stream is still loading its initial data, with the
        /// checkpoint-after-initial-data placeholder holding the checkpoint slot, must still
        /// complete. This exercises the delete side of the checkpoint-completion wish
        /// handling, the counterpart of the stop case: a delete tears down directly and runs
        /// no checkpoint cycle, so the completion must not depend on or schedule one.
        /// </summary>
        [Fact]
        public async Task DeleteDuringInitialDataCheckpointPlaceholderCompletes()
        {
            GenerateData();
            // The option installs the startup checkpoint placeholder; the delay keeps the
            // stream parked in that window so the delete reliably lands while it is held.
            WaitForCheckpointAfterInitialData = true;
            InitialDataDelay = TimeSpan.FromSeconds(3);

            await StartStream("INSERT INTO output SELECT userkey, firstName FROM users");

            // The stream reaches Running (installing the placeholder) before its delayed
            // initial data. Wait for Running so the delete lands during the placeholder
            // window, not during the earlier starting phase.
            var runningDeadline = DateTime.UtcNow.AddSeconds(5);
            while (State != StreamStateValue.Running && DateTime.UtcNow < runningDeadline)
            {
                await Task.Delay(10);
            }
            Assert.Equal(StreamStateValue.Running, State);

            // Delete while the placeholder still holds the checkpoint slot. Not awaited inline:
            // the delete task only completes once the teardown runs.
            var deleteTask = DeleteStream();

            var completed = await Task.WhenAny(deleteTask, Task.Delay(TimeSpan.FromSeconds(30)));
            Assert.True(completed == deleteTask, "DeleteAsync hung: the delete landed during the initial-data checkpoint placeholder.");
            await deleteTask;

            // The teardown finishes reaching the deleted state shortly after the task settles.
            var deletedDeadline = DateTime.UtcNow.AddSeconds(10);
            while (State != StreamStateValue.Deleted && DateTime.UtcNow < deletedDeadline)
            {
                await Task.Delay(50);
            }
            Assert.Equal(StreamStateValue.Deleted, State);
        }

        /// <summary>
        /// When a delete keeps failing it gives up and surfaces the failure, but the stream
        /// stays in the deleting state. A stop issued afterwards must still complete, the
        /// blocks are already torn down so there is nothing left to stop.
        /// </summary>
        [Fact]
        public async Task StopAfterDeleteGivesUpCompletes()
        {
            var originalMax = DeletingStreamState.MaxDeleteAttempts;
            var originalDelay = DeletingStreamState.DeleteRetryDelay;
            DeletingStreamState.MaxDeleteAttempts = 3;
            DeletingStreamState.DeleteRetryDelay = TimeSpan.FromMilliseconds(10);
            try
            {
                GenerateData();
                // Permanent delete failure, well above the shortened attempt budget.
                SinkDeleteFailCount = 100;

                await StartStream("INSERT INTO output SELECT userkey, firstName FROM users");
                await WaitForUpdate();

                // The delete gives up after the attempt budget and faults its task.
                await Assert.ThrowsAnyAsync<Exception>(() => DeleteStream());

                // A stop after the give-up must complete rather than hang forever.
                var stopTask = StopStream();
                var finished = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(15)));
                Assert.True(finished == stopTask, "Stop hung after the delete gave up");
                await stopTask;
            }
            finally
            {
                DeletingStreamState.MaxDeleteAttempts = originalMax;
                DeletingStreamState.DeleteRetryDelay = originalDelay;
            }
        }

        /// <summary>
        /// A delete whose storage fails a few times but recovers within the retry budget
        /// must still reach the deleted state, the retries must not turn a transient fault
        /// into a permanent failure.
        /// </summary>
        [Fact]
        public async Task DeleteRecoversFromTransientStorageFailure()
        {
            var originalMax = DeletingStreamState.MaxDeleteAttempts;
            var originalDelay = DeletingStreamState.DeleteRetryDelay;
            DeletingStreamState.MaxDeleteAttempts = 10;
            DeletingStreamState.DeleteRetryDelay = TimeSpan.FromMilliseconds(10);
            try
            {
                GenerateData();
                // Fails twice, then succeeds, comfortably within the budget.
                SinkDeleteFailCount = 2;

                await StartStream("INSERT INTO output SELECT userkey, firstName FROM users");
                await WaitForUpdate();

                var deleteTask = DeleteStream();
                // Observe the fault if the delete gives up, the assertion below reports it.
                _ = deleteTask.ContinueWith(t => { _ = t.Exception; }, TaskScheduler.Default);

                var deadline = DateTime.UtcNow.AddSeconds(15);
                while (State != StreamStateValue.Deleted && DateTime.UtcNow < deadline)
                {
                    await Task.Delay(50);
                }
                Assert.Equal(StreamStateValue.Deleted, State);
            }
            finally
            {
                DeletingStreamState.MaxDeleteAttempts = originalMax;
                DeletingStreamState.DeleteRetryDelay = originalDelay;
            }
        }
    }
}
