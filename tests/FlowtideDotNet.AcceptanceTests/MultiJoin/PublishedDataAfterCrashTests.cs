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
using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Base.Engine.Internal.StateMachine;
using FlowtideDotNet.Core.ColumnStore;

namespace FlowtideDotNet.AcceptanceTests.MultiJoin
{
    [Collection("Multi join row budget")]
    public class PublishedDataAfterCrashTests
    {
        private sealed class InFlightPublishTestStream : FlowtideTestStream
        {
            private int _publishCount;
            public readonly TaskCompletionSource SecondPublishEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
            public readonly TaskCompletionSource ReleaseSecondPublish = new(TaskCreationOptions.RunContinuationsAsynchronously);

            public InFlightPublishTestStream(string testName) : base(testName)
            {
            }

            protected override void OnDataUpdate(EventBatchData actualData)
            {
                if (Interlocked.Increment(ref _publishCount) == 2)
                {
                    SecondPublishEntered.TrySetResult();
                    ReleaseSecondPublish.Task.GetAwaiter().GetResult();
                }
                base.OnDataUpdate(actualData);
            }
        }

        [Fact]
        public async Task InFlightPreCrashSinkPublishAfterFailureNotificationDoesNotLookCurrent()
        {
            TimeSpan delayBefore = FailureStreamState.RecoveryRestartDelay;
            FailureStreamState.RecoveryRestartDelay = TimeSpan.FromSeconds(5);
            await using var stream = new InFlightPublishTestStream("PublishedDataAfterCrash/f45_inflight_publish")
            {
                MultiJoinMaxInputs = 5
            };
            try
            {
                stream.AddOrUpdateCompany(new Company { CompanyId = "c1", Name = "Acme" });
                stream.AddUser(new User { UserKey = 1, FirstName = "Alice", CompanyId = "c1" });
                stream.AddOrder(new Order { OrderKey = 10, UserKey = 1 });

                await stream.StartStream(@"
                    INSERT INTO output
                    SELECT o.orderkey, u.firstName, c.name
                    FROM orders o
                    INNER JOIN users u ON o.userkey = u.userkey
                    INNER JOIN companies c ON u.companyId = c.companyId",
                    ignoreSameDataCheck: true);

                var watch = System.Diagnostics.Stopwatch.StartNew();
                while (!stream.PublishedDataIsCurrent())
                {
                    Assert.True(watch.Elapsed < TimeSpan.FromSeconds(15), "Initial checkpoint did not publish");
                    await stream.SchedulerTick();
                    await Task.Delay(10);
                }

                stream.AddOrUpdateCompany(new Company { CompanyId = "c2", Name = "Beta" });
                stream.AddUser(new User { UserKey = 2, FirstName = "Bob", CompanyId = "c2" });
                stream.AddOrder(new Order { OrderKey = 20, UserKey = 2 });
                await stream.Trigger("changes");

                // Tick until the uncommitted pre-crash MockDataSink.OnCheckpoint enters onDataChange.
                watch.Restart();
                while (!stream.SecondPublishEntered.Task.IsCompleted)
                {
                    Assert.True(watch.Elapsed < TimeSpan.FromSeconds(15), "Second sink publish was not reached");
                    await stream.SchedulerTick();
                    await Task.Delay(10);
                }

                // Crash() increments _failureNotifications synchronously while the pre-crash checkpoint is still uncommitted.
                await stream.Crash();
                stream.ReleaseSecondPublish.TrySetResult();
                await Task.Delay(50);

                // Recovery has not run (RecoveryRestartDelay = 5 s), so no post-crash checkpoint has published anything.
                Assert.False(stream.PublishedDataIsCurrent(),
                    "An uncommitted pre-crash sink checkpoint stamped itself with the post-crash FailureCount.");
            }
            finally
            {
                stream.ReleaseSecondPublish.TrySetResult();
                FailureStreamState.RecoveryRestartDelay = delayBefore;
            }
        }
    }
}
