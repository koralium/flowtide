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

using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests
{
    /// <summary>
    /// A permanently failing stream used to restart forever after dispose.
    /// One CI run leaked twelve and wrote 2 GB of logs.
    /// </summary>
    [Collection("Acceptance tests")]
    public class StreamDisposeTests : FlowtideAcceptanceBase
    {
        // A hop of 5.7 minutes is rejected on every start
        private const string PermanentlyFailingQuery = @"
            INSERT INTO output
            SELECT window_start
            FROM orders
            INNER JOIN hopping_window(Orderdate, 5.7, 'MINUTE', 10, 'MINUTE');";

        public StreamDisposeTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        private async Task StartPermanentlyFailingStream()
        {
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 7, 0) });
            var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await StartStream(PermanentlyFailingQuery);
                await WaitForUpdate();
            });
            Assert.Contains("must be a whole number", ex.ToString());
        }

        [Fact]
        public async Task DisposeStopsAPermanentlyFailingStreamFromRestarting()
        {
            await StartPermanentlyFailingStream();

            await DisposeStream();

            var failuresAtDispose = FailureNotificationCount;

            // Well over the restart delay, an unstopped loop shows up clearly
            await Task.Delay(TimeSpan.FromSeconds(1));

            var restarts = FailureNotificationCount - failuresAtDispose;

            // At most one, a restart already in flight may still report
            Assert.True(restarts <= 1, $"The stream restarted {restarts} times after it was disposed, a disposed stream must not start again.");
        }

        [Fact]
        public async Task RestartsOfAPermanentlyFailingStreamBackOff()
        {
            await StartPermanentlyFailingStream();

            var failuresBefore = FailureNotificationCount;

            await Task.Delay(TimeSpan.FromSeconds(2));

            var restarts = FailureNotificationCount - failuresBefore;

            // The 50ms waits run 50, 50, 50, 100, 200, 400, 800, 1600
            // so two seconds buys eight restarts, not thirty
            Assert.True(restarts < 15, $"The stream restarted {restarts} times in two seconds, the restarts of a permanently failing stream must back off.");

            // It backs off but never gives up, an outage that clears must recover
            Assert.True(restarts > 0, "The stream stopped restarting entirely, the backoff must keep retrying.");
        }

        /// <summary>
        /// The stream starts fine and dies on every checkpoint instead.
        /// It reaches running on every hop, which is not a recovery.
        /// </summary>
        [Fact]
        public async Task RestartsBackOffWhenEveryCheckpointFails()
        {
            // Far above any recovery budget, the sink never stops crashing
            EgressCrashOnCheckpoint(1000000);
            GenerateData();
            await StartStream("INSERT INTO output SELECT userkey, firstName FROM users");

            // Lets the stream pass its grace count before the count is sampled
            await Task.Delay(TimeSpan.FromSeconds(2));

            var failuresBefore = FailureNotificationCount;

            await Task.Delay(TimeSpan.FromSeconds(2));

            var restarts = FailureNotificationCount - failuresBefore;

            // Past the grace count, so a handful of restarts and not thirty
            Assert.True(restarts < 15, $"The stream restarted {restarts} times in two seconds, a stream that fails on every checkpoint must back off too.");
        }
    }
}
