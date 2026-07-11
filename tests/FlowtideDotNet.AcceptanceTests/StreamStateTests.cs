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
using System.Diagnostics;
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests
{
    public class StreamStateTests : FlowtideAcceptanceBase
    {
        public StreamStateTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        /// <summary>
        /// The minimum time between checkpoints throttles regular running checkpoints so a
        /// chatty source cannot trigger a checkpoint storm. It must not apply to the stop
        /// drain: a stop schedules its drain checkpoint cycles on a tight cadence, and
        /// clamping those up to the minimum interval delays every stop by that interval, and
        /// when the interval is at or above the stop drain timeout a distributed stop that
        /// needs another drain cycle force-faults instead of draining gracefully. The stop
        /// must complete promptly regardless of the minimum interval.
        /// </summary>
        [Fact]
        public async Task StopIsNotDelayedByTheMinimumCheckpointInterval()
        {
            // Well above the time the stop itself needs, so a stop clamped to this interval
            // is clearly distinguishable from a prompt one.
            MinimumTimeBetweenCheckpoints = TimeSpan.FromSeconds(20);

            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT userkey, firstName FROM users");

            // Runs a checkpoint so the minimum-interval throttle is armed for what follows.
            await WaitForUpdate();

            var stopwatch = Stopwatch.StartNew();
            await StopStream();
            stopwatch.Stop();

            Assert.Equal(StreamStateValue.NotStarted, State);
            Assert.True(
                stopwatch.Elapsed < TimeSpan.FromSeconds(10),
                $"The stop took {stopwatch.Elapsed.TotalSeconds:F1}s, it was delayed by the minimum checkpoint interval instead of draining on its own cadence.");
        }

        [Fact]
        public async Task TestStopStream()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT * FROM users");

            await WaitForUpdate();

            await StopStream();

            Assert.Equal(StreamStateValue.NotStarted, State);
        }

        /// <summary>
        /// A stop that lands while the stream is still loading its initial data must still
        /// finish. With the checkpoint-after-initial-data option a checkpoint placeholder
        /// holds the checkpoint slot during startup; a stop arriving then transitions to
        /// Stopping, whose stop cycle can only be queued behind the placeholder. When the
        /// initial data completes and the placeholder clears, the queued stop cycle has to
        /// actually run, otherwise the stop never completes and the caller hangs forever.
        /// </summary>
        [Fact]
        public async Task StopDuringInitialDataCheckpointPlaceholderCompletes()
        {
            GenerateData();
            // The option installs the startup checkpoint placeholder; the delay keeps the
            // stream parked in that window so the stop reliably lands while it is held.
            WaitForCheckpointAfterInitialData = true;
            InitialDataDelay = TimeSpan.FromSeconds(3);

            await StartStream(@"
            INSERT INTO output
            SELECT userkey, firstName FROM users");

            // The stream reaches Running (installing the checkpoint placeholder) before its
            // initial data, which is delayed. Wait for Running so the stop lands during the
            // placeholder window, not during the earlier starting phase.
            var runningDeadline = DateTime.UtcNow.AddSeconds(5);
            while (State != StreamStateValue.Running && DateTime.UtcNow < runningDeadline)
            {
                await Task.Delay(10);
            }
            Assert.Equal(StreamStateValue.Running, State);

            // Stop while the placeholder still holds the checkpoint slot. Not awaited: the
            // stop only completes once its drain runs, which is the behavior under test.
            var stopTask = StopStream();

            var completed = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(30)));
            Assert.True(completed == stopTask, "StopAsync hung: the stop landed during the initial-data checkpoint placeholder and no stop cycle was ever scheduled.");
            await stopTask;
            Assert.Equal(StreamStateValue.NotStarted, State);
        }

        //[Fact]
        //public async Task TestStopStreamThenStart()
        //{
        //    GenerateData();
        //    await StartStream(@"
        //    INSERT INTO output
        //    SELECT userkey FROM users");

        //    await WaitForUpdate();

        //    await StopStream();

        //    Assert.Equal(StreamStateValue.NotStarted, State);

        //    GenerateData();

        //    await StartStream();

        //    await WaitForUpdate();

        //    AssertCurrentDataEqual(Users.Select(x => new { x.UserKey }));

        //    Assert.Equal(StreamStateValue.Running, State);
        //}
    }
}
