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

using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Base.Engine;
using FlowtideDotNet.Base.Engine.Internal.StateMachine;
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests
{
    [Collection("StreamContext test hooks")]
    public class StopAtRunningTransitionTests : FlowtideAcceptanceBase
    {
        private const string Token = "StopAtRunningTransitionTests";

        public StopAtRunningTransitionTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper, true)
        {
        }

        /// <summary>
        /// A stop between the running swap and its initialize must complete.
        /// </summary>
        [Fact]
        public async Task StopBetweenTheRunningSwapAndItsInitializeCompletes()
        {
            var name = $"{Token}_{nameof(StopBetweenTheRunningSwapAndItsInitializeCompletes)}";
            // The stop timer fires long before the initial data completes.
            var stream = new FlowtideTestStream(name) { InitialDataDelay = TimeSpan.FromSeconds(2) };
            var logs = new RingBufferLoggerProvider();
            stream.AddLoggerProvider(logs);
            var listener = new StopOnRunning(stream);
            stream.AddStateChangeListener(listener);
            int stopTimerFires = 0;
            try
            {
                StreamContext.ScheduledCheckpointFiredHookForTests = streamName =>
                {
                    if (streamName != name || !listener.Fired || Interlocked.Increment(ref stopTimerFires) != 1)
                    {
                        return;
                    }
                    // Lets the stale running initialize install its placeholder first.
                    SpinWait.SpinUntil(() => logs.LinesContaining("is in running state").Count > 0, TimeSpan.FromSeconds(10));
                    Thread.Sleep(100);
                };

                // Empty source, nothing else schedules a checkpoint that could become the stop cycle.
                await stream.StartStream("INSERT INTO output SELECT userkey FROM users");
                var stopTask = await listener.StopRequested.Task.WaitAsync(TimeSpan.FromSeconds(30));

                var completed = await Task.WhenAny(stopTask, Task.Delay(TimeSpan.FromSeconds(20)));
                Assert.True(completed == stopTask,
                    $"The stop hung: state {stream.State}, stop timer fires {Volatile.Read(ref stopTimerFires)}, " +
                    $"shutdown checkpoints {logs.LinesContaining("Starting shutdown checkpoint").Count}, " +
                    $"initial data done {logs.LinesContaining("All ingress blocks completed their initial data").Count}.");
                await stopTask;
                Assert.True(stopTask.IsCompletedSuccessfully);
                Assert.Equal(StreamStateValue.NotStarted, stream.State);
                // The lock held the stop out of the gap, it did not merely arrive late.
                Assert.False(listener.StopReturnedInGap);

                var shutdown = Assert.Single(logs.LinesContaining("Starting shutdown checkpoint"));
                var initialDone = Assert.Single(logs.LinesContaining("All ingress blocks completed their initial data"));
                var all = logs.LinesContaining(string.Empty).ToList();
                Assert.True(all.IndexOf(initialDone) < all.IndexOf(shutdown), "The stop cycle ran before the initial data completed.");
                Assert.Empty(logs.LinesContaining("timed out waiting"));
                Assert.Empty(logs.LinesContaining("Stream error"));
                Assert.Single(logs.LinesContaining($"Stopped stream: `{name}`"));
            }
            finally
            {
                StreamContext.ScheduledCheckpointFiredHookForTests = null;
                try
                {
                    await stream.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(30));
                }
                catch (TimeoutException)
                {
                    // A hung stop must not also hang the test run.
                }
            }
        }

        private sealed class StopOnRunning(FlowtideTestStream stream) : IStreamStateChangeListener
        {
            private int _armed = 1;

            public volatile bool Fired;

            public volatile bool StopReturnedInGap;

            public TaskCompletionSource<Task> StopRequested { get; } = new TaskCompletionSource<Task>(TaskCreationOptions.RunContinuationsAsynchronously);

            public void OnStreamStateChange(StreamStateChangeNotification notification)
            {
                // Called after the running swap, before RunningStreamState.Initialize.
                if (notification.State == StreamStateValue.Running && Interlocked.Exchange(ref _armed, 0) == 1)
                {
                    Fired = true;
                    // From another thread like any caller, bounded since a lock held here may block it.
                    var dispatch = Task.Factory.StartNew(() => stream.StopStream());
                    StopReturnedInGap = dispatch.Wait(TimeSpan.FromSeconds(2));
                    StopRequested.TrySetResult(dispatch.Unwrap());
                }
            }
        }
    }
}
