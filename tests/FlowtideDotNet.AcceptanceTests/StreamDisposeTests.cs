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
using FlowtideDotNet.Core;
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

        private sealed class GatedSinkTestStream(string name) : FlowtideTestStream(name)
        {
            public readonly ManualResetEventSlim SinkGate = new(false);

            protected override void AddWriteResolvers(IConnectorManager connectorManger)
            {
                connectorManger.AddSink(new MockSinkFactory("*", _ => { }, 0, _ => { }, onChangeRowsReceived: _ => SinkGate.Wait(TimeSpan.FromSeconds(60))));
            }

            public decimal Gauge(string displayName, string gauge) => GetDiagnosticsGraph().Nodes.Values
                .Where(n => n.DisplayName == displayName)
                .SelectMany(n => n.Gauges).Where(g => g.Name == gauge)
                .SelectMany(g => g.Dimensions.Values).Select(d => d.Value).DefaultIfEmpty(0).Max();
        }

        [Fact]
        public async Task DisposeOfAPausedStreamWithAParkedMessageCompletes()
        {
            var stream = new GatedSinkTestStream($"{nameof(StreamDisposeTests)}_{nameof(DisposeOfAPausedStreamWithAParkedMessageCompletes)}") { SourceBatchSize = 1 };
            stream.Generate(2000);
            try
            {
                await stream.StartStream("INSERT INTO output SELECT userkey + 1 as k, firstName FROM users");

                // The blocked sink fills the projection's output, the pause then parks its next input
                var deadline = DateTime.UtcNow.AddSeconds(30);
                while ((stream.Gauge("Projection", "flowtide_backpressure") == 0 || stream.Gauge("Normalize", "flowtide_backpressure") == 0) && DateTime.UtcNow < deadline) await Task.Delay(10);
                stream.Pause();
                stream.SinkGate.Set();

                // Parked: a steady input backlog with an empty output and an idle sink
                decimal busy = -1;
                deadline = DateTime.UtcNow.AddSeconds(30);
                while (DateTime.UtcNow < deadline)
                {
                    var now = stream.Gauge("Projection", "flowtide_busy");
                    if (now > 0 && now == busy && stream.Gauge("Projection", "flowtide_backpressure") == 0 && stream.Gauge("Mock Data Sink", "flowtide_InputQueue") == 0) break;
                    busy = now;
                    await Task.Delay(500);
                }
                Assert.True(busy > 0 && busy == stream.Gauge("Projection", "flowtide_busy"), "No message is parked in the paused projection");
            }
            catch
            {
                // A failed setup must not leave the stream running for the rest of the suite
                stream.SinkGate.Set();
                await stream.DisposeAsync();
                throw;
            }

            var dispose = stream.DisposeAsync().AsTask();
            var disposed = await Task.WhenAny(dispose, Task.Delay(TimeSpan.FromSeconds(20))) == dispose;
            if (!disposed)
            {
                // Unpark so the hung dispose does not outlive the test
                stream.Resume();
                await dispose.WaitAsync(TimeSpan.FromSeconds(20));
            }
            Assert.True(disposed, "Disposing a paused stream with a message parked at the pause gate hung");
            // A dispose that completed by throwing must fail the test too
            await dispose;
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

            // Ensure the stream is actually in a restart loop before asserting that dispose stops it.
            var failuresBeforeDispose = FailureNotificationCount;
            await Task.Delay(TimeSpan.FromMilliseconds(200));
            Assert.True(FailureNotificationCount > failuresBeforeDispose, "Expected the permanently failing stream to restart at least once before dispose.");

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
