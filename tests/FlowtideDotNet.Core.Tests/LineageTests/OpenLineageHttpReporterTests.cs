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
using FlowtideDotNet.Core.Lineage;
using FlowtideDotNet.Core.Lineage.Internal;
using FlowtideDotNet.Core.Lineage.Internal.Models;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using System.Net;

namespace FlowtideDotNet.Core.Tests.LineageTests
{
    public class OpenLineageHttpReporterTests
    {
        private const string Url = "http://localhost/api/v1/lineage";

        private readonly ConcurrentQueue<int> _delays = new ConcurrentQueue<int>();

        [Fact]
        public async Task StartCompleteStartDeliversInOrder()
        {
            var handler = new CapturingHttpMessageHandler();
            var runId = Guid.NewGuid();
            var reporter = CreateReporter(handler, runId: runId);

            Notify(reporter, StreamStateValue.Starting, StreamStateValue.Stopping, StreamStateValue.NotStarted, StreamStateValue.Starting);
            await handler.WaitForRequestsAsync(3);

            var requests = handler.Requests;
            Assert.Equal(["START", "COMPLETE", "START"], requests.Select(x => x.EventType));
            Assert.All(requests, x =>
            {
                Assert.Equal(HttpMethod.Post, x.Method);
                Assert.Equal(new Uri(Url), x.RequestUri);
                Assert.Equal("application/json", x.MediaType);
                Assert.Equal("utf-8", x.CharSet);
                Assert.Equal(runId, x.RunId);
            });
            // Restart after COMPLETE still delivers.
            Assert.False(reporter.ReportingTask.IsCompleted);
        }

        [Fact]
        public async Task SkippedStatesPublishNothing()
        {
            var handler = new CapturingHttpMessageHandler();
            var reporter = CreateReporter(handler);

            // Trailing Starting is the sentinel, the queue is FIFO.
            Notify(reporter,
                StreamStateValue.Starting,
                StreamStateValue.Running,
                StreamStateValue.Failure,
                StreamStateValue.NotStarted,
                StreamStateValue.Stopping,
                StreamStateValue.Deleting,
                StreamStateValue.Deleted,
                StreamStateValue.Starting);
            await handler.WaitForRequestsAsync(4);

            Assert.Equal(["START", "RUNNING", "FAIL", "START"], handler.Requests.Select(x => x.EventType));
        }

        [Fact]
        public async Task ServerErrorRetriesSameEvent()
        {
            var handler = new CapturingHttpMessageHandler(attempt => new HttpResponseMessage(attempt == 0 ? HttpStatusCode.InternalServerError : HttpStatusCode.OK));
            var reporter = CreateReporter(handler);

            Notify(reporter, StreamStateValue.Starting, StreamStateValue.Running);
            await handler.WaitForRequestsAsync(3);

            var requests = handler.Requests;
            Assert.Equal(["START", "START", "RUNNING"], requests.Select(x => x.EventType));
            // Serialized once, the retry keeps the event time.
            Assert.Equal(requests[0].Body, requests[1].Body);
            Assert.Equal([1], _delays);
        }

        [Fact]
        public async Task TimeoutRetriesAndLoopSurvives()
        {
            var handler = new CapturingHttpMessageHandler(attempt =>
            {
                if (attempt == 0)
                {
                    throw new TaskCanceledException("timeout", new TimeoutException());
                }
                return new HttpResponseMessage(HttpStatusCode.OK);
            });
            var reporter = CreateReporter(handler);

            Notify(reporter, StreamStateValue.Starting);
            await handler.WaitForRequestsAsync(2);
            Notify(reporter, StreamStateValue.Running);
            await handler.WaitForRequestsAsync(3);

            Assert.Equal(["START", "START", "RUNNING"], handler.Requests.Select(x => x.EventType));
            Assert.Equal([1], _delays);
            Assert.False(reporter.ReportingTask.IsCompleted);
        }

        [Fact]
        public async Task HttpRequestExceptionRetriesSameEvent()
        {
            var handler = new CapturingHttpMessageHandler(attempt =>
            {
                if (attempt == 0)
                {
                    throw new HttpRequestException("connection refused");
                }
                return new HttpResponseMessage(HttpStatusCode.OK);
            });
            var reporter = CreateReporter(handler);

            Notify(reporter, StreamStateValue.Starting);
            await handler.WaitForRequestsAsync(2);

            Assert.Equal(["START", "START"], handler.Requests.Select(x => x.EventType));
            Assert.Equal([1], _delays);
        }

        [Fact]
        public async Task ErrorCountResetsAfterSuccess()
        {
            var statuses = new[] { HttpStatusCode.InternalServerError, HttpStatusCode.InternalServerError, HttpStatusCode.OK, HttpStatusCode.BadGateway, HttpStatusCode.OK };
            var handler = new CapturingHttpMessageHandler(attempt => new HttpResponseMessage(statuses[attempt]));
            var reporter = CreateReporter(handler);

            Notify(reporter, StreamStateValue.Starting, StreamStateValue.Running);
            await handler.WaitForRequestsAsync(5);

            Assert.Equal(["START", "START", "START", "RUNNING", "RUNNING"], handler.Requests.Select(x => x.EventType));
            Assert.Equal([1, 2, 1], _delays);
        }

        [Fact]
        public async Task UnexpectedErrorDropsEventAndContinues()
        {
            var handler = new CapturingHttpMessageHandler();
            var logs = new ListLoggerProvider();
            var calls = 0;
            var options = new OpenLineageHttpOptions()
            {
                Url = Url,
                OnRequest = _ =>
                {
                    if (Interlocked.Increment(ref calls) == 1)
                    {
                        throw new InvalidOperationException("bad header");
                    }
                }
            };
            var reporter = CreateReporter(handler, options, logs.CreateLogger("test"));

            Notify(reporter, StreamStateValue.Starting, StreamStateValue.Running);
            await handler.WaitForRequestsAsync(1);

            Assert.Equal(["RUNNING"], handler.Requests.Select(x => x.EventType));
            var error = Assert.Single(logs.Entries, x => x.Level == LogLevel.Error);
            Assert.IsType<InvalidOperationException>(error.Exception);
            Assert.Contains("dropped", error.Message);
            Assert.Empty(_delays);
            Assert.False(reporter.ReportingTask.IsCompleted);
        }

        [Fact]
        public async Task LoopRunsOnDefaultScheduler()
        {
            var handler = new CapturingHttpMessageHandler();
            var exclusive = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;

            // Built on a custom scheduler, as an Orleans grain would.
            var reporter = await Task.Factory.StartNew(() => CreateReporter(handler), CancellationToken.None, TaskCreationOptions.None, exclusive);
            Notify(reporter, StreamStateValue.Starting);
            await handler.WaitForRequestsAsync(1);

            Assert.Same(TaskScheduler.Default, Assert.Single(handler.Requests).Scheduler);
        }

        [Theory]
        [InlineData(StreamStateValue.NotStarted, StreamStateValue.Starting, "Start")]
        [InlineData(StreamStateValue.Starting, StreamStateValue.Running, "Running")]
        [InlineData(StreamStateValue.Running, StreamStateValue.Failure, "Fail")]
        [InlineData(StreamStateValue.Stopping, StreamStateValue.Failure, "Fail")]
        [InlineData(StreamStateValue.Failure, StreamStateValue.Starting, "Start")]
        [InlineData(StreamStateValue.Stopping, StreamStateValue.NotStarted, "Complete")]
        [InlineData(StreamStateValue.Running, StreamStateValue.Stopping, null)]
        [InlineData(StreamStateValue.Failure, StreamStateValue.NotStarted, null)]
        [InlineData(StreamStateValue.NotStarted, StreamStateValue.NotStarted, null)]
        [InlineData(StreamStateValue.Running, StreamStateValue.Deleting, null)]
        [InlineData(StreamStateValue.Deleting, StreamStateValue.Deleted, null)]
        public void StateMapping(StreamStateValue previous, StreamStateValue state, string? expected)
        {
            var mapped = OpenLineageHttpReporter.TryMapState(previous, state, out var eventType);
            Assert.Equal(expected, mapped ? eventType.ToString() : null);
        }

        [Theory]
        [InlineData(1, 1)]
        [InlineData(2, 2)]
        [InlineData(15, 15)]
        [InlineData(16, 15)]
        [InlineData(1000, 15)]
        public void DefaultRetryDelayCapsAtFifteenSeconds(int errorCount, int expectedSeconds)
        {
            Assert.Equal(TimeSpan.FromSeconds(expectedSeconds), OpenLineageHttpReporter.DefaultRetryDelay(errorCount));
        }

        [Fact]
        public void MissingUrlThrows()
        {
            var e = Assert.Throws<ArgumentException>(() => CreateReporter(new CapturingHttpMessageHandler(), new OpenLineageHttpOptions()));
            Assert.Equal("OpenLineageOptions.Url must be set", e.Message);
        }

        private OpenLineageHttpReporter CreateReporter(
            CapturingHttpMessageHandler handler,
            OpenLineageHttpOptions? options = null,
            ILogger? logger = null,
            Guid? runId = null)
        {
            options ??= new OpenLineageHttpOptions() { Url = Url };
            var lineage = new StreamLineage("stream", null, DateTimeOffset.UnixEpoch, [], []);
            var template = LineageEventCreator.CreateFromLineage(runId ?? Guid.NewGuid(), lineage, false);
            // Zero delay keeps retries instant, attempts are recorded.
            return new OpenLineageHttpReporter(
                logger ?? new ListLoggerProvider().CreateLogger("test"),
                template,
                options,
                new HttpClient(handler),
                errorCount =>
                {
                    _delays.Enqueue(errorCount);
                    return TimeSpan.Zero;
                });
        }

        private static void Notify(OpenLineageHttpReporter reporter, params StreamStateValue[] states)
        {
            foreach (var state in states)
            {
                var streamName = "stream";
                var value = state;
                reporter.OnStreamStateChange(new StreamStateChangeNotification(ref streamName, ref value));
            }
        }
    }
}
