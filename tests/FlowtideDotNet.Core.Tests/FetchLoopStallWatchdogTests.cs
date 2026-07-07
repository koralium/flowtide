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

using FlowtideDotNet.Base;
using FlowtideDotNet.Core.Operators.Exchange;
using Microsoft.Extensions.Logging;

namespace FlowtideDotNet.Core.Tests
{
    /// <summary>
    /// Tests the fetch loop stall watchdog. When substreams deadlock on each others
    /// checkpoint barriers every stream sits in a healthy looking running state while
    /// nothing moves, the only liveness signal is the fetch loop, which iterates many times
    /// per second when healthy. The watchdog must detect a loop that is stuck delivering an
    /// event and start a recovery.
    /// </summary>
    public class FetchLoopStallWatchdogTests
    {
        private class RecordingLogger : ILogger
        {
            public readonly List<string> Warnings = new List<string>();

            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                if (logLevel == LogLevel.Warning)
                {
                    lock (Warnings)
                    {
                        Warnings.Add(formatter(state, exception));
                    }
                }
            }
        }

        private class SingleEventHandler : ISubstreamCommunicationHandler
        {
            private int _served;

            public Task<IReadOnlyList<SubstreamEventData>> FetchData(IReadOnlySet<int> targetIds, int numberOfEvents, CancellationToken cancellationToken)
            {
                if (Interlocked.Exchange(ref _served, 1) == 0)
                {
                    return Task.FromResult<IReadOnlyList<SubstreamEventData>>(new List<SubstreamEventData>()
                    {
                        new SubstreamEventData() { ExchangeTargetId = 1, StreamEvent = new Checkpoint(1, 2) }
                    });
                }
                return Task.FromResult<IReadOnlyList<SubstreamEventData>>(Array.Empty<SubstreamEventData>());
            }

            public void Initialize(
                Func<IReadOnlySet<int>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>> getDataFunction,
                Func<long, Task> callFailAndRecover,
                Func<long, long, Task<SubstreamInitializeResponse>> initializeFromTarget,
                Func<long, long, Task> callRecieveCheckpointDone)
            {
            }

            public Task SendCheckpointDone(long checkpointVersion, long targetCheckpointEpoch) => Task.CompletedTask;
            public Task SendFailAndRecover(long restoreVersion) => Task.CompletedTask;
            public Task<SubstreamInitializeResponse> SendInitializeRequest(long restoreVersion, long checkpointEpoch, CancellationToken cancellationToken)
                => Task.FromResult(new SubstreamInitializeResponse(false, true, restoreVersion));
        }

        [Fact]
        public async Task StalledDeliveryTriggersTheWatchdog()
        {
            var previousLimit = SubstreamCommunicationPoint.StallLimit;
            var previousInterval = SubstreamCommunicationPoint.StallCheckInterval;
            SubstreamCommunicationPoint.StallLimit = TimeSpan.FromMilliseconds(500);
            SubstreamCommunicationPoint.StallCheckInterval = TimeSpan.FromMilliseconds(100);
            try
            {
                var logger = new RecordingLogger();
                var handler = new SingleEventHandler();
                var communicationPoint = new SubstreamCommunicationPoint(logger, "substream_0", "substream_1", handler);

                // The subscriber never completes, modelling a delivery into a pipeline that
                // deadlocked, the fetch loop blocks on it forever.
                communicationPoint.Subscribe(1, _ => new TaskCompletionSource().Task);

                var deadline = DateTime.UtcNow.AddSeconds(10);
                while (DateTime.UtcNow < deadline)
                {
                    lock (logger.Warnings)
                    {
                        if (logger.Warnings.Any(w => w.Contains("stalled")))
                        {
                            return;
                        }
                    }
                    await Task.Delay(50);
                }
                Assert.Fail("The stall watchdog did not detect the blocked fetch loop");
            }
            finally
            {
                SubstreamCommunicationPoint.StallLimit = previousLimit;
                SubstreamCommunicationPoint.StallCheckInterval = previousInterval;
            }
        }
    }
}
