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
    // Serialized with the other class that mutates the process-global
    // SubstreamCommunicationPoint.StallLimit/StallCheckInterval statics; running them
    // concurrently lets one capture the other's already-shortened value as its "original"
    // and permanently leak a non-default value, and exposes the shortened limit to any
    // SubstreamCommunicationPoint a concurrent test constructs.
    [Collection("SubstreamStallStatics")]
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
                        new SubstreamEventData() { ExchangeTargetId = 1, StreamEvent = new Checkpoint(1, 2, 1) }
                    });
                }
                return Task.FromResult<IReadOnlyList<SubstreamEventData>>(Array.Empty<SubstreamEventData>());
            }

            public void Initialize(
                Func<IReadOnlySet<int>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>> getDataFunction,
                Func<long, Task> callFailAndRecover,
                Func<long, long, bool, Task<SubstreamInitializeResponse>> initializeFromTarget,
                Func<long, long, bool, Task> callRecieveCheckpointDone)
            {
            }

            public Task SendCheckpointDone(long checkpointVersion, long targetCheckpointEpoch, bool coversPeerStopBarrier) => Task.CompletedTask;
            public Task SendFailAndRecover(long restoreVersion) => Task.CompletedTask;
            public Task<SubstreamInitializeResponse> SendInitializeRequest(long restoreVersion, long checkpointEpoch, bool cleanHandoff, CancellationToken cancellationToken)
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

        /// <summary>
        /// A fetch held at a barrier keeps the loop iterating in the paused branch, so the
        /// ordinary liveness tick stays fresh. A hold that never ends - a read loop wedged
        /// downstream so the paired barrier is never consumed - must still be caught, bounded
        /// by the longer paused limit.
        /// </summary>
        [Fact]
        public async Task AFetchPausedForeverTriggersTheWatchdog()
        {
            var previousLimit = SubstreamCommunicationPoint.StallLimit;
            var previousInterval = SubstreamCommunicationPoint.StallCheckInterval;
            var previousPausedLimit = SubstreamCommunicationPoint.PausedStallLimit;
            SubstreamCommunicationPoint.StallLimit = TimeSpan.FromSeconds(30);
            SubstreamCommunicationPoint.PausedStallLimit = TimeSpan.FromMilliseconds(500);
            SubstreamCommunicationPoint.StallCheckInterval = TimeSpan.FromMilliseconds(100);
            try
            {
                var logger = new RecordingLogger();
                var handler = new SingleEventHandler();
                var communicationPoint = new SubstreamCommunicationPoint(logger, "substream_0", "substream_1", handler);

                // The reader pauses at the delivered barrier (like the real read operator) and
                // never resumes, modelling a pipeline wedged behind that barrier. The fetch
                // loop then only ever iterates the paused branch, so the ordinary tick stays
                // fresh and only the paused limit can catch it.
                communicationPoint.Subscribe(1, ev =>
                {
                    if (ev is ICheckpointEvent)
                    {
                        communicationPoint.PauseFetch(1);
                    }
                    return Task.CompletedTask;
                });

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
                Assert.Fail("The stall watchdog did not detect the fetch held at a barrier forever");
            }
            finally
            {
                SubstreamCommunicationPoint.StallLimit = previousLimit;
                SubstreamCommunicationPoint.StallCheckInterval = previousInterval;
                SubstreamCommunicationPoint.PausedStallLimit = previousPausedLimit;
            }
        }
    }
}
