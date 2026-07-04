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

using FlowtideDotNet.Core.Operators.Exchange;
using FlowtideDotNet.Storage.Memory;
using Microsoft.Extensions.Logging.Abstractions;

namespace FlowtideDotNet.Core.Tests
{
    /// <summary>
    /// Regression tests for the failure handling of the substream exchange. The failure
    /// state machine retries stop and dispose until every operator handled its failure, an
    /// exception from these paths therefore wedges the stream in an endless recovery loop,
    /// which froze whole clusters before: a fail and recover request from another substream
    /// arriving before this substreams exchange had initialized used to throw.
    /// </summary>
    public class SubstreamRecoveryRegressionTests
    {
        private class RecordingHandler : ISubstreamCommunicationHandler
        {
            public Func<long, Task>? CallFailAndRecover;
            public int SendFailAndRecoverCalls;

            public Task<IReadOnlyList<SubstreamEventData>> FetchData(IReadOnlySet<int> targetIds, int numberOfEvents, CancellationToken cancellationToken)
            {
                return Task.FromResult<IReadOnlyList<SubstreamEventData>>(Array.Empty<SubstreamEventData>());
            }

            public void Initialize(
                Func<IReadOnlySet<int>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>> getDataFunction,
                Func<long, Task> callFailAndRecover,
                Func<long, Task<SubstreamInitializeResponse>> initializeFromTarget,
                Func<long, Task> callRecieveCheckpointDone)
            {
                CallFailAndRecover = callFailAndRecover;
            }

            public Task SendCheckpointDone(long checkpointVersion)
            {
                return Task.CompletedTask;
            }

            public Task SendFailAndRecover(long restoreVersion)
            {
                Interlocked.Increment(ref SendFailAndRecoverCalls);
                return Task.CompletedTask;
            }

            public Task<SubstreamInitializeResponse> SendInitializeRequest(long restoreVersion, CancellationToken cancellationToken)
            {
                return Task.FromResult(new SubstreamInitializeResponse(false, true, restoreVersion));
            }
        }

        /// <summary>
        /// A fail and recover from the other substream before any exchange operator has
        /// registered must complete without an error, there is nothing running that needs to
        /// roll back, the initialize handshake reconciles the versions when the stream
        /// starts.
        /// </summary>
        [Fact]
        public async Task FailAndRecoverBeforeAnyOperatorRegisteredIsIgnored()
        {
            var handler = new RecordingHandler();
            var communicationPoint = new SubstreamCommunicationPoint(NullLogger.Instance, "substream_0", "substream_1", handler);

            Assert.NotNull(handler.CallFailAndRecover);
            await handler.CallFailAndRecover!(5);
        }

        /// <summary>
        /// A fail and recover from the other substream to a target that exists but has not
        /// been initialized yet must complete without an error, the stream has no committed
        /// work past the recovery point.
        /// </summary>
        [Fact]
        public async Task FailAndRecoverBeforeTargetInitializedIsIgnored()
        {
            var handler = new RecordingHandler();
            var communicationPoint = new SubstreamCommunicationPoint(NullLogger.Instance, "substream_0", "substream_1", handler);
            // The target registers itself with the communication point in its constructor,
            // like the exchange operator creates it, but it is never initialized.
            var target = new SubstreamTarget(1, 1, communicationPoint, () => { });

            Assert.NotNull(handler.CallFailAndRecover);
            await handler.CallFailAndRecover!(5);
        }

        /// <summary>
        /// Notifying the other substream about a failure is fire and forget with one
        /// notification per recovery point in flight, several operators failing at once must
        /// not send one notification each.
        /// </summary>
        [Fact]
        public async Task NotifyFailAndRecoverCollapsesConcurrentNotifications()
        {
            var handler = new RecordingHandler();
            var communicationPoint = new SubstreamCommunicationPoint(NullLogger.Instance, "substream_0", "substream_1", handler);

            for (int i = 0; i < 10; i++)
            {
                communicationPoint.NotifyFailAndRecover(7);
            }

            // The notification runs on the thread pool, give it a moment to finish.
            var deadline = DateTime.UtcNow.AddSeconds(5);
            while (handler.SendFailAndRecoverCalls == 0 && DateTime.UtcNow < deadline)
            {
                await Task.Delay(10);
            }
            Assert.True(handler.SendFailAndRecoverCalls >= 1, "The fail and recover notification was never sent");
            Assert.True(handler.SendFailAndRecoverCalls < 10, $"Expected the concurrent notifications to collapse, got {handler.SendFailAndRecoverCalls} sends");
        }
    }
}
