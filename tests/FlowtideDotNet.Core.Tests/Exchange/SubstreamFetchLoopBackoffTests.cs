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
using Microsoft.Extensions.Logging.Abstractions;

namespace FlowtideDotNet.Core.Tests.Exchange
{
    /// <summary>
    /// The fetch loop polls the other substream for events; when the substreams are idle
    /// every poll returns empty. In Orleans each poll is a grain call, so an idle stream
    /// with many substream pairs generates thousands of calls per second doing nothing.
    /// Consecutive empty fetches must back off; any data resets the backoff so an active
    /// stream keeps its low latency.
    /// </summary>
    public class SubstreamFetchLoopBackoffTests
    {
        private sealed class CountingEmptyHandler : ISubstreamCommunicationHandler
        {
            private int _fetchCount;

            public int FetchCount => Volatile.Read(ref _fetchCount);

            public Task<IReadOnlyList<SubstreamEventData>> FetchData(IReadOnlySet<int> targetIds, int numberOfEvents, CancellationToken cancellationToken)
            {
                Interlocked.Increment(ref _fetchCount);
                return Task.FromResult<IReadOnlyList<SubstreamEventData>>(Array.Empty<SubstreamEventData>());
            }

            public void Initialize(
                Func<IReadOnlySet<int>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>> getDataFunction,
                Func<long, Task> callFailAndRecover,
                Func<long, long, bool, Task<SubstreamInitializeResponse>> targetInitializeRequest,
                Func<long, long, bool, Task> callRecieveCheckpointDone)
            {
            }

            public void SetReceiveAllocatorResolver(Func<int, FlowtideDotNet.Storage.Memory.IMemoryAllocator> allocatorResolver)
            {
            }

            public void OnStreamFailure()
            {
            }

            public Task<IReadOnlyList<SubstreamEventData>> GetData(IReadOnlySet<int> targetIds, int numberOfEvents, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task SendFailAndRecover(long restoreVersion) => Task.CompletedTask;
            public Task FailAndRecover(long restorePoint) => Task.CompletedTask;
            public Task<SubstreamInitializeResponse> SendInitializeRequest(long restoreVersion, long checkpointEpoch, bool cleanHandoff, CancellationToken cancellationToken) => Task.FromResult(new SubstreamInitializeResponse(false, true, restoreVersion));
            public Task<SubstreamInitializeResponse> TargetInitializeRequest(long restoreVersion, long peerCheckpointEpoch, bool cleanHandoff) => throw new NotImplementedException();
            public Task SendCheckpointDone(long checkpointVersion, long targetCheckpointEpoch, bool coversPeerStopBarrier) => Task.CompletedTask;
            public Task TargetCheckpointDone(long checkpointVersion, long checkpointEpoch, bool coversPeerStopBarrier) => throw new NotImplementedException();
        }

        [Fact]
        public async Task EmptyFetchesBackOff()
        {
            var handler = new CountingEmptyHandler();
            var point = new SubstreamCommunicationPoint(NullLogger.Instance, "subA", "subB", handler);

            // A subscribed target starts the fetch loop; the handler always answers empty.
            point.Subscribe(1, _ => Task.CompletedTask);

            await Task.Delay(TimeSpan.FromSeconds(2));
            point.Unsubscribe(1);

            var count = handler.FetchCount;
            Assert.True(count < 60,
                $"An idle fetch loop made {count} polls in two seconds; consecutive empty fetches must back off, in Orleans every poll is a grain call.");
            Assert.True(count > 3, $"The fetch loop only polled {count} times in two seconds, it appears stalled rather than backed off.");
        }
    }
}
