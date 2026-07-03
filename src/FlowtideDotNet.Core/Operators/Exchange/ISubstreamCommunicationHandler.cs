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

using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Core.Operators.Exchange
{
    public interface ISubstreamCommunicationHandler
    {
        /// <summary>
        /// Provides the memory allocators used when received events are deserialized, keyed
        /// by exchange target id. The allocator belongs to the read operator that consumes
        /// the target, so received data is accounted on that operator. Handlers that pass
        /// events by reference, for example inside a single process, can ignore this.
        /// </summary>
        void SetReceiveAllocatorResolver(Func<int, IMemoryAllocator> allocatorResolver)
        {
        }

        /// <summary>
        /// Registers the local callbacks that are invoked when the other substream sends
        /// requests to this substream.
        /// </summary>
        /// <param name="getDataFunction">Returns events for the given exchange targets.</param>
        /// <param name="callFailAndRecover">Fails and recovers this substream to the given version.</param>
        /// <param name="initializeFromTarget">Handles an initialize request from the other substream.</param>
        /// <param name="callRecieveCheckpointDone">Handles a checkpoint done message with the completed checkpoint version.</param>
        void Initialize(
            Func<IReadOnlySet<int>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>> getDataFunction,
            Func<long, Task> callFailAndRecover,
            Func<long, Task<SubstreamInitializeResponse>> initializeFromTarget,
            Func<long, Task> callRecieveCheckpointDone);

        /// <summary>
        /// Fetches events from the other substream.
        /// </summary>
        /// <param name="targetIds">The exchange targets to fetch from.</param>
        /// <param name="numberOfEvents">Max number of events to fetch in total.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        Task<IReadOnlyList<SubstreamEventData>> FetchData(
            IReadOnlySet<int> targetIds,
            int numberOfEvents,
            CancellationToken cancellationToken);

        Task SendFailAndRecover(long restoreVersion);

        Task<SubstreamInitializeResponse> SendInitializeRequest(long restoreVersion, CancellationToken cancellationToken);

        /// <summary>
        /// Notifies the other substream that a checkpoint has completed in this substream.
        /// </summary>
        /// <param name="checkpointVersion">The completed checkpoint version.</param>
        Task SendCheckpointDone(long checkpointVersion);
    }
}
