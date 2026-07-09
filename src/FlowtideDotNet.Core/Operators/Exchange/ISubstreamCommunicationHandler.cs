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
        /// Provides the allocators (keyed by exchange target id) used when received events are
        /// deserialized, so received data is accounted on the consuming read operator. Handlers that
        /// pass events by reference (e.g. in-process) can ignore this.
        /// </summary>
        void SetReceiveAllocatorResolver(Func<int, IMemoryAllocator> allocatorResolver)
        {
        }

        /// <summary>
        /// Called when the local stream fails, before rollback and restart. Handlers that fetch
        /// destructively over a network change their fetch epoch here so pre-failure fetches are
        /// recognized as stale and refused. In-process handlers can ignore this.
        /// </summary>
        void OnStreamFailure()
        {
        }

        /// <summary>
        /// Registers the local callbacks that are invoked when the other substream sends
        /// requests to this substream.
        /// </summary>
        /// <param name="getDataFunction">Returns events for the given exchange targets.</param>
        /// <param name="callFailAndRecover">Fails and recovers this substream to the given version.</param>
        /// <param name="initializeFromTarget">Handles an initialize request from the other substream:
        /// the restore point, the requestors checkpoint epoch, and whether the requestor resumes
        /// from a clean handoff stop.</param>
        /// <param name="callRecieveCheckpointDone">Handles a checkpoint done message with the completed checkpoint version and the epoch it was sent under.</param>
        void Initialize(
            Func<IReadOnlySet<int>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>> getDataFunction,
            Func<long, Task> callFailAndRecover,
            Func<long, long, bool, Task<SubstreamInitializeResponse>> initializeFromTarget,
            Func<long, long, Task> callRecieveCheckpointDone);

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

        /// <summary>
        /// Runs the initialize handshake against the other substream.
        /// </summary>
        /// <param name="restoreVersion">The version this substream restored to.</param>
        /// <param name="checkpointEpoch">This substreams current checkpoint epoch.</param>
        /// <param name="cleanHandoff">True when this substream resumes from a clean handoff stop
        /// (a planned migration): everything it consumed was committed and its queues were frozen
        /// at the final barrier, so the other substream can accept the reconnect without rolling
        /// back.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        Task<SubstreamInitializeResponse> SendInitializeRequest(long restoreVersion, long checkpointEpoch, bool cleanHandoff, CancellationToken cancellationToken);

        /// <summary>
        /// Notifies the other substream that a checkpoint has completed in this substream.
        /// </summary>
        /// <param name="checkpointVersion">The completed checkpoint version.</param>
        /// <param name="targetCheckpointEpoch">The receiving substream's checkpoint epoch as last learned through the handshake, so it can drop the ack if it is stale.</param>
        Task SendCheckpointDone(long checkpointVersion, long targetCheckpointEpoch);
    }
}
