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

namespace FlowtideDotNet.Core.Engine.Distributed
{
    /// <summary>
    /// Communication handler that dispatches messages directly to the other substream
    /// running in the same process.
    /// The behavior mirrors the Orleans communication handler, if the other substream has not
    /// been built yet, initialize requests answer with not started, fetches return no data and
    /// notifications are ignored.
    /// </summary>
    internal class LocalSubstreamCommunicationHandler : ISubstreamCommunicationHandler
    {
        private readonly LocalSubstreamCommunicationHub _hub;
        private readonly string _selfSubstreamName;
        private readonly string _targetSubstreamName;

        private Func<IReadOnlyDictionary<int, long>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>>? _getDataFunction;
        private Func<long, Task>? _callFailAndRecover;
        private Func<long, Task<SubstreamInitializeResponse>>? _initializeFromTarget;
        private Func<long, IReadOnlyDictionary<int, long>, Task>? _callRecieveCheckpointDone;

        public LocalSubstreamCommunicationHandler(LocalSubstreamCommunicationHub hub, string selfSubstreamName, string targetSubstreamName)
        {
            _hub = hub;
            _selfSubstreamName = selfSubstreamName;
            _targetSubstreamName = targetSubstreamName;
        }

        /// <summary>
        /// True after the local communication point has registered its callbacks.
        /// </summary>
        public bool IsInitialized => _initializeFromTarget != null;

        public void Initialize(
            Func<IReadOnlyDictionary<int, long>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>> getDataFunction,
            Func<long, Task> callFailAndRecover,
            Func<long, Task<SubstreamInitializeResponse>> initializeFromTarget,
            Func<long, IReadOnlyDictionary<int, long>, Task> callRecieveCheckpointDone)
        {
            _getDataFunction = getDataFunction;
            _callFailAndRecover = callFailAndRecover;
            _initializeFromTarget = initializeFromTarget;
            _callRecieveCheckpointDone = callRecieveCheckpointDone;
        }

        public Task<IReadOnlyList<SubstreamEventData>> FetchData(IReadOnlyDictionary<int, long> targetFromEventIds, int numberOfEvents, CancellationToken cancellationToken)
        {
            if (_hub.TryGetPeerHandler(_selfSubstreamName, _targetSubstreamName, out var peer) &&
                peer._getDataFunction != null)
            {
                return peer._getDataFunction(targetFromEventIds, numberOfEvents, cancellationToken);
            }
            return Task.FromResult<IReadOnlyList<SubstreamEventData>>(new List<SubstreamEventData>());
        }

        public Task<SubstreamInitializeResponse> SendInitializeRequest(long restoreVersion, CancellationToken cancellationToken)
        {
            if (_hub.TryGetPeerHandler(_selfSubstreamName, _targetSubstreamName, out var peer) &&
                peer._initializeFromTarget != null)
            {
                return peer._initializeFromTarget(restoreVersion);
            }
            return Task.FromResult(new SubstreamInitializeResponse(true, false, restoreVersion));
        }

        public Task SendFailAndRecover(long restoreVersion)
        {
            if (_hub.TryGetPeerHandler(_selfSubstreamName, _targetSubstreamName, out var peer) &&
                peer._callFailAndRecover != null)
            {
                return peer._callFailAndRecover(restoreVersion);
            }
            // The other substream has not started yet, there is nothing to recover.
            return Task.CompletedTask;
        }

        public Task SendCheckpointDone(long checkpointVersion, IReadOnlyDictionary<int, long> consumedEventIds)
        {
            if (_hub.TryGetPeerHandler(_selfSubstreamName, _targetSubstreamName, out var peer) &&
                peer._callRecieveCheckpointDone != null)
            {
                return peer._callRecieveCheckpointDone(checkpointVersion, consumedEventIds);
            }
            // The other substream has not started yet, there is no pending checkpoint
            // that waits for this notification.
            return Task.CompletedTask;
        }
    }
}
