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

using FlowtideDotNet.Base.Engine.Internal.StateMachine;
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

        private Func<IReadOnlySet<int>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>>? _getDataFunction;
        private Func<long, Task>? _callFailAndRecover;
        private Func<long, long, bool, Task<SubstreamInitializeResponse>>? _initializeFromTarget;
        private Func<long, long, bool, Task>? _callRecieveCheckpointDone;

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
            Func<IReadOnlySet<int>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>> getDataFunction,
            Func<long, Task> callFailAndRecover,
            Func<long, long, bool, Task<SubstreamInitializeResponse>> initializeFromTarget,
            Func<long, long, bool, Task> callRecieveCheckpointDone)
        {
            _getDataFunction = getDataFunction;
            _callFailAndRecover = callFailAndRecover;
            _initializeFromTarget = initializeFromTarget;
            _callRecieveCheckpointDone = callRecieveCheckpointDone;
        }

        public Task<IReadOnlyList<SubstreamEventData>> FetchData(IReadOnlySet<int> targetIds, int numberOfEvents, CancellationToken cancellationToken)
        {
            if (_hub.TryGetPeerHandler(_selfSubstreamName, _targetSubstreamName, out var peer) &&
                peer._getDataFunction != null)
            {
                if (StreamContext.OwnStartInitGate.Value != null)
                {
                    return Detached((peer._getDataFunction, targetIds, numberOfEvents, cancellationToken), static s => s.Item1(s.targetIds, s.numberOfEvents, s.cancellationToken));
                }
                return peer._getDataFunction(targetIds, numberOfEvents, cancellationToken);
            }
            return Task.FromResult<IReadOnlyList<SubstreamEventData>>(new List<SubstreamEventData>());
        }

        public Task<SubstreamInitializeResponse> SendInitializeRequest(long restoreVersion, long checkpointEpoch, bool cleanHandoff, CancellationToken cancellationToken)
        {
            if (_hub.TryGetPeerHandler(_selfSubstreamName, _targetSubstreamName, out var peer) &&
                peer._initializeFromTarget != null)
            {
                if (StreamContext.OwnStartInitGate.Value != null)
                {
                    return Detached((peer._initializeFromTarget, restoreVersion, checkpointEpoch, cleanHandoff), static s => s.Item1(s.restoreVersion, s.checkpointEpoch, s.cleanHandoff));
                }
                return peer._initializeFromTarget(restoreVersion, checkpointEpoch, cleanHandoff);
            }
            return Task.FromResult(new SubstreamInitializeResponse(true, false, restoreVersion));
        }

        public Task SendFailAndRecover(long restoreVersion)
        {
            if (_hub.TryGetPeerHandler(_selfSubstreamName, _targetSubstreamName, out var peer) &&
                peer._callFailAndRecover != null)
            {
                if (StreamContext.OwnStartInitGate.Value != null)
                {
                    return Detached((peer._callFailAndRecover, restoreVersion), static s => s.Item1(s.restoreVersion));
                }
                return peer._callFailAndRecover(restoreVersion);
            }
            // The other substream has not started yet, there is nothing to recover.
            return Task.CompletedTask;
        }

        public Task SendCheckpointDone(long checkpointVersion, long targetCheckpointEpoch, bool coversPeerStopBarrier)
        {
            if (_hub.TryGetPeerHandler(_selfSubstreamName, _targetSubstreamName, out var peer) &&
                peer._callRecieveCheckpointDone != null)
            {
                if (StreamContext.OwnStartInitGate.Value != null)
                {
                    return Detached((peer._callRecieveCheckpointDone, checkpointVersion, targetCheckpointEpoch, coversPeerStopBarrier), static s => s.Item1(s.checkpointVersion, s.targetCheckpointEpoch, s.coversPeerStopBarrier));
                }
                return peer._callRecieveCheckpointDone(checkpointVersion, targetCheckpointEpoch, coversPeerStopBarrier);
            }
            // The other substream has not started yet, there is no pending checkpoint
            // that waits for this notification.
            return Task.CompletedTask;
        }

        /// <summary>
        /// Calls the peer without this substream's start marker, work it spawns must not count as this start's own chain.
        /// </summary>
        private static async Task<TResult> Detached<TState, TResult>(TState state, Func<TState, Task<TResult>> call)
        {
            StreamContext.OwnStartInitGate.Value = null;
            return await call(state);
        }

        private static async Task Detached<TState>(TState state, Func<TState, Task> call)
        {
            StreamContext.OwnStartInitGate.Value = null;
            await call(state);
        }
    }
}
