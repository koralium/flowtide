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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Cluster.Orleans.Messages
{
    [GenerateSerializer]
    [Immutable]
    public class InitSubstreamResponse
    {
        public InitSubstreamResponse(bool notStarted, bool success, long restoreVersion, long checkpointEpoch = 0, long recordedFetchEpoch = 0, long recordedCheckpointEpoch = 0, bool cleanReconnect = false)
        {
            NotStarted = notStarted;
            Success = success;
            RestoreVersion = restoreVersion;
            CheckpointEpoch = checkpointEpoch;
            RecordedFetchEpoch = recordedFetchEpoch;
            RecordedCheckpointEpoch = recordedCheckpointEpoch;
            CleanReconnect = cleanReconnect;
        }

        [Id(2)]
        public bool NotStarted { get; }

        [Id(0)]
        public bool Success { get; }

        [Id(1)]
        public long RestoreVersion { get; }

        /// <summary>
        /// The responding substream's checkpoint epoch, recorded by the requestor as the peer epoch
        /// and used to tag its checkpoint done acks.
        /// </summary>
        [Id(3)]
        public long CheckpointEpoch { get; }

        /// <summary>
        /// The fetch epoch the responding grain has recorded for the requestor after handling this
        /// handshake. Fetch epochs are drawn from a per-process clock-based seed, so after a silo
        /// failover a live requestor can announce a lower epoch than its dead predecessor and be
        /// refused as stale. The refusal is answered as an already reconciled success, so this value
        /// is the requestor's only way to detect it: when it is higher than the announced epoch, the
        /// requestor raises its seed above it and re-runs the handshake (see
        /// OrleansCommunicationHandler.SendInitializeRequest), instead of being permanently fenced
        /// out of its own data.
        /// </summary>
        [Id(4)]
        public long RecordedFetchEpoch { get; }

        /// <summary>
        /// The checkpoint epoch the responding substream has recorded for the requestor, the
        /// checkpoint-epoch counterpart of <see cref="RecordedFetchEpoch"/>: when it is higher than
        /// the epoch the requestor announced, a dead generation's record still stands and the
        /// requestor re-seeds above it and re-announces, see
        /// SubstreamCommunicationPoint.SendInitializeRequest.
        /// </summary>
        [Id(5)]
        public long RecordedCheckpointEpoch { get; }

        /// <summary>
        /// The responder accepted the requestors clean handoff reconnect and kept running. The
        /// requestor then completes startup from restored state instead of waiting for a restart.
        /// </summary>
        [Id(6)]
        public bool CleanReconnect { get; }
    }
}
