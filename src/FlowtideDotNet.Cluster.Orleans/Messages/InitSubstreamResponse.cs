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
        public InitSubstreamResponse(bool notStarted, bool success, long restoreVersion, long checkpointEpoch = 0, long recordedFetchEpoch = 0, long recordedCheckpointEpoch = 0, bool cleanReconnect = false, bool peerDraining = false, long waveCounter = 0, Guid waveId = default, bool peerInInit = false)
        {
            NotStarted = notStarted;
            Success = success;
            RestoreVersion = restoreVersion;
            CheckpointEpoch = checkpointEpoch;
            RecordedFetchEpoch = recordedFetchEpoch;
            RecordedCheckpointEpoch = recordedCheckpointEpoch;
            CleanReconnect = cleanReconnect;
            PeerDraining = peerDraining;
            WaveCounter = waveCounter;
            WaveId = waveId;
            PeerInInit = peerInInit;
        }

        /// <summary>
        /// With NotStarted, the substream is stopping, budget not spent.
        /// </summary>
        [Id(7)]
        public bool PeerDraining { get; }

        /// <summary>
        /// The recovery the answering substream is in.
        /// </summary>
        [Id(8)]
        public long WaveCounter { get; }

        [Id(9)]
        public Guid WaveId { get; }

        [Id(10)]
        public bool PeerInInit { get; }

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
        /// Recorded fetch epoch, a higher one makes the requestor re-announce.
        /// </summary>
        [Id(4)]
        public long RecordedFetchEpoch { get; }

        /// <summary>
        /// Recorded checkpoint epoch, a higher one makes the requestor re-announce.
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
