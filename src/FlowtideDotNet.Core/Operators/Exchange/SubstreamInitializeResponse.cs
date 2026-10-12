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

namespace FlowtideDotNet.Core.Operators.Exchange
{
    public class SubstreamInitializeResponse
    {
        public SubstreamInitializeResponse(bool notStarted, bool success, long restoreVersion, long checkpointEpoch = 0, long recordedCheckpointEpoch = 0, bool cleanReconnect = false, bool peerDraining = false, RecoveryWave wave = default, bool peerInInit = false)
        {
            Wave = wave;
            PeerInInit = peerInInit;
            NotStarted = notStarted;
            Success = success;
            RestoreVersion = restoreVersion;
            CheckpointEpoch = checkpointEpoch;
            RecordedCheckpointEpoch = recordedCheckpointEpoch;
            CleanReconnect = cleanReconnect;
            PeerDraining = peerDraining;
        }

        public bool NotStarted { get; }

        /// <summary>
        /// The recovery the answering substream is in.
        /// </summary>
        public RecoveryWave Wave { get; }

        /// <summary>
        /// The answering substream has not finished its own start in that recovery.
        /// </summary>
        public bool PeerInInit { get; }

        /// <summary>
        /// Peer draining, wait it out, start retry budget not spent.
        /// </summary>
        public bool PeerDraining { get; }

        public bool Success { get; }

        public long RestoreVersion { get; }

        /// <summary>
        /// The responding substream's current checkpoint epoch. The requester records it as the
        /// peer epoch and tags its checkpoint done acks with it, so a stale ack from before a
        /// restart carries an old epoch and is dropped by the receiver.
        /// </summary>
        public long CheckpointEpoch { get; }

        /// <summary>
        /// Recorded requestor epoch, a higher one makes the requestor re-announce.
        /// </summary>
        public long RecordedCheckpointEpoch { get; }

        /// <summary>
        /// The responder accepted the requestors clean handoff reconnect and kept running. The
        /// requestor then completes startup from restored state instead of waiting for a restart.
        /// </summary>
        public bool CleanReconnect { get; }
    }
}
