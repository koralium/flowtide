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
        public SubstreamInitializeResponse(bool notStarted, bool success, long restoreVersion, long checkpointEpoch = 0, long recordedCheckpointEpoch = 0)
        {
            NotStarted = notStarted;
            Success = success;
            RestoreVersion = restoreVersion;
            CheckpointEpoch = checkpointEpoch;
            RecordedCheckpointEpoch = recordedCheckpointEpoch;
        }

        public bool NotStarted { get; }

        public bool Success { get; }

        public long RestoreVersion { get; }

        /// <summary>
        /// The responding substream's current checkpoint epoch. The requester records it as the
        /// peer epoch and tags its checkpoint done acks with it, so a stale ack from before a
        /// restart carries an old epoch and is dropped by the receiver.
        /// </summary>
        public long CheckpointEpoch { get; }

        /// <summary>
        /// The checkpoint epoch the responder has recorded for the REQUESTOR after handling this
        /// handshake (its record is highest-wins). Epochs are clock-seeded per process, so after a
        /// hard fail over onto a process whose clock seed is behind, a live requestor announces a
        /// lower epoch than its dead predecessor and the responder keeps the dead record - every
        /// ack the responder sends is then tagged with it and dropped by the requestor. When this
        /// value is higher than the epoch the requestor announced, it re-seeds above it and re-runs
        /// the handshake (see SubstreamCommunicationPoint.SendInitializeRequest) instead of being
        /// permanently fenced out of its acks.
        /// </summary>
        public long RecordedCheckpointEpoch { get; }
    }
}
