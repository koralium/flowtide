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
    public class CheckpointDoneRequest
    {
        public CheckpointDoneRequest(string requestor, long checkpointVersion, long checkpointEpoch, bool coversPeerStopBarrier = false)
        {
            Requestor = requestor;
            CheckpointVersion = checkpointVersion;
            CheckpointEpoch = checkpointEpoch;
            CoversPeerStopBarrier = coversPeerStopBarrier;
        }

        [Id(0)]
        public string Requestor { get; }

        [Id(1)]
        public long CheckpointVersion { get; }

        /// <summary>
        /// The receiving substream's checkpoint epoch as the sender last learned it through the
        /// handshake. The receiver drops the ack if it no longer matches its current epoch.
        /// </summary>
        [Id(2)]
        public long CheckpointEpoch { get; }

        /// <summary>
        /// True when the committed cycle covers consuming the receiving substreams stop
        /// barriers. A stopping receiver only confirms its drain on a stamped ack; an
        /// unstamped one can be for a cycle committed before the barrier was consumed.
        /// </summary>
        [Id(3)]
        public bool CoversPeerStopBarrier { get; }
    }
}
