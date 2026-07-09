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

namespace FlowtideDotNet.Orleans.Messages
{
    [GenerateSerializer]
    [Immutable]
    public class InitSubstreamRequest
    {
        public InitSubstreamRequest(string requestor, long restorePoint, long fetchEpoch, long checkpointEpoch = 0, bool cleanHandoff = false)
        {
            Requestor = requestor;
            RestorePoint = restorePoint;
            FetchEpoch = fetchEpoch;
            CheckpointEpoch = checkpointEpoch;
            CleanHandoff = cleanHandoff;
        }

        [Id(0)]
        public string Requestor { get; }

        [Id(1)]
        public long RestorePoint { get; }

        /// <summary>
        /// The requestors current fetch epoch, the receiving grain only serves fetches that
        /// carry this epoch until the next handshake announces a new one, see
        /// <see cref="FetchDataRequest.FetchEpoch"/>.
        /// </summary>
        [Id(2)]
        public long FetchEpoch { get; }

        /// <summary>
        /// The requestors current checkpoint epoch, recorded by the receiver so it can tag its
        /// checkpoint done acks with it, letting the requestor drop acks from a previous epoch.
        /// </summary>
        [Id(3)]
        public long CheckpointEpoch { get; }

        /// <summary>
        /// The requestor resumes from a clean handoff stop (e.g. a planned grain migration),
        /// so the receiver can accept the reconnect without rolling back.
        /// </summary>
        [Id(4)]
        public bool CleanHandoff { get; }
    }
}
