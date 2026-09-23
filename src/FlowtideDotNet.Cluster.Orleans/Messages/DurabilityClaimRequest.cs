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

namespace FlowtideDotNet.Cluster.Orleans.Messages
{
    [GenerateSerializer]
    [Immutable]
    public class DurabilityClaimRequest
    {
        public DurabilityClaimRequest(string requestor, long version, int radius, long initVersion, long waveCounter, Guid waveId, long senderCheckpointEpoch, long targetCheckpointEpoch, bool requestReply = false)
        {
            RequestReply = requestReply;
            Requestor = requestor;
            Version = version;
            Radius = radius;
            InitVersion = initVersion;
            WaveCounter = waveCounter;
            WaveId = waveId;
            SenderCheckpointEpoch = senderCheckpointEpoch;
            TargetCheckpointEpoch = targetCheckpointEpoch;
        }

        [Id(0)]
        public string Requestor { get; }

        [Id(1)]
        public long Version { get; }

        /// <summary>
        /// Every substream within this many hops of the sender is durable at the version.
        /// </summary>
        [Id(2)]
        public int Radius { get; }

        /// <summary>
        /// The senders checkpoint epoch, a claim from before its restart is dropped.
        /// </summary>
        [Id(3)]
        public long SenderCheckpointEpoch { get; }

        /// <summary>
        /// The receiving substream's checkpoint epoch as the sender last learned it.
        /// </summary>
        [Id(4)]
        public long TargetCheckpointEpoch { get; }

        /// <summary>
        /// The sender still waits for an agreement and asks for everything the receiver claims.
        /// </summary>
        [Id(5)]
        public bool RequestReply { get; }

        /// <summary>
        /// The recovery of the group the claim belongs to.
        /// </summary>
        [Id(6)]
        public long WaveCounter { get; }

        [Id(7)]
        public Guid WaveId { get; }

        /// <summary>
        /// The version the requestor started its run at.
        /// </summary>
        [Id(8)]
        public long InitVersion { get; }
    }
}
