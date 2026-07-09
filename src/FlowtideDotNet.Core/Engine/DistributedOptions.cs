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

namespace FlowtideDotNet.Core.Engine
{
    public class DistributedOptions
    {
        public DistributedOptions(string substreamName, IPullBucketExchangeReadFactory? pullBucketExchangeReadFactory, ISubstreamCommunicationHandlerFactory communicationHandlerFactory)
        {
            SubstreamName = substreamName;
            PullBucketExchangeReadFactory = pullBucketExchangeReadFactory;
            CommunicationHandlerFactory = communicationHandlerFactory;
        }

        public string SubstreamName { get; }

        /// <summary>
        /// True when this substream resumes from a clean handoff stop, for example after a
        /// planned grain migration: the previous instance committed everything it consumed and
        /// froze its queues at a final barrier before stopping. The initialize handshake then
        /// announces the clean handoff so the other substreams accept the reconnect without
        /// rolling back. One-shot, applies to the next start only.
        /// </summary>
        public bool AnnounceCleanHandoff { get; set; }

        /// <summary>
        /// Factory to create the read operators to read from other streams.
        /// Only required when the plan contains pull bucket exchange targets.
        /// </summary>
        public IPullBucketExchangeReadFactory? PullBucketExchangeReadFactory { get; }

        public ISubstreamCommunicationHandlerFactory CommunicationHandlerFactory { get; }
    }
}
