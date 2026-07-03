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
    public class CheckpointDoneRequest
    {
        public CheckpointDoneRequest(string requestor, long checkpointVersion, IReadOnlyDictionary<int, long> consumedEventIds)
        {
            Requestor = requestor;
            CheckpointVersion = checkpointVersion;
            ConsumedEventIds = consumedEventIds;
        }

        [Id(0)]
        public string Requestor { get; }

        [Id(1)]
        public long CheckpointVersion { get; }

        /// <summary>
        /// The last event id per exchange target that is included in the completed checkpoint,
        /// the receiving substream can remove events up to and including these ids.
        /// </summary>
        [Id(2)]
        public IReadOnlyDictionary<int, long> ConsumedEventIds { get; }
    }
}
