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
    public class FetchDataRequest
    {
        public FetchDataRequest(string requestor, IReadOnlySet<int> targetIds, int numberOfEvents)
        {
            TargetIds = targetIds;
            NumberOfEvents = numberOfEvents;
            Requestor = requestor;
        }

        [Id(0)]
        public IReadOnlySet<int> TargetIds { get; set; }

        [Id(1)]
        public int NumberOfEvents { get; set; }

        [Id(3)]
        public string Requestor { get; set; }
    }
}
