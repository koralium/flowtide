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

using FlowtideDotNet.Base;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Operators.Exchange
{
    internal class ExchangeFetchDataMessage
    {
        /// <summary>
        /// Input
        /// </summary>
        public long FromEventId { get; set; }

        /// <summary>
        /// Set during output with a list of events
        /// </summary>
        public List<IStreamEvent>? OutEvents { get; set; }

        /// <summary>
        /// Set during output with the last eventid in the out events
        /// </summary>
        public long LastEventId { get; set; }
    }
}
