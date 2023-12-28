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

using FlexBuffers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Operators.Aggregate
{
    internal class AggregateRowState
    {
        /// <summary>
        /// State for each measure
        /// </summary>
        public byte[]?[] MeasureStates { get; set; }

        /// <summary>
        /// Total weight of all input events.
        /// If this goes to 0, all rows with these aggregates should be removed.
        /// </summary>
        public long Weight { get; set; }

        /// <summary>
        /// The previous value of the aggregate.
        /// Used when doing negation on updates to remove previous value from the stream.
        /// </summary>
        public byte[]? PreviousValue { get; set; }
    }
}
