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

namespace FlowtideDotNet.Substrait.Relations
{
    /// <summary>
    /// This must be different from reference relation since it must be known if its output should be sent on the loop output
    /// or on the egress output from the fixed point vertex.
    /// </summary>
    public class IterationReferenceReadRelation : Relation
    {
        public override int OutputLength => ReferenceOutputLength;

        public int ReferenceOutputLength { get; set; }

        public required string IterationName { get; set; }

        public override TReturn Accept<TReturn, TState>(RelationVisitor<TReturn, TState> visitor, TState state)
        {
            return visitor.VisitIterationReferenceReadRelation(this, state);
        }
    }
}
