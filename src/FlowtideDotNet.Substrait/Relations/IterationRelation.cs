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

using FlowtideDotNet.Substrait.Expressions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Substrait.Relations
{
    public class IterationRelation : Relation
    {
        public override int OutputLength
        {
            get
            {
                return LoopPlan.OutputLength;
            }
        }

        public Relation? Input { get; set; }

        public required string IterationName { get; set; }

        /// <summary>
        /// The loop plan
        /// </summary>
        public required Relation LoopPlan { get; set; }

        public Expression? ShouldIterateCondition { get; set; }

        public override TReturn Accept<TReturn, TState>(RelationVisitor<TReturn, TState> visitor, TState state)
        {
            return visitor.VisitIterationRelation(this, state);
        }
    }
}
