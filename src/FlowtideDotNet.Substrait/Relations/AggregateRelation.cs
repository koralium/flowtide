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
    public class AggregateGrouping
    {
        public required List<Expression> GroupingExpressions { get; set; }
    }

    public class AggregateMeasure
    {
        public required AggregateFunction Measure { get; set; }

        public Expression? Filter { get; set; }
    }

    public class AggregateRelation : Relation
    {
        public required Relation Input { get; set; }

        public List<AggregateGrouping>? Groupings { get; set; }

        public List<AggregateMeasure>? Measures { get; set; }

        public override int OutputLength => CalculateOutputLength();

        private int CalculateOutputLength()
        {
            int length = 0;
            if (Groupings != null)
            {
                foreach(var group in Groupings)
                {
                    length += group.GroupingExpressions.Count;
                }
                // Add one length for the groupings identifier
                if (Groupings.Count > 1)
                {
                    length += 1;
                }
            }
            if (Measures != null)
            {
                length += Measures.Count;
            }
            return length;
        }

        public override TReturn Accept<TReturn, TState>(RelationVisitor<TReturn, TState> visitor, TState state)
        {
            return visitor.VisitAggregateRelation(this, state);
        }
    }
}
