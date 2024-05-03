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

namespace FlowtideDotNet.Substrait.Relations
{
    public class AggregateGrouping
    {
        public required List<Expression> GroupingExpressions { get; set; }

        public override bool Equals(object? obj)
        {
            return obj is AggregateGrouping grouping &&
                   GroupingExpressions.SequenceEqual(grouping.GroupingExpressions);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            foreach (var expression in GroupingExpressions)
            {
                code.Add(expression);
            }
            return code.ToHashCode();
        }
    }

    public class AggregateMeasure
    {
        public required AggregateFunction Measure { get; set; }

        public Expression? Filter { get; set; }

        public override bool Equals(object? obj)
        {
            return obj is AggregateMeasure measure &&
                   Equals(Measure, measure.Measure) &&
                   Equals(Filter, measure.Filter);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Measure, Filter);
        }
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

        public override bool Equals(object? obj)
        {
            if (obj is AggregateRelation relation)
            {
                if (Groupings == null && relation.Groupings != null)
                {
                    return false;
                }
                if (Groupings != null && relation.Groupings == null)
                {
                    return false;
                }
                if (Groupings != null && relation.Groupings != null &&
                    !Groupings.SequenceEqual(relation.Groupings))
                {
                    return false;
                }
                if (Measures == null && relation.Measures != null)
                {
                    return false;
                }
                if (Measures != null && relation.Measures == null)
                {
                    return false;
                }
                if (Measures != null && relation.Measures != null &&
                    !Measures.SequenceEqual(relation.Measures))
                {
                    return false;
                }
                return EmitEquals(relation.Emit) &&
                    Equals(Input, relation.Input);
            }
            return false;
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(Input);
            if (Emit != null)
            {
                foreach(var e in Emit)
                {
                    code.Add(e);
                }
            }
            if (Groupings != null)
            {
                foreach (var grouping in Groupings)
                {
                    code.Add(grouping);
                }
            }
            if (Measures != null)
            {
                foreach (var measure in Measures)
                {
                    code.Add(measure);
                }
            }
            return code.ToHashCode();
        }
    }
}
