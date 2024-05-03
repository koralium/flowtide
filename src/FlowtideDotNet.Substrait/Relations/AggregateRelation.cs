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
using static SqlParser.Ast.FetchDirection;

namespace FlowtideDotNet.Substrait.Relations
{
    public class AggregateGrouping : IEquatable<AggregateGrouping>
    {
        public required List<Expression> GroupingExpressions { get; set; }

        public override bool Equals(object? obj)
        {
            return obj is AggregateGrouping grouping &&
                   GroupingExpressions.SequenceEqual(grouping.GroupingExpressions);
        }

        public bool Equals(AggregateGrouping? other)
        {
            return other != null &&
                   GroupingExpressions.SequenceEqual(other.GroupingExpressions);
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

        public static bool operator ==(AggregateGrouping? left, AggregateGrouping? right)
        {
            return EqualityComparer<AggregateGrouping>.Default.Equals(left, right);
        }

        public static bool operator !=(AggregateGrouping? left, AggregateGrouping? right)
        {
            return !(left == right);
        }
    }

    public class AggregateMeasure : IEquatable<AggregateMeasure>
    {
        public required AggregateFunction Measure { get; set; }

        public Expression? Filter { get; set; }

        public override bool Equals(object? obj)
        {
            return obj is AggregateMeasure measure &&
                   Equals(measure);
        }

        public bool Equals(AggregateMeasure? other)
        {
            return other != null &&
                   Equals(Measure, other.Measure) &&
                   Equals(Filter, other.Filter);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Measure, Filter);
        }

        public static bool operator ==(AggregateMeasure? left, AggregateMeasure? right)
        {
            return EqualityComparer<AggregateMeasure>.Default.Equals(left, right);
        }

        public static bool operator !=(AggregateMeasure? left, AggregateMeasure? right)
        {
            return !(left == right);
        }
    }

    public class AggregateRelation : Relation, IEquatable<AggregateRelation>
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
            return obj is AggregateRelation relation &&
                Equals(relation);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(Input);
            code.Add(base.GetHashCode());
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

        public bool Equals(AggregateRelation? other)
        {
            if (other == null)
            {
                return false;
            }
            if (Groupings == null && other.Groupings != null)
            {
                return false;
            }
            if (Groupings != null && other.Groupings == null)
            {
                return false;
            }
            if (Groupings != null && other.Groupings != null &&
                !Groupings.SequenceEqual(other.Groupings))
            {
                return false;
            }
            if (Measures == null && other.Measures != null)
            {
                return false;
            }
            if (Measures != null && other.Measures == null)
            {
                return false;
            }
            if (Measures != null && other.Measures != null &&
                !Measures.SequenceEqual(other.Measures))
            {
                return false;
            }
            return base.Equals(other) &&
                Equals(Input, other.Input);
        }

        public static bool operator ==(AggregateRelation? left, AggregateRelation? right)
        {
            return EqualityComparer<AggregateRelation>.Default.Equals(left, right);
        }

        public static bool operator !=(AggregateRelation? left, AggregateRelation? right)
        {
            return !(left == right);
        }
    }
}
