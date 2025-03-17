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
    public sealed class TopNRelation : Relation, IEquatable<TopNRelation>
    {
        public override int OutputLength
        {
            get
            {
                if (EmitSet)
                {
                    return Emit!.Count;
                }
                return Input.OutputLength;
            }
        }

        public required Relation Input { get; set; }

        public required List<SortField> Sorts { get; set; }

        public int Offset { get; set; }

        public int Count { get; set; }

        public override TReturn Accept<TReturn, TState>(RelationVisitor<TReturn, TState> visitor, TState state)
        {
            return visitor.VisitTopNRelation(this, state);
        }

        public override bool Equals(object? obj)
        {
            return obj is TopNRelation relation &&
                Equals(relation);
        }

        public bool Equals(TopNRelation? other)
        {
            return other != null &&
                base.Equals(other) &&
                Equals(Input, other.Input) &&
                Sorts.SequenceEqual(other.Sorts) &&
                Equals(Offset, other.Offset) &&
                Equals(Count, other.Count);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(base.GetHashCode());
            code.Add(Input);
            foreach (var sort in Sorts)
            {
                code.Add(sort);
            }
            code.Add(Offset);
            code.Add(Count);
            return code.ToHashCode();
        }

        public static bool operator ==(TopNRelation? left, TopNRelation? right)
        {
            return EqualityComparer<TopNRelation>.Default.Equals(left, right);
        }

        public static bool operator !=(TopNRelation? left, TopNRelation? right)
        {
            return !(left == right);
        }
    }
}
