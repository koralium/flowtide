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
using static SqlParser.Ast.FetchDirection;

namespace FlowtideDotNet.Substrait.Relations
{
    public sealed class SortRelation : Relation, IEquatable<SortRelation>
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

        public override TReturn Accept<TReturn, TState>(RelationVisitor<TReturn, TState> visitor, TState state)
        {
            return visitor.VisitSortRelation(this, state);
        }

        public override bool Equals(object? obj)
        {
            return obj is SortRelation relation &&
                Equals(relation);
        }

        public bool Equals(SortRelation? other)
        {
            return other != null &&
                base.Equals(other) &&
                Equals(Input, other.Input) &&
                Sorts.SequenceEqual(other.Sorts);
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
            return code.ToHashCode();
        }

        public static bool operator ==(SortRelation? left, SortRelation? right)
        {
            return EqualityComparer<SortRelation>.Default.Equals(left, right);
        }

        public static bool operator !=(SortRelation? left, SortRelation? right)
        {
            return !(left == right);
        }
    }
}
