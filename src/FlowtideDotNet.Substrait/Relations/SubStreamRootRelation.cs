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
    public sealed class SubStreamRootRelation : Relation, IEquatable<SubStreamRootRelation>
    {
        public required Relation Input { get; set; }

        public required string Name { get; set; }

        public override int OutputLength => Input.OutputLength;

        public override TReturn Accept<TReturn, TState>(RelationVisitor<TReturn, TState> visitor, TState state)
        {
            return visitor.VisitSubStreamRootRelation(this, state);
        }

        public override bool Equals(object? obj)
        {
            return obj is SubStreamRootRelation relation &&
                   Equals(relation);
        }

        public bool Equals(SubStreamRootRelation? other)
        {
            return other != null &&
                base.Equals(other) &&
                Equals(Input, other.Input) &&
                Equals(Name, other.Name);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(base.GetHashCode(), Input, Name);
        }

        public static bool operator ==(SubStreamRootRelation? left, SubStreamRootRelation? right)
        {
            return EqualityComparer<SubStreamRootRelation>.Default.Equals(left, right);
        }

        public static bool operator !=(SubStreamRootRelation? left, SubStreamRootRelation? right)
        {
            return !(left == right);
        }
    }
}
