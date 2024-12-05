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

using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Substrait.Relations
{
    public sealed class VirtualTableReadRelation : Relation, IEquatable<VirtualTableReadRelation>
    {
        public required NamedStruct BaseSchema { get; set; }
        
        public required VirtualTable Values { get; set; }

        public override int OutputLength
        {
            get
            {
                if (EmitSet)
                {
                    return Emit.Count;
                }
                return BaseSchema.Names.Count;
            }
        }

        public override TReturn Accept<TReturn, TState>(RelationVisitor<TReturn, TState> visitor, TState state)
        {
            return visitor.VisitVirtualTableReadRelation(this, state);
        }

        public override bool Equals(object? obj)
        {
            return obj is VirtualTableReadRelation relation &&
                Equals(relation);
        }

        public bool Equals(VirtualTableReadRelation? other)
        {
            return other != null &&
                base.Equals(other) &&
                Equals(BaseSchema, other.BaseSchema) &&
                Equals(Values, other.Values);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(base.GetHashCode());
            code.Add(BaseSchema);
            code.Add(Values);
            return code.ToHashCode();
        }

        public static bool operator ==(VirtualTableReadRelation? left, VirtualTableReadRelation? right)
        {
            return EqualityComparer<VirtualTableReadRelation>.Default.Equals(left, right);
        }

        public static bool operator !=(VirtualTableReadRelation? left, VirtualTableReadRelation? right)
        {
            return !(left == right);
        }
    }
}
