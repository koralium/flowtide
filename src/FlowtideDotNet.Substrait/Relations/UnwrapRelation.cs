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
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Substrait.Relations
{
    public sealed class UnwrapRelation : Relation, IEquatable<UnwrapRelation>
    {
        public override int OutputLength
        {
            get
            {
                if (EmitSet)
                {
                    return Emit!.Count;
                }
                // Expressions are appended, so if emit is not set it is input + extra columns
                return Input.OutputLength + BaseSchema.Names.Count;
            }
        }

        public required Relation Input { get; set; }

        /// <summary>
        /// Contains the schema of the used properties from the unwrap
        /// </summary>
        public required NamedStruct BaseSchema { get; set; }

        public Expression? Filter { get; set; }

        public required Expression Field { get; set; }

        public override TReturn Accept<TReturn, TState>(RelationVisitor<TReturn, TState> visitor, TState state)
        {
            return visitor.VisitUnwrapRelation(this, state);
        }

        public override bool Equals(object? obj)
        {
            return obj is UnwrapRelation relation &&
                Equals(relation);
        }

        public bool Equals(UnwrapRelation? other)
        {
            return other != null &&
                base.Equals(other) &&
                Equals(Input, other.Input) &&
                Equals(BaseSchema, other.BaseSchema) &&
                Equals(Filter, other.Filter) &&
                Equals(Field, other.Field);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(base.GetHashCode());
            code.Add(Input);
            code.Add(BaseSchema);
            code.Add(Filter);
            code.Add(Field);
            return code.ToHashCode();
        }

        public static bool operator ==(UnwrapRelation? left, UnwrapRelation? right)
        {
            return EqualityComparer<UnwrapRelation>.Default.Equals(left, right);
        }

        public static bool operator !=(UnwrapRelation? left, UnwrapRelation? right)
        {
            return !(left == right);
        }
    }
}
