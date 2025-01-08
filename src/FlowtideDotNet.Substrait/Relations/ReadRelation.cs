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
    public sealed class ReadRelation : Relation, IEquatable<ReadRelation>
    {
        public required NamedStruct BaseSchema { get; set; }

        public required NamedTable NamedTable { get; set; }

        public Expression? Filter { get; set; }

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
            return visitor.VisitReadRelation(this, state);
        }

        public override bool Equals(object? obj)
        {
            return obj is ReadRelation relation &&
                Equals(relation);
        }

        public bool Equals(ReadRelation? other)
        {
            return other != null &&
                base.Equals(other) &&
                Equals(BaseSchema, other.BaseSchema) &&
                Equals(NamedTable, other.NamedTable) &&
                Equals(Filter, other.Filter);
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(base.GetHashCode());
            code.Add(BaseSchema);
            code.Add(NamedTable);
            code.Add(Filter);

            return code.ToHashCode();
        }

        public static bool operator ==(ReadRelation? left, ReadRelation? right)
        {
            return EqualityComparer<ReadRelation>.Default.Equals(left, right);
        }

        public static bool operator !=(ReadRelation? left, ReadRelation? right)
        {
            return !(left == right);
        }

        public ReadRelation Copy()
        {
            var schema = new NamedStruct()
            {
                Names = BaseSchema.Names.ToList(),
                Nullable = BaseSchema.Nullable,
            };
            if (BaseSchema.Struct != null)
            {
                schema.Struct = new Struct()
                {
                    Types = BaseSchema.Struct.Types.ToList()
                };
            }
            return new ReadRelation
            {
                BaseSchema = schema,
                NamedTable = new NamedTable()
                {
                    Names = NamedTable.Names.ToList()
                },
                Filter = Filter,
                Emit = Emit
            };
        }
    }
}
