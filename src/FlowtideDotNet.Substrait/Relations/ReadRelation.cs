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
    public class ReadRelation : Relation
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
            if (obj is ReadRelation relation)
            {
                return EmitEquals(relation.Emit) && 
                   Equals(BaseSchema, relation.BaseSchema) &&
                   Equals(NamedTable, relation.NamedTable) &&
                   Equals(Filter, relation.Filter);
            }
            return false; 
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            if (Emit != null)
            {
                foreach (var emit in Emit)
                {
                    code.Add(emit);
                }
            }
            code.Add(BaseSchema);
            code.Add(NamedTable);
            code.Add(Filter);

            return code.ToHashCode();
        }
    }
}
