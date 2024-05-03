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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Substrait.Relations
{
    public class TableFunctionRelation : Relation
    {
        public required TableFunction TableFunction { get; set; }

        public Relation? Input { get; set; }

        public JoinType Type { get; set; }

        public Expression? JoinCondition { get; set; }

        public override int OutputLength
        {
            get
            {
                if (EmitSet)
                {
                    return Emit.Count;
                }
                if (Input == null)
                {
                    return TableFunction.TableSchema.Names.Count;
                }
                return Input.OutputLength + TableFunction.TableSchema.Names.Count;
            }
        }

        public override TReturn Accept<TReturn, TState>(RelationVisitor<TReturn, TState> visitor, TState state)
        {
            return visitor.VisitTableFunctionRelation(this, state);
        }

        public override bool Equals(object? obj)
        {
            if (obj is TableFunctionRelation relation)
            {
                return base.Equals(relation) &&
                   Equals(TableFunction, relation.TableFunction) &&
                   Equals(Input, relation.Input) &&
                   Equals(Type, relation.Type) &&
                   Equals(JoinCondition, relation.JoinCondition);
            }
            return false;
        }

        public override int GetHashCode()
        {
            var code = new HashCode();
            code.Add(base.GetHashCode());
            code.Add(TableFunction);
            code.Add(Input);
            code.Add(Type);
            code.Add(JoinCondition);
            return code.ToHashCode();
        }
    }
}
