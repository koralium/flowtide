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


using FlowtideDotNet.Substrait.Expressions.IfThen;
using FlowtideDotNet.Substrait.Expressions.Literals;

namespace FlowtideDotNet.Substrait.Expressions
{
    public class ExpressionVisitor<TOutput, TState>
    {
        public virtual TOutput? Visit(Expression expression, TState state)
        {
            if (expression == null)
            {
                return default;
            }
            return expression.Accept(this, state);
        }

        public virtual TOutput? VisitDirectFieldReference(DirectFieldReference directFieldReference, TState state)
        {
            return default;
        }

        public virtual TOutput? VisitStringLiteral(StringLiteral stringLiteral, TState state)
        {
            return default;
        }

        public virtual TOutput? VisitArrayLiteral(ArrayLiteral arrayLiteral, TState state)
        {
            foreach (var expr in arrayLiteral.Expressions)
            {
                expr.Accept(this, state);
            }
            return default;
        }

        public virtual TOutput? VisitNumericLiteral(NumericLiteral numericLiteral, TState state)
        {
            return default;
        }

        public virtual TOutput? VisitNullLiteral(NullLiteral nullLiteral, TState state)
        {
            return default;
        }

        public virtual TOutput? VisitIfThen(IfThenExpression ifThenExpression, TState state)
        {
            if (ifThenExpression.Ifs != null)
            {
                foreach(var ifClause in ifThenExpression.Ifs)
                {
                    ifClause.If.Accept(this, state);
                    ifClause.Then.Accept(this, state);
                }
            }
            if (ifThenExpression.Else != null)
            {
                ifThenExpression.Else.Accept(this, state);
            }
            return default;
        }

        public virtual TOutput? VisitBoolLiteral(BoolLiteral boolLiteral, TState state)
        {
            return default;
        }

        public virtual TOutput? VisitScalarFunction(ScalarFunction scalarFunction, TState state)
        {
            if (scalarFunction.Arguments != null)
            {
                foreach(var arg in scalarFunction.Arguments)
                {
                    arg.Accept(this, state);
                }
            }
            return default;
        }

        public virtual TOutput? VisitSingularOrList(SingularOrListExpression singularOrList, TState state)
        {
            if (singularOrList.Value != null)
            {
                singularOrList.Value.Accept(this, state);
            }
            if (singularOrList.Options != null)
            {
                foreach(var opt in singularOrList.Options)
                {
                    opt.Accept(this, state);
                }
            }
            return default;
        }

        public virtual TOutput? VisitMultiOrList(MultiOrListExpression multiOrList, TState state)
        {
            if (multiOrList.Value != null)
            {
                foreach(var e in multiOrList.Value)
                {
                    e.Accept(this, state);
                }
            }
            if (multiOrList.Options != null)
            {
                foreach(var record in multiOrList.Options)
                {
                    if (record.Fields != null)
                    {
                        foreach(var field in record.Fields)
                        {
                            field.Accept(this, state);
                        }
                    }
                }
            }
            return default;
        }

        public virtual TOutput? VisitMapNestedExpression(MapNestedExpression mapNestedExpression, TState state)
        {
            if (mapNestedExpression.KeyValues != null)
            {
                foreach(var kv in mapNestedExpression.KeyValues)
                {
                    Visit(kv.Key, state);
                    Visit(kv.Value, state);
                }
            }
            return default;
        }

        public virtual TOutput? VisitListNestedExpression(ListNestedExpression listNestedExpression, TState state)
        {
            if (listNestedExpression.Values != null)
            {
                foreach(var val in listNestedExpression.Values)
                {
                    Visit(val, state);
                }
            }
            return default;
        }
    }
}
