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

using FlowtideDotNet.Substrait.Expressions.Functions;
using FlowtideDotNet.Substrait.Expressions.IfThen;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.Expressions.ScalarFunctions;

namespace FlowtideDotNet.Substrait.Expressions
{
    public class ExpressionVisitor<TOutput, TState>
    {
        public virtual TOutput? Visit(Expression expression, TState state)
        {
            return expression.Accept(this, state);
        }

        public virtual TOutput? VisitEqualsFunction(EqualsFunction equalsFunction, TState state)
        {
            equalsFunction.Left.Accept(this, state);
            equalsFunction.Right.Accept(this, state);
            return default;
        }

        public virtual TOutput? VisitDirectFieldReference(DirectFieldReference directFieldReference, TState state)
        {
            return default;
        }

        public virtual TOutput? VisitAndFunction(AndFunction andFunction, TState state)
        {
            foreach(var arg in andFunction.Arguments)
            {
                arg.Accept(this, state);
            }
            return default;
        }

        public virtual TOutput? VisitStringLiteral(StringLiteral stringLiteral, TState state)
        {
            return default;
        }

        public virtual TOutput? VisitOrFunction(OrFunction orFunction, TState state)
        {
            foreach(var arg in orFunction.Arguments)
            {
                arg.Accept(this, state);
            }
            return default;
        }

        public virtual TOutput? VisitGetDateTimeFunction(GetDateTimeFunction getDateTimeFunction, TState state)
        {
            return default;
        }

        public virtual TOutput? VisitBooleanComparison(BooleanComparison booleanComparison, TState state)
        {
            booleanComparison.Left.Accept(this, state);
            booleanComparison.Right.Accept(this, state);
            return default;
        }

        public virtual TOutput? VisitConcatFunction(ConcatFunction concatFunction, TState state)
        {
            foreach(var expr in concatFunction.Expressions)
            {
                expr.Accept(this, state);
            }
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

        public virtual TOutput? VisitMakeArrayFunction(MakeArrayFunction makeArrayFunction, TState state)
        {
            foreach (var expr in makeArrayFunction.Expressions)
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

        public virtual TOutput? VisitIsNotNull(IsNotNullFunction isNotNullFunction, TState state)
        {
            isNotNullFunction.Expression.Accept(this, state);
            return default;
        }

        public virtual TOutput? VisitBoolLiteral(BoolLiteral boolLiteral, TState state)
        {
            return default;
        }
    }
}
