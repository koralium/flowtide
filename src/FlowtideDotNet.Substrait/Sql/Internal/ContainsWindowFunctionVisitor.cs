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

using SqlParser.Ast;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Substrait.Sql.Internal
{
    internal class ContainsWindowFunctionVisitor : BaseExpressionVisitor<bool, object?>
    {
        private readonly SqlFunctionRegister sqlFunctionRegister;
        private HashSet<Expression.Function> _windowFunctions;

        public ContainsWindowFunctionVisitor(SqlFunctionRegister sqlFunctionRegister)
        {
            this.sqlFunctionRegister = sqlFunctionRegister;
            _windowFunctions = new HashSet<Expression.Function>();
        }

        public IReadOnlySet<Expression.Function> WindowFunctions => _windowFunctions;

        public bool VisitSelectItem(SelectItem selectItem)
        {
            if (selectItem is SelectItem.ExpressionWithAlias expressionWithAlias)
            {
                return Visit(expressionWithAlias.Expression, default);
            }
            if (selectItem is SelectItem.UnnamedExpression unnamedExpression)
            {
                return Visit(unnamedExpression.Expression, default);
            }
            return false;
        }

        protected override bool VisitBinaryOperation(Expression.BinaryOp binaryOp, object? state)
        {
            return Visit(binaryOp.Left, state) | Visit(binaryOp.Right, state);
        }

        protected override bool VisitCaseExpression(Expression.Case caseExpression, object? state)
        {
            var containsAggregate = false;
            foreach (var cond in caseExpression.Conditions)
            {
                containsAggregate |= Visit(cond, state);
            }
            foreach (var result in caseExpression.Results)
            {
                containsAggregate |= Visit(result, state);
            }
            if (caseExpression.ElseResult != null)
            {
                containsAggregate |= Visit(caseExpression.ElseResult, state);
            }

            return containsAggregate;
        }

        protected override bool VisitFunction(Expression.Function function, object? state)
        {
            bool containsAggregate = false;
            if (function.Over != null)
            {
                _windowFunctions.Add(function);
                containsAggregate = true;
            }
            
            var argList = BuiltInSqlFunctions.GetFunctionArguments(function.Args);
            if (argList.Args != null)
            {
                foreach (var arg in argList.Args)
                {
                    if (arg is FunctionArg.Named namedFunctionArg)
                    {
                        if (namedFunctionArg.Arg is FunctionArgExpression.FunctionExpression namedFuncArgExpr)
                        {
                            containsAggregate |= Visit(namedFuncArgExpr.Expression, state);
                        }
                    }
                }
            }

            return containsAggregate;
        }

        protected override bool VisitIsNotNull(Expression.IsNotNull isNotNull, object? state)
        {
            return Visit(isNotNull.Expression, state);
        }

        protected override bool VisitCompoundIdentifier(Expression.CompoundIdentifier compoundIdentifier, object? state)
        {
            return false;
        }

        protected override bool VisitLiteralValue(Expression.LiteralValue literalValue, object? state)
        {
            return false;
        }

        protected override bool VisitFloor(Expression.Floor floor, object? state)
        {
            return Visit(floor.Expression, state);
        }

        protected override bool VisitCeil(Expression.Ceil ceil, object? state)
        {
            return Visit(ceil.Expression, state);
        }

        protected override bool VisitUnaryOperation(Expression.UnaryOp unaryOp, object? state)
        {
            return Visit(unaryOp.Expression, state);
        }

        protected override bool VisitBetween(Expression.Between between, object? state)
        {
            bool containsAggregate = false;
            containsAggregate |= Visit(between.Expression, state);
            containsAggregate |= Visit(between.Low, state);
            containsAggregate |= Visit(between.High, state);
            return containsAggregate;
        }

        protected override bool VisitIsNull(Expression.IsNull isNull, object? state)
        {
            return Visit(isNull.Expression, state);
        }

        protected override bool VisitInList(Expression.InList inList, object? state)
        {
            bool containsAggregate = false;
            containsAggregate |= Visit(inList.Expression, state);
            foreach (var o in inList.List)
            {
                containsAggregate |= Visit(o, state);
            }
            return containsAggregate;
        }

        protected override bool VisitTrim(Expression.Trim trim, object? state)
        {
            return Visit(trim.Expression, state);
        }

        protected override bool VisitCast(Expression.Cast cast, object? state)
        {
            return Visit(cast.Expression, state);
        }

        protected override bool VisitSubstring(Expression.Substring substring, object? state)
        {
            return Visit(substring.Expression, state);
        }

        protected override bool VisitNested(Expression.Nested nested, object? state)
        {
            return Visit(nested.Expression, state);
        }

        protected override bool VisitLike(Expression.Like like, object? state)
        {
            bool containsAggregate = false;
            if (like.Expression != null)
            {
                containsAggregate |= Visit(like.Expression, state);
            }
            containsAggregate |= Visit(like.Pattern, state);
            return containsAggregate;
        }
    }
}
