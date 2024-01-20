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

namespace FlowtideDotNet.Substrait.Sql
{
    public abstract class BaseExpressionVisitor<TReturn, TState>
    {
        public virtual TReturn Visit(Expression expression, TState state)
        {
            if (expression is Expression.BinaryOp binaryOp)
            {
                return VisitBinaryOperation(binaryOp, state);
            }
            if (expression is Expression.CompoundIdentifier compoundIdentifier)
            {
                return VisitCompoundIdentifier(compoundIdentifier, state);
            }
            if (expression is Expression.Identifier identifier)
            {
                // Convert the identifier to a compound identifier
                var compound = new Expression.CompoundIdentifier(new SqlParser.Sequence<Ident>(new List<Ident>() { identifier.Ident }));
                return VisitCompoundIdentifier(compound, state);
            }
            if (expression is Expression.LiteralValue literalValue)
            {
                return VisitLiteralValue(literalValue, state);
            }
            if (expression is Expression.Case caseExpr)
            {
                return VisitCaseExpression(caseExpr, state);
            }
            if (expression is Expression.Function function)
            {
                return VisitFunction(function, state);
            }
            if (expression is Expression.IsNotNull isNotNull)
            {
                return VisitIsNotNull(isNotNull, state);
            }
            if (expression is Expression.Floor floor)
            {
                return VisitFloor(floor, state);
            }
            if (expression is Expression.Ceil ceil)
            {
                return VisitCeil(ceil, state);
            }
            if (expression is Expression.UnaryOp unaryOp)
            {
                return VisitUnaryOperation(unaryOp, state);
            }
            if (expression is Expression.Between between)
            {
                return VisitBetween(between, state);
            }
            if (expression is Expression.IsNull isNull)
            {
                return VisitIsNull(isNull, state);
            }
            if (expression is Expression.InList inList)
            {
                return VisitInList(inList, state);
            }
            if (expression is Expression.Trim trim)
            {
                return VisitTrim(trim, state);
            }
            if (expression is Expression.Cast cast)
            {
                return VisitCast(cast, state);
            }
            throw new NotImplementedException($"The expression '{expression.GetType().Name}' is not supported in SQL");
        }

        protected virtual TReturn VisitCast(Expression.Cast cast, TState state)
        {
            throw new NotImplementedException($"The expression '{cast.GetType().Name}' is not supported in SQL");
        }

        protected virtual TReturn VisitTrim(Expression.Trim trim, TState state)
        {
            throw new NotImplementedException($"The expression '{trim.GetType().Name}' is not supported in SQL");
        }

        protected virtual TReturn VisitInList(Expression.InList inList, TState state)
        {
            throw new NotImplementedException($"The expression '{inList.GetType().Name}' is not supported in SQL");
        }

        protected virtual TReturn VisitIsNull(Expression.IsNull isNull, TState state)
        {
            throw new NotImplementedException($"The expression '{isNull.GetType().Name}' is not supported in SQL");
        }

        protected virtual TReturn VisitBetween(Expression.Between between, TState state)
        {
            throw new NotImplementedException($"The expression '{between.GetType().Name}' is not supported in SQL");
        }

        protected virtual TReturn VisitUnaryOperation(Expression.UnaryOp unaryOp, TState state)
        {
            throw new NotImplementedException($"The unary expression '{unaryOp.Op.ToString()}' is not supported in SQL");
        }

        protected virtual TReturn VisitCeil(Expression.Ceil ceil, TState state)
        {
            throw new NotImplementedException("Ceil is not supported in SQL");
        }

        protected virtual TReturn VisitFloor(Expression.Floor floor, TState state)
        {
            throw new NotImplementedException("Floor is not supported in SQL");
        }

        protected virtual TReturn VisitIsNotNull(Expression.IsNotNull isNotNull, TState state)
        {
            throw new NotImplementedException($"The expression '{isNotNull.GetType().Name}' is not supported in SQL");
        }

        protected virtual TReturn VisitFunction(Expression.Function function, TState state)
        {
            throw new NotImplementedException($"The function {function.Name} is not supported in SQL.");
        }

        protected virtual TReturn VisitCaseExpression(Expression.Case caseExpression, TState state)
        {
            throw new NotImplementedException($"The expression '{caseExpression.GetType().Name}' is not supported in SQL");
        }

        protected virtual TReturn VisitLiteralValue(Expression.LiteralValue literalValue, TState state)
        {
            throw new NotImplementedException();
        }

        protected virtual TReturn VisitCompoundIdentifier(Expression.CompoundIdentifier compoundIdentifier, TState state)
        {
            throw new NotImplementedException();
        }

        protected virtual TReturn VisitBinaryOperation(Expression.BinaryOp binaryOp, TState state)
        {
            throw new NotImplementedException();
        }
    }
}
