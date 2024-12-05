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

namespace FlowtideDotNet.Substrait.Sql.Internal
{
    internal class SqlBaseVisitor<TReturn, TState>
    {
        public virtual TReturn Visit(Statement statement, TState state)
        {
            if (statement is Statement.Insert insert)
            {
                return VisitInsertStatement(insert, state);
            }
            if (statement is Statement.Select select)
            {
                return VisitQuery(select.Query, state);
            }
            if (statement is Statement.CreateTable createTable)
            {
                return VisitCreateTable(createTable, state);
            }
            if (statement is Statement.CreateView createView)
            {
                return VisitCreateView(createView, state);
            }
            if (statement is BeginSubStream beginSubStream)
            {
                return VisitBeginSubStream(beginSubStream);
            }
            throw new NotImplementedException();
        }

        protected virtual TReturn VisitBeginSubStream(BeginSubStream beginSubStream)
        {
            throw new NotImplementedException();
        }

        protected virtual TReturn VisitCreateView(Statement.CreateView createView, TState state)
        {
            throw new NotImplementedException();
        }

        protected virtual TReturn VisitCreateTable(Statement.CreateTable createTable, TState state)
        {
            throw new NotImplementedException();
        }

        protected virtual TReturn VisitInsertStatement(Statement.Insert insert, TState state)
        {
            throw new NotImplementedException();
        }

        protected virtual TReturn VisitQuery(Query query, TState state)
        {
            throw new NotImplementedException();
        }

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

        public virtual TReturn Visit(SetExpression setExpression, TState state)
        {
            if (setExpression is SetExpression.Insert insert)
            {
                return VisitInsertSetExpression(insert, state);
            }
            if (setExpression is SetExpression.QueryExpression queryExpression)
            {
                return VisitQuery(queryExpression.Query, state);
            }
            if (setExpression is SetExpression.SelectExpression selectExpression)
            {
                return VisitSelectSetExpression(selectExpression, state);
            }
            if (setExpression is SetExpression.SetOperation setOperation)
            {
                return VisitSetOperation(setOperation, state);
            }
            if (setExpression is SetExpression.ValuesExpression valuesExpression)
            {
                return VisitValuesExpression(valuesExpression, state);
            }
            throw new NotImplementedException($"{setExpression.GetType().Name} is not yet supported in SQL.");
        }

        protected virtual TReturn VisitValuesExpression(SetExpression.ValuesExpression valuesExpression, TState state)
        {
            throw new NotImplementedException();
        }

        protected virtual TReturn VisitSetOperation(SetExpression.SetOperation setOperation, TState state)
        {
            throw new NotImplementedException($"{setOperation.GetType().Name} is not yet supported in SQL.");
        }

        private TReturn VisitInsertSetExpression(SetExpression.Insert insert, TState state)
        {
            return Visit(insert.Statement, state);
        }

        private TReturn VisitQuerySetExpression(SetExpression.QueryExpression queryExpression, TState state)
        {
            throw new NotImplementedException();
        }

        private TReturn VisitSelectSetExpression(SetExpression.SelectExpression selectExpression, TState state)
        {
            return VisitSelect(selectExpression.Select, state);
        }

        protected virtual TReturn VisitSelect(Select select, TState state)
        {
            throw new NotImplementedException();
        }

        public virtual TReturn Visit(TableWithJoins tableWithJoins, TState state)
        {
            return VisitTableWithJoins(tableWithJoins, state);
        }

        protected virtual TReturn VisitTableWithJoins(TableWithJoins tableWithJoins, TState state)
        {
            throw new NotImplementedException();
        }

        public virtual TReturn Visit(TableFactor tableFactor, TState state)
        {
            if (tableFactor is TableFactor.Table table)
            {
                return VisitTable(table, state);
            }
            if (tableFactor is TableFactor.Derived derived)
            {
                return VisitDerivedTable(derived, state);
            }
            throw new NotImplementedException();
        }

        protected virtual TReturn VisitDerivedTable(TableFactor.Derived derived, TState state)
        {
            throw new NotImplementedException();
        }

        protected virtual TReturn VisitTable(TableFactor.Table table, TState state)
        {
            throw new NotImplementedException();
        }
    }
}
