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
using SqlParser;

namespace FlowtideDotNet.Substrait.Sql.Internal
{
    internal enum FunctionType
    {
        NotExist = 0,
        Scalar = 1,
        Aggregate = 2,
        Table = 3
    }

    internal class SqlFunctionRegister : ISqlFunctionRegister
    {
        private readonly Dictionary<string, Func<SqlParser.Ast.Expression.Function, SqlExpressionVisitor, EmitData, Expression>> _scalarFunctions;
        private readonly Dictionary<string, Func<SqlParser.Ast.Expression.Function, SqlExpressionVisitor, EmitData, AggregateFunction>> _aggregateFunctions;
        private readonly Dictionary<string, Func<SqlTableFunctionArgument, SqlTableFunctionResult>> _tableFunctions;

        public SqlFunctionRegister()
        {
            _scalarFunctions = new Dictionary<string, Func<SqlParser.Ast.Expression.Function, SqlExpressionVisitor, EmitData, Expression>>(StringComparer.OrdinalIgnoreCase);
            _aggregateFunctions = new Dictionary<string, Func<SqlParser.Ast.Expression.Function, SqlExpressionVisitor, EmitData, AggregateFunction>>(StringComparer.OrdinalIgnoreCase);
            _tableFunctions = new Dictionary<string, Func<SqlTableFunctionArgument, SqlTableFunctionResult>>(StringComparer.OrdinalIgnoreCase);
        }

        public void RegisterScalarFunction(string name, Func<SqlParser.Ast.Expression.Function, SqlExpressionVisitor, EmitData, Expression> mapFunc)
        {
            _scalarFunctions.Add(name, mapFunc);
        }

        public Func<SqlParser.Ast.Expression.Function, SqlExpressionVisitor, EmitData, Expression> GetScalarMapper(string name)
        {
            return _scalarFunctions[name];
        }

        public Func<SqlParser.Ast.Expression.Function, SqlExpressionVisitor, EmitData, AggregateFunction> GetAggregateMapper(string name)
        {
            return _aggregateFunctions[name];
        }

        public Func<SqlTableFunctionArgument, SqlTableFunctionResult> GetTableMapper(string name)
        {
            if (!_tableFunctions.ContainsKey(name))
            {
                throw new InvalidOperationException($"Table function '{name}' not found");
            }
            return _tableFunctions[name];
        }

        public FunctionType GetFunctionType(string name)
        {
            if (_scalarFunctions.ContainsKey(name))
            {
                return FunctionType.Scalar;
            }
            if (_aggregateFunctions.ContainsKey(name))
            {
                return FunctionType.Aggregate;
            }
            if (_tableFunctions.ContainsKey(name))
            {
                return FunctionType.Table;
            }
            return FunctionType.NotExist;
        }

        public void RegisterAggregateFunction(string name, Func<SqlParser.Ast.Expression.Function, SqlExpressionVisitor, EmitData, AggregateFunction> mapFunc)
        {
            _aggregateFunctions.Add(name, mapFunc);
        }

        public void RegisterTableFunction(string name, Func<SqlTableFunctionArgument, SqlTableFunctionResult> mapFunc)
        {
            _tableFunctions.Add(name, mapFunc);
        }
    }
}
