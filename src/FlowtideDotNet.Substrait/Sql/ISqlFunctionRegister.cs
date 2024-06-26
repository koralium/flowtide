﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

namespace FlowtideDotNet.Substrait.Sql
{
    public interface ISqlFunctionRegister
    {
        void RegisterScalarFunction(string name, Func<SqlParser.Ast.Expression.Function, SqlExpressionVisitor, EmitData, Expression> mapFunc);

        void RegisterAggregateFunction(string name, Func<SqlParser.Ast.Expression.Function, SqlExpressionVisitor, EmitData, AggregateFunction> mapFunc);

        void RegisterTableFunction(string name, Func<SqlTableFunctionArgument, TableFunction> mapFunc);
    }
}
