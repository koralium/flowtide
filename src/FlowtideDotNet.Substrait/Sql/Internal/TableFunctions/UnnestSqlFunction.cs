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
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Type;
using SqlParser.Ast;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Substrait.Sql.Internal.TableFunctions
{
    internal static class UnnestSqlFunction
    {
        public static void AddUnnest(ISqlFunctionRegister sqlFunctionRegister)
        {
            sqlFunctionRegister.RegisterTableFunction("unnest", (arg) =>
            {
                if (arg.Arguments.Count != 1)
                {
                    throw new ArgumentException("Unnest function requires exactly one argument");
                }

                if (arg.Arguments[0] is FunctionArg.Unnamed unnamed &&
                unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcArg)
                {
                    var result = arg.ExpressionVisitor.Visit(funcArg.Expression, arg.EmitData);

                    var tableFunction = new TableFunction()
                    {
                        ExtensionName = FunctionsTableGeneric.Unnest,
                        ExtensionUri = FunctionsTableGeneric.Uri,
                        Arguments = new List<Expressions.Expression>() { result.Expr }
                    };

                    var columnName = $"{result.Expr}_value";
                    if (arg.TableAlias != null)
                    {
                        columnName = arg.TableAlias;
                    }

                    NamedStruct outputSchema = new NamedStruct()
                    {
                        Names = new List<string>() { columnName },
                        Struct = new Struct()
                        {
                            Types = new List<SubstraitBaseType>()
                            {
                                new AnyType()
                            }
                        }
                    };

                    return new SqlTableFunctionResult(tableFunction, outputSchema);
                }

                throw new ArgumentException("Unnest function requires a function argument");
            });
        }
    }
}
