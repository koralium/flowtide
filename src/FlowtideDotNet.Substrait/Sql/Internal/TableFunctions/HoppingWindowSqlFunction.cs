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

namespace FlowtideDotNet.Substrait.Sql.Internal.TableFunctions
{
    /// <summary>
    /// Registers the <c>hopping_window(timestamp, hop_amount, hop_unit, size_amount, size_unit)</c>
    /// table function. For each input row it emits one row per hopping window the timestamp falls
    /// into, adding <c>window_start</c> and <c>window_end</c> columns. Overlapping windows are
    /// produced when the hop is smaller than the size.
    /// </summary>
    internal static class HoppingWindowSqlFunction
    {
        public static void AddHoppingWindow(ISqlFunctionRegister sqlFunctionRegister)
        {
            sqlFunctionRegister.RegisterTableFunction("hopping_window", (arg) =>
            {
                if (arg.Arguments.Count != 5)
                {
                    throw new ArgumentException("hopping_window function requires five arguments: (timestamp, hop_amount, hop_unit, size_amount, size_unit)");
                }

                Expressions.Expression VisitArgument(int index)
                {
                    if (arg.Arguments[index] is FunctionArg.Unnamed unnamed &&
                        unnamed.FunctionArgExpression is FunctionArgExpression.FunctionExpression funcArg)
                    {
                        return arg.ExpressionVisitor.Visit(funcArg.Expression, arg.EmitData).Expr;
                    }
                    throw new ArgumentException($"hopping_window argument {index} is not a valid expression");
                }

                var arguments = new List<Expressions.Expression>()
                {
                    VisitArgument(0),
                    VisitArgument(1),
                    VisitArgument(2),
                    VisitArgument(3),
                    VisitArgument(4),
                };

                var outputSchema = new NamedStruct()
                {
                    Names = new List<string>() { "window_start", "window_end" },
                    Struct = new Struct()
                    {
                        Types = new List<SubstraitBaseType>()
                        {
                            new TimestampType(),
                            new TimestampType()
                        }
                    }
                };

                return new TableFunction()
                {
                    ExtensionName = FunctionsDatetime.HoppingWindow,
                    ExtensionUri = FunctionsDatetime.Uri,
                    Arguments = arguments,
                    TableSchema = outputSchema
                };
            });
        }
    }
}
