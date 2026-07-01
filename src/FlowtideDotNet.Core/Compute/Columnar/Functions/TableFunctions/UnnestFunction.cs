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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System.Linq.Expressions;
using System.Reflection;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.TableFunctions
{
    internal static class UnnestFunction
    {
        private static StringValue _keyValue = new StringValue("key");
        private static StringValue _valueValue = new StringValue("value");

        private static readonly MethodInfo _unnestMethod = GetUnnestMethod();
        private static MethodInfo GetUnnestMethod()
        {
            var method = typeof(UnnestFunction).GetMethod(nameof(DoUnnest), BindingFlags.Static | BindingFlags.Public);
            if (method == null)
            {
                throw new InvalidOperationException("Could not find unnest method");
            }
            return method;
        }

        public static void AddBuiltInUnnestFunction(FunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterColumnTableFunction(FunctionsTableGeneric.Uri, FunctionsTableGeneric.Unnest,
                (tableFunc, parameterInfo, visitor, memoryAllocator, outputParam) =>
                {
                    if (tableFunc.Arguments.Count != 1)
                    {
                        throw new ArgumentException("Unnest function requires exactly one argument");
                    }
                    if (tableFunc.TableSchema.Names.Count != 1)
                    {
                        throw new ArgumentException("Unnest function requires exactly one column in the schema");
                    }

                    var expr = visitor.Visit(tableFunc.Arguments[0], parameterInfo);

                    if (expr == null)
                    {
                        throw new InvalidOperationException("Unnest function requires an argument");
                    }

                    var genericMethod = _unnestMethod.MakeGenericMethod(expr.Type);
                    return new TableFunctionResult(Expression.Call(genericMethod, expr, outputParam));
                });
        }

        public static void DoUnnest<T>(T value, ITableFunctionOutput output)
            where T : IDataValue
        {
            var column = output.Columns[0];
            if (value.Type == ArrowTypeId.List)
            {
                var list = value.AsList;

                for (int i = 0; i < list.Count; i++)
                {
                    column.Add(list.GetAt(i));
                    output.CommitRow(1, 0);
                }
            }
            else if (value.Type == ArrowTypeId.Map)
            {
                var map = value.AsMap;

                var mapLength = map.GetLength();

                for (int i = 0; i < mapLength; i++)
                {
                    column.Add(new MapValue(
                        new KeyValuePair<IDataValue, IDataValue>(_keyValue, map.GetKeyAt(i)),
                        new KeyValuePair<IDataValue, IDataValue>(_valueValue, map.GetValueAt(i))
                    ));
                    output.CommitRow(1, 0);
                }
            }
        }
    }
}
