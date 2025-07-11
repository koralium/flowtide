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
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System.Linq.Expressions;
using System.Reflection;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.TableFunctions
{
    internal static class UnnestFunction
    {
        private static StringValue _keyValue = new StringValue("key");
        private static StringValue _valueValue = new StringValue("value");

        private static MethodInfo _unnestMethod = GetUnnestMethod();
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
                (tableFunc, parameterInfo, visitor, memoryAllocator) =>
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
                    return new TableFunctionResult(Expression.Call(genericMethod, expr, Expression.Constant(memoryAllocator)));
                });
        }

        public static IEnumerable<EventBatchWeighted> DoUnnest<T>(T value, IMemoryAllocator memoryAllocator)
            where T : IDataValue
        {
            List<RowEvent> output = new List<RowEvent>();

            PrimitiveList<int> weights = new PrimitiveList<int>(memoryAllocator);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(memoryAllocator);
            IColumn[] outputColumns = [Column.Create(memoryAllocator)];
            if (value.Type == ArrowTypeId.List)
            {
                var list = value.AsList;

                for (int i = 0; i < list.Count; i++)
                {
                    weights.Add(1);
                    iterations.Add(0);
                    outputColumns[0].Add(list.GetAt(i));
                }
            }
            else if (value.Type == ArrowTypeId.Map)
            {
                var map = value.AsMap;

                var mapLength = map.GetLength();

                for (int i = 0; i < mapLength; i++)
                {
                    weights.Add(1);
                    iterations.Add(0);
                    outputColumns[0].Add(new MapValue(
                        new KeyValuePair<IDataValue, IDataValue>(_keyValue, map.GetKeyAt(i)),
                        new KeyValuePair<IDataValue, IDataValue>(_valueValue, map.GetValueAt(i))
                    ));
                }
            }

            EventBatchWeighted[] result = [new EventBatchWeighted(weights, iterations, new EventBatchData(outputColumns))];
            return result;
        }
    }
}
