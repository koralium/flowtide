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
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Compute.Columnar.Functions.StatefulAggregations.ListAgg;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System.Linq.Expressions;
using System.Text;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.StatefulAggregations.StringAgg
{
    internal class ColumnStringAggAggregationSingleton
    {
        private readonly int keyLength;
        public readonly DataValueContainer dataValueContainer;
        public readonly IBPlusTreeIterator<ListAggColumnRowReference, int, ListAggKeyStorageContainer, PrimitiveListValueContainer<int>> iterator;

        public ColumnStringAggAggregationSingleton(
            IBPlusTree<ListAggColumnRowReference, int, ListAggKeyStorageContainer, PrimitiveListValueContainer<int>> tree,
            int keyLength)
        {
            Tree = tree;
            this.keyLength = keyLength;
            SearchComparer = new ListAggSearchComparer(keyLength);
            dataValueContainer = new DataValueContainer();
            iterator = tree.CreateIterator();
        }
        public int KeyLength => keyLength;
        public ListAggSearchComparer SearchComparer { get; }
        public string? Seperator { get; set; }
        public IBPlusTree<ListAggColumnRowReference, int, ListAggKeyStorageContainer, PrimitiveListValueContainer<int>> Tree { get; }
    }

    internal static class ColumnStringAggAggregation
    {

        private static async Task<ColumnStringAggAggregationSingleton> Initialize(int groupingLength, IStateManagerClient stateManagerClient, IMemoryAllocator memoryAllocator)
        {
            List<int> insertPrimaryKeys = new List<int>();
            for (int i = 0; i < groupingLength + 1; i++)
            {
                insertPrimaryKeys.Add(i);
            }
            List<int> searchPrimaryKeys = new List<int>();
            for (int i = 0; i < groupingLength; i++)
            {
                searchPrimaryKeys.Add(i);
            }
            var tree = await stateManagerClient.GetOrCreateTree("stringaggtree",
                new FlowtideDotNet.Storage.Tree.BPlusTreeOptions<ListAggColumnRowReference, int, ListAggKeyStorageContainer, PrimitiveListValueContainer<int>>()
                {
                    Comparer = new ListAggInsertComparer(groupingLength),
                    KeySerializer = new ListAggKeyStorageSerializer(groupingLength, memoryAllocator),
                    ValueSerializer = new PrimitiveListValueContainerSerializer<int>(memoryAllocator),
                    UseByteBasedPageSizes = true,
                    MemoryAllocator = memoryAllocator
                });

            return new ColumnStringAggAggregationSingleton(tree, groupingLength);
        }

        private static System.Linq.Expressions.Expression StringAggMapFunction(
           Substrait.Expressions.AggregateFunction function,
           ColumnParameterInfo parametersInfo,
           ColumnarExpressionVisitor visitor,
           ParameterExpression stateParameter,
           ParameterExpression weightParameter,
           ParameterExpression singletonAccess,
           ParameterExpression groupingKeyParameter)
        {
            if (function.Arguments.Count != 2)
            {
                throw new InvalidOperationException("String_agg must have two arguments.");
            }
            var arg = visitor.Visit(function.Arguments[0], parametersInfo);
            var arg2 = visitor.Visit(function.Arguments[1], parametersInfo);

            if (arg2 is not ConstantExpression constantExpression)
            {
                throw new InvalidOperationException("Seperator must be a constant string.");
            }
            if (constantExpression.Value is not StringValue stringVal)
            {
                throw new InvalidOperationException("Seperator must be a constant string.");
            }
            var seperator = stringVal.ToString();
            var expr = GetStringAggBody(arg!.Type);
            var body = expr.Body;
            var replacer = new ParameterReplacerVisitor(expr.Parameters[0], arg!);
            Expression e = replacer.Visit(body);
            replacer = new ParameterReplacerVisitor(expr.Parameters[1], System.Linq.Expressions.Expression.Constant(seperator));
            e = replacer.Visit(e);
            replacer = new ParameterReplacerVisitor(expr.Parameters[2], stateParameter);
            e = replacer.Visit(e);
            replacer = new ParameterReplacerVisitor(expr.Parameters[3], weightParameter);
            e = replacer.Visit(e);
            replacer = new ParameterReplacerVisitor(expr.Parameters[4], singletonAccess);
            e = replacer.Visit(e);
            replacer = new ParameterReplacerVisitor(expr.Parameters[5], groupingKeyParameter);
            e = replacer.Visit(e);
            return e;
        }

        private static System.Linq.Expressions.LambdaExpression GetStringAggBody(System.Type inputType)
        {
            var methodInfo = typeof(ColumnStringAggAggregation).GetMethod(nameof(DoStringAgg), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!.MakeGenericMethod(inputType);

            var ev = System.Linq.Expressions.Expression.Parameter(inputType, "ev");
            var seperator = System.Linq.Expressions.Expression.Parameter(typeof(string), "seperator");
            var state = System.Linq.Expressions.Expression.Parameter(typeof(ColumnReference), "state");
            var weight = System.Linq.Expressions.Expression.Parameter(typeof(long), "weight");
            var singleton = System.Linq.Expressions.Expression.Parameter(typeof(ColumnStringAggAggregationSingleton), "singleton");
            var groupingKey = System.Linq.Expressions.Expression.Parameter(typeof(ColumnRowReference), "groupingKey");

            var call = System.Linq.Expressions.Expression.Call(methodInfo, ev, seperator, state, weight, singleton, groupingKey);
            return System.Linq.Expressions.Expression.Lambda(call, ev, seperator, state, weight, singleton, groupingKey);
        }

        private static async ValueTask DoStringAgg<T>(T column, string seperator, ColumnReference currentState, long weight, ColumnStringAggAggregationSingleton singleton, ColumnRowReference groupingKey)
            where T : IDataValue
        {
            if (column.IsNull)
            {
                return;
            }
            if (column.Type != ArrowTypeId.String)
            {
                return;
            }

            if (singleton.Seperator == null)
            {
                singleton.Seperator = seperator;
            }

            var columnRowRef = new ListAggColumnRowReference()
            {
                batch = groupingKey.referenceBatch,
                index = groupingKey.RowIndex,
                insertValue = column
            };
            await singleton.Tree.RMW(columnRowRef, (int)weight, (input, current, exists) =>
            {
                if (exists)
                {
                    current += input;

                    if (current == 0)
                    {
                        return (0, GenericWriteOperation.Delete);
                    }
                    return (current, GenericWriteOperation.Upsert);
                }
                return (input, GenericWriteOperation.Upsert);
            });
        }

        private static async Task Commit(ColumnStringAggAggregationSingleton singleton)
        {
            await singleton.Tree.Commit();
        }

        private static async ValueTask StringAggGetValue(ColumnReference state, ColumnRowReference groupingKey, ColumnStringAggAggregationSingleton singleton, ColumnStore.Column outputColumn)
        {
            var rowReference = new ListAggColumnRowReference()
            {
                batch = groupingKey.referenceBatch,
                index = groupingKey.RowIndex
            };

            var iterator = singleton.iterator;
            await iterator.Seek(rowReference, singleton.SearchComparer);

            if (!singleton.SearchComparer.noMatch)
            {
                StringBuilder sb = new StringBuilder();
                bool firstPage = true;
                await foreach (var page in iterator)
                {
                    if (!firstPage)
                    {
                        var index = singleton.SearchComparer.FindIndex(in rowReference, page.Keys!);
                        if (singleton.SearchComparer.noMatch)
                        {
                            break;
                        }
                    }
                    firstPage = false;

                    // Iterate over all the values
                    for (int i = singleton.SearchComparer.start; i <= singleton.SearchComparer.end; i++)
                    {
                        page.Keys._data.GetColumn(singleton.KeyLength).GetValueAt(i, singleton.dataValueContainer, default);
                        var weight = page.Values.Get(i);

                        // Add an item for each weight
                        for (int k = 0; k < weight; k++)
                        {
                            sb.Append(singleton.dataValueContainer.AsString.ToString()).Append(singleton.Seperator);
                        }
                    }
                }
                if (sb.Length > 0)
                {
                    sb.Remove(sb.Length - singleton.Seperator!.Length, singleton.Seperator.Length);
                }
                outputColumn.Add(new StringValue(sb.ToString()));
            }
            else
            {
                outputColumn.Add(NullValue.Instance);
            }
        }

        public static void Register(IFunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterStatefulColumnAggregateFunction<ColumnStringAggAggregationSingleton>(
                FunctionsString.Uri,
                FunctionsString.StringAgg,
                Initialize,
                (singleton) => { },
                Commit,
                StringAggMapFunction,
                StringAggGetValue
                );
        }
    }
}
