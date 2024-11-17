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

using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System.Linq.Expressions;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.StatefulAggregations.MinMax
{
    internal static class ColumnMinMaxAggregation
    {
        internal class MinMaxColumnAggregationSingleton
        {
            internal readonly IMinMaxSearchComparer searchComparer;
            internal readonly IBPlusTreeIterator<ListAggColumnRowReference, int, ListAggKeyStorageContainer, PrimitiveListValueContainer<int>> iterator;
            private readonly int keyLength;

            public MinMaxColumnAggregationSingleton(
                IBPlusTree<ListAggColumnRowReference, int, ListAggKeyStorageContainer, PrimitiveListValueContainer<int>> tree,
                IMinMaxSearchComparer searchComparer,
                IBPlusTreeIterator<ListAggColumnRowReference, int, ListAggKeyStorageContainer, PrimitiveListValueContainer<int>> iterator,
                int keyLength)
            {
                Tree = tree;
                this.searchComparer = searchComparer;
                this.iterator = iterator;
                this.keyLength = keyLength;
            }

            public int KeyLength => keyLength;
            public IBPlusTree<ListAggColumnRowReference, int, ListAggKeyStorageContainer, PrimitiveListValueContainer<int>> Tree { get; }
        }

        public static void RegisterMin(IFunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterStatefulColumnAggregateFunction<MinMaxColumnAggregationSingleton>(
                FunctionsArithmetic.Uri,
                FunctionsArithmetic.Min,
                InitializeMin,
                (singleton) => { },
                Commit,
                MinMaxMapFunction,
                MinMaxGetValue
                );
        }

        public static void RegisterMax(IFunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterStatefulColumnAggregateFunction<MinMaxColumnAggregationSingleton>(
                FunctionsArithmetic.Uri,
                FunctionsArithmetic.Max,
                InitializeMax,
                (singleton) => { },
                Commit,
                MinMaxMapFunction,
                MinMaxGetValue
                );
        }

        private static System.Linq.Expressions.Expression MinMaxMapFunction(
            Substrait.Expressions.AggregateFunction function,
            ColumnParameterInfo parametersInfo,
            ColumnarExpressionVisitor visitor,
            ParameterExpression stateParameter,
            ParameterExpression weightParameter,
            ParameterExpression singletonAccess,
            ParameterExpression groupingKeyParameter)
        {
            if (function.Arguments.Count != 1)
            {
                throw new InvalidOperationException("Min must have one argument.");
            }
            var arg = visitor.Visit(function.Arguments[0], parametersInfo);
            var expr = GetMinMaxBody(arg!.Type);
            var body = expr.Body;
            var replacer = new ParameterReplacerVisitor(expr.Parameters[0], arg!);
            System.Linq.Expressions.Expression e = replacer.Visit(body);
            replacer = new ParameterReplacerVisitor(expr.Parameters[1], stateParameter);
            e = replacer.Visit(e);
            replacer = new ParameterReplacerVisitor(expr.Parameters[2], weightParameter);
            e = replacer.Visit(e);
            replacer = new ParameterReplacerVisitor(expr.Parameters[3], singletonAccess);
            e = replacer.Visit(e);
            replacer = new ParameterReplacerVisitor(expr.Parameters[4], groupingKeyParameter);
            e = replacer.Visit(e);
            return e;
        }

        private static System.Linq.Expressions.LambdaExpression GetMinMaxBody(System.Type inputType)
        {
            var methodInfo = typeof(ColumnMinMaxAggregation).GetMethod(nameof(DoMinMax), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!.MakeGenericMethod(inputType);

            var ev = System.Linq.Expressions.Expression.Parameter(inputType, "ev");
            var state = System.Linq.Expressions.Expression.Parameter(typeof(ColumnReference), "state");
            var weight = System.Linq.Expressions.Expression.Parameter(typeof(long), "weight");
            var singleton = System.Linq.Expressions.Expression.Parameter(typeof(MinMaxColumnAggregationSingleton), "singleton");
            var groupingKey = System.Linq.Expressions.Expression.Parameter(typeof(ColumnRowReference), "groupingKey");

            var call = System.Linq.Expressions.Expression.Call(methodInfo, ev, state, weight, singleton, groupingKey);
            return System.Linq.Expressions.Expression.Lambda(call, ev, state, weight, singleton, groupingKey);
        }

        private static async ValueTask DoMinMax<T>(T column, ColumnReference currentState, long weight, MinMaxColumnAggregationSingleton singleton, ColumnRowReference groupingKey)
            where T : IDataValue
        {
            if (column.IsNull)
            {
                return;
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

        private static async ValueTask MinMaxGetValue(ColumnReference state, ColumnRowReference groupingKey, MinMaxColumnAggregationSingleton singleton, Column outputColumn)
        {
            var rowReference = new ListAggColumnRowReference()
            {
                batch = groupingKey.referenceBatch,
                index = groupingKey.RowIndex
            };

            var iterator = singleton.iterator;
            await iterator.Seek(rowReference, singleton.searchComparer);

            if (singleton.searchComparer.NoMatch)
            {
                outputColumn.Add(NullValue.Instance);
                return;
            }
            var enumerator = iterator.GetAsyncEnumerator();
            await enumerator.MoveNextAsync();
            var page = enumerator.Current;

            var value = page.Keys._data.Columns[singleton.KeyLength].GetValueAt(singleton.searchComparer.Start, default);
            outputColumn.Add(value);
        }

        private static Task<MinMaxColumnAggregationSingleton> InitializeMin(int groupingLength, IStateManagerClient stateManagerClient, IMemoryAllocator memoryAllocator)
        {
            return InitializeMinMax(groupingLength, stateManagerClient, new MinInsertComparer(groupingLength), new MinSearchComparer(groupingLength), "mintree", memoryAllocator);
        }

        private static Task<MinMaxColumnAggregationSingleton> InitializeMax(int groupingLength, IStateManagerClient stateManagerClient, IMemoryAllocator memoryAllocator)
        {
            return InitializeMinMax(groupingLength, stateManagerClient, new MaxInsertComparer(groupingLength), new MaxSearchComparer(groupingLength), "maxtree", memoryAllocator);
        }

        private static async Task Commit(MinMaxColumnAggregationSingleton singleton)
        {
            await singleton.Tree.Commit();
        }

        private static async Task<MinMaxColumnAggregationSingleton> InitializeMinMax(
            int groupingLength,
            IStateManagerClient stateManagerClient,
            IBplusTreeComparer<ListAggColumnRowReference, ListAggKeyStorageContainer> comparer,
            IMinMaxSearchComparer searchComparer,
            string treeName,
            IMemoryAllocator memoryAllocator)
        {
            var tree = await stateManagerClient.GetOrCreateTree(treeName,
                new FlowtideDotNet.Storage.Tree.BPlusTreeOptions<ListAggColumnRowReference, int, ListAggKeyStorageContainer, PrimitiveListValueContainer<int>>()
                {
                    Comparer = comparer,
                    KeySerializer = new ListAggKeyStorageSerializer(groupingLength, memoryAllocator),
                    ValueSerializer = new PrimitiveListValueContainerSerializer<int>(memoryAllocator),
                    UseByteBasedPageSizes = true,
                    MemoryAllocator = memoryAllocator
                });

            return new MinMaxColumnAggregationSingleton(tree, searchComparer, tree.CreateIterator(), groupingLength);
        }
    }
}
