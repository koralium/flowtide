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
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System.Linq.Expressions;
using FlowtideDotNet.Core.ColumnStore.DataValues;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.StatefulAggregations.MinMax
{
    /// <summary>
    /// Implements min / max by aggregation.
    /// </summary>
    internal static class ColumnMinMaxByAggregation
    {

        internal class MinMaxByColumnAggregationSingleton
        {
            internal readonly IMinMaxSearchComparer searchComparer;
            internal readonly IBPlusTreeIterator<ListAggColumnRowReference, MinMaxByValue, ListAggKeyStorageContainer, MinMaxByValueContainer> iterator;
            private readonly int keyLength;

            public MinMaxByColumnAggregationSingleton(
                IBPlusTree<ListAggColumnRowReference, MinMaxByValue, ListAggKeyStorageContainer, MinMaxByValueContainer> tree,
                IMinMaxSearchComparer searchComparer,
                IBPlusTreeIterator<ListAggColumnRowReference, MinMaxByValue, ListAggKeyStorageContainer, MinMaxByValueContainer> iterator,
                int keyLength)
            {
                Tree = tree;
                this.searchComparer = searchComparer;
                this.iterator = iterator;
                this.keyLength = keyLength;
            }

            public int KeyLength => keyLength;
            public IBPlusTree<ListAggColumnRowReference, MinMaxByValue, ListAggKeyStorageContainer, MinMaxByValueContainer> Tree { get; }
        }

        public static void RegisterMinBy(IFunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterStatefulColumnAggregateFunction<MinMaxByColumnAggregationSingleton>(
                FunctionsArithmetic.Uri,
                FunctionsArithmetic.MinBy,
                InitializeMin,
                (singleton) => { },
                Commit,
                MinMaxByMapFunction,
                MinMaxByGetValue
                );
        }

        public static void RegisterMaxBy(IFunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterStatefulColumnAggregateFunction<MinMaxByColumnAggregationSingleton>(
                FunctionsArithmetic.Uri,
                FunctionsArithmetic.MaxBy,
                InitializeMax,
                (singleton) => { },
                Commit,
                MinMaxByMapFunction,
                MinMaxByGetValue
                );
        }

        private static System.Linq.Expressions.Expression MinMaxByMapFunction(
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
                throw new InvalidOperationException("Min_by/Max_by must have two arguments.");
            }
            var arg1 = visitor.Visit(function.Arguments[0], parametersInfo);
            var arg2 = visitor.Visit(function.Arguments[1], parametersInfo);
            var expr = GetMinMaxBody(arg1!.Type, arg2!.Type);
            var body = expr.Body;
            var replacer = new ParameterReplacerVisitor(expr.Parameters[0], arg1!);
            System.Linq.Expressions.Expression e = replacer.Visit(body);
            replacer = new ParameterReplacerVisitor(expr.Parameters[1], arg2!);
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

        private static System.Linq.Expressions.LambdaExpression GetMinMaxBody(System.Type inputType1, System.Type inputType2)
        {
            var methodInfo = typeof(ColumnMinMaxByAggregation).GetMethod(nameof(DoMinMaxBy), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!.MakeGenericMethod(inputType1, inputType2);

            var ev1 = System.Linq.Expressions.Expression.Parameter(inputType1, "ev1");
            var ev2 = System.Linq.Expressions.Expression.Parameter(inputType1, "ev2");
            var state = System.Linq.Expressions.Expression.Parameter(typeof(ColumnReference), "state");
            var weight = System.Linq.Expressions.Expression.Parameter(typeof(long), "weight");
            var singleton = System.Linq.Expressions.Expression.Parameter(typeof(MinMaxByColumnAggregationSingleton), "singleton");
            var groupingKey = System.Linq.Expressions.Expression.Parameter(typeof(ColumnRowReference), "groupingKey");

            var call = System.Linq.Expressions.Expression.Call(methodInfo, ev1, ev2, state, weight, singleton, groupingKey);
            return System.Linq.Expressions.Expression.Lambda(call, ev1, ev2, state, weight, singleton, groupingKey);
        }

        private static async ValueTask DoMinMaxBy<T1, T2>(T1 value, T2 order, ColumnReference currentState, long weight, MinMaxByColumnAggregationSingleton singleton, ColumnRowReference groupingKey)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (order.IsNull)
            {
                return;
            }
            var columnRowRef = new ListAggColumnRowReference()
            {
                batch = groupingKey.referenceBatch,
                index = groupingKey.RowIndex,
                insertValue = order
            };
            var byValue = new MinMaxByValue()
            {
                Value = value,
                Weight = (int)weight
            };
            await singleton.Tree.RMWNoResult(in columnRowRef, in byValue, (input, current, exists) =>
            {
                if (exists)
                {
                    current.Weight += input.Weight;

                    if (current.Weight == 0)
                    {
                        return (current, GenericWriteOperation.Delete);
                    }

                    current.Value = input.Value;
                    return (current, GenericWriteOperation.Upsert);
                }
                return (input, GenericWriteOperation.Upsert);
            });
        }

        private static async ValueTask MinMaxByGetValue(ColumnReference state, ColumnRowReference groupingKey, MinMaxByColumnAggregationSingleton singleton, Column outputColumn)
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
            if (await enumerator.MoveNextAsync())
            {
                var page = enumerator.Current;

                var value = page.Values._data.GetValueAt(singleton.searchComparer.Start, default);
                outputColumn.Add(value);
            }
            else
            {
                outputColumn.Add(NullValue.Instance);
            }

        }

        private static async Task Commit(MinMaxByColumnAggregationSingleton singleton)
        {
            await singleton.Tree.Commit();
        }

        private static Task<MinMaxByColumnAggregationSingleton> InitializeMin(int groupingLength, IStateManagerClient stateManagerClient, IMemoryAllocator memoryAllocator)
        {
            return InitializeMinMaxBy(groupingLength, stateManagerClient, new MinInsertComparer(groupingLength), new MinSearchComparer(groupingLength), "mintree", memoryAllocator);
        }

        private static Task<MinMaxByColumnAggregationSingleton> InitializeMax(int groupingLength, IStateManagerClient stateManagerClient, IMemoryAllocator memoryAllocator)
        {
            return InitializeMinMaxBy(groupingLength, stateManagerClient, new MaxInsertComparer(groupingLength), new MaxSearchComparer(groupingLength), "maxtree", memoryAllocator);
        }

        private static async Task<MinMaxByColumnAggregationSingleton> InitializeMinMaxBy(
            int groupingLength,
            IStateManagerClient stateManagerClient,
            IBplusTreeComparer<ListAggColumnRowReference, ListAggKeyStorageContainer> comparer,
            IMinMaxSearchComparer searchComparer,
            string treeName,
            IMemoryAllocator memoryAllocator)
        {
            var tree = await stateManagerClient.GetOrCreateTree(treeName,
                new FlowtideDotNet.Storage.Tree.BPlusTreeOptions<ListAggColumnRowReference, MinMaxByValue, ListAggKeyStorageContainer, MinMaxByValueContainer>()
                {
                    Comparer = comparer,
                    KeySerializer = new ListAggKeyStorageSerializer(groupingLength, memoryAllocator),
                    ValueSerializer = new MinMaxByValueContainerSerializer(memoryAllocator),
                    UseByteBasedPageSizes = true,
                    MemoryAllocator = memoryAllocator
                });

            return new MinMaxByColumnAggregationSingleton(tree, searchComparer, tree.CreateIterator(), groupingLength);
        }
    }
}
