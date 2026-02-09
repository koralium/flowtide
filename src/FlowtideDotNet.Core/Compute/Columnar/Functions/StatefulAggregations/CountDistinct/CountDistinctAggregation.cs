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
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Compute.Columnar.Functions.StatefulAggregations.MinMax;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System.Linq.Expressions;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.StatefulAggregations.CountDistinct
{
    internal static class CountDistinctAggregation
    {
        internal class CountDistinctAggregationSingleton
        {
            internal readonly IBPlusTreeIterator<ListAggColumnRowReference, int, ListAggKeyStorageContainer, PrimitiveListValueContainer<int>> iterator;
            private readonly int keyLength;
            internal DataValueContainer _valueContainer;

            public CountDistinctAggregationSingleton(
                IBPlusTree<ListAggColumnRowReference, int, ListAggKeyStorageContainer, PrimitiveListValueContainer<int>> tree,
                IBPlusTreeIterator<ListAggColumnRowReference, int, ListAggKeyStorageContainer, PrimitiveListValueContainer<int>> iterator,
                int keyLength)
            {
                Tree = tree;
                this.iterator = iterator;
                this.keyLength = keyLength;
                _valueContainer = new DataValueContainer();
            }

            public int KeyLength => keyLength;
            public IBPlusTree<ListAggColumnRowReference, int, ListAggKeyStorageContainer, PrimitiveListValueContainer<int>> Tree { get; }
        }

        public static void RegisterCountDistinct(IFunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterStatefulColumnAggregateFunction<CountDistinctAggregationSingleton>(
                FunctionsAggregateGeneric.Uri,
                FunctionsAggregateGeneric.CountDistinct,
                InitializeCountDistinct,
                (singleton) => { },
                Commit,
                CountDistinctMapFunction,
                CountDistinctGetValue
                );
        }

        private static System.Linq.Expressions.Expression CountDistinctMapFunction(
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
                throw new InvalidOperationException("count_distinct must have exactly one argument.");
            }
            var arg = visitor.Visit(function.Arguments[0], parametersInfo);
            var expr = GetCountDistinctBody(arg!.Type);
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

        private static System.Linq.Expressions.LambdaExpression GetCountDistinctBody(System.Type inputType)
        {
            var methodInfo = typeof(CountDistinctAggregation).GetMethod(nameof(DoCountDistinct), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!.MakeGenericMethod(inputType);

            var ev = System.Linq.Expressions.Expression.Parameter(inputType, "ev");
            var state = System.Linq.Expressions.Expression.Parameter(typeof(ColumnReference), "state");
            var weight = System.Linq.Expressions.Expression.Parameter(typeof(long), "weight");
            var singleton = System.Linq.Expressions.Expression.Parameter(typeof(CountDistinctAggregationSingleton), "singleton");
            var groupingKey = System.Linq.Expressions.Expression.Parameter(typeof(ColumnRowReference), "groupingKey");

            var call = System.Linq.Expressions.Expression.Call(methodInfo, ev, state, weight, singleton, groupingKey);
            return System.Linq.Expressions.Expression.Lambda(call, ev, state, weight, singleton, groupingKey);
        }

        private static async ValueTask DoCountDistinct<T>(T column, ColumnReference currentState, long weight, CountDistinctAggregationSingleton singleton, ColumnRowReference groupingKey)
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

            currentState.GetValue(singleton._valueContainer);

            long currentCount = 0;

            if (singleton._valueContainer.Type == ArrowTypeId.Int64)
            {
                currentCount = singleton._valueContainer.AsLong;
            }
            
            await singleton.Tree.RMWNoResult(columnRowRef, (int)weight, (input, current, exists) =>
            {
                if (exists)
                {
                    if (current == 0 && input > 0)
                    {
                        currentCount++;
                    }
                    current += input;

                    if (current == 0 && input < 0)
                    {
                        currentCount--;
                    }

                    if (current == 0)
                    {
                        return (0, GenericWriteOperation.Delete);
                    }
                    return (current, GenericWriteOperation.Upsert);
                }
                if (input > 0)
                {
                    currentCount++;
                }
                return (input, GenericWriteOperation.Upsert);
            });

            currentState.Update(new Int64Value(currentCount));
        }

        private static ValueTask CountDistinctGetValue(ColumnReference state, ColumnRowReference groupingKey, CountDistinctAggregationSingleton singleton, Column outputColumn)
        {
            state.GetValue(singleton._valueContainer);
            if (singleton._valueContainer.Type == ArrowTypeId.Null)
            {
                outputColumn.Add(new Int64Value(0));
            }
            else
            {
                outputColumn.Add(singleton._valueContainer);
            }
            return ValueTask.CompletedTask;
        }

        private static async Task Commit(CountDistinctAggregationSingleton singleton)
        {
            await singleton.Tree.Commit();
        }

        private static async Task<CountDistinctAggregationSingleton> InitializeCountDistinct(
            int groupingLength,
            IStateManagerClient stateManagerClient,
            IMemoryAllocator memoryAllocator)
        {
            var tree = await stateManagerClient.GetOrCreateTree("count_distinct",
                new FlowtideDotNet.Storage.Tree.BPlusTreeOptions<ListAggColumnRowReference, int, ListAggKeyStorageContainer, PrimitiveListValueContainer<int>>()
                {
                    Comparer = new MinInsertComparer(groupingLength),
                    KeySerializer = new ListAggKeyStorageSerializer(groupingLength, memoryAllocator),
                    ValueSerializer = new PrimitiveListValueContainerSerializer<int>(memoryAllocator),
                    UseByteBasedPageSizes = true,
                    MemoryAllocator = memoryAllocator
                });

            return new CountDistinctAggregationSingleton(tree, tree.CreateIterator(), groupingLength);
        }
    }
}
