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

using FlexBuffers;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Compute.Columnar.Functions.StatefulAggregations.ListAgg;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Compute.Internal.StatefulAggregations;
using FlowtideDotNet.Core.Storage;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.StatefulAggregations
{
    internal class ColumnListAggAggregationSingleton
    {
        private readonly int keyLength;
        public readonly DataValueContainer dataValueContainer;
        public readonly IBPlusTreeIterator<ListAggColumnRowReference, int, ListAggKeyStorageContainer, ListValueContainer<int>> iterator;

        public ColumnListAggAggregationSingleton(IBPlusTree<ListAggColumnRowReference, int, ListAggKeyStorageContainer, ListValueContainer<int>> tree, int keyLength)
        {
            Tree = tree;
            this.keyLength = keyLength;
            SearchComparer = new ListAggSearchComparer(keyLength);
            dataValueContainer = new DataValueContainer();
            iterator = tree.CreateIterator();
        }
        public int KeyLength => keyLength;
        public ListAggSearchComparer SearchComparer { get; }
        public IBPlusTree<ListAggColumnRowReference, int, ListAggKeyStorageContainer, ListValueContainer<int>> Tree { get; }
    }

    internal class ColumnListAggAggregation
    {
        private static async Task<ColumnListAggAggregationSingleton> Initialize(int groupingLength, IStateManagerClient stateManagerClient, IMemoryAllocator memoryAllocator)
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
            var tree = await stateManagerClient.GetOrCreateTree("listaggtree",
                new FlowtideDotNet.Storage.Tree.BPlusTreeOptions<ListAggColumnRowReference, int, ListAggKeyStorageContainer, ListValueContainer<int>>()
                {
                    Comparer = new ListAggInsertComparer(groupingLength),
                    KeySerializer = new ListAggKeyStorageSerializer(groupingLength, memoryAllocator),
                    ValueSerializer = new ValueListSerializer<int>(new IntSerializer())
                });

            return new ColumnListAggAggregationSingleton(tree, groupingLength);
        }

        private static System.Linq.Expressions.Expression ListAggMapFunction(
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
                throw new InvalidOperationException("List_agg must have one argument.");
            }
            var arg = visitor.Visit(function.Arguments[0], parametersInfo);
            
            var expr = GetListAggBody(arg!.Type);
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

        private static System.Linq.Expressions.LambdaExpression GetListAggBody(System.Type inputType)
        {
            var methodInfo = typeof(ColumnListAggAggregation).GetMethod(nameof(DoListAgg), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!.MakeGenericMethod(inputType);
            
            var ev = System.Linq.Expressions.Expression.Parameter(inputType, "ev");
            var state = System.Linq.Expressions.Expression.Parameter(typeof(ColumnReference), "state");
            var weight = System.Linq.Expressions.Expression.Parameter(typeof(long), "weight");
            var singleton = System.Linq.Expressions.Expression.Parameter(typeof(ColumnListAggAggregationSingleton), "singleton");
            var groupingKey = System.Linq.Expressions.Expression.Parameter(typeof(ColumnRowReference), "groupingKey");

            var call = System.Linq.Expressions.Expression.Call(methodInfo, ev, state, weight, singleton, groupingKey);
            return System.Linq.Expressions.Expression.Lambda(call, ev, state, weight, singleton, groupingKey);
        }

        private static async ValueTask DoListAgg<T>(T column, ColumnReference currentState, long weight, ColumnListAggAggregationSingleton singleton, ColumnRowReference groupingKey)
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
            await singleton.Tree.RMWNoResult(columnRowRef, (int)weight, (input, current, exists) =>
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

        private static async ValueTask ListAggGetValue(ColumnReference state, ColumnRowReference groupingKey, ColumnListAggAggregationSingleton singleton, ColumnStore.Column outputColumn)
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
                bool firstPage = true;
                await foreach(var page in iterator)
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
                            outputColumn.AddToNewList(singleton.dataValueContainer);
                        }
                    }
                }
            }

            // End the list, if no values where added an empty list is created.
            outputColumn.EndNewList();
        }

        private static async Task Commit(ColumnListAggAggregationSingleton singleton)
        {
            await singleton.Tree.Commit();
        }

        public static void Register(IFunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterStatefulColumnAggregateFunction<ColumnListAggAggregationSingleton>(
                FunctionsList.Uri,
                FunctionsList.ListAgg,
                Initialize,
                (singleton) => { },
                Commit,
                ListAggMapFunction,
                ListAggGetValue
                );
        }
    }
}
