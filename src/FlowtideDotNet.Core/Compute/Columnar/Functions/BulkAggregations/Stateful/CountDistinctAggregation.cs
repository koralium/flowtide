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
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Core.Compute.Columnar.Functions.BulkAggregations;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.BulkAggregations.Stateful
{
    public class CountDistinctAggregation : ISharedTreeColumnAggregation
    {
        private readonly Expression _valueExpression;
        private readonly Func<EventBatchData, int, IDataValue> _projectionFunction;
        private long[]? _batchDeltas;
        private int _groupingLength;
        private IBPlusTree<BulkGroupValueRowReference, int, BulkGroupValueKeyContainer, PrimitiveListValueContainer<int>>? _tree;
        private readonly DataValueContainer _dataValueContainer = new();
        private int[]? _groupSortLookup;

        public CountDistinctAggregation(Expression valueExpression, Func<EventBatchData, int, IDataValue> projectionFunction)
        {
            _valueExpression = valueExpression;
            _projectionFunction = projectionFunction;
        }

        public Expression ValueExpression => _valueExpression;
        public Func<EventBatchData, int, IDataValue> ValueProjection => _projectionFunction;

        public void BindSharedTree(IBPlusTree<BulkGroupValueRowReference, int, BulkGroupValueKeyContainer, PrimitiveListValueContainer<int>> sharedTree, int groupingLength)
        {
            _tree = sharedTree;
            _groupingLength = groupingLength;
        }

        public void SetGroupMapping(int[] groupSortLookup)
        {
            _groupSortLookup = groupSortLookup;
        }

        public void NewBatch(PrimitiveList<int> weights, EventBatchData batchData)
        {
            if (_batchDeltas == null || _batchDeltas.Length < batchData.Count)
            {
                _batchDeltas = new long[batchData.Count];
            }
            System.Array.Clear(_batchDeltas, 0, batchData.Count);
        }

        public void OnValueMutated(BulkGroupValueRowReference key, bool exists, int oldWeight, int newWeight, int sortedIndex)
        {
            var groupIndex = _groupSortLookup != null ? _groupSortLookup[key.index] : 0;
            if (oldWeight == 0 && newWeight > 0)
            {
                _batchDeltas![groupIndex]++;
            }
            else if (oldWeight > 0 && newWeight == 0)
            {
                _batchDeltas![groupIndex]--;
            }
        }

        public bool Compute(ReadOnlySpan<int> indices, PrimitiveList<int> weights, EventBatchData data, ColumnReference groupState, int sortedIndex)
        {
            var delta = _batchDeltas![sortedIndex];
            if (delta != 0)
            {
                groupState.GetValue(_dataValueContainer);
                long currentCount = 0;
                if (_dataValueContainer.Type == ArrowTypeId.Int64)
                {
                    currentCount = _dataValueContainer.AsLong;
                }
                
                groupState.Update(new Int64Value(currentCount + delta));
                return true;
            }
            return false;
        }

        public ValueTask GetValuesAsync(IColumn[] groupingValuesSorted, ColumnReference[] groupStates, int startIndex, int length, Column outputColumn)
        {
            for (int i = 0; i < length; i++)
            {
                groupStates[startIndex + i].GetValue(_dataValueContainer);
                if (_dataValueContainer.Type == ArrowTypeId.Null)
                {
                    outputColumn.Add(new Int64Value(0));
                }
                else
                {
                    outputColumn.Add(_dataValueContainer);
                }
            }
            return ValueTask.CompletedTask;
        }

        public ValueTask FetchValuesAsync(IColumn[] groupingValuesSorted, int length, Column outputColumn)
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask StoreAsync(PrimitiveList<int> weights, IColumn[] groupValueColumns, EventBatchData incoming, ReadOnlySpan<int> sortedByGroupIndices)
        {
            return ValueTask.CompletedTask;
        }

        public Task InitializeAsync(int groupingLength, IStateManagerClient stateManagerClient, IMemoryAllocator memoryAllocator)
        {
            return Task.CompletedTask;
        }

        public Task CommitAsync()
        {
            return Task.CompletedTask;
        }
    }

    internal class CountDistinctAggregationDefinition : IBulkAggregationDefinition
    {
        public IColumnBulkAggregation Create(AggregateFunction aggregateFunction, IFunctionsRegister functionsRegister)
        {
            var compiledValue = ColumnProjectCompiler.CompileToValue(aggregateFunction.Arguments[0], functionsRegister);
            return new CountDistinctAggregation(aggregateFunction.Arguments[0], compiledValue);
        }

        public static void Register(IFunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterBulkAggregationFunction(FunctionsAggregateGeneric.Uri, FunctionsAggregateGeneric.CountDistinct, new CountDistinctAggregationDefinition());
        }
    }
}
