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

using Apache.Arrow.Memory;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Compute.Columnar.Functions.StatefulAggregations;
using FlowtideDotNet.Core.Compute.Columnar.Functions.StatefulAggregations.MinMax;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Core.Compute.Columnar.Functions.BulkAggregations;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.BulkAggregations.Stateful
{
    internal class MinByAggregation : ISharedTreeColumnAggregation
    {
        private readonly Expression _valueExpression;
        private readonly Func<EventBatchData, int, IDataValue> _valueProjection;
        private readonly Expression _orderByExpression;
        private readonly Func<EventBatchData, int, IDataValue> _orderByProjection;

        private Column? _projectedValueColumn;
        private Column? _projectedOrderByColumn;
        private IMemoryAllocator? _memoryAllocator;
        private IBPlusTree<BulkGroupValueRowReference, int, BulkGroupValueKeyContainer, PrimitiveListValueContainer<int>>? _tree;
        private IBPlusTreeBulkInserter<BulkGroupValueRowReference, int, BulkGroupValueKeyContainer, PrimitiveListValueContainer<int>>? _bulkInserter;
        private IBplusTreeBulkSearch<BulkGroupValueRowReference, int, BulkGroupValueKeyContainer, PrimitiveListValueContainer<int>, BulkMinSearchComparer>? _bulkSearcher;
        private int _groupingLength;
        private int[]? _sortedBuffer;

        public MinByAggregation(
            Expression valueExpression, Func<EventBatchData, int, IDataValue> valueProjection,
            Expression orderByExpression, Func<EventBatchData, int, IDataValue> orderByProjection)
        {
            _valueExpression = valueExpression;
            _valueProjection = valueProjection;
            _orderByExpression = orderByExpression;
            _orderByProjection = orderByProjection;
        }

        public Expression ValueExpression => _valueExpression;
        public Func<EventBatchData, int, IDataValue> ValueProjection => _valueProjection;

        public bool SupportsSharedTree => false;

        public void BindSharedTree(
            IBPlusTree<BulkGroupValueRowReference, int, BulkGroupValueKeyContainer, PrimitiveListValueContainer<int>> sharedTree,
            int groupingLength)
        {
            throw new NotSupportedException("MinBy aggregation does not support shared trees.");
        }

        public void OnValueMutated(BulkGroupValueRowReference key, bool exists, int oldWeight, int newWeight, int sortedIndex)
        {
            throw new NotSupportedException("MinBy aggregation does not support shared trees.");
        }

        public async Task CommitAsync()
        {
            await _tree!.Commit();
        }

        private struct MinByRowMutator : IRowMutator<BulkGroupValueRowReference, int>
        {
            public void GetSizePrefixSum(BulkGroupValueRowReference[] keys, ReadOnlySpan<int> indices, Span<int> sizes)
            {
            }

            public GenericWriteOperation Process(BulkGroupValueRowReference key, bool exists, in int existing, ref int incoming, int sortedIndex)
            {
                if (exists)
                {
                    incoming += existing;
                    if (incoming == 0)
                    {
                        return GenericWriteOperation.Delete;
                    }
                    return GenericWriteOperation.Upsert;
                }
                return GenericWriteOperation.Upsert;
            }
        }

        public ValueTask StoreAsync(PrimitiveList<int> weights, IColumn[] groupValueColumns, EventBatchData incoming, ReadOnlySpan<int> sortedByGroupIndices)
        {
            var len = sortedByGroupIndices.Length;
            BulkGroupValueRowReference[] rowReferences = new BulkGroupValueRowReference[len];
            int[] weightArray = new int[len];

            // allColumns has: grouping keys, then order column, then value column
            var allColumns = new IColumn[groupValueColumns.Length + 2];
            System.Array.Copy(groupValueColumns, allColumns, groupValueColumns.Length);
            allColumns[groupValueColumns.Length] = _projectedOrderByColumn!;
            allColumns[groupValueColumns.Length + 1] = _projectedValueColumn!;
            var groupingBatch = new EventBatchData(allColumns);

            int actualLength = 0;
            for (int i = 0; i < len; i++)
            {
                var physicalIndex = sortedByGroupIndices[i];
                var orderVal = _projectedOrderByColumn!.GetValueAt(physicalIndex, default);
                if (orderVal.IsNull)
                {
                    continue;
                }

                rowReferences[actualLength] = new BulkGroupValueRowReference()
                {
                    batch = groupingBatch,
                    index = physicalIndex
                };
                weightArray[actualLength] = weights.Get(physicalIndex);
                actualLength++;
            }

            int totalBatchSize = 0;
            for (int i = 0; i < groupValueColumns.Length; i++)
            {
                totalBatchSize += groupValueColumns[i].GetByteSize();
            }
            totalBatchSize += _projectedOrderByColumn!.GetByteSize();
            totalBatchSize += _projectedValueColumn!.GetByteSize();

            return _bulkInserter!.ApplyBatch(rowReferences, weightArray, actualLength, new MinByRowMutator(), totalBatchSize);
        }

        public bool Compute(ReadOnlySpan<int> indices, PrimitiveList<int> weights, EventBatchData data, ColumnReference groupState, int sortedIndex)
        {
            return true;
        }

        public ValueTask GetValuesAsync(IColumn[] groupingValuesSorted, ColumnReference[] groupStates, int startIndex, int length, Column outputColumn)
        {
            return ValueTask.CompletedTask;
        }

        public async Task InitializeAsync(int groupingLength, IStateManagerClient stateManagerClient, IMemoryAllocator memoryAllocator)
        {
            _groupingLength = groupingLength;
            _memoryAllocator = memoryAllocator;
            _tree = await stateManagerClient.GetOrCreateTree("minbytree",
                new FlowtideDotNet.Storage.Tree.BPlusTreeOptions<BulkGroupValueRowReference, int, BulkGroupValueKeyContainer, PrimitiveListValueContainer<int>>()
                {
                    Comparer = new BulkMinInsertComparer(groupingLength + 1), // group keys + order column + value column
                    KeySerializer = new BulkGroupValueKeyStorageSerializer(groupingLength + 1, memoryAllocator),
                    ValueSerializer = new PrimitiveListValueContainerSerializer<int>(memoryAllocator),
                    UseByteBasedPageSizes = true,
                    MemoryAllocator = memoryAllocator
                });

            _bulkInserter = _tree.CreateBulkInserter();
            _bulkSearcher = _tree.CreateBulkSearcher(new BulkMinSearchComparer(groupingLength));
        }

        public void NewBatch(PrimitiveList<int> weights, EventBatchData batchData)
        {
            Debug.Assert(_memoryAllocator != null);

            if (_projectedValueColumn != null)
            {
                _projectedValueColumn.Dispose();
            }
            if (_projectedOrderByColumn != null)
            {
                _projectedOrderByColumn.Dispose();
            }

            _projectedValueColumn = new Column(_memoryAllocator);
            _projectedOrderByColumn = new Column(_memoryAllocator);

            for (int i = 0; i < weights.Count; i++)
            {
                var val = _valueProjection(batchData, i);
                _projectedValueColumn.Add(val);
                var order = _orderByProjection(batchData, i);
                _projectedOrderByColumn.Add(order);
            }
        }

        public async ValueTask FetchValuesAsync(IColumn[] groupingValuesSorted, int length, Column outputColumn)
        {
            var batch = new EventBatchData(groupingValuesSorted);
            BulkGroupValueRowReference[] rowReferences = new BulkGroupValueRowReference[length];

            for (int i = 0; i < length; i++)
            {
                rowReferences[i] = new BulkGroupValueRowReference()
                {
                    batch = batch,
                    index = i,
                };
            }

            if (_sortedBuffer == null || _sortedBuffer.Length < length)
            {
                _sortedBuffer = new int[length];
            }
            for (int i = 0; i < length; i++)
            {
                _sortedBuffer[i] = i;
            }

            await _bulkSearcher!.Start(rowReferences, length, _sortedBuffer);
            int foundCount = 0;
            while (foundCount < length && await _bulkSearcher.MoveNextLeaf())
            {
                var leaf = _bulkSearcher.CurrentLeaf;

                for (int i = 0; i < _bulkSearcher.CurrentResults.Count; i++)
                {
                    var result = _bulkSearcher.CurrentResults[i];
                    if (result.LowerBound >= 0)
                    {
                        // The value column is at index _groupingLength + 1
                        outputColumn.Add(leaf.keys._data.Columns[_groupingLength + 1].GetValueAt(result.LowerBound, default));
                    }
                    else
                    {
                        outputColumn.Add(NullValue.Instance);
                    }
                }
            }
        }
    }

    internal class MinByAggregationDefinition : IBulkAggregationDefinition
    {
        public IColumnBulkAggregation Create(AggregateFunction aggregateFunction, IFunctionsRegister functionsRegister)
        {
            var compiledValue = ColumnProjectCompiler.CompileToValue(aggregateFunction.Arguments[0], functionsRegister);
            var compiledOrderBy = ColumnProjectCompiler.CompileToValue(aggregateFunction.Arguments[1], functionsRegister);
            return new MinByAggregation(aggregateFunction.Arguments[0], compiledValue, aggregateFunction.Arguments[1], compiledOrderBy);
        }

        public static void Register(IFunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterBulkAggregationFunction(FunctionsArithmetic.Uri, FunctionsArithmetic.MinBy, new MinByAggregationDefinition());
        }
    }
}
