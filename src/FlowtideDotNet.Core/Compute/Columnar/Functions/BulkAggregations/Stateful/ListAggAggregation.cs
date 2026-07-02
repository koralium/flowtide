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
    internal class ListAggAggregation : ISharedTreeColumnAggregation
    {
        private readonly Expression _valueExpression;
        private readonly Func<EventBatchData, int, IDataValue> _projectionFunction;
        private Column? _projectedDataColumn;
        private IMemoryAllocator? _memoryAllocator;
        private IBPlusTree<BulkGroupValueRowReference, int, BulkGroupValueKeyContainer, PrimitiveListValueContainer<int>>? _tree;
        private IBPlusTreeBulkInserter<BulkGroupValueRowReference, int, BulkGroupValueKeyContainer, PrimitiveListValueContainer<int>>? _bulkInserter;
        private IBplusTreeBulkSearch<BulkGroupValueRowReference, int, BulkGroupValueKeyContainer, PrimitiveListValueContainer<int>, BulkListAggSearchComparer>? _bulkSearcher;
        private int _groupingLength;
        private bool _isShared;
        private BulkGroupValueRowReference[] _storeRowReferencesBuffer = Array.Empty<BulkGroupValueRowReference>();
        private int[] _storeWeightArrayBuffer = Array.Empty<int>();
        private BulkGroupValueRowReference[] _fetchRowReferencesBuffer = Array.Empty<BulkGroupValueRowReference>();

        public ListAggAggregation(Expression valueExpression, Func<EventBatchData, int, IDataValue> projectionFunction)
        {
            _valueExpression = valueExpression;
            _projectionFunction = projectionFunction;
        }

        public Expression ValueExpression => _valueExpression;
        public Func<EventBatchData, int, IDataValue> ValueProjection => _projectionFunction;

        public void BindSharedTree(
            IBPlusTree<BulkGroupValueRowReference, int, BulkGroupValueKeyContainer, PrimitiveListValueContainer<int>> sharedTree,
            int groupingLength)
        {
            _tree = sharedTree;
            _groupingLength = groupingLength;
            _bulkSearcher = _tree.CreateBulkSearcher(new BulkListAggSearchComparer(groupingLength));
            _isShared = true;
        }

        public void OnValueMutated(BulkGroupValueRowReference key, bool exists, int oldWeight, int newWeight, int sortedIndex)
        {
            // ListAgg queries values on watermark/demand, no-op reactively.
        }

        public async Task CommitAsync()
        {
            if (_isShared)
            {
                return;
            }
            await _tree!.Commit();
        }

        private struct ListAggRowMutator : IRowMutator<BulkGroupValueRowReference, int>
        {
            public void GetSizePrefixSum(BulkGroupValueRowReference[] keys, ReadOnlySpan<int> indices, Span<int> sizes)
            {
                var columns = keys[0].batch.GetColumns_Unsafe();
                for (int i = 0; i < columns.Length; i++)
                {
                    columns[i].GetPrefixSumByteSizes(indices, sizes);
                }
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
            if (_isShared)
            {
                return ValueTask.CompletedTask;
            }

            var len = sortedByGroupIndices.Length;
            if (_storeRowReferencesBuffer.Length < len)
            {
                _storeRowReferencesBuffer = new BulkGroupValueRowReference[len];
                _storeWeightArrayBuffer = new int[len];
            }
            var rowReferences = _storeRowReferencesBuffer;
            var weightArray = _storeWeightArrayBuffer;

            var allColumns = new IColumn[groupValueColumns.Length + 1];
            System.Array.Copy(groupValueColumns, allColumns, groupValueColumns.Length);
            allColumns[groupValueColumns.Length] = _projectedDataColumn!;
            var groupingBatch = new EventBatchData(allColumns);

            for (int i = 0; i < len; i++)
            {
                rowReferences[i] = new BulkGroupValueRowReference()
                {
                    batch = groupingBatch,
                    index = i
                };
                weightArray[i] = weights.Get(i);
            }

            int totalBatchSize = 0;
            for (int i = 0; i < groupValueColumns.Length; i++)
            {
                totalBatchSize += groupValueColumns[i].GetByteSize();
            }
            totalBatchSize += _projectedDataColumn!.GetByteSize();

            return _bulkInserter!.ApplyBatch(rowReferences, weightArray, len, new ListAggRowMutator(), totalBatchSize);
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
            _memoryAllocator = memoryAllocator;
            if (_isShared)
            {
                return;
            }
            _groupingLength = groupingLength;
            _tree = await stateManagerClient.GetOrCreateTree("listaggtree",
                new FlowtideDotNet.Storage.Tree.BPlusTreeOptions<BulkGroupValueRowReference, int, BulkGroupValueKeyContainer, PrimitiveListValueContainer<int>>()
                {
                    Comparer = new BulkMinInsertComparer(groupingLength),
                    KeySerializer = new BulkGroupValueKeyStorageSerializer(groupingLength, memoryAllocator),
                    ValueSerializer = new PrimitiveListValueContainerSerializer<int>(memoryAllocator),
                    UseByteBasedPageSizes = true,
                    MemoryAllocator = memoryAllocator,
                    UsePreviousPointers = true
                });

            _bulkInserter = _tree.CreateBulkInserter();
            _bulkSearcher = _tree.CreateBulkSearcher(new BulkListAggSearchComparer(groupingLength));
        }

        public void NewBatch(PrimitiveList<int> weights, EventBatchData batchData)
        {
            if (_isShared)
            {
                return;
            }
            Debug.Assert(_memoryAllocator != null);

            if (_projectedDataColumn != null)
            {
                _projectedDataColumn.Dispose();
            }

            _projectedDataColumn = new Column(_memoryAllocator);

            for (int i = 0; i < weights.Count; i++)
            {
                var value = _projectionFunction(batchData, i);
                _projectedDataColumn.Add(value);
            }
        }

        public async ValueTask FetchValuesAsync(IColumn[] groupingValuesSorted, int length, Column outputColumn)
        {
            var batch = new EventBatchData(groupingValuesSorted);
            if (_fetchRowReferencesBuffer.Length < length)
            {
                _fetchRowReferencesBuffer = new BulkGroupValueRowReference[length];
            }
            var rowReferences = _fetchRowReferencesBuffer;

            for (int i = 0; i < length; i++)
            {
                rowReferences[i] = new BulkGroupValueRowReference()
                {
                    batch = batch,
                    index = i,
                };
            }

            int currentKeyIndex = -1;

            await _bulkSearcher!.Start(rowReferences, length);
            while (await _bulkSearcher.MoveNextLeaf())
            {
                var leaf = _bulkSearcher.CurrentLeaf;
                var results = _bulkSearcher.CurrentResults;

                for (int i = 0; i < results.Count; i++)
                {
                    var result = results[i];
                    if (result.Found)
                    {
                        if (result.KeyIndex > currentKeyIndex)
                        {
                            if (currentKeyIndex >= 0)
                            {
                                outputColumn.EndNewList();
                            }
                            for (int k = currentKeyIndex + 1; k < result.KeyIndex; k++)
                            {
                                outputColumn.EndNewList();
                            }
                            currentKeyIndex = result.KeyIndex;
                        }

                        int lowerBound = result.LowerBound;
                        int upperBound = result.UpperBound;

                        for (int idx = lowerBound; idx <= upperBound; idx++)
                        {
                            var value = leaf.keys._data.Columns[_groupingLength].GetValueAt(idx, default);
                            var weight = leaf.values.Get(idx);

                            for (int w = 0; w < weight; w++)
                            {
                                outputColumn.AddToNewList(value);
                            }
                        }
                    }
                }
            }

            if (currentKeyIndex >= 0)
            {
                outputColumn.EndNewList();
            }
            for (int k = currentKeyIndex + 1; k < length; k++)
            {
                outputColumn.EndNewList();
            }
        }
    }

    internal class ListAggAggregationDefinition : IBulkAggregationDefinition
    {
        public IColumnBulkAggregation Create(AggregateFunction aggregateFunction, IFunctionsRegister functionsRegister)
        {
            var compiledValue = ColumnProjectCompiler.CompileToValue(aggregateFunction.Arguments[0], functionsRegister);
            return new ListAggAggregation(aggregateFunction.Arguments[0], compiledValue);
        }

        public static void Register(IFunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterBulkAggregationFunction(FunctionsList.Uri, FunctionsList.ListAgg, new ListAggAggregationDefinition());
        }
    }
}
