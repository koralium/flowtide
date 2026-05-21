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
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static SqlParser.Ast.AlterPolicyOperation;
using static SqlParser.Ast.DataType;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.BulkAggregations.Stateful
{
    internal class MinAggregation : IColumnBulkAggregation
    {
        private readonly Func<EventBatchData, int, IDataValue> _projectionFunction;
        private DataValueContainer _dataValueContainer;
        private Column? _projectedDataColumn;
        private IMemoryAllocator? _memoryAllocator;
        private IBPlusTree<ListAggColumnRowReference, int, ListAggKeyStorageContainer, PrimitiveListValueContainer<int>>? _tree;
        private IBPlusTreeBulkInserter<ListAggColumnRowReference, int, ListAggKeyStorageContainer, PrimitiveListValueContainer<int>>? _bulkInserter;
        private IBplusTreeBulkSearch<ListAggColumnRowReference, int, ListAggKeyStorageContainer, PrimitiveListValueContainer<int>, MinSearchComparer>? _bulkSearcher;
        private int _groupingLength;

        public MinAggregation(Func<EventBatchData, int, IDataValue> projectionFunction)
        {
            _projectionFunction = projectionFunction;
            _dataValueContainer = new DataValueContainer();
        }
        
        public async Task CommitAsync()
        {
            await _tree!.Commit();
        }

        private struct MinRowMutator : IRowMutator<ListAggColumnRowReference, int>
        {
            public void GetSizePrefixSum(ListAggColumnRowReference[] keys, ReadOnlySpan<int> indices, Span<int> sizes)
            {
                // TODO: Fix later, just initial
            }

            public GenericWriteOperation Process(ListAggColumnRowReference key, bool exists, in int input, ref int existing)
            {
                if (exists)
                {
                    existing += input;
                    if (existing == 0)
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
            ListAggColumnRowReference[] rowReferences = new ListAggColumnRowReference[weights.Count];
            int[] weightArray = new int[weights.Count];

            for (int i = 0; i < weights.Count; i++)
            {
                rowReferences[i] = new ListAggColumnRowReference()
                {
                    batch = incoming,
                    index = sortedByGroupIndices[i],
                    // Should be optimized later, just like this for now
                    insertValue = _projectedDataColumn!.GetValueAt(sortedByGroupIndices[i], default)
                };
                weightArray[i] = weights.Get(i);
            }

            int totalBatchSize = 0;
            for (int i = 0; i < groupValueColumns.Length; i++)
            {
                totalBatchSize += groupValueColumns[i].GetByteSize();
            }
            totalBatchSize += _projectedDataColumn!.GetByteSize();

            return _bulkInserter!.ApplyBatch(rowReferences, weightArray, weights.Count, new MinRowMutator(), totalBatchSize);
        }

        public bool Compute<TValue>(int groupStartIndex, int groupEndIndex, PrimitiveList<int> weights, ReadOnlySpan<int> indices, EventBatchData data, ColumnReference groupState)
        {
            // Could compare values here, issue is if we get a delete.
            // Fetching the new value should be possible in GetValuesAsync
            return true;
        }

        public async ValueTask GetValuesAsync(IColumn[] groupingValuesSorted, ColumnReference[] groupStates, Column outputColumn)
        {
            var batch = new EventBatchData(groupingValuesSorted);
            ListAggColumnRowReference[] rowReferences = new ListAggColumnRowReference[groupStates.Length];

            for (int i = 0; i < groupStates.Length; i++)
            {
                rowReferences[i] = new ListAggColumnRowReference()
                {
                    batch = batch,
                    index = i,
                };
            }

            int maxKeyIndex = -1;
            await _bulkSearcher!.Start(rowReferences, groupStates.Length);
            while(await _bulkSearcher.MoveNextLeaf())
            {
                var leaf = _bulkSearcher.CurrentLeaf;
                for (int i = 0; i < _bulkSearcher.CurrentResults.Count; i++)
                {
                    var result = _bulkSearcher.CurrentResults[i];
                    if (result.KeyIndex < maxKeyIndex)
                    {
                        continue;
                    }
                    if (result.LowerBound >= 0)
                    {
                        leaf.keys._data.Columns[_groupingLength].GetValueAt(result.LowerBound, _dataValueContainer, default);
                        outputColumn.Add(_dataValueContainer);
                    }
                }
            }
        }

        public async Task InitializeAsync(int groupingLength, IStateManagerClient stateManagerClient, IMemoryAllocator memoryAllocator)
        {
            _groupingLength = groupingLength;
            _memoryAllocator = memoryAllocator;
            _tree = await stateManagerClient.GetOrCreateTree("mintree",
                new FlowtideDotNet.Storage.Tree.BPlusTreeOptions<ListAggColumnRowReference, int, ListAggKeyStorageContainer, PrimitiveListValueContainer<int>>()
                {
                    Comparer = new MinInsertComparer(groupingLength),
                    KeySerializer = new ListAggKeyStorageSerializer(groupingLength, memoryAllocator),
                    ValueSerializer = new PrimitiveListValueContainerSerializer<int>(memoryAllocator),
                    UseByteBasedPageSizes = true,
                    MemoryAllocator = memoryAllocator
                });

            _bulkInserter = _tree.CreateBulkInserter();
            _bulkSearcher = _tree.CreateBulkSearcher(new MinSearchComparer(groupingLength));
        }

        public void NewBatch(PrimitiveList<int> weights, EventBatchData batchData)
        {
            Debug.Assert(_memoryAllocator != null);

            if (_projectedDataColumn != null)
            {
                _projectedDataColumn.Dispose();
            }

            _projectedDataColumn = new Column(_memoryAllocator);

            for (int i = 0; i < weights.Count; i++)
            {
                var value = _projectionFunction(batchData, i);
                _projectedDataColumn.Append(value);
            }
        }
    }
}
