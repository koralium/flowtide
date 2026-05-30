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
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Core.Compute.Columnar.Functions.BulkAggregations;

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

            public GenericWriteOperation Process(ListAggColumnRowReference key, bool exists, in int existing, ref int incoming, int sortedIndex)
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
            ListAggColumnRowReference[] rowReferences = new ListAggColumnRowReference[len];
            int[] weightArray = new int[len];

            var allColumns = new IColumn[groupValueColumns.Length + 1];
            System.Array.Copy(groupValueColumns, allColumns, groupValueColumns.Length);
            allColumns[groupValueColumns.Length] = _projectedDataColumn!;
            var groupingBatch = new EventBatchData(allColumns);

            for (int i = 0; i < len; i++)
            {
                var physicalIndex = sortedByGroupIndices[i];
                rowReferences[i] = new ListAggColumnRowReference()
                {
                    batch = groupingBatch,
                    index = physicalIndex,
                    // Should be optimized later, just like this for now
                    insertValue = _projectedDataColumn!.GetValueAt(physicalIndex, default)
                };
                weightArray[i] = weights.Get(physicalIndex);
            }

            int totalBatchSize = 0;
            for (int i = 0; i < groupValueColumns.Length; i++)
            {
                totalBatchSize += groupValueColumns[i].GetByteSize();
            }
            totalBatchSize += _projectedDataColumn!.GetByteSize();

            return _bulkInserter!.ApplyBatch(rowReferences, weightArray, len, new MinRowMutator(), totalBatchSize);
        }

        public bool Compute(ReadOnlySpan<int> indices, PrimitiveList<int> weights, EventBatchData data, ColumnReference groupState)
        {
            // Could compare values here, issue is if we get a delete.
            // Fetching the new value should be possible in GetValuesAsync
            return true;
        }

        public ValueTask GetValuesAsync(IColumn[] groupingValuesSorted, ColumnReference[] groupStates, int startIndex, int length, Column outputColumn)
        {
            // We do everything in fetch async
            return ValueTask.CompletedTask;
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
                _projectedDataColumn.Add(value);
            }
        }

        public async ValueTask FetchValuesAsync(IColumn[] groupingValuesSorted, int length, Column outputColumn)
        {
            var batch = new EventBatchData(groupingValuesSorted);
            ListAggColumnRowReference[] rowReferences = new ListAggColumnRowReference[length];

            for (int i = 0; i < length; i++)
            {
                rowReferences[i] = new ListAggColumnRowReference()
                {
                    batch = batch,
                    index = i,
                };
            }

            int currentIndex = 0;
            await _bulkSearcher!.Start(rowReferences, length);
            while (await _bulkSearcher.MoveNextLeaf())
            {
                var leaf = _bulkSearcher.CurrentLeaf;

                for (int i = 0; i < _bulkSearcher.CurrentResults.Count; i++)
                {
                    var result = _bulkSearcher.CurrentResults[i];
                    while (currentIndex < result.KeyIndex)
                    {
                        outputColumn.Add(NullValue.Instance);
                        currentIndex++;
                    }
                    if (result.LowerBound >= 0)
                    {
                        leaf.keys._data.Columns[_groupingLength].GetValueAt(result.LowerBound, _dataValueContainer, default);
                        outputColumn.Add(_dataValueContainer);
                    }
                    else
                    {
                        outputColumn.Add(NullValue.Instance);
                    }
                    currentIndex++;
                }
            }

            while (currentIndex < length)
            {
                outputColumn.Add(NullValue.Instance);
                currentIndex++;
            }
        }
    }

    internal class MinAggregationDefinition : IBulkAggregationDefinition
    {
        public IColumnBulkAggregation Create(AggregateFunction aggregateFunction, IFunctionsRegister functionsRegister)
        {
            var compiledValue = ColumnProjectCompiler.CompileToValue(aggregateFunction.Arguments[0], functionsRegister);
            return new MinAggregation(compiledValue);
        }

        public static void Register(IFunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterBulkAggregationFunction(FunctionsArithmetic.Uri, FunctionsArithmetic.Min, new MinAggregationDefinition());
        }
    }
}
