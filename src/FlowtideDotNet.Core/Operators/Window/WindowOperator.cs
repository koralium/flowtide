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

using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Vertices.Unary;
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Core.ColumnStore;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics;
using FlowtideDotNet.Core.Operators.Aggregate.Column;
using FlowtideDotNet.Core.ColumnStore.DataValues;

namespace FlowtideDotNet.Core.Operators.Window
{
    internal class WindowOperator : UnaryVertex<StreamEventBatch>
    {
        private readonly ConsistentPartitionWindowRelation _relation;
        private IColumnComparer<ColumnRowReference>? _sortComparer;
        private IBPlusTree<ColumnRowReference, ColumnAggregateStateReference, ColumnKeyStorageContainer, ColumnAggregateValueContainer>? _persistentTree;

        /// <summary>
        /// This tree contains partitions that have been updated.
        /// </summary>
        private IBPlusTree<ColumnRowReference, int, AggregateKeyStorageContainer, PrimitiveListValueContainer<int>>? _temporaryTree;
        private List<int> _partitionColumns;
        private List<int> _otherColumns;
        private List<Action<EventBatchData, int, Column>> _partitionCalculateExpressions;

        private ColumnStore.Column[]? m_temporaryStateValues;
        private EventBatchData? m_temporaryStateBatch;

        private ColumnStore.IColumn[]? _partitionBatchColumns;
        /// <summary>
        /// Batch used when inserting into the temporary tree
        /// </summary>
        private EventBatchData? _partitionBatch;

        public WindowOperator(ConsistentPartitionWindowRelation relation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            _relation = relation;
            if (_relation.WindowFunctions.Count > 1)
            {
                throw new NotSupportedException("Only one window function is supported at this time per window operator");
            }
            
            // Create the comparer for the ordering
            if (relation.OrderBy.Count > 0)
            {
                _sortComparer = SortFieldCompareCompiler.CreateComparer(relation.OrderBy, functionsRegister);
            }
            CreatePartitionColumnExpressions(functionsRegister);
        }

        [MemberNotNull(nameof(_partitionColumns), nameof(_partitionCalculateExpressions), nameof(_otherColumns))]
        private void CreatePartitionColumnExpressions(IFunctionsRegister functionsRegister)
        {
            List<int> partitionColumns = new List<int>();
            int extraColumnCounter = _relation.Input.OutputLength;
            List<Action<EventBatchData, int, Column>> partitionCalculateExpressions = new List<Action<EventBatchData, int, Column>>();
            foreach (var partitionExpr in _relation.PartitionBy)
            {
                if (partitionExpr is DirectFieldReference directFieldReference &&
                    directFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
                {
                    partitionColumns.Add(structReferenceSegment.Field);
                }
                else
                {
                    var exprFunc = ColumnProjectCompiler.Compile(partitionExpr, functionsRegister);
                    partitionCalculateExpressions.Add(exprFunc);
                    partitionColumns.Add(extraColumnCounter++);
                }
            }
            _partitionColumns = partitionColumns;
            _partitionCalculateExpressions = partitionCalculateExpressions;

            _otherColumns = new List<int>();
            for (int i = 0; i < _relation.Input.OutputLength; i++)
            {
                if (!_partitionColumns.Contains(i))
                {
                    _otherColumns.Add(i);
                }
            }

        }

        public override string DisplayName => "WindowOperator";

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override Task OnCheckpoint()
        {
            return Task.CompletedTask;
        }

        protected override async IAsyncEnumerable<StreamEventBatch> OnWatermark(Watermark watermark)
        {
            var temporaryTreeIterator = _temporaryTree!.CreateIterator();
            await temporaryTreeIterator.SeekFirst();

            var persistentTreeIterator = _persistentTree!.CreateIterator();

            var windowSum = new WindowSumCalculator();
            await windowSum.Initialize(_persistentTree, _relation.PartitionBy.Count, MemoryAllocator);

            await foreach(var partitionPage in temporaryTreeIterator)
            {
                foreach(var partitionKv in partitionPage)
                {
                    await windowSum.ComputeRowSlidingWindow(partitionKv.Key, new WindowPartitionStartSearchComparer(_partitionColumns), -1, 0);
                }
            }

            yield break;
        }

        public override async IAsyncEnumerable<StreamEventBatch> OnRecieve(StreamEventBatch msg, long time)
        {
            Debug.Assert(_persistentTree != null);
            Debug.Assert(_temporaryTree != null);
            Debug.Assert(_partitionBatchColumns != null);
            // Need to calculate any partition expressions into values if they are not already
            Column[] extraPartitionColumns = new Column[_partitionCalculateExpressions.Count];

            for (int i = 0; i < extraPartitionColumns.Length; i++)
            {
                extraPartitionColumns[i] = new Column(MemoryAllocator);
            }

            IColumn[] columns = new IColumn[msg.Data.EventBatchData.Columns.Count + extraPartitionColumns.Length];
            for (int i = 0; i < msg.Data.EventBatchData.Columns.Count; i++)
            {
                columns[i] = msg.Data.EventBatchData.Columns[i];
            }
            for (int i = 0; i < extraPartitionColumns.Length; i++)
            {
                columns[msg.Data.EventBatchData.Columns.Count + i] = extraPartitionColumns[i];
            }
            var eventBatchWithPartitionColumns = new EventBatchData(columns);

            for (int i = 0; i < msg.Data.Weights.Count; i++)
            {
                // Calculate partition values that are more complex than simply a column value
                // These values are stored in the extraPartitionColumns array
                for (int k = 0; k < _partitionCalculateExpressions.Count; k++)
                {
                    _partitionCalculateExpressions[k](msg.Data.EventBatchData, i, extraPartitionColumns[k]);
                }

                var rowRef = new ColumnRowReference()
                {
                    referenceBatch = eventBatchWithPartitionColumns,
                    RowIndex = i
                };


                int stateIndex = m_temporaryStateBatch.Count;
                for (int w = 0; w < _relation.WindowFunctions.Count; w++)
                {
                    var stateColumn = m_temporaryStateValues[w];
                    stateColumn.Add(NullValue.Instance);
                    var previousValueColumn = m_temporaryStateValues[w + _relation.WindowFunctions.Count];
                    previousValueColumn.Add(NullValue.Instance);
                }

                var aggState = new ColumnAggregateStateReference()
                {
                    weight = msg.Data.Weights[i],
                    referenceBatch = m_temporaryStateBatch,
                    RowIndex = stateIndex,
                    valueSent = false
                };


                await _persistentTree.RMWNoResult(in rowRef, in aggState, (input, current, exist) =>
                {
                    if (exist)
                    {
                        current.weight += input.weight;

                        if (current.weight == 0)
                        {
                            // Must output the entire row here and the previous value
                            return (current, GenericWriteOperation.Delete);
                        }

                        return (current, GenericWriteOperation.Upsert);
                    }
                    
                    return (input, GenericWriteOperation.Upsert);
                });

                for(int p = 0; p < _partitionColumns.Count; p++)
                {
                    _partitionBatchColumns[p] = columns[_partitionColumns[p]];
                }

                var partitionRowRef = new ColumnRowReference()
                {
                    referenceBatch = _partitionBatch,
                    RowIndex = i
                };

                // Even if the row was deleted we still need to update the temporary tree
                // Since rows that depend on this row will need to be updated
                await _temporaryTree.RMWNoResult(partitionRowRef, 1, (input, current, exists) =>
                {
                    if (exists)
                    {
                        // If the partition already exists in the tree, no need to force a write on the page
                        return (current, GenericWriteOperation.None);
                    }
                    return (input, GenericWriteOperation.Upsert);
                });

                // Need to calculate the window function
                var iterator = _persistentTree.CreateIterator();
                await iterator.Seek(rowRef);
            }
            yield break;
        }

        protected override async Task InitializeOrRestore(IStateManagerClient stateManagerClient)
        {
            _persistentTree = await stateManagerClient.GetOrCreateTree("persistent",
                new FlowtideDotNet.Storage.Tree.BPlusTreeOptions<ColumnRowReference, ColumnAggregateStateReference, ColumnKeyStorageContainer, ColumnAggregateValueContainer>()
                {
                    Comparer = new WindowInsertComparer(_sortComparer, _partitionColumns, _otherColumns),
                    KeySerializer = new ColumnStoreSerializer(_relation.Input.OutputLength + _partitionCalculateExpressions.Count, MemoryAllocator),
                    MemoryAllocator = MemoryAllocator,
                    ValueSerializer = new ColumnAggregateValueSerializer(_relation.WindowFunctions.Count, MemoryAllocator),
                    UseByteBasedPageSizes = true
                });

            _temporaryTree = await stateManagerClient.GetOrCreateTree("temporary",
                new FlowtideDotNet.Storage.Tree.BPlusTreeOptions<ColumnRowReference, int, AggregateKeyStorageContainer, PrimitiveListValueContainer<int>>()
                {
                    Comparer = new AggregateInsertComparer(_partitionColumns.Count),
                    KeySerializer = new AggregateKeySerializer(_partitionColumns.Count, MemoryAllocator),
                    MemoryAllocator = MemoryAllocator,
                    ValueSerializer = new PrimitiveListValueContainerSerializer<int>(MemoryAllocator),
                    UseByteBasedPageSizes = true
                });

            m_temporaryStateValues = new ColumnStore.Column[_relation.WindowFunctions.Count * 2];

            for (int i = 0; i < m_temporaryStateValues.Length; i++)
            {
                m_temporaryStateValues[i] = ColumnFactory.Get(MemoryAllocator);
            }

            m_temporaryStateBatch = new EventBatchData(m_temporaryStateValues);

            _partitionBatchColumns = new IColumn[_partitionColumns.Count];
            _partitionBatch = new EventBatchData(_partitionBatchColumns);
        }
    }
}
