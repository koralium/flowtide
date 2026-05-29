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
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.Sort;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Core.Compute.Columnar.Functions.BulkAggregations;
using FlowtideDotNet.Core.Operators.Aggregate.Column;
using FlowtideDotNet.Core.Operators.Join.MergeJoin;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Storage.Tree.Internal;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Aggregate.Bulk
{
    internal class BulkAggregateOperator : UnaryVertex<StreamEventBatch>
    {
        private readonly AggregateRelation _aggregateRelation;
        private readonly FunctionsRegister _functionsRegister;

        private IColumnBulkAggregation[] _measures;
        private Func<EventBatchData, int, bool>?[] _measureFilters;
        private readonly List<int> m_groupOutputIndices;
        private List<Action<EventBatchData, int, ColumnStore.Column>>? groupExpressions;
        private ColumnStore.Column[]? m_groupValues;
        private readonly BatchSorter _batchSorter;
        private readonly int m_outputCount;

        private ColumnStore.Column[]? m_temporaryStateValues;
        private EventBatchData? m_temporaryStateBatch;

        private int[] _groupedSortIndices = Array.Empty<int>();
        private int[] _duplicateTags = Array.Empty<int>();
        private int[] _groupSortLookup = Array.Empty<int>();
        private int[] _noDuplicateIndices = Array.Empty<int>();
        private bool[] _outputToTemp = Array.Empty<bool>();
        private bool[] _isDeleted = Array.Empty<bool>();
        private int[][] _measureLookups;
        private ColumnRowReference[] _rowReferenceBuffer;
        private ColumnAggregateStateReference[] _rowValuesBuffer;

        private IBPlusTree<ColumnRowReference, ColumnAggregateStateReference, AggregateKeyStorageContainer, ColumnAggregateValueContainer>? _tree;
        private IBPlusTreeBulkInserter<ColumnRowReference, ColumnAggregateStateReference, AggregateKeyStorageContainer, ColumnAggregateValueContainer>? _treeBulkInserter;
        private IBPlusTree<ColumnRowReference, int, AggregateKeyStorageContainer, PrimitiveListValueContainer<int>>? _temporaryTree;
        private IBPlusTreeBulkInserter<ColumnRowReference, int, AggregateKeyStorageContainer, PrimitiveListValueContainer<int>>? _temporaryTreeBulkInserter;
        private IBplusTreeBulkSearch<ColumnRowReference, ColumnAggregateStateReference, AggregateKeyStorageContainer, ColumnAggregateValueContainer, AggregateSearchComparer>? _treeBulkSearch;

        private ColumnStore.Column[] outputColumns;
        private PrimitiveList<int>? weights;
        private PrimitiveList<uint>? iterations;

        public BulkAggregateOperator(AggregateRelation aggregateRelation, FunctionsRegister functionsRegister, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            this._aggregateRelation = aggregateRelation;
            this._functionsRegister = functionsRegister;
            _measures = new IColumnBulkAggregation[aggregateRelation.Measures?.Count ?? 0];
            _measureFilters = new Func<EventBatchData, int, bool>?[aggregateRelation.Measures?.Count ?? 0];
            _measureLookups = new int[aggregateRelation.Measures?.Count ?? 0][];
            _rowReferenceBuffer = Array.Empty<ColumnRowReference>();
            _rowValuesBuffer = Array.Empty<ColumnAggregateStateReference>();

            int groupLength = 0;
            if (aggregateRelation.Groupings != null && aggregateRelation.Groupings.Count > 0)
            {
                if (aggregateRelation.Groupings.Count > 1)
                {
                    throw new InvalidOperationException("Aggregate operator only supports one grouping set at this point");
                }

                var grouping = aggregateRelation.Groupings[0];
                groupLength = grouping.GroupingExpressions.Count;
            }

            m_outputCount = groupLength + (_measures?.Length ?? 0);
            outputColumns = new ColumnStore.Column[m_outputCount];

            m_groupOutputIndices = new List<int>();
            for (int i = 0; i < groupLength; i++)
            {
                if (aggregateRelation.EmitSet)
                {
                    var emitIndex = aggregateRelation.Emit.IndexOf(i);
                    m_groupOutputIndices.Add(emitIndex);
                }
                else
                {
                    m_groupOutputIndices.Add(i);
                }
            }

            _batchSorter = new BatchSorter(groupLength);

            if (aggregateRelation.Measures != null)
            {
                for (int i = 0; i < aggregateRelation.Measures.Count; i++)
                {
                    if (_functionsRegister.TryGetBulkAggregationFunction(aggregateRelation.Measures[i].Measure.ExtensionUri, aggregateRelation.Measures[i].Measure.ExtensionName, out var func))
                    {
                        var measure = func.Create(aggregateRelation.Measures[i].Measure, functionsRegister);
                        _measures[i] = measure;
                        var filter = aggregateRelation.Measures[i].Filter;
                        if (filter != null)
                        {
                            _measureFilters[i] = ColumnBooleanCompiler.Compile(filter, _functionsRegister);
                        }
                    }
                    else
                    {
                        throw new NotSupportedException("Bulk aggregate operator only works with bulk ready aggregations");
                    }
                }
            }
        }

        public override string DisplayName => "Aggregation";

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
            Debug.Assert(m_groupValues != null);
            Debug.Assert(m_temporaryStateValues != null);

            for (int i = 0; i < m_groupValues.Length; i++)
            {
                m_groupValues[i].Dispose();
                m_groupValues[i] = ColumnFactory.Get(MemoryAllocator);
            }
            for (int i = 0; i < m_temporaryStateValues.Length; i++)
            {
                m_temporaryStateValues[i].Dispose();
                m_temporaryStateValues[i] = ColumnFactory.Get(MemoryAllocator);
            }

            return Task.CompletedTask;
        }

        protected override async IAsyncEnumerable<StreamEventBatch> OnWatermark(Watermark watermark)
        {
            Debug.Assert(_temporaryTree != null);
            Debug.Assert(_treeBulkSearch != null);
            Debug.Assert(m_groupValues != null);

            // TODO: Have this be reused between watermarks
            ColumnRowReference[] rowReferences = Array.Empty<ColumnRowReference>();
            ColumnReference[] columnReferences = Array.Empty<ColumnReference>();
            int[] indices = Array.Empty<int>();

            var outputColumnCount = m_outputCount;
            int groupLength = m_groupValues.Length;

            using var iterator = _temporaryTree.CreateIterator();
            await iterator.SeekFirst();
            await foreach(var page in iterator)
            {
                var currentLeaf = page.CurrentPage;

                if (currentLeaf.keys.Count == 0)
                {
                    continue;
                }

                if (rowReferences.Length < currentLeaf.keys.Count)
                {
                    rowReferences = new ColumnRowReference[currentLeaf.keys.Count];
                    columnReferences = new ColumnReference[currentLeaf.keys.Count];
                    indices = new int[currentLeaf.keys.Count];
                    // indices will always just be incremented since data is always sorted from the tree
                    for (int i = 0; i < indices.Length; i++)
                    {
                        indices[i] = i;
                    }
                }

                for (int i = 0; i < currentLeaf.keys.Count; i++)
                {
                    // Map row references, this is simply for the bulk search interface
                    rowReferences[i] = new ColumnRowReference()
                    {
                        referenceBatch = currentLeaf.keys._data,
                        RowIndex = i
                    };
                }

                for (int m = 0; m < _measures.Length; m++)
                {
                    await _measures[m].FetchValuesAsync(page.Keys._data.GetColumns_Unsafe(), 0, page.Keys.Count, outputColumns[groupLength + m]);
                }

                for (int c = 0; c < groupLength; c++)
                {
                    for (int i = 0; i < currentLeaf.keys.Count; i++)
                    {
                        outputColumns[c].Add(currentLeaf.keys._data.Columns[c].GetValueAt(i, default));
                    }
                }

                for (int i = 0; i < currentLeaf.keys.Count; i++)
                {
                    weights.Add(1);
                    iterations.Add(0);
                }

                // Current leaf have data sorted already, so no need to sort, take data and search in persisted tree to get states
                // TODO: Fix row references and indices
                await _treeBulkSearch.Start(rowReferences, currentLeaf.keys.Count, indices);

                while (await _treeBulkSearch.MoveNextLeaf())
                {
                    var persistedLeaf = _treeBulkSearch.CurrentLeaf;
                    var currentResults = _treeBulkSearch.CurrentResults;
                    var previousValueSent = persistedLeaf.values._previousValueSent;

                    var firstIndex = currentResults[0].KeyIndex;
                    var lastIndex = currentResults[currentResults.Count - 1].KeyIndex;
                    var outputCount = 0;
                    if (_measures.Length > 0)
                    {
                        outputCount = outputColumns[m_groupValues.Length].Count;
                    }

                    for (int m = 0; m < _measures.Length; m++)
                    {
                        for (int c = 0; c < currentResults.Count; c++)
                        {
                            var stateCol = persistedLeaf.values._eventBatch.GetColumn(m);
                            columnReferences[firstIndex + c] = new ColumnReference(stateCol, currentResults[c].LowerBound, persistedLeaf);
                        }

                        await _measures[m].GetValuesAsync(page.Keys._data.GetColumns_Unsafe(), columnReferences, firstIndex, lastIndex - firstIndex + 1, outputColumns[groupLength + m]);
                    }

                    // Add output for previously sent data for retraction
                    persistedLeaf.EnterWriteLock();
                    for (int i = 0; i < currentResults.Count; i++)
                    {
                        var lower = currentResults[i].LowerBound;
                        var valueSent = previousValueSent[lower];
                        if (valueSent)
                        {
                            // Previous value has been sent, so we need to send a retraction
                            weights.Add(-1);
                            for (int c = 0; c < groupLength; c++)
                            {
                                outputColumns[c].Add(currentLeaf.keys._data.Columns[c].GetValueAt(currentResults[i].KeyIndex, default));
                            }
                            for (int m = 0; m < _measures.Length; m++)
                            {
                                var previousValueCol = persistedLeaf.values._eventBatch.GetColumn(_measures.Length + m);
                                outputColumns[groupLength + m].Add(previousValueCol.GetValueAt(lower, default));
                            }
                        }
                        previousValueSent[lower] = true;
                    }

                    // Update previous sent value column for all data
                    for (int m = 0; m < _measures.Length; m++)
                    {
                        var previousValueCol = persistedLeaf.values._eventBatch.GetColumn(_measures.Length + m);
                        for (int c = 0; c < currentResults.Count; c++)
                        {
                            previousValueCol.UpdateAt(currentResults[c].LowerBound, outputColumns[groupLength + m].GetValueAt(outputCount + c, default));
                        }
                    }

                    persistedLeaf.ExitWriteLock();

                    {
                        // Save leaf
                        var bTree = (BPlusTree<ColumnRowReference, ColumnAggregateStateReference, AggregateKeyStorageContainer, ColumnAggregateValueContainer>)_tree;
                        var isFull = bTree.m_stateClient.AddOrUpdate(persistedLeaf.Id, persistedLeaf);
                        if (isFull)
                        {
                            await bTree.m_stateClient.WaitForNotFullAsync();
                        }
                    }
                    

                    if (weights.Count >= 100)
                    {
                        yield return new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(outputColumns)));
                        InitOutputColumns();
                    }
                }
            }

            if (weights.Count > 0)
            {
                yield return new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(outputColumns)));
                InitOutputColumns();
            }

            await _temporaryTree.Clear();
        }

        public void InitOutputColumns()
        {
            outputColumns = new ColumnStore.Column[m_outputCount];
            for (int i = 0; i < m_outputCount; i++)
            {
                outputColumns[i] = new ColumnStore.Column(MemoryAllocator);
            }
            weights = new PrimitiveList<int>(MemoryAllocator);
            iterations = new PrimitiveList<uint>(MemoryAllocator);
        }

        public override async IAsyncEnumerable<StreamEventBatch> OnRecieve(StreamEventBatch msg, long time)
        {
            Debug.Assert(m_groupValues != null);
            Debug.Assert(m_temporaryStateValues != null);
            Debug.Assert(m_temporaryStateBatch != null);
            Debug.Assert(_temporaryTreeBulkInserter != null);

            // Call each measure to initialize the new batch
            for (int i = 0; i< _measures.Length; i++)
            {
                _measures[i].NewBatch(msg.Data.Weights, msg.Data.EventBatchData);
            }

            // Pre compute group by columns, so we can do sorting and have better cache locality for the measures computation
            for (int i = 0; i < m_groupValues.Length; i++)
            {
                m_groupValues[i].Clear();
            }
            for (int i = 0; i < m_temporaryStateValues.Length; i++)
            {
                m_temporaryStateValues[i].Clear();
                // add null for all temporary state values, so if its a new value it can update the state during compute
                m_temporaryStateValues[i].InsertNullRange(0, msg.Data.Count);
            }

            var data = msg.Data;
            var dataCount = data.Count;

            if (groupExpressions != null)
            {
                // Take column by column to try and reuse cache
                for (int k = 0; k < groupExpressions.Count; k++)
                {
                    for (int i = 0; i < dataCount; i++)
                    {
                        groupExpressions[k](data.EventBatchData, i, m_groupValues[k]);
                    }
                }
            }

            var groupValuesBatch = new EventBatchData(m_groupValues);

            // Start of sorting
            if (_groupedSortIndices.Length < dataCount)
            {
                _groupedSortIndices = new int[dataCount];
                _groupSortLookup = new int[dataCount];
                _duplicateTags = new int[dataCount];
                _noDuplicateIndices = new int[dataCount];
                _outputToTemp = new bool[dataCount];
                _isDeleted = new bool[dataCount];
            }

            for (int i = 0; i < dataCount; i++)
            {
                _groupedSortIndices[i] = i;
            }

            // Do sorting based on group columns
            // Create indirection lookup from original value to sorted value, so filters can run in sequential order but adding the sorted index
            SortData(dataCount);

            for (int i = 0; i < dataCount; i++)
            {
                _groupSortLookup[_groupedSortIndices[i]] = i;
            }

            // Create lookup arrays for measures with filters, so we can iterate only on the relevant rows for each measure in the aggregation phase
            for (int i = 0; i < _measures.Length; i++)
            {
                if (_measureLookups[i] == null || _measureLookups[i].Length < dataCount)
                {
                    // Create lookup array for each measure
                    _measureLookups[i] = new int[dataCount];
                }
                var lookupArray = _measureLookups[i];
                int rowCounter = 0;
                var measureFilter = _measureFilters[i];
                if (measureFilter != null)
                {
                    for (int j = 0; j < dataCount; j++)
                    {
                        var physicalIndex = _groupedSortIndices[j];
                        if (measureFilter(data.EventBatchData, physicalIndex))
                        {
                            lookupArray[rowCounter++] = physicalIndex;
                        }
                    }
                }
                else
                {
                    for (int j = 0; j < dataCount; j++)
                    {
                        lookupArray[rowCounter++] = _groupedSortIndices[j];
                    }
                }
                
                await _measures[i].StoreAsync(data.Weights, m_groupValues, data.EventBatchData, lookupArray.AsSpan(0, rowCounter));
            }

            if (_rowReferenceBuffer.Length < dataCount)
            {
                _rowReferenceBuffer = new ColumnRowReference[dataCount];
                _rowValuesBuffer = new ColumnAggregateStateReference[dataCount];
            }

            List<AggregateComputeRange> computeRanges = new List<AggregateComputeRange>();

            int uniqueCounter = 0;
            if (dataCount > 0)
            {
                int lastTag = _duplicateTags[0];
                int uniqueIndex = 0;
                var weightCounter = data.Weights[_groupedSortIndices[uniqueIndex]];

                for (int i = 1; i < dataCount; i++)
                {
                    
                    if (_duplicateTags[i] == lastTag)
                    {
                        var sortedIndex = _groupedSortIndices[i];
                        weightCounter += data.Weights[sortedIndex];
                    }
                    else
                    {
                        var sortedIndex = _groupedSortIndices[uniqueIndex];
                        _rowReferenceBuffer[sortedIndex] = new ColumnRowReference()
                        {
                            referenceBatch = groupValuesBatch,
                            RowIndex = sortedIndex
                        };
                        _rowValuesBuffer[sortedIndex] = new ColumnAggregateStateReference()
                        {
                            referenceBatch = m_temporaryStateBatch,
                            RowIndex = sortedIndex,
                            valueSent = false,
                            weight = weightCounter
                        };
                        _noDuplicateIndices[uniqueCounter] = sortedIndex;
                        _duplicateTags[uniqueCounter] = uniqueCounter;

                        computeRanges.Add(new AggregateComputeRange()
                        {
                            start = uniqueIndex,
                            length = i - uniqueIndex
                        });
                        uniqueCounter++;

                        weightCounter = data.Weights[sortedIndex];
                        lastTag = _duplicateTags[i];
                        uniqueIndex = i;
                    }
                }
                var sortedIndexLast = _groupedSortIndices[uniqueIndex];
                _rowReferenceBuffer[sortedIndexLast] = new ColumnRowReference()
                {
                    referenceBatch = groupValuesBatch,
                    RowIndex = sortedIndexLast
                };
                _rowValuesBuffer[sortedIndexLast] = new ColumnAggregateStateReference()
                {
                    referenceBatch = m_temporaryStateBatch,
                    RowIndex = sortedIndexLast,
                    valueSent = false,
                    weight = weightCounter
                };
                _noDuplicateIndices[uniqueCounter] = sortedIndexLast;
                _duplicateTags[uniqueCounter] = uniqueCounter;
                computeRanges.Add(new AggregateComputeRange()
                {
                    start = uniqueIndex,
                    length = dataCount - uniqueIndex
                });

                uniqueCounter++;
            }

            int totalBatchSize = 0;
            for (int i = 0; i < m_groupValues.Length; i++)
            {
                totalBatchSize += m_groupValues[i].GetByteSize();
            }

            Debug.Assert(_treeBulkInserter != null);
            Debug.Assert(weights != null);

            for (int i = 0; i < uniqueCounter; i++)
            {
                _outputToTemp[i] = false;
                _isDeleted[i] = false;
            }
            
            // TODO: Might need to handle deleted rows here, if valueSent is set, output directly
            var mutator = new BulkAggregateMutator(
                _measures, 
                data.Weights, 
                data.EventBatchData, 
                _groupedSortIndices, 
                computeRanges, 
                outputColumns, 
                weights, 
                _outputToTemp,
                _isDeleted);

            // Apply the batch, mutator handles calling compute on measures for a group of values
            await _treeBulkInserter.ApplyBatch(_rowReferenceBuffer, _rowValuesBuffer, uniqueCounter, _noDuplicateIndices, _duplicateTags, mutator, totalBatchSize);

            // Temporary should be moved
            int[] tempIndices = new int[uniqueCounter];
            int[] tmpValues = new int[dataCount];
            int[] tmpDuplicateTags = new int[uniqueCounter];

            int count = 0;
            for (int i = 0; i < uniqueCounter; i++)
            {
                if (_isDeleted[i])
                {
                    var lookupIndex = _noDuplicateIndices[i];
                    tempIndices[count] = lookupIndex;
                    tmpValues[lookupIndex] = -1;
                    tmpDuplicateTags[count] = count;
                    count++;
                }
                else if (_outputToTemp[i])
                {
                    var lookupIndex = _noDuplicateIndices[i];
                    tempIndices[count] = lookupIndex;
                    tmpValues[lookupIndex] = 1;
                    tmpDuplicateTags[count] = count;
                    count++;
                }
            }

            // We now only insert 
            var tempMutator = new BulkTemporaryMutator();
            await _temporaryTreeBulkInserter.ApplyBatch(_rowReferenceBuffer, tmpValues, count, tempIndices, tmpDuplicateTags, tempMutator, totalBatchSize);
            
            // If there is any output here directly, we output it
            // This is deletes from persisted tree
            // Watermark only handles existing values
            if (weights.Count > 0)
            {
                Debug.Assert(iterations != null);
                for (int i = 0; i < weights.Count; i++)
                {
                    iterations.Add(0);
                }
                yield return new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(outputColumns)));
                InitOutputColumns();
            }
        }

        protected override async Task InitializeOrRestore(IStateManagerClient stateManagerClient)
        {
            InitOutputColumns();
            if (_aggregateRelation.Groupings != null && _aggregateRelation.Groupings.Count > 0)
            {
                if (_aggregateRelation.Groupings.Count > 1)
                {
                    throw new InvalidOperationException("Aggregate operator only supports one grouping set at this point");
                }

                var grouping = _aggregateRelation.Groupings[0];

                m_groupValues = new ColumnStore.Column[grouping.GroupingExpressions.Count];

                for (int i = 0; i < grouping.GroupingExpressions.Count; i++)
                {
                    m_groupValues[i] = ColumnFactory.Get(MemoryAllocator);
                }

                if (groupExpressions == null)
                {
                    groupExpressions = new List<Action<EventBatchData, int, ColumnStore.Column>>();
                    foreach (var expr in grouping.GroupingExpressions)
                    {
                        groupExpressions.Add(ColumnProjectCompiler.Compile(expr, _functionsRegister));
                    }
                }
            }
            else
            {
                m_groupValues = Array.Empty<ColumnStore.Column>();
            }

            m_temporaryStateValues = new ColumnStore.Column[(_aggregateRelation.Measures?.Count ?? 0) * 2];

            for (int i = 0; i < m_temporaryStateValues.Length; i++)
            {
                m_temporaryStateValues[i] = ColumnFactory.Get(MemoryAllocator);
            }
            m_temporaryStateBatch = new EventBatchData(m_temporaryStateValues);

            for (int i = 0; i < _measures.Length; i++)
            {
                // Initialize all mesures
                await _measures[i].InitializeAsync(m_groupValues.Length, stateManagerClient.GetChildManager(i.ToString()), MemoryAllocator);
            }

            _tree = await stateManagerClient.GetOrCreateTree("grouping_set_1_v1",
                new FlowtideDotNet.Storage.Tree.BPlusTreeOptions<ColumnRowReference, ColumnAggregateStateReference, AggregateKeyStorageContainer, ColumnAggregateValueContainer>()
                {
                    KeySerializer = new AggregateKeySerializer(m_groupValues.Length, MemoryAllocator),
                    ValueSerializer = new ColumnAggregateValueSerializer(_measures.Length, MemoryAllocator),
                    Comparer = new AggregateInsertComparer(m_groupValues.Length),
                    UseByteBasedPageSizes = true,
                    MemoryAllocator = MemoryAllocator
                });

            _temporaryTree = await stateManagerClient.GetOrCreateTree("grouping_set_1_v1_temp",
                new FlowtideDotNet.Storage.Tree.BPlusTreeOptions<ColumnRowReference, int, AggregateKeyStorageContainer, PrimitiveListValueContainer<int>>()
                {
                    KeySerializer = new AggregateKeySerializer(m_groupValues.Length, MemoryAllocator),
                    ValueSerializer = new PrimitiveListValueContainerSerializer<int>(MemoryAllocator),
                    Comparer = new AggregateInsertComparer(m_groupValues.Length),
                    UseByteBasedPageSizes = true,
                    MemoryAllocator = MemoryAllocator
                });

            _treeBulkInserter = _tree.CreateBulkInserter();
            _temporaryTreeBulkInserter = _temporaryTree.CreateBulkInserter();
            _treeBulkSearch = _tree.CreateBulkSearcher(new AggregateSearchComparer(m_groupValues.Length));
        }

        private void SortData(int dataCount)
        {
            Debug.Assert(m_groupValues != null);
            if (m_groupValues.Length > 0)
            {
                for (int i = 0; i < dataCount; i++)
                {
                    _groupedSortIndices[i] = i;
                }
                var groupSortIndicesSpan = _groupedSortIndices.AsSpan(0, dataCount);
                var duplicateTagsSpan = _duplicateTags.AsSpan(0, dataCount);
                _batchSorter.SortDataWithTags(m_groupValues, ref groupSortIndicesSpan, ref duplicateTagsSpan);
            }
            else
            {
                for (int i = 0; i < dataCount; i++)
                {
                    _duplicateTags[i] = 0;
                }
            }
        }
    }
}
