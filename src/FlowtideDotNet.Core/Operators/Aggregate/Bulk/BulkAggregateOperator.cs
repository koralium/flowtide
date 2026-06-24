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
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Sort;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Core.Compute.Columnar.Functions.BulkAggregations;
using FlowtideDotNet.Core.Compute.Columnar.Functions.BulkAggregations.Stateful;
using FlowtideDotNet.Core.Operators.Aggregate.Column;
using FlowtideDotNet.Core.Operators.Join.MergeJoin;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Storage.Tree.Internal;
using FlowtideDotNet.Substrait.Expressions;
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
        private struct GroupExpressionInfo
        {
            public int GroupIndex;
            public Action<EventBatchData, int, ColumnStore.Column> Expression;
        }

        private readonly int m_groupLength;
        private List<GroupExpressionInfo>? groupExpressions;
        private int[]? m_groupDirectFields;
        private IColumn[]? m_groupValues;
        private readonly BatchSorter _batchSorter;

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

        private ColumnRowReference[] _watermarkRowReferences = Array.Empty<ColumnRowReference>();
        private ColumnReference[] _watermarkColumnReferences = Array.Empty<ColumnReference>();
        private int[] _watermarkIndices = Array.Empty<int>();
        private int[]? _pageStartBuffer;
        private DataValueContainer? _watermarkValueContainer;

        private int[] _tempIndices = Array.Empty<int>();
        private int[] _tempValues = Array.Empty<int>();
        private int[] _tempDuplicateTags = Array.Empty<int>();

        private readonly List<AggregateComputeRange> _computeRanges = new List<AggregateComputeRange>();

        private IBPlusTree<ColumnRowReference, ColumnAggregateStateReference, AggregateKeyStorageContainer, ColumnAggregateValueContainer>? _tree;
        private IBPlusTreeBulkInserter<ColumnRowReference, ColumnAggregateStateReference, AggregateKeyStorageContainer, ColumnAggregateValueContainer>? _treeBulkInserter;
        private IBPlusTree<ColumnRowReference, int, AggregateKeyStorageContainer, PrimitiveListValueContainer<int>>? _temporaryTree;
        private IBPlusTreeBulkInserter<ColumnRowReference, int, AggregateKeyStorageContainer, PrimitiveListValueContainer<int>>? _temporaryTreeBulkInserter;
        private IBplusTreeBulkSearch<ColumnRowReference, ColumnAggregateStateReference, AggregateKeyStorageContainer, ColumnAggregateValueContainer, AggregateSearchComparer>? _treeBulkSearch;
        private IObjectState<bool>? m_hasSentInitialData;
        private readonly Dictionary<string, SharedGroupValueTree> _sharedTrees = new();

        private ColumnStore.Column[] outputColumns;
        private PrimitiveList<int>? weights;
        private PrimitiveList<uint>? iterations;
        private uint m_currentIteration;

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

            m_groupLength = groupLength;
            outputColumns = new ColumnStore.Column[groupLength + _measures.Length];

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
                        throw new NotSupportedException($"Bulk aggregate operator only works with bulk ready aggregations. Not found: {aggregateRelation.Measures[i].Measure.ExtensionUri}:{aggregateRelation.Measures[i].Measure.ExtensionName}");
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

        public override async Task OnCheckpoint()
        {
            Debug.Assert(_tree != null);
            Debug.Assert(m_groupValues != null);
            Debug.Assert(m_temporaryStateValues != null);
            Debug.Assert(m_hasSentInitialData != null);

            await _tree.Commit();

            if (_temporaryTree != null)
            {
                await _temporaryTree.Commit();
            }

            foreach (var sharedTree in _sharedTrees.Values)
            {
                await sharedTree.Tree.Commit();
            }

            for (int i = 0; i < _measures.Length; i++)
            {
                await _measures[i].CommitAsync();
            }

            for (int i = 0; i < m_groupValues.Length; i++)
            {
                if (m_groupDirectFields![i] == -1)
                {
                    m_groupValues[i].Dispose();
                    m_groupValues[i] = ColumnFactory.Get(MemoryAllocator);
                }
                else
                {
                    m_groupValues[i] = null!;
                }
            }
            for (int i = 0; i < m_temporaryStateValues.Length; i++)
            {
                m_temporaryStateValues[i].Dispose();
                m_temporaryStateValues[i] = ColumnFactory.Get(MemoryAllocator);
            }

            await m_hasSentInitialData.Commit();
        }

        protected override async IAsyncEnumerable<StreamEventBatch> OnWatermark(Watermark watermark)
        {
            Debug.Assert(_temporaryTree != null);
            Debug.Assert(_treeBulkSearch != null);
            Debug.Assert(m_groupValues != null);
            Debug.Assert(m_hasSentInitialData != null);
            Debug.Assert(weights != null);
            Debug.Assert(iterations != null);

            int groupLength = m_groupValues.Length;

            if (!m_hasSentInitialData.Value)
            {
                if (groupLength == 0)
                {
                    bool isEmpty = true;
                    using (var testIterator = _tree!.CreateIterator())
                    {
                        await testIterator.SeekFirst();
                        await foreach (var page in testIterator)
                        {
                            if (page.CurrentPage.keys.Count > 0)
                            {
                                isEmpty = false;
                                break;
                            }
                        }
                    }

                    if (isEmpty)
                    {
                        for (int i = 0; i < m_temporaryStateValues!.Length; i++)
                        {
                            m_temporaryStateValues[i].Clear();
                            m_temporaryStateValues[i].Add(NullValue.Instance);
                        }
                        var emptyKey = new ColumnRowReference()
                        {
                            referenceBatch = new EventBatchData(m_groupValues),
                            RowIndex = 0
                        };
                        var defaultState = new ColumnAggregateStateReference()
                        {
                            referenceBatch = m_temporaryStateBatch!,
                            RowIndex = 0,
                            valueSent = false,
                            weight = 0
                        };
                        await _tree.Upsert(emptyKey, defaultState);
                    }
                }

                int totalProcessed = 0;
                using var iterator = _tree!.CreateIterator();
                await iterator.SeekFirst();
                await foreach (var page in iterator)
                {
                    var currentLeaf = page.CurrentPage;
                    totalProcessed += currentLeaf.keys.Count;

                    if (currentLeaf.keys.Count == 0)
                    {
                        continue;
                    }

                    if (_watermarkColumnReferences.Length < currentLeaf.keys.Count)
                    {
                        _watermarkColumnReferences = new ColumnReference[currentLeaf.keys.Count];
                    }

                    for (int m = 0; m < _measures.Length; m++)
                    {
                        var measureColIndex = groupLength + m;
                        await _measures[m].FetchValuesAsync(currentLeaf.keys._data.GetColumns_Unsafe(), currentLeaf.keys.Count, outputColumns[measureColIndex]);

                        var stateCol = currentLeaf.values._eventBatch.GetColumn(m);
                        for (int c = 0; c < currentLeaf.keys.Count; c++)
                        {
                            _watermarkColumnReferences[c] = new ColumnReference(stateCol, c, currentLeaf);
                        }

                        await _measures[m].GetValuesAsync(currentLeaf.keys._data.GetColumns_Unsafe(), _watermarkColumnReferences, 0, currentLeaf.keys.Count, outputColumns[measureColIndex]);
                    }

                    var sourceColumns = currentLeaf.keys._data.GetColumns_Unsafe();
                    for (int c = 0; c < groupLength; c++)
                    {
                        outputColumns[c].InsertRangeFrom(outputColumns[c].Count, sourceColumns[c], 0, currentLeaf.keys.Count);
                    }

                    weights!.InsertStaticRange(weights.Count, 1, currentLeaf.keys.Count);
                    iterations!.InsertStaticRange(iterations.Count, m_currentIteration, currentLeaf.keys.Count);

                    currentLeaf.EnterWriteLock();
                    var previousValueSent = currentLeaf.values._previousValueSent;
                    for (int i = 0; i < currentLeaf.keys.Count; i++)
                    {
                        previousValueSent[i] = true;
                    }

                    Debug.Assert(_watermarkValueContainer != null);
                    for (int m = 0; m < _measures.Length; m++)
                    {
                        var previousValueCol = currentLeaf.values._eventBatch.GetColumn(_measures.Length + m);
                        var measureColIndex = groupLength + m;
                        var pageStart = outputColumns[measureColIndex].Count - currentLeaf.keys.Count;
                        for (int c = 0; c < currentLeaf.keys.Count; c++)
                        {
                            outputColumns[measureColIndex].GetValueAt(pageStart + c, _watermarkValueContainer, default);
                            previousValueCol.UpdateAt(c, _watermarkValueContainer);
                        }
                    }
                    currentLeaf.ExitWriteLock();

                    {
                        var bTree = (BPlusTree<ColumnRowReference, ColumnAggregateStateReference, AggregateKeyStorageContainer, ColumnAggregateValueContainer>)_tree!;
                        var isFull = bTree.m_stateClient.AddOrUpdate(currentLeaf.Id, currentLeaf);
                        if (isFull)
                        {
                            await bTree.m_stateClient.WaitForNotFullAsync();
                        }
                    }

                    if (weights.Count >= 100)
                    {
                        yield return new StreamEventBatch(new EventBatchWeighted(weights, iterations, GetEmitBatchData()));
                        InitOutputColumns();
                    }
                }

                if (weights.Count > 0)
                {
                    yield return new StreamEventBatch(new EventBatchWeighted(weights, iterations, GetEmitBatchData()));
                    InitOutputColumns();
                }

                m_hasSentInitialData.Value = true;
            }
            else
            {
                int totalProcessed = 0;
                using var iterator = _temporaryTree.CreateIterator();
                await iterator.SeekFirst();
                await foreach (var page in iterator)
                {
                    var currentLeaf = page.CurrentPage;
                    totalProcessed += currentLeaf.keys.Count;

                    if (currentLeaf.keys.Count == 0)
                    {
                        continue;
                    }

                    if (_watermarkRowReferences.Length < currentLeaf.keys.Count)
                    {
                        _watermarkRowReferences = new ColumnRowReference[currentLeaf.keys.Count];
                        _watermarkColumnReferences = new ColumnReference[currentLeaf.keys.Count];
                        _watermarkIndices = new int[currentLeaf.keys.Count];
                        // indices will always just be incremented since data is always sorted from the tree
                        for (int i = 0; i < _watermarkIndices.Length; i++)
                        {
                            _watermarkIndices[i] = i;
                        }
                    }

                    for (int i = 0; i < currentLeaf.keys.Count; i++)
                    {
                        // Map row references, this is simply for the bulk search interface
                        _watermarkRowReferences[i] = new ColumnRowReference()
                        {
                            referenceBatch = currentLeaf.keys._data,
                            RowIndex = i
                        };
                    }

                    Debug.Assert(_pageStartBuffer != null);
                    var pageStart = _pageStartBuffer;
                    for (int m = 0; m < _measures.Length; m++)
                    {
                        var measureColIndex = groupLength + m;
                        pageStart[m] = outputColumns[measureColIndex].Count;
                        await _measures[m].FetchValuesAsync(page.Keys._data.GetColumns_Unsafe(), page.Keys.Count, outputColumns[measureColIndex]);
                    }

                    var sourceColumns = currentLeaf.keys._data.GetColumns_Unsafe();
                    for (int c = 0; c < groupLength; c++)
                    {
                        outputColumns[c].InsertRangeFrom(outputColumns[c].Count, sourceColumns[c], 0, currentLeaf.keys.Count);
                    }

                    weights!.InsertStaticRange(weights.Count, 1, currentLeaf.keys.Count);
                    iterations!.InsertStaticRange(iterations.Count, m_currentIteration, currentLeaf.keys.Count);

                    // Current leaf have data sorted already, so no need to sort, take data and search in persisted tree to get states
                    // TODO: Fix row references and indices
                    await _treeBulkSearch.Start(_watermarkRowReferences, currentLeaf.keys.Count, _watermarkIndices);

                    while (await _treeBulkSearch.MoveNextLeaf())
                    {
                        var persistedLeaf = _treeBulkSearch.CurrentLeaf;
                        var currentResults = _treeBulkSearch.CurrentResults;
                        var previousValueSent = persistedLeaf.values._previousValueSent;

                        var firstIndex = currentResults[0].KeyIndex;
                        var lastIndex = currentResults[currentResults.Count - 1].KeyIndex;

                        for (int m = 0; m < _measures.Length; m++)
                        {
                            var measureColIndex = groupLength + m;
                            for (int c = 0; c < currentResults.Count; c++)
                            {
                                var stateCol = persistedLeaf.values._eventBatch.GetColumn(m);
                                _watermarkColumnReferences[firstIndex + c] = new ColumnReference(stateCol, currentResults[c].LowerBound, persistedLeaf);
                            }

                            await _measures[m].GetValuesAsync(page.Keys._data.GetColumns_Unsafe(), _watermarkColumnReferences, firstIndex, lastIndex - firstIndex + 1, outputColumns[measureColIndex]);
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
                                iterations!.Add(m_currentIteration);
                                for (int c = 0; c < groupLength; c++)
                                {
                                    outputColumns[c].InsertRangeFrom(outputColumns[c].Count, currentLeaf.keys._data.Columns[c], currentResults[i].KeyIndex, 1);
                                }
                                for (int m = 0; m < _measures.Length; m++)
                                {
                                    var previousValueCol = persistedLeaf.values._eventBatch.GetColumn(_measures.Length + m);
                                    outputColumns[groupLength + m].InsertRangeFrom(outputColumns[groupLength + m].Count, previousValueCol, lower, 1);
                                }
                            }
                            previousValueSent[lower] = true;
                        }

                        // Update previous sent value column for all data
                        Debug.Assert(_watermarkValueContainer != null);
                        for (int m = 0; m < _measures.Length; m++)
                        {
                            var previousValueCol = persistedLeaf.values._eventBatch.GetColumn(_measures.Length + m);
                            var measureColIndex = groupLength + m;
                            for (int c = 0; c < currentResults.Count; c++)
                            {
                                outputColumns[measureColIndex].GetValueAt(pageStart[m] + currentResults[c].KeyIndex, _watermarkValueContainer, default);
                                previousValueCol.UpdateAt(currentResults[c].LowerBound, _watermarkValueContainer);
                            }
                        }

                        persistedLeaf.ExitWriteLock();

                        {
                            var bTree = (BPlusTree<ColumnRowReference, ColumnAggregateStateReference, AggregateKeyStorageContainer, ColumnAggregateValueContainer>)_tree!;
                            var isFull = bTree.m_stateClient.AddOrUpdate(persistedLeaf.Id, persistedLeaf);
                            if (isFull)
                            {
                                await bTree.m_stateClient.WaitForNotFullAsync();
                            }
                        }
                    }

                    if (weights.Count >= 100)
                    {
                        yield return new StreamEventBatch(new EventBatchWeighted(weights, iterations, GetEmitBatchData()));
                        InitOutputColumns();
                    }
                }

                if (weights.Count > 0)
                {
                    yield return new StreamEventBatch(new EventBatchWeighted(weights, iterations, GetEmitBatchData()));
                    InitOutputColumns();
                }
                Console.WriteLine($"[WATERMARK INC] processed {totalProcessed} keys from temporary tree");
            }

            await _temporaryTree.Clear();
        }

        private EventBatchData GetEmitBatchData()
        {
            if (_aggregateRelation.EmitSet)
            {
                var emit = _aggregateRelation.Emit;
                var emitColumns = new ColumnStore.Column[emit.Count];
                for (int i = 0; i < emit.Count; i++)
                {
                    emitColumns[i] = outputColumns[emit[i]];
                }
                return new EventBatchData(emitColumns);
            }
            return new EventBatchData(outputColumns);
        }

        public void InitOutputColumns()
        {
            var totalColumns = m_groupLength + _measures.Length;
            outputColumns = new ColumnStore.Column[totalColumns];
            for (int i = 0; i < totalColumns; i++)
            {
                outputColumns[i] = new ColumnStore.Column(MemoryAllocator);
            }
            weights = new PrimitiveList<int>(MemoryAllocator);
            iterations = new PrimitiveList<uint>(MemoryAllocator);
        }

        public override async IAsyncEnumerable<StreamEventBatch> OnRecieve(StreamEventBatch msg, long time)
        {
            if (msg.Data.Count > 0)
            {
                // Take first iteration
                m_currentIteration = msg.Data.Iterations[0];
            }
            Debug.Assert(m_groupValues != null);
            Debug.Assert(m_temporaryStateValues != null);
            Debug.Assert(m_temporaryStateBatch != null);
            Debug.Assert(_temporaryTreeBulkInserter != null);

            // Call each measure to initialize the new batch
            for (int i = 0; i< _measures.Length; i++)
            {
                _measures[i].NewBatch(msg.Data.Weights, msg.Data.EventBatchData);
            }

            foreach (var sharedTree in _sharedTrees.Values)
            {
                sharedTree.NewBatch(msg.Data.Weights, msg.Data.EventBatchData, MemoryAllocator);
            }

            var data = msg.Data;
            var dataCount = data.Count;

            // Pre compute group by columns, so we can do sorting and have better cache locality for the measures computation
            for (int i = 0; i < m_groupValues.Length; i++)
            {
                var directFieldIndex = m_groupDirectFields![i];
                if (directFieldIndex != -1)
                {
                    m_groupValues[i] = data.EventBatchData.Columns[directFieldIndex];
                }
                else
                {
                    ((ColumnStore.Column)m_groupValues[i]).Clear();
                }
            }
            for (int i = 0; i < m_temporaryStateValues.Length; i++)
            {
                m_temporaryStateValues[i].Clear();
                // add null for all temporary state values, so if its a new value it can update the state during compute
                m_temporaryStateValues[i].InsertNullRange(0, msg.Data.Count);
            }

            if (groupExpressions != null)
            {
                // Take column by column to try and reuse cache
                for (int k = 0; k < groupExpressions.Count; k++)
                {
                    var exprInfo = groupExpressions[k];
                    var targetColumn = (ColumnStore.Column)m_groupValues[exprInfo.GroupIndex];
                    for (int i = 0; i < dataCount; i++)
                    {
                        exprInfo.Expression(data.EventBatchData, i, targetColumn);
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
                _groupSortLookup[_groupedSortIndices[i]] = _duplicateTags[i];
            }

            for (int i = 0; i < _measures.Length; i++)
            {
                if (_measures[i] is ISharedTreeColumnAggregation sharedMeasure)
                {
                    sharedMeasure.SetGroupMapping(_groupSortLookup);
                }
            }

            // Create lookup arrays for measures with filters, so we can iterate only on the relevant rows for each measure in the aggregation phase
            for (int i = 0; i < _measures.Length; i++)
            {
                var measureFilter = _measureFilters[i];
                if (measureFilter != null)
                {
                    if (_measureLookups[i] == null || _measureLookups[i].Length < dataCount)
                    {
                        // Create lookup array for each measure
                        _measureLookups[i] = new int[dataCount];
                    }
                    var lookupArray = _measureLookups[i];
                    int rowCounter = 0;
                    for (int j = 0; j < dataCount; j++)
                    {
                        var physicalIndex = _groupedSortIndices[j];
                        if (measureFilter(data.EventBatchData, physicalIndex))
                        {
                            lookupArray[rowCounter++] = physicalIndex;
                        }
                    }
                    await _measures[i].StoreAsync(data.Weights, m_groupValues, data.EventBatchData, lookupArray.AsSpan(0, rowCounter));
                }
                else
                {
                    await _measures[i].StoreAsync(data.Weights, m_groupValues, data.EventBatchData, _groupedSortIndices.AsSpan(0, dataCount));
                }
            }

            foreach (var sharedTree in _sharedTrees.Values)
            {
                await sharedTree.StoreAsync(data.Weights, m_groupValues, _groupedSortIndices.AsSpan(0, dataCount), data.EventBatchData);
            }

            if (_rowReferenceBuffer.Length < dataCount)
            {
                _rowReferenceBuffer = new ColumnRowReference[dataCount];
                _rowValuesBuffer = new ColumnAggregateStateReference[dataCount];
            }

            _computeRanges.Clear();

            int uniqueCounter = 0;
            if (dataCount > 0)
            {
                if (m_groupValues.Length == 0)
                {
                    long weightCounter = 0;
                    for (int i = 0; i < dataCount; i++)
                    {
                        weightCounter += data.Weights[i];
                    }

                    _rowReferenceBuffer[0] = new ColumnRowReference()
                    {
                        referenceBatch = groupValuesBatch,
                        RowIndex = 0
                    };
                    _rowValuesBuffer[0] = new ColumnAggregateStateReference()
                    {
                        referenceBatch = m_temporaryStateBatch,
                        RowIndex = 0,
                        valueSent = false,
                        weight = (int)weightCounter
                    };
                    _noDuplicateIndices[0] = 0;
                    _duplicateTags[0] = 0;
                    _computeRanges.Add(new AggregateComputeRange()
                    {
                        start = 0,
                        length = dataCount
                    });
                    uniqueCounter = 1;
                }
                else
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

                            _computeRanges.Add(new AggregateComputeRange()
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
                    _computeRanges.Add(new AggregateComputeRange()
                    {
                        start = uniqueIndex,
                        length = dataCount - uniqueIndex
                    });

                    uniqueCounter++;
                }
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
                _computeRanges, 
                outputColumns, 
                weights, 
                _outputToTemp,
                _isDeleted);

            // Apply the batch, mutator handles calling compute on measures for a group of values
            await _treeBulkInserter.ApplyBatch(_rowReferenceBuffer, _rowValuesBuffer, uniqueCounter, _noDuplicateIndices, _duplicateTags, mutator, totalBatchSize);

            // Temporary should be moved
            if (_tempIndices.Length < uniqueCounter)
            {
                _tempIndices = new int[uniqueCounter];
                _tempDuplicateTags = new int[uniqueCounter];
            }
            if (_tempValues.Length < dataCount)
            {
                _tempValues = new int[dataCount];
            }

            int count = 0;
            for (int i = 0; i < uniqueCounter; i++)
            {
                if (_isDeleted[i])
                {
                    var lookupIndex = _noDuplicateIndices[i];
                    _tempIndices[count] = lookupIndex;
                    _tempValues[lookupIndex] = -1;
                    _tempDuplicateTags[count] = count;
                    count++;
                }
                else if (_outputToTemp[i])
                {
                    var lookupIndex = _noDuplicateIndices[i];
                    _tempIndices[count] = lookupIndex;
                    _tempValues[lookupIndex] = 1;
                    _tempDuplicateTags[count] = count;
                    count++;
                }
            }

            // We now only insert 
            if (m_hasSentInitialData!.Value)
            {
                var tempMutator = new BulkTemporaryMutator();
                await _temporaryTreeBulkInserter.ApplyBatch(_rowReferenceBuffer, _tempValues, count, _tempIndices, _tempDuplicateTags, tempMutator, totalBatchSize);
            }
            
            // If there is any output here directly, we output it
            // This is deletes from persisted tree
            // Watermark only handles existing values
            if (weights.Count > 0)
            {
                Debug.Assert(iterations != null);
                iterations.InsertStaticRange(iterations.Count, m_currentIteration, weights.Count);
                yield return new StreamEventBatch(new EventBatchWeighted(weights, iterations, GetEmitBatchData()));
                InitOutputColumns();
            }
        }

        protected override async Task InitializeOrRestore(IStateManagerClient stateManagerClient)
        {
            _sharedTrees.Clear();
            InitOutputColumns();

            m_hasSentInitialData = await stateManagerClient.GetOrCreateObjectStateAsync<bool>("initialDataSent");
            if (_aggregateRelation.Groupings != null && _aggregateRelation.Groupings.Count > 0)
            {
                if (_aggregateRelation.Groupings.Count > 1)
                {
                    throw new InvalidOperationException("Aggregate operator only supports one grouping set at this point");
                }

                var grouping = _aggregateRelation.Groupings[0];

                m_groupDirectFields = new int[grouping.GroupingExpressions.Count];
                m_groupValues = new IColumn[grouping.GroupingExpressions.Count];

                if (groupExpressions == null)
                {
                    groupExpressions = new List<GroupExpressionInfo>();
                    for (int i = 0; i < grouping.GroupingExpressions.Count; i++)
                    {
                        var expr = grouping.GroupingExpressions[i];
                        if (expr is DirectFieldReference directFieldReference &&
                            directFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment &&
                            structReferenceSegment.Child == null)
                        {
                            m_groupDirectFields[i] = structReferenceSegment.Field;
                            m_groupValues[i] = null!;
                        }
                        else
                        {
                            m_groupDirectFields[i] = -1;
                            m_groupValues[i] = ColumnFactory.Get(MemoryAllocator);
                            groupExpressions.Add(new GroupExpressionInfo
                            {
                                GroupIndex = i,
                                Expression = ColumnProjectCompiler.Compile(expr, _functionsRegister)
                            });
                        }
                    }

                    if (groupExpressions.Count == 0)
                    {
                        groupExpressions = null;
                    }
                }
            }
            else
            {
                m_groupValues = Array.Empty<IColumn>();
                m_groupDirectFields = Array.Empty<int>();
            }

            m_temporaryStateValues = new ColumnStore.Column[(_aggregateRelation.Measures?.Count ?? 0) * 2];

            for (int i = 0; i < m_temporaryStateValues.Length; i++)
            {
                m_temporaryStateValues[i] = ColumnFactory.Get(MemoryAllocator);
            }
            m_temporaryStateBatch = new EventBatchData(m_temporaryStateValues);
            _pageStartBuffer = new int[_measures.Length];
            _watermarkValueContainer = new DataValueContainer();

            // Group and bind all ISharedTreeColumnAggregation measures by unique value expression and filter
            for (int i = 0; i < _measures.Length; i++)
            {
                if (_measures[i] is ISharedTreeColumnAggregation sharedMeasure && sharedMeasure.SupportsSharedTree)
                {
                    var filter = _aggregateRelation.Measures[i].Filter;
                    var keyString = sharedMeasure.ValueExpression.ToString() + (filter != null ? "_" + filter.ToString() : "") + "_" + sharedMeasure.IgnoreNulls.ToString();
                    if (!_sharedTrees.TryGetValue(keyString, out var sharedTree))
                    {
                        sharedTree = new SharedGroupValueTree($"sharedtree_{i}", sharedMeasure.ValueProjection, _measureFilters[i], sharedMeasure.IgnoreNulls);
                        var bTree = await stateManagerClient.GetOrCreateTree(sharedTree.TreeName,
                            new FlowtideDotNet.Storage.Tree.BPlusTreeOptions<BulkGroupValueRowReference, int, BulkGroupValueKeyContainer, PrimitiveListValueContainer<int>>()
                            {
                                Comparer = new BulkMinInsertComparer(m_groupValues.Length),
                                KeySerializer = new BulkGroupValueKeyStorageSerializer(m_groupValues.Length, MemoryAllocator),
                                ValueSerializer = new PrimitiveListValueContainerSerializer<int>(MemoryAllocator),
                                UseByteBasedPageSizes = true,
                                MemoryAllocator = MemoryAllocator,
                                UsePreviousPointers = true
                            });
                        sharedTree.Tree = bTree;
                        sharedTree.BulkInserter = bTree.CreateBulkInserter();
                        _sharedTrees[keyString] = sharedTree;
                    }
                    sharedTree.BindMeasure(sharedMeasure);
                    sharedMeasure.BindSharedTree(sharedTree.Tree, m_groupValues.Length);
                }
            }

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
