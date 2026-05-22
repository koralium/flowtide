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

using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.Sort;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Core.Compute.Columnar.Functions.BulkAggregations;
using FlowtideDotNet.Core.Operators.Aggregate.Column;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
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

        private int[] _groupedSortIndices = Array.Empty<int>();
        private int[] _groupSortLookup = Array.Empty<int>();
        private int[][] _measureLookups;
        private ColumnRowReference[] _rowReferenceBuffer;
        private ColumnAggregateStateReference[] _rowValuesBuffer;

        private IBPlusTree<ColumnRowReference, ColumnAggregateStateReference, AggregateKeyStorageContainer, ColumnAggregateValueContainer>? _tree;
        private IBPlusTreeBulkInserter<ColumnRowReference, ColumnAggregateStateReference, AggregateKeyStorageContainer, ColumnAggregateValueContainer>? _treeBulkInserter;

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
            return Task.CompletedTask;
        }

        public override async IAsyncEnumerable<StreamEventBatch> OnRecieve(StreamEventBatch msg, long time)
        {
            Debug.Assert(m_groupValues != null);

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


            // Start of sorting
            if (_groupedSortIndices.Length < dataCount)
            {
                _groupedSortIndices = new int[dataCount];
                _groupSortLookup = new int[dataCount];
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
                        if (measureFilter(data.EventBatchData, j))
                        {
                            lookupArray[rowCounter++] = _groupSortLookup[j];
                        }
                    }
                }
                else
                {
                    for (int j = 0; j < dataCount; j++)
                    {
                        lookupArray[rowCounter++] = _groupSortLookup[j];
                    }
                }
                
                await _measures[i].StoreAsync(data.Weights, m_groupValues, data.EventBatchData, lookupArray.AsSpan(0, rowCounter));
            }

            if (_rowReferenceBuffer.Length < dataCount)
            {
                _rowReferenceBuffer = new ColumnRowReference[dataCount];
                _rowValuesBuffer = new ColumnAggregateStateReference[dataCount];
            }
            for (int i = 0; i < dataCount; i++)
            {
                _rowReferenceBuffer[i] = new ColumnRowReference()
                {
                    referenceBatch = data.EventBatchData,
                    RowIndex = i
                };
                _rowValuesBuffer[i] = new ColumnAggregateStateReference()
                {
                    referenceBatch = data.EventBatchData,
                    RowIndex = i,
                    valueSent = false,
                    weight = data.Weights[i]
                };
            }

            int totalBatchSize = 0;
            for (int i = 0; i < m_groupValues.Length; i++)
            {
                totalBatchSize += m_groupValues[i].GetByteSize();
            }

            Debug.Assert(_treeBulkInserter != null);
            
            var mutator = new BulkAggregateMutator(_measures);
            // Before this step, pick out a single group value from each group, so we only run update for a single representative of each group, and we can do it in batch with the bulk inserter
            // This helps reduce the amount of calculations where a compute 
            await _treeBulkInserter.ApplyBatch(_rowReferenceBuffer, _rowValuesBuffer, dataCount, mutator, totalBatchSize);

            yield break;
        }

        protected override async Task InitializeOrRestore(IStateManagerClient stateManagerClient)
        {
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

            _treeBulkInserter = _tree.CreateBulkInserter();
        }

        private void SortData(int dataCount)
        {
            Debug.Assert(m_groupValues != null);
            var groupSortIndicesSpan = _groupedSortIndices.AsSpan(0, dataCount);
            _batchSorter.SortData(m_groupValues, ref groupSortIndicesSpan);
        }
    }
}
