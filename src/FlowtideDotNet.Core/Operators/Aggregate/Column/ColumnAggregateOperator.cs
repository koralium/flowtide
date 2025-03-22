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
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Base.Vertices.Unary;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Aggregate.Column
{
    internal class ColumnAggregateOperator : UnaryVertex<StreamEventBatch>
    {
        private readonly AggregateRelation m_aggregateRelation;
        private readonly FunctionsRegister m_functionsRegister;

        private ICounter<long>? _eventsCounter;
        private ICounter<long>? _eventsProcessed;

        /// <summary>
        /// Temporary until column based aggregates are implemented
        /// </summary>
        private List<IColumnAggregateContainer> m_measures;

        private List<Action<EventBatchData, int, ColumnStore.Column>>? groupExpressions;
        private ColumnStore.Column[]? m_groupValues;
        private EventBatchData? m_groupValuesBatch;

        private ColumnStore.Column[]? m_temporaryStateValues;
        private EventBatchData? m_temporaryStateBatch;

        private readonly int m_outputCount;
        private readonly List<int> m_groupOutputIndices;
        private readonly List<int> m_measureOutputIndices;

        private IBPlusTree<ColumnRowReference, ColumnAggregateStateReference, AggregateKeyStorageContainer, ColumnAggregateValueContainer>? _tree;
        private IBPlusTreeIterator<ColumnRowReference, ColumnAggregateStateReference, AggregateKeyStorageContainer, ColumnAggregateValueContainer>? _treeIterator;
        private IBPlusTree<ColumnRowReference, int, AggregateKeyStorageContainer, PrimitiveListValueContainer<int>>? _temporaryTree;

#if DEBUG_WRITE
        private StreamWriter? allInput;
        private StreamWriter? outputWriter;
#endif

        public ColumnAggregateOperator(AggregateRelation aggregateRelation, FunctionsRegister functionsRegister, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            m_measures = new List<IColumnAggregateContainer>();
            m_aggregateRelation = aggregateRelation;
            m_functionsRegister = functionsRegister;
            m_outputCount = aggregateRelation.OutputLength;

            int groupLength = 0;
            if (m_aggregateRelation.Groupings != null && m_aggregateRelation.Groupings.Count > 0)
            {
                if (m_aggregateRelation.Groupings.Count > 1)
                {
                    throw new InvalidOperationException("Aggregate operator only supports one grouping set at this point");
                }

                var grouping = m_aggregateRelation.Groupings[0];
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

            m_measureOutputIndices = new List<int>();
            if (aggregateRelation.Measures != null)
            {
                for (int i = 0; i < aggregateRelation.Measures.Count; i++)
                {
                    if (aggregateRelation.EmitSet)
                    {
                        var emitIndex = aggregateRelation.Emit.IndexOf(i + groupLength);
                        m_measureOutputIndices.Add(emitIndex);
                    }
                    else
                    {
                        m_measureOutputIndices.Add(i + groupLength);
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
            await _tree.Commit();

#if DEBUG_WRITE
            allInput!.WriteLine("Checkpoint");
            allInput!.Flush();
#endif

            // Commit each measure
            foreach (var measure in m_measures)
            {
                await measure.Commit();
            }

            // Reset all cache columns
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
            // Reset iterator
            _treeIterator!.Dispose();
            _treeIterator = _tree.CreateIterator();
        }

        protected override async IAsyncEnumerable<StreamEventBatch> OnWatermark(Watermark watermark)
        {
            Debug.Assert(_tree != null, "Tree should not be null");
            Debug.Assert(_temporaryTree != null, "Temporary tree should not be null");
            Debug.Assert(m_groupValuesBatch != null);
            Debug.Assert(_eventsCounter != null);
            Debug.Assert(m_temporaryStateValues != null);
            Debug.Assert(m_temporaryStateBatch != null);
            Debug.Assert(m_groupValues != null);

#if DEBUG_WRITE
            allInput!.WriteLine("Watermark");
            allInput!.Flush();
            outputWriter!.WriteLine("Watermark");
#endif

            PrimitiveList<int> outputWeights = new PrimitiveList<int>(MemoryAllocator);
            PrimitiveList<uint> outputIterations = new PrimitiveList<uint>(MemoryAllocator);

            var outputColumnCount = m_outputCount;
            ColumnStore.Column[] outputColumns = new ColumnStore.Column[outputColumnCount];

            for (int i = 0; i < outputColumnCount; i++)
            {
                outputColumns[i] = ColumnFactory.Get(MemoryAllocator);
            }

            for (int i = 0; i < m_temporaryStateValues.Length; i++)
            {
                m_temporaryStateValues[i].Clear();
            }

            if (groupExpressions == null || groupExpressions.Count == 0)
            {
                // No groups, create an empty row with no columns to find info in the tree.
                var emptyRow = new ColumnRowReference() { referenceBatch = m_groupValuesBatch, RowIndex = 0 };
                var comparer = new AggregateSearchComparer(0);
                await _treeIterator!.Seek(emptyRow, comparer);

                if (!comparer.noMatch)
                {
                    var enumerator = _treeIterator.GetAsyncEnumerator();
                    await enumerator.MoveNextAsync();
                    var page = enumerator.Current;
                    bool deleteAdded = false;

                    var val = page.Values.Get(comparer.start);

                    for (int i = 0; i < m_measures.Count; i++)
                    {
                        var measureEmitIndex = m_measureOutputIndices[i];
                        if (measureEmitIndex >= 0)
                        {
                            await m_measures[i].GetValue(emptyRow, new ColumnReference(val.referenceBatch.Columns[i], val.RowIndex, page), outputColumns[measureEmitIndex]);
                            var previousValueColumn = val.referenceBatch.Columns[m_measures.Count + i];
                            var previousValue = previousValueColumn.GetValueAt(val.RowIndex, default);
                            var newValueIndex = outputColumns[measureEmitIndex].Count - 1;

                            if (val.valueSent)
                            {
                                deleteAdded = true;
                                outputColumns[measureEmitIndex].Add(previousValue);
                            }
                            // Must fetch this after the previous value has been added to the output
                            // Since it could force a resize of the column and invalidate the memory.
                            var newValue = outputColumns[measureEmitIndex].GetValueAt(newValueIndex, default);
                            previousValueColumn.UpdateAt(val.RowIndex, newValue);
                        }
                    }
                    if (!val.valueSent)
                    {
                        page.EnterWriteLock();
                        page.Values._previousValueSent.Update(comparer.start, true);
                        page.ExitWriteLock();
                    }

                    outputIterations.Add(0);
                    outputWeights.Add(1);
                    if (deleteAdded)
                    {
                        outputIterations.Add(0);
                        outputWeights.Add(-1);
                    }

                    // Save all the changes to the page
                    await page.SavePage(true);
                }
                else
                {
                    var notFoundRowIndex = m_temporaryStateBatch.Count;
                    // Not found happens when there was no row that matched
                    for (int i = 0; i < m_measures.Count; i++)
                    {
                        var measureEmitIndex = m_measureOutputIndices[i];

                        if (measureEmitIndex >= 0)
                        {
                            var stateColumn = m_temporaryStateValues[i];
                            stateColumn.Add(NullValue.Instance);
                            await m_measures[i].GetValue(emptyRow, new ColumnReference(stateColumn, notFoundRowIndex, default), outputColumns[measureEmitIndex]);

                            // Fetch the value added to the output
                            var newValue = outputColumns[measureEmitIndex].GetValueAt(outputColumns[measureEmitIndex].Count - 1, default);
                            // Add it to the state for previous value, so the value can be removed if changed
                            m_temporaryStateValues[m_measures.Count + i].Add(newValue);
                        }

                    }
                    outputIterations.Add(0);
                    outputWeights.Add(1);
                    // Add value to the tree so next time the value will be returned 
                    await _tree.Upsert(emptyRow, new ColumnAggregateStateReference()
                    {
                        referenceBatch = m_temporaryStateBatch,
                        RowIndex = notFoundRowIndex,
                        weight = 1
                    });
                }

                _eventsCounter.Add(outputWeights.Count);
                var outputBatch = new StreamEventBatch(new EventBatchWeighted(outputWeights, outputIterations, new EventBatchData(outputColumns)));

#if DEBUG_WRITE
                foreach(var ev in outputBatch.Events)
                {
                    outputWriter!.WriteLine($"{ev.Weight} {ev.ToJson()}");
                }
                outputWriter!.Flush();
#endif

                yield return outputBatch;
            }
            else
            {
                // This iterator can probably be created in init instead to save on memory allocations
                using var iterator = _temporaryTree.CreateIterator();
                await iterator.SeekFirst();

                await foreach (var page in iterator)
                {
                    foreach (var kv in page)
                    {
                        var comparer = new AggregateSearchComparer(m_groupValues.Length);
                        await _treeIterator!.Seek(kv.Key, comparer);

                        if (comparer.noMatch)
                        {
                            continue;
                        }

                        var enumerator = _treeIterator.GetAsyncEnumerator();
                        await enumerator.MoveNextAsync();
                        var treePage = enumerator.Current;

                        var val = treePage.Values.Get(comparer.start);

                        if (val.weight == 0)
                        {
                            if (val.valueSent)
                            {
                                // Copy key values into output
                                for (int i = 0; i < kv.Key.referenceBatch.Columns.Count; i++)
                                {
                                    var emitIndex = m_groupOutputIndices[i];
                                    if (emitIndex >= 0)
                                    {
                                        outputColumns[emitIndex].Add(kv.Key.referenceBatch.Columns[i].GetValueAt(kv.Key.RowIndex, default));
                                    }
                                }
                                // Add the previous value sent to output
                                for (int i = 0; i < m_measures.Count; i++)
                                {
                                    var measureEmitIndex = m_measureOutputIndices[i];
                                    if (measureEmitIndex >= 0)
                                    {
                                        var previousValueColumn = val.referenceBatch.Columns[m_measures.Count + i];
                                        var previousValue = previousValueColumn.GetValueAt(val.RowIndex, default);
                                        outputColumns[measureEmitIndex].Add(previousValue);
                                    }
                                }
                                outputIterations.Add(0);
                                outputWeights.Add(-1);
                            }
                            await _tree.Delete(kv.Key);
                            continue;
                        }

                        // Copy key values into output
                        for (int i = 0; i < kv.Key.referenceBatch.Columns.Count; i++)
                        {
                            var emitIndex = m_groupOutputIndices[i];
                            if (emitIndex >= 0)
                            {
                                var colVal = kv.Key.referenceBatch.Columns[i].GetValueAt(kv.Key.RowIndex, default);
                                outputColumns[emitIndex].Add(colVal);
                                if (val.valueSent)
                                {
                                    // Add the key value a second time if value has already been sent to add it for deletes.
                                    outputColumns[emitIndex].Add(colVal);
                                }
                            }
                        }


                        // Add measure values
                        for (int i = 0; i < m_measures.Count; i++)
                        {
                            var measureEmitIndex = m_measureOutputIndices[i];
                            if (measureEmitIndex >= 0)
                            {
                                await m_measures[i].GetValue(kv.Key, new ColumnReference(val.referenceBatch.Columns[i], val.RowIndex, treePage), outputColumns[measureEmitIndex]);
                                var previousValueColumn = val.referenceBatch.Columns[m_measures.Count + i];
                                var previousValue = previousValueColumn.GetValueAt(val.RowIndex, default);

                                var newValueIndex = outputColumns[measureEmitIndex].Count - 1;

                                if (val.valueSent)
                                {
                                    outputColumns[measureEmitIndex].Add(previousValue);
                                }

                                // Fetch the value added to the output
                                // If the previous value was added to the output column a resize could have happened and the memory invalidated.
                                var newValue = outputColumns[measureEmitIndex].GetValueAt(newValueIndex, default);
                                treePage.EnterWriteLock();
                                previousValueColumn.UpdateAt(val.RowIndex, newValue);
                                treePage.ExitWriteLock();
                            }
                        }


                        outputIterations.Add(0);
                        outputWeights.Add(1);

                        if (val.valueSent)
                        {
                            outputIterations.Add(0);
                            outputWeights.Add(-1);
                        }
                        else
                        {
                            // Mark the value as sent
                            treePage.EnterWriteLock();
                            treePage.Values._previousValueSent.Update(comparer.start, true);
                            treePage.ExitWriteLock();
                        }

                        await treePage.SavePage(true);

                        if (outputWeights.Count >= 100)
                        {
                            _eventsCounter.Add(outputWeights.Count);
                            var batch = new StreamEventBatch(new EventBatchWeighted(outputWeights, outputIterations, new EventBatchData(outputColumns)));
#if DEBUG_WRITE
                            foreach (var ev in batch.Events)
                            {
                                outputWriter!.WriteLine($"{ev.Weight} {ev.ToJson()}");
                            }
                            outputWriter!.Flush();
#endif
                            yield return batch;

                            // Reset all the batch columns
                            outputWeights = new PrimitiveList<int>(MemoryAllocator);
                            outputIterations = new PrimitiveList<uint>(MemoryAllocator);
                            outputColumns = new ColumnStore.Column[outputColumnCount];
                            for (int i = 0; i < outputColumnCount; i++)
                            {
                                outputColumns[i] = ColumnFactory.Get(MemoryAllocator);
                            }
                        }
                    }
                }

                if (outputWeights.Count > 0)
                {
                    _eventsCounter.Add(outputWeights.Count);
                    var outputBatch = new StreamEventBatch(new EventBatchWeighted(outputWeights, outputIterations, new EventBatchData(outputColumns)));

#if DEBUG_WRITE
                foreach (var ev in outputBatch.Events)
                {
                    outputWriter!.WriteLine($"{ev.Weight} {ev.ToJson()}");
                }
                outputWriter!.Flush();
#endif

                    yield return outputBatch;
                }
                else
                {
                    outputWeights.Dispose();
                    outputIterations.Dispose();
                    for (int i = 0; i < outputColumns.Length; i++)
                    {
                        outputColumns[i].Dispose();
                        outputColumns[i] = null!;
                    }
                }


                await _temporaryTree.Clear();
            }
            yield break;
        }

        public override async IAsyncEnumerable<StreamEventBatch> OnRecieve(StreamEventBatch msg, long time)
        {
            Debug.Assert(_tree != null, "Tree should not be null");
            Debug.Assert(_temporaryTree != null, "Temporary tree should not be null");
            Debug.Assert(_treeIterator != null);
            Debug.Assert(_eventsProcessed != null);
            Debug.Assert(m_groupValues != null);
            Debug.Assert(m_temporaryStateValues != null);
            Debug.Assert(m_groupValuesBatch != null);
            Debug.Assert(m_temporaryStateBatch != null);

            for (int i = 0; i < m_groupValues.Length; i++)
            {
                m_groupValues[i].Clear();
            }
            for (int i = 0; i < m_temporaryStateValues.Length; i++)
            {
                m_temporaryStateValues[i].Clear();
            }

#if DEBUG_WRITE
            foreach (var ev in msg.Events)
            {
                allInput!.WriteLine($"{ev.Weight} {ev.ToJson()}");
            }
            allInput!.Flush();
#endif


            var data = msg.Data;

            _eventsProcessed.Add(data.Count);

            for (int i = 0; i < data.Count; i++)
            {
                var groupIndex = i;
                if (groupExpressions != null)
                {
                    Debug.Assert(m_groupValues != null);
                    for (int k = 0; k < groupExpressions.Count; k++)
                    {
                        groupExpressions[k](data.EventBatchData, i, m_groupValues[k]);
                    }

                    await _temporaryTree.RMWNoResult(new ColumnRowReference()
                    {
                        referenceBatch = m_groupValuesBatch,
                        RowIndex = groupIndex
                    }, default, (_, current, exist) =>
                    {
                        if (exist)
                        {
                            return (1, GenericWriteOperation.None);
                        }
                        else
                        {
                            return (1, GenericWriteOperation.Upsert);
                        }
                    });
                }

                var comparer = new AggregateSearchComparer(m_groupValues.Length);
                await _treeIterator.Seek(new ColumnRowReference()
                {
                    referenceBatch = m_groupValuesBatch,
                    RowIndex = groupIndex
                }, comparer);


                if (!comparer.noMatch)
                {
                    var enumerator = _treeIterator.GetAsyncEnumerator();
                    if (await enumerator.MoveNextAsync())
                    {
                        var page = enumerator.Current;
                        var index = comparer.start;

                        var state = page.Values.Get(index);

                        if (m_measures.Count > 0)
                        {
                            for (int k = 0; k < m_measures.Count; k++)
                            {
                                var stateColumn = page.Values._eventBatch.Columns[k]; //.Get(index);
                                await m_measures[k].Compute(new ColumnRowReference()
                                {
                                    referenceBatch = m_groupValuesBatch,
                                    RowIndex = groupIndex
                                }, data.EventBatchData, i, new ColumnReference(stateColumn, index, page), msg.Data.Weights.Get(i));
                            }
                        }

                        page.EnterWriteLock();
                        var currentWeight = page.Values._weights.Get(index);
                        page.Values._weights.Update(index, currentWeight + msg.Data.Weights.Get(i));
                        page.ExitWriteLock();
                        await page.SavePage(true);
                    }
                }
                else
                {

                    // No match, a new row must be added to the tree.
                    int temporaryStateIndex = 0;
                    if (m_measures.Count > 0)
                    {
                        temporaryStateIndex = m_temporaryStateValues[0].Count;
                        for (int k = 0; k < m_measures.Count; k++)
                        {
                            var col = m_temporaryStateValues[k];
                            // Add a null value for state.
                            col.Add(NullValue.Instance);
                            await m_measures[k].Compute(new ColumnRowReference()
                            {
                                referenceBatch = m_groupValuesBatch,
                                RowIndex = groupIndex
                            }, data.EventBatchData, i, new ColumnReference(col, temporaryStateIndex, default), msg.Data.Weights.Get(i));
                        }
                        for (int k = 0; k < m_measures.Count; k++)
                        {
                            // Add null values for the previous value
                            var col = m_temporaryStateValues[m_measures.Count + k];
                            col.Add(NullValue.Instance);
                        }
                    }


                    await _tree.Upsert(new ColumnRowReference()
                    {
                        referenceBatch = m_groupValuesBatch,
                        RowIndex = groupIndex
                    }, new ColumnAggregateStateReference()
                    {
                        referenceBatch = m_temporaryStateBatch,
                        RowIndex = temporaryStateIndex,
                        weight = msg.Data.Weights.Get(i),
                        valueSent = false
                    });
                }
            }
            yield break;
        }

        protected override async Task InitializeOrRestore(IStateManagerClient stateManagerClient)
        {
#if DEBUG_WRITE
            if (!Directory.Exists("debugwrite"))
            {
                Directory.CreateDirectory("debugwrite");
            }
            if (allInput == null)
            {
                allInput = File.CreateText($"debugwrite/{StreamName}_{Name}.all.txt");
                outputWriter = File.CreateText($"debugwrite/{StreamName}_{Name}.output.txt");
            }
            else
            {
                allInput.WriteLine("Restart");
                allInput.Flush();
            }

#endif
            // Setup metrics
            if (_eventsCounter == null)
            {
                _eventsCounter = Metrics.CreateCounter<long>("events");
            }
            if (_eventsProcessed == null)
            {
                _eventsProcessed = Metrics.CreateCounter<long>("events_processed");
            }

            if (m_aggregateRelation.Groupings != null && m_aggregateRelation.Groupings.Count > 0)
            {
                if (m_aggregateRelation.Groupings.Count > 1)
                {
                    throw new InvalidOperationException("Aggregate operator only supports one grouping set at this point");
                }

                var grouping = m_aggregateRelation.Groupings[0];

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
                        groupExpressions.Add(ColumnProjectCompiler.Compile(expr, m_functionsRegister));
                    }
                }
            }
            else
            {
                m_groupValues = new ColumnStore.Column[0];
            }
            m_groupValuesBatch = new EventBatchData(m_groupValues);

            m_temporaryStateValues = new ColumnStore.Column[(m_aggregateRelation.Measures?.Count ?? 0) * 2];

            for (int i = 0; i < m_temporaryStateValues.Length; i++)
            {
                m_temporaryStateValues[i] = ColumnFactory.Get(MemoryAllocator);
            }

            m_temporaryStateBatch = new EventBatchData(m_temporaryStateValues);

            if (m_aggregateRelation.Measures != null && m_aggregateRelation.Measures.Count > 0)
            {
                m_measures.Clear();
                for (int i = 0; i < m_aggregateRelation.Measures.Count; i++)
                {
                    var measure = m_aggregateRelation.Measures[i];
                    var aggregateContainer = await ColumnMeasureCompiler.CompileMeasure(groupExpressions?.Count ?? 0, stateManagerClient.GetChildManager(i.ToString()), measure.Measure, m_functionsRegister, MemoryAllocator);
                    m_measures.Add(aggregateContainer);
                }
            }

            _tree = await stateManagerClient.GetOrCreateTree("grouping_set_1_v1",
                new FlowtideDotNet.Storage.Tree.BPlusTreeOptions<ColumnRowReference, ColumnAggregateStateReference, AggregateKeyStorageContainer, ColumnAggregateValueContainer>()
                {
                    KeySerializer = new AggregateKeySerializer(m_groupValues.Length, MemoryAllocator),
                    ValueSerializer = new ColumnAggregateValueSerializer(m_measures.Count, MemoryAllocator),
                    Comparer = new AggregateInsertComparer(m_groupValues.Length),
                    UseByteBasedPageSizes = true,
                    MemoryAllocator = MemoryAllocator
                });
            _treeIterator = _tree.CreateIterator();
            _temporaryTree = await stateManagerClient.GetOrCreateTree("grouping_set_1_v1_temp",
                new FlowtideDotNet.Storage.Tree.BPlusTreeOptions<ColumnRowReference, int, AggregateKeyStorageContainer, PrimitiveListValueContainer<int>>()
                {
                    KeySerializer = new AggregateKeySerializer(m_groupValues.Length, MemoryAllocator),
                    ValueSerializer = new PrimitiveListValueContainerSerializer<int>(MemoryAllocator),
                    Comparer = new AggregateInsertComparer(m_groupValues.Length),
                    UseByteBasedPageSizes = true,
                    MemoryAllocator = MemoryAllocator
                });
            await _temporaryTree.Clear();
        }
    }
}
