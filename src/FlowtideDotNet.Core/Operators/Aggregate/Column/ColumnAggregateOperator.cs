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

using FlexBuffers;
using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Vertices.Unary;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Memory;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Operators.Set;
using FlowtideDotNet.Core.Storage;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using static SqlParser.Ast.TableConstraint;
using static Substrait.Protobuf.AggregateRel.Types;

namespace FlowtideDotNet.Core.Operators.Aggregate.Column
{
    internal class ColumnAggregateOperator : UnaryVertex<StreamEventBatch, AggregateOperatorState>
    {
        private readonly AggregateRelation m_aggregateRelation;
        private readonly FunctionsRegister m_functionsRegister;

        /// <summary>
        /// Temporary until column based aggregates are implemented
        /// </summary>
        private List<IColumnAggregateContainer> m_measures;

        private List<Action<EventBatchData, int, ColumnStore.Column>>? groupExpressions;
        private ColumnStore.Column[] m_groupValues;
        private EventBatchData m_groupValuesBatch;

        private ColumnStore.Column[] m_temporaryStateValues;
        private EventBatchData m_temporaryStateBatch;

        /// <summary>
        /// Helper column that only contains a null value.
        /// This is used to help avoid creating a new column for each null value.
        /// </summary>
        //private ColumnStore.Column nullStateColumn;

        private IBPlusTree<ColumnRowReference, ColumnAggregateStateReference, AggregateKeyStorageContainer, ColumnAggregateValueContainer>? _tree;
        private IBPlusTreeIterator<ColumnRowReference, ColumnAggregateStateReference, AggregateKeyStorageContainer, ColumnAggregateValueContainer>? _treeIterator;
        private IBPlusTree<ColumnRowReference, int, AggregateKeyStorageContainer, ListValueContainer<int>>? _temporaryTree;

        public ColumnAggregateOperator(AggregateRelation aggregateRelation, FunctionsRegister functionsRegister, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            m_measures = new List<IColumnAggregateContainer>();
            m_aggregateRelation = aggregateRelation;
            m_functionsRegister = functionsRegister;

            if (aggregateRelation.Groupings != null && aggregateRelation.Groupings.Count > 0)
            {
                if (aggregateRelation.Groupings.Count > 1)
                {
                    throw new InvalidOperationException("Aggregate operator only supports one grouping set at this point");
                }

                var grouping = aggregateRelation.Groupings[0];

                m_groupValues = new ColumnStore.Column[grouping.GroupingExpressions.Count];

                for (int i = 0; i < grouping.GroupingExpressions.Count; i++)
                {
                    m_groupValues[i] = ColumnFactory.Get(GlobalMemoryManager.Instance);
                }

                groupExpressions = new List<Action<EventBatchData, int, ColumnStore.Column>>();
                foreach (var expr in grouping.GroupingExpressions)
                {
                    groupExpressions.Add(ColumnProjectCompiler.Compile(expr, functionsRegister));
                }
            }
            else
            {
                m_groupValues = new ColumnStore.Column[0];
            }
            m_groupValuesBatch = new EventBatchData(m_groupValues);

            m_temporaryStateValues = new ColumnStore.Column[(aggregateRelation.Measures?.Count ?? 0) * 2];

            for (int i = 0; i < m_temporaryStateValues.Length; i++)
            {
                m_temporaryStateValues[i] = ColumnFactory.Get(GlobalMemoryManager.Instance);
            }

            m_temporaryStateBatch = new EventBatchData(m_temporaryStateValues);
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

        public override Task<AggregateOperatorState> OnCheckpoint()
        {
            return Task.FromResult(new AggregateOperatorState());
        }

        protected override async IAsyncEnumerable<StreamEventBatch> OnWatermark(Watermark watermark)
        {
            Debug.Assert(_tree != null, "Tree should not be null");
            Debug.Assert(_temporaryTree != null, "Temporary tree should not be null");

            PrimitiveList<int> outputWeights = new PrimitiveList<int>(GlobalMemoryManager.Instance);
            PrimitiveList<uint> outputIterations = new PrimitiveList<uint>(GlobalMemoryManager.Instance);

            var outputColumnCount = (groupExpressions?.Count ?? 0) + m_measures.Count;
            ColumnStore.Column[] outputColumns = new ColumnStore.Column[outputColumnCount];

            for (int i = 0; i < outputColumnCount; i++)
            {
                outputColumns[i] = ColumnFactory.Get(GlobalMemoryManager.Instance);
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
                        await m_measures[i].GetValue(emptyRow, new ColumnReference(val.referenceBatch.Columns[i], val.RowIndex), outputColumns[i]);
                        var previousValueColumn = val.referenceBatch.Columns[m_measures.Count + i];
                        var previousValue = previousValueColumn.GetValueAt(val.RowIndex, default);
                        var newValue = outputColumns[i].GetValueAt(outputColumns[i].Count - 1, default);
                        if (val.valueSent)
                        {
                            deleteAdded = true;
                            outputColumns[i].Add(previousValue);
                        }
                        previousValueColumn.UpdateAt(val.RowIndex, newValue);
                    }
                    if (!val.valueSent)
                    {
                        page.Values._previousValueSent.Update(comparer.start, true);
                    }
                    outputIterations.Add(0);
                    outputWeights.Add(1);
                    if (deleteAdded)
                    {
                        outputIterations.Add(0);
                        outputWeights.Add(-1);
                    }

                    // Save all the changes to the page
                    await page.SavePage();
                }
                else
                {
                    var notFoundRowIndex = m_temporaryStateBatch.Count;
                    // Not found happens when there was no row that matched
                    for (int i = 0; i < m_measures.Count; i++)
                    {
                        var stateColumn = m_temporaryStateValues[i];
                        stateColumn.Add(NullValue.Instance);
                        await m_measures[i].GetValue(emptyRow, new ColumnReference(stateColumn, notFoundRowIndex), outputColumns[i]);
                        
                        // Fetch the value added to the output
                        var newValue = outputColumns[i].GetValueAt(outputColumns[i].Count - 1, default);
                        // Add it to the state for previous value, so the value can be removed if changed
                        m_temporaryStateValues[m_measures.Count + i].Add(newValue);
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

                yield return new StreamEventBatch(new EventBatchWeighted(outputWeights, outputIterations, new EventBatchData(outputColumns)));
            }
            else
            {
                // This iterator can probably be created in init instead to save on memory allocations
                var iterator = _temporaryTree.CreateIterator();
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
                                    outputColumns[i].Add(kv.Key.referenceBatch.Columns[i].GetValueAt(kv.Key.RowIndex, default));
                                }
                                // Add the previous value sent to output
                                for (int i = 0; i < m_measures.Count; i++)
                                {
                                    var previousValueColumn = val.referenceBatch.Columns[m_measures.Count + i];
                                    var previousValue = previousValueColumn.GetValueAt(val.RowIndex, default);
                                    outputColumns[groupExpressions.Count + i].Add(previousValue);
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
                            var colVal = kv.Key.referenceBatch.Columns[i].GetValueAt(kv.Key.RowIndex, default);
                            outputColumns[i].Add(colVal);
                            if (val.valueSent)
                            {
                                // Add the key value a second time if value has already been sent to add it for deletes.
                                outputColumns[i].Add(colVal);
                            }
                        }

                        // Add measure values
                        for (int i = 0; i < m_measures.Count; i++)
                        {
                            await m_measures[i].GetValue(kv.Key, new ColumnReference(val.referenceBatch.Columns[i], val.RowIndex), outputColumns[groupExpressions.Count + i]);
                            var previousValueColumn = val.referenceBatch.Columns[m_measures.Count + i];
                            var previousValue = previousValueColumn.GetValueAt(val.RowIndex, default);
                            var newValue = outputColumns[groupExpressions.Count + i].GetValueAt(outputColumns[groupExpressions.Count + i].Count - 1, default);
                            if (val.valueSent)
                            {
                                outputColumns[groupExpressions.Count + i].Add(previousValue);
                            }
                            previousValueColumn.UpdateAt(val.RowIndex, newValue);
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
                            treePage.Values._previousValueSent.Update(comparer.start, true);
                        }

                        await treePage.SavePage();
                    }
                }

                yield return new StreamEventBatch(new EventBatchWeighted(outputWeights, outputIterations, new EventBatchData(outputColumns)));

                await _temporaryTree.Clear();
            }
            yield break;
        }

        public override async IAsyncEnumerable<StreamEventBatch> OnRecieve(StreamEventBatch msg, long time)
        {
            Debug.Assert(_tree != null, "Tree should not be null");
            Debug.Assert(_temporaryTree != null, "Temporary tree should not be null");
            Debug.Assert(_treeIterator != null);

            for(int i = 0; i < m_groupValues.Length; i++)
            {
                m_groupValues[i].Clear();
            }
            for (int i = 0; i < m_temporaryStateValues.Length; i++)
            {
                m_temporaryStateValues[i].Clear();
            }

            var data = msg.Data;
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
                                }, data.EventBatchData, i, new ColumnReference(stateColumn, index), msg.Data.Weights.Get(i));
                            }
                        }

                        var currentWeight = page.Values._weights.Get(index);
                        page.Values._weights.Update(index, currentWeight + msg.Data.Weights.Get(i));

                        await page.SavePage();
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
                            }, data.EventBatchData, i, new ColumnReference(col, temporaryStateIndex), msg.Data.Weights.Get(i));
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

        protected override async Task InitializeOrRestore(AggregateOperatorState? state, IStateManagerClient stateManagerClient)
        {
            if (m_aggregateRelation.Measures != null && m_aggregateRelation.Measures.Count > 0)
            {
                m_measures.Clear();
                for (int i = 0; i < m_aggregateRelation.Measures.Count; i++)
                {
                    var measure = m_aggregateRelation.Measures[i];
                    var aggregateContainer = await ColumnMeasureCompiler.CompileMeasure(groupExpressions?.Count ?? 0, stateManagerClient.GetChildManager(i.ToString()), measure.Measure, m_functionsRegister);
                    m_measures.Add(aggregateContainer);
                }
            }

            _tree = await stateManagerClient.GetOrCreateTree("grouping_set_1_v1",
                new FlowtideDotNet.Storage.Tree.BPlusTreeOptions<ColumnRowReference, ColumnAggregateStateReference, AggregateKeyStorageContainer, ColumnAggregateValueContainer>()
                {
                    KeySerializer = new AggregateKeySerializer(m_groupValues.Length),
                    ValueSerializer = new ColumnAggregateValueSerializer(m_measures.Count),
                    Comparer = new AggregateInsertComparer(m_groupValues.Length)
                });
            _treeIterator = _tree.CreateIterator();
            _temporaryTree = await stateManagerClient.GetOrCreateTree("grouping_set_1_v1_temp",
                new FlowtideDotNet.Storage.Tree.BPlusTreeOptions<ColumnRowReference, int, AggregateKeyStorageContainer, ListValueContainer<int>>()
                {
                    KeySerializer = new AggregateKeySerializer(m_groupValues.Length),
                    ValueSerializer = new ValueListSerializer<int>(new IntSerializer()),
                    Comparer = new AggregateInsertComparer(m_groupValues.Length)
                });
            await _temporaryTree.Clear();
        }
    }
}
