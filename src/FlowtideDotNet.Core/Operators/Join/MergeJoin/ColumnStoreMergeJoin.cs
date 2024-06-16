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

using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Base.Vertices.MultipleInput;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Operators.Join.NestedLoopJoin;
using FlowtideDotNet.Core.Utils;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Join.MergeJoin
{
    internal class ColumnStoreMergeJoin : MultipleInputVertex<StreamEventBatch, JoinState>
    {
        protected IBPlusTree<ColumnRowReference, JoinStorageValue, ColumnKeyStorageContainer, ListValueContainer<JoinStorageValue>>? _leftTree;
        protected IBPlusTree<ColumnRowReference, JoinStorageValue, ColumnKeyStorageContainer, ListValueContainer<JoinStorageValue>>? _rightTree;

        private readonly MergeJoinRelation _mergeJoinRelation;
        private MergeJoinInsertComparer _leftInsertComparer;
        private MergeJoinInsertComparer _rightInsertComparer;
        private MergeJoinSearchComparer _searchLeftComparer;
        private MergeJoinSearchComparer _searchRightComparer;

        private List<int> _leftOutputColumns;
        private List<int> _rightOutputColumns;

        // Metrics
        private ICounter<long>? _eventsCounter;
        private ICounter<long>? _eventsProcessed;


        // To be deprecated when functions also work with column store
        protected readonly Func<RowEvent, RowEvent, bool> _postCondition;

        public ColumnStoreMergeJoin(MergeJoinRelation mergeJoinRelation, FunctionsRegister functionsRegister, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(2, executionDataflowBlockOptions)
        {
            this._mergeJoinRelation = mergeJoinRelation;
            var leftColumns = GetCompareColumns(mergeJoinRelation.LeftKeys, 0);
            var rightColumns = GetCompareColumns(mergeJoinRelation.RightKeys, mergeJoinRelation.Left.OutputLength);
            _leftInsertComparer = new MergeJoinInsertComparer(leftColumns, mergeJoinRelation.Left.OutputLength);
            _rightInsertComparer = new MergeJoinInsertComparer(rightColumns, mergeJoinRelation.Right.OutputLength);

            _searchLeftComparer = new MergeJoinSearchComparer(leftColumns, rightColumns);
            _searchRightComparer = new MergeJoinSearchComparer(rightColumns, leftColumns);

            _leftOutputColumns = GetOutputColumns(mergeJoinRelation, 0, mergeJoinRelation.Left.OutputLength);
            _rightOutputColumns = GetOutputColumns(mergeJoinRelation, mergeJoinRelation.Left.OutputLength, mergeJoinRelation.Right.OutputLength);

            if (mergeJoinRelation.PostJoinFilter != null)
            {
                _postCondition = BooleanCompiler.Compile<RowEvent>(mergeJoinRelation.PostJoinFilter, functionsRegister, mergeJoinRelation.Left.OutputLength);
            }
        }

        private static List<int> GetOutputColumns(MergeJoinRelation mergeJoinRelation, int relative, int maxSize)
        {
            List<int> columns = new List<int>();
            if (mergeJoinRelation.EmitSet)
            {
                for (int i = 0; i < mergeJoinRelation.Emit.Count; i++)
                {
                    var index = mergeJoinRelation.Emit[i];
                    if (index >= relative)
                    {
                        index = index - relative;
                        if (index < maxSize)
                        {
                            columns.Add(index);
                        }
                    }
                    
                }
            }
            return columns;
        }

        private static List<int> GetCompareColumns(List<FieldReference> fieldReferences, int relativeIndex)
        {
            List<int> leftKeys = new List<int>();
            for (int i = 0; i < fieldReferences.Count; i++)
            {
                if (fieldReferences[i] is DirectFieldReference directFieldReference &&
                    directFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
                {
                    leftKeys.Add(structReferenceSegment.Field - relativeIndex);
                }
                else
                {
                    throw new NotImplementedException("Merge join can only have keys that use struct reference segments at this time");
                }
            }
            return leftKeys;
        }

        public override string DisplayName => "Merge Join";

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override Task<JoinState?> OnCheckpoint()
        {
            return Task.FromResult(new JoinState());
        }

        private async IAsyncEnumerable<StreamEventBatch> OnRecieveLeft(StreamEventBatch msg, long time)
        {
            var it = _rightTree!.CreateIterator();
            List<Column> rightColumns = new List<Column>();
            List<int> foundOffsets = new List<int>();
            List<int> weights = new List<int>();
            List<uint> iterations = new List<uint>();
            for (int i = 0; i < _rightOutputColumns.Count; i++)
            {
                rightColumns.Add(new Column());
            }
            for (int i = 0; i < msg.Data.Weights.Count; i++)
            {
                var columnReference = new ColumnRowReference()
                {
                    referenceBatch = msg.Data.EventBatchData,
                    RowIndex = i
                };

                await it.Seek(in columnReference, _searchRightComparer);
                int weight = msg.Data.Weights[i];
                int joinWeight = 0;
                if (!_searchRightComparer.noMatch)
                {
                    bool firstPage = true;
                    
                    await foreach(var page in it)
                    {
                        var pageKeyStorage = page.Keys as ColumnKeyStorageContainer;
                        if (!firstPage)
                        {
                            // Locate indices again
                            var index = _searchRightComparer.FindIndex(in columnReference, pageKeyStorage!);
                            if (_searchRightComparer.noMatch)
                            {
                                break;
                            }
                        }
                        firstPage = false;
                        // All in this range matched the key comparisons
                        for (int k = _searchRightComparer.start; k <= _searchRightComparer.end; k++)
                        {
                            if (_postCondition != null)
                            {
                                var rightEvent = RowEventToEventBatchData.RowReferenceToRowEvent(1, 0, new ColumnRowReference() { referenceBatch = pageKeyStorage!._data, RowIndex = k });
                                var leftEvent = RowEventToEventBatchData.RowReferenceToRowEvent(1, 0, columnReference);
                                if (!_postCondition(leftEvent, rightEvent))
                                {
                                    _searchRightComparer.end = int.MinValue;
                                    break;
                                }
                            }
                            int outputWeight = page.Values.Get(k).Weight * msg.Data.Weights[i];
                            joinWeight += outputWeight;
                            for (int z = 0; z < rightColumns.Count; z++)
                            {
                                var val = pageKeyStorage._data.Columns[_rightOutputColumns[z]].GetValueAt(k);
                                rightColumns[z].Add(val);
                            }
                            foundOffsets.Add(i);
                            iterations.Add(msg.Data.Iterations[i]);
                            weights.Add(outputWeight);
                        }
                        if (_searchRightComparer.end < (page.Keys.Count - 1))
                        {
                            break;
                        }
                    }
                }

                // If it is a left join we must output the left side even if there is no match
                if (joinWeight == 0 && _mergeJoinRelation.Type == JoinType.Left)
                {
                    foundOffsets.Add(i);
                    iterations.Add(msg.Data.Iterations[i]);
                    weights.Add(1);
                    for (int z = 0; z < rightColumns.Count; z++)
                    {
                        rightColumns[z].Add(NullValue.Instance);
                    }
                }
                await _leftTree!.RMW(in columnReference, new JoinStorageValue() { Weight = weight, JoinWeight = joinWeight }, (input, current, found) =>
                {
                    if (found)
                    {
                        current!.Weight += input!.Weight;
                        current.JoinWeight += input.JoinWeight;
                        if (current.Weight == 0)
                        {
                            return (default, GenericWriteOperation.Delete);
                        }
                        return (current, GenericWriteOperation.Upsert);
                    }
                    return (input, GenericWriteOperation.Upsert);
                });
            }

            if (foundOffsets.Count > 0)
            {
                List<IColumn> outputColumns = new List<IColumn>();
                for (int i = 0; i < _leftOutputColumns.Count; i++)
                {
                    outputColumns.Add(new ColumnWithOffset(msg.Data.EventBatchData.Columns[_leftOutputColumns[i]], foundOffsets, true));
                }
                for (int i = 0; i < rightColumns.Count; i++)
                {
                    outputColumns.Add(rightColumns[i]);
                }
                yield return new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(outputColumns)));
            }
        }

        private async IAsyncEnumerable<StreamEventBatch> OnRecieveRight(StreamEventBatch msg, long time)
        {
            var it = _leftTree!.CreateIterator();
            List<Column> leftColumns = new List<Column>();
            List<int> foundOffsets = new List<int>();
            List<int> weights = new List<int>();
            List<uint> iterations = new List<uint>();
            for (int i = 0; i < _leftOutputColumns.Count; i++)
            {
                leftColumns.Add(new Column());
            }
            for (int i = 0; i < msg.Data.Weights.Count; i++)
            {
                var columnReference = new ColumnRowReference()
                {
                    referenceBatch = msg.Data.EventBatchData,
                    RowIndex = i
                };

                await it.Seek(in columnReference, _searchLeftComparer);

                int weight = msg.Data.Weights[i];
                int joinWeight = 0;
                if (!_searchLeftComparer.noMatch)
                {
                
                    bool firstPage = true;
                    
                    await foreach (var page in it)
                    {
                        bool pageUpdated = false;
                        var pageKeyStorage = page.Keys as ColumnKeyStorageContainer;
                        if (!firstPage)
                        {
                            // Locate indices again
                            var index = _searchLeftComparer.FindIndex(in columnReference, pageKeyStorage!);
                            if (_searchLeftComparer.noMatch)
                            {
                                break;
                            }
                        }
                        firstPage = false;
                        // All in this range matched the key comparisons
                        for (int k = _searchLeftComparer.start; k <= _searchLeftComparer.end; k++)
                        {
                            if (_postCondition != null)
                            {
                                var leftEvent = RowEventToEventBatchData.RowReferenceToRowEvent(1, 0, new ColumnRowReference() { referenceBatch = pageKeyStorage!._data, RowIndex = k });
                                var rightEvent = RowEventToEventBatchData.RowReferenceToRowEvent(1, 0, columnReference);
                                if (!_postCondition(leftEvent, rightEvent))
                                {
                                    _searchLeftComparer.end = int.MinValue;
                                    break;
                                }
                            }
                            var joinStorageValue = page.Values.Get(k);
                            int outputWeight = joinStorageValue.Weight * weight;
                            joinWeight += outputWeight;
                            for (int z = 0; z < leftColumns.Count; z++)
                            {
                                var val = pageKeyStorage!._data.Columns[_leftOutputColumns[z]].GetValueAt(k);
                                leftColumns[z].Add(val);
                            }
                            foundOffsets.Add(i);
                            iterations.Add(msg.Data.Iterations[i]);
                            weights.Add(outputWeight);

                            if (_mergeJoinRelation.Type == JoinType.Left)
                            {
                                pageUpdated = true;
                                if (joinStorageValue.JoinWeight == 0)
                                {
                                    // If it was zero before, we must emit a left with right null to negate previous value

                                    // TODO: Can optimize here since we are copying the same value from left two times.
                                    // If offsets where also used on these values, we could just copy the offset
                                    for (int z = 0; z < leftColumns.Count; z++)
                                    {
                                        var val = pageKeyStorage!._data.Columns[_leftOutputColumns[z]].GetValueAt(k);
                                        leftColumns[z].Add(val);
                                    }
                                    foundOffsets.Add(msg.Data.Weights.Count);
                                    weights.Add(-joinStorageValue.Weight);
                                    iterations.Add(msg.Data.Iterations[i]);
                                    
                                    //output.Add(CreateLeftWithNullRightEvent(-kv.Value.Weight, kv.Key, e.Iteration));
                                }

                                joinStorageValue.JoinWeight += outputWeight;

                                if (joinStorageValue.JoinWeight == 0)
                                {
                                    // Became 0 this time, must emit a left with right null
                                    for (int z = 0; z < leftColumns.Count; z++)
                                    {
                                        var val = pageKeyStorage!._data.Columns[_leftOutputColumns[z]].GetValueAt(k);
                                        leftColumns[z].Add(val);
                                    }
                                    foundOffsets.Add(msg.Data.Weights.Count);
                                    weights.Add(joinStorageValue.Weight);
                                    iterations.Add(msg.Data.Iterations[i]);

                                    //output.Add(CreateLeftWithNullRightEvent(kv.Value.Weight, kv.Key, e.Iteration));
                                }
                            }
                        }
                        if (pageUpdated)
                        {
                            await page.SavePage();
                        }
                        if (_searchLeftComparer.end < (page.Keys.Count - 1))
                        {
                            break;
                        }
                    }
                }
                await _rightTree!.RMW(in columnReference, new JoinStorageValue() { Weight = weight, JoinWeight = joinWeight }, (input, current, found) =>
                {
                    if (found)
                    {
                        current!.Weight += input!.Weight;
                        current.JoinWeight += input.JoinWeight;
                        if (current.Weight == 0)
                        {
                            return (default, GenericWriteOperation.Delete);
                        }
                        return (current, GenericWriteOperation.Upsert);
                    }
                    return (input, GenericWriteOperation.Upsert);
                });
            }

            if (foundOffsets.Count > 0)
            {
                List<IColumn> outputColumns = new List<IColumn>();
                for (int i = 0; i < leftColumns.Count; i++)
                {
                    outputColumns.Add(leftColumns[i]);
                }
                for (int i = 0; i < _rightOutputColumns.Count; i++)
                {
                    outputColumns.Add(new ColumnWithOffset(msg.Data.EventBatchData.Columns[_rightOutputColumns[i]], foundOffsets, true));
                }
                yield return new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(outputColumns)));
            }
        }

        public override IAsyncEnumerable<StreamEventBatch> OnRecieve(int targetId, StreamEventBatch msg, long time)
        {
            Debug.Assert(_eventsProcessed != null, nameof(_eventsProcessed));
            _eventsProcessed.Add(msg.Events.Count);
            if (targetId == 0)
            {
                return OnRecieveLeft(msg, time);
            }
            else
            {
                return OnRecieveRight(msg, time);
            }
        }

        protected override async Task InitializeOrRestore(JoinState? state, IStateManagerClient stateManagerClient)
        {
            Logger.InitializingMergeJoinOperator(StreamName, Name);
            if (_eventsCounter == null)
            {
                _eventsCounter = Metrics.CreateCounter<long>("events");
            }
            if (_eventsProcessed == null)
            {
                _eventsProcessed = Metrics.CreateCounter<long>("events_processed");
            }

            _leftTree = await stateManagerClient.GetOrCreateTree("left",
                new BPlusTreeOptions<ColumnRowReference, JoinStorageValue, ColumnKeyStorageContainer, ListValueContainer<JoinStorageValue>>()
                {
                    Comparer = _leftInsertComparer,
                    KeySerializer = new ColumnStoreSerializer(_mergeJoinRelation.Left.OutputLength),
                    ValueSerializer = new ValueListSerializer<JoinStorageValue>(new JoinStorageValueBPlusTreeSerializer())
                });
            _rightTree = await stateManagerClient.GetOrCreateTree("right",
                new BPlusTreeOptions<ColumnRowReference, JoinStorageValue, ColumnKeyStorageContainer, ListValueContainer<JoinStorageValue>>()
                {
                    Comparer = _rightInsertComparer,
                    KeySerializer = new ColumnStoreSerializer(_mergeJoinRelation.Right.OutputLength),
                    ValueSerializer = new ValueListSerializer<JoinStorageValue>(new JoinStorageValueBPlusTreeSerializer())
                });
        }
    }
}
