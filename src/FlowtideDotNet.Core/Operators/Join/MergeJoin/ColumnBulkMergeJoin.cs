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
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Core.Utils;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Storage.Tree.Internal;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Join.MergeJoin
{
    internal struct JoinWeightsMutator : IRowMutator<ColumnRowReference, JoinWeights>
    {
        public GenericWriteOperation Process(ColumnRowReference key, bool exists, in JoinWeights existingData, ref JoinWeights incomingData)
        {
            if (exists)
            {
                incomingData.weight += existingData.weight;
                incomingData.joinWeight += existingData.joinWeight;
                if (incomingData.weight == 0)
                {
                    return GenericWriteOperation.Delete;
                }
                return GenericWriteOperation.Upsert;
            }
            return GenericWriteOperation.Upsert;
        }
    }

    internal class ColumnBulkMergeJoin : MultipleInputVertex<StreamEventBatch>
    {
        protected IBPlusTree<ColumnRowReference, JoinWeights, ColumnKeyStorageContainer, JoinWeightsValueContainer>? _leftTree;
        protected IBPlusTree<ColumnRowReference, JoinWeights, ColumnKeyStorageContainer, JoinWeightsValueContainer>? _rightTree;
        private IBPlusTreeBulkInserter<ColumnRowReference, JoinWeights, ColumnKeyStorageContainer, JoinWeightsValueContainer>? _leftInserter;
        private IBPlusTreeBulkInserter<ColumnRowReference, JoinWeights, ColumnKeyStorageContainer, JoinWeightsValueContainer>? _rightInserter;
        private IBplusTreeBulkSearch<ColumnRowReference, JoinWeights, ColumnKeyStorageContainer, JoinWeightsValueContainer, MergeJoinSearchComparer>? _rightSearcher;
        private IBplusTreeBulkSearch<ColumnRowReference, JoinWeights, ColumnKeyStorageContainer, JoinWeightsValueContainer, MergeJoinSearchComparer>? _leftSearcher;


        private readonly MergeJoinRelation _mergeJoinRelation;
        private readonly MergeJoinInsertComparer _leftInsertComparer;
        private readonly MergeJoinInsertComparer _rightInsertComparer;
        private readonly MergeJoinSearchComparer _searchLeftComparer;
        private readonly MergeJoinSearchComparer _searchRightComparer;

        private readonly List<int> _leftOutputColumns;
        private readonly List<int> _rightOutputColumns;

        private readonly List<int> _leftOutputIndices;
        private readonly List<int> _rightOutputIndices;

        // Metrics
        private ICounter<long>? _eventsCounter;
        private ICounter<long>? _eventsProcessed;

        protected readonly Func<EventBatchData, int, EventBatchData, int, bool>? _postCondition;
        private readonly DataValueContainer _dataValueContainer;
        private const int MaxRowSize = 100;

#if DEBUG_WRITE
        // Debug data
        private StreamWriter? allInput;
        private StreamWriter? leftInput;
        private StreamWriter? rightInput;
#endif

        public ColumnBulkMergeJoin(MergeJoinRelation mergeJoinRelation, FunctionsRegister functionsRegister, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(2, executionDataflowBlockOptions)
        {
            this._mergeJoinRelation = mergeJoinRelation;
            _dataValueContainer = new DataValueContainer();
            var leftColumns = GetCompareColumns(mergeJoinRelation.LeftKeys, 0);
            var rightColumns = GetCompareColumns(mergeJoinRelation.RightKeys, mergeJoinRelation.Left.OutputLength);
            _leftInsertComparer = new MergeJoinInsertComparer(leftColumns, mergeJoinRelation.Left.OutputLength);
            _rightInsertComparer = new MergeJoinInsertComparer(rightColumns, mergeJoinRelation.Right.OutputLength);

            _searchLeftComparer = new MergeJoinSearchComparer(leftColumns, rightColumns);
            _searchRightComparer = new MergeJoinSearchComparer(rightColumns, leftColumns);

            (_leftOutputColumns, _leftOutputIndices) = GetOutputColumns(mergeJoinRelation, 0, mergeJoinRelation.Left.OutputLength);
            (_rightOutputColumns, _rightOutputIndices) = GetOutputColumns(mergeJoinRelation, mergeJoinRelation.Left.OutputLength, mergeJoinRelation.Right.OutputLength);

            if (mergeJoinRelation.PostJoinFilter != null)
            {
                _postCondition = ColumnBooleanCompiler.CompileTwoInputs(mergeJoinRelation.PostJoinFilter, functionsRegister, mergeJoinRelation.Left.OutputLength);
            }
        }

        private static (List<int> incomingIndices, List<int> outgoingIndex) GetOutputColumns(MergeJoinRelation mergeJoinRelation, int relative, int maxSize)
        {
            List<int> columns = new List<int>();
            List<int> outgoingIndices = new List<int>();
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
                            outgoingIndices.Add(i);
                        }
                    }

                }
            }
            else
            {
                for (int i = 0; i < mergeJoinRelation.OutputLength - relative; i++)
                {
                    if (i < maxSize)
                    {
                        columns.Add(i);
                        outgoingIndices.Add(i + relative);
                    }
                }
            }
            return (columns, outgoingIndices);
        }

        private static List<KeyValuePair<int, ReferenceSegment?>> GetCompareColumns(List<FieldReference> fieldReferences, int relativeIndex)
        {
            List<KeyValuePair<int, ReferenceSegment?>> leftKeys = new List<KeyValuePair<int, ReferenceSegment?>>();
            for (int i = 0; i < fieldReferences.Count; i++)
            {
                if (fieldReferences[i] is DirectFieldReference directFieldReference &&
                    directFieldReference.ReferenceSegment is StructReferenceSegment structReferenceSegment)
                {
                    leftKeys.Add(new KeyValuePair<int, ReferenceSegment?>(structReferenceSegment.Field - relativeIndex, structReferenceSegment.Child));
                }
                else
                {
                    throw new NotImplementedException("Merge join can only have keys that use struct reference segments at this time");
                }
            }
            return leftKeys;
        }

        public override string DisplayName => "Merge Join Bulk";

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
            Debug.Assert(_leftTree != null);
            Debug.Assert(_rightTree != null);

            await _leftTree.Commit();
            await _rightTree.Commit();
        }

        public override IAsyncEnumerable<StreamEventBatch> OnRecieve(int targetId, StreamEventBatch msg, long time)
        {
            Debug.Assert(_eventsProcessed != null, nameof(_eventsProcessed));

#if DEBUG_WRITE
            allInput!.WriteLine("New batch");
            foreach (var e in msg.Events)
            {
                allInput!.WriteLine($"{targetId}, {e.Weight} {e.ToJson()}");
            }
            if (targetId == 0)
            {
                foreach (var e in msg.Events)
                {
                    leftInput!.WriteLine($"{e.Weight} {e.ToJson()}");
                }
                leftInput!.Flush();
            }
            else
            {
                foreach (var e in msg.Events)
                {
                    rightInput!.WriteLine($"{e.Weight} {e.ToJson()}");
                }
                rightInput!.Flush();
            }
            
            allInput!.Flush();
#endif

            _eventsProcessed.Add(msg.Data.Weights.Count);
            if (targetId == 0)
            {
                return OnRecieveLeft(msg, time);
            }
            else
            {
                return OnRecieveRight(msg, time);
            }
        }

        private async IAsyncEnumerable<StreamEventBatch> OnRecieveLeft(StreamEventBatch msg, long time)
        {
            Debug.Assert(_rightTree != null);
            Debug.Assert(_eventsCounter != null);
            Debug.Assert(_leftInserter != null);
            Debug.Assert(_rightSearcher != null);

            var memoryManager = MemoryAllocator;

            List<Column> rightColumns = new List<Column>();
            PrimitiveList<int> foundOffsets = new PrimitiveList<int>(memoryManager);
            PrimitiveList<int> weights = new PrimitiveList<int>(memoryManager);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(memoryManager);

            for (int i = 0; i < _rightOutputColumns.Count; i++)
            {
                rightColumns.Add(Column.Create(memoryManager));
            }

            int keyLength = msg.Data.Weights.Count;
            ColumnRowReference[] keys = new ColumnRowReference[keyLength];
            JoinWeights[] insertValues = new JoinWeights[keyLength];

            for (int i = 0; i < keyLength; i++)
            {
                keys[i] = new ColumnRowReference()
                {
                    referenceBatch = msg.Data.EventBatchData,
                    RowIndex = i
                };
                insertValues[i] = new JoinWeights()
                {
                    weight = msg.Data.Weights[i],
                    joinWeight = 0 
                };
            }

            var sortedIndices = _leftInserter.SortAndGetIndices(keys, keyLength);

            await _rightSearcher.Start(keys, keyLength, sortedIndices);

            bool emitLeftAlways = _mergeJoinRelation.Type == JoinType.Left || _mergeJoinRelation.Type == JoinType.Outer;
            int leafTransitionsCount = 0;
            while (await _rightSearcher.MoveNextLeaf())
            {
                leafTransitionsCount++;
                var leafNode = _rightSearcher.CurrentLeaf;
                var pageKeyStorage = leafNode.keys;
                var pageValues = leafNode.values;
                bool pageUpdated = false;

                var results = _rightSearcher.CurrentResults;
                for (int r = 0; r < results.Count; r++)
                {
                    var result = results[r];
                    var keyIndex = result.KeyIndex;
                    int weight = msg.Data.Weights[keyIndex];

                    int lowerBound = result.LowerBound < 0 ? ~result.LowerBound : result.LowerBound;
                    int upperBound = result.UpperBound;

                    for (int k = lowerBound; k <= upperBound; k++)
                    {
                        if (_postCondition != null)
                        {
                            if (!_postCondition(keys[keyIndex].referenceBatch, keys[keyIndex].RowIndex, pageKeyStorage._data, k))
                            {
                                continue;
                            }
                        }

                        var joinStorageValue = pageValues.Get(k);
                        int outWeight = joinStorageValue.weight * weight;
                        insertValues[keyIndex].joinWeight += outWeight;

                        for (int z = 0; z < rightColumns.Count; z++)
                        {
                            pageKeyStorage._data.Columns[_rightOutputColumns[z]].GetValueAt(k, _dataValueContainer, default);
                            rightColumns[z].Add(_dataValueContainer);
                        }
                        foundOffsets.Add(keyIndex);
                        iterations.Add(msg.Data.Iterations[keyIndex]);
                        weights.Add(outWeight);

                        if (_mergeJoinRelation.Type == JoinType.Right || _mergeJoinRelation.Type == JoinType.Outer)
                        {
                            pageUpdated = true;
                            if (joinStorageValue.joinWeight == 0)
                            {
                                for (int z = 0; z < rightColumns.Count; z++)
                                {
                                    pageKeyStorage._data.Columns[_rightOutputColumns[z]].GetValueAt(k, _dataValueContainer, default);
                                    rightColumns[z].Add(_dataValueContainer);
                                }
                                foundOffsets.Add(keyLength);
                                weights.Add(-joinStorageValue.weight);
                                iterations.Add(msg.Data.Iterations[keyIndex]);
                            }

                            joinStorageValue.joinWeight += outWeight;

                            if (joinStorageValue.joinWeight == 0)
                            {
                                for (int z = 0; z < rightColumns.Count; z++)
                                {
                                    pageKeyStorage._data.Columns[_rightOutputColumns[z]].GetValueAt(k, _dataValueContainer, default);
                                    rightColumns[z].Add(_dataValueContainer);
                                }
                                foundOffsets.Add(keyLength);
                                weights.Add(joinStorageValue.weight);
                                iterations.Add(msg.Data.Iterations[keyIndex]);
                            }
                            
                            pageValues.Update(k, joinStorageValue);
                        }

                        if (foundOffsets.Count >= MaxRowSize)
                        {
                            var outputBatch = BuildOutputBatch(msg, foundOffsets, weights, iterations, null, rightColumns, true);
                            _eventsCounter.Add(outputBatch.Data.Weights.Count);
                            yield return outputBatch;
                            ResetOutputLists(ref foundOffsets, ref weights, ref iterations, null, rightColumns);
                        }
                    }
                }

                if (pageUpdated)
                {
                    var bTree = (BPlusTree<ColumnRowReference, JoinWeights, ColumnKeyStorageContainer, JoinWeightsValueContainer>)_rightTree;
                    var isFull = bTree.m_stateClient.AddOrUpdate(leafNode.Id, leafNode);
                    if (isFull)
                    {
                        await bTree.m_stateClient.WaitForNotFullAsync();
                    }
                }
            }

            if (emitLeftAlways)
            {
                for (int i = 0; i < keyLength; i++)
                {
                    if (insertValues[i].joinWeight == 0)
                    {
                        foundOffsets.Add(i);
                        iterations.Add(msg.Data.Iterations[i]);
                        weights.Add(msg.Data.Weights[i]);
                        for (int z = 0; z < rightColumns.Count; z++)
                        {
                            rightColumns[z].Add(NullValue.Instance);
                        }

                        if (foundOffsets.Count >= MaxRowSize)
                        {
                            var outputBatch = BuildOutputBatch(msg, foundOffsets, weights, iterations, null, rightColumns, true);
                            _eventsCounter.Add(outputBatch.Data.Weights.Count);
                            yield return outputBatch;
                            ResetOutputLists(ref foundOffsets, ref weights, ref iterations, null, rightColumns);
                        }
                    }
                }
            }

            if (foundOffsets.Count > 0)
            {
                var outputBatch = BuildOutputBatch(msg, foundOffsets, weights, iterations, null, rightColumns, true);
                _eventsCounter.Add(outputBatch.Data.Weights.Count);
                yield return outputBatch;
            }
            else
            {
                for (int z = 0; z < rightColumns.Count; z++)
                {
                    rightColumns[z].Dispose();
                }
                foundOffsets.Dispose();
                weights.Dispose();
                iterations.Dispose();
            }


            await _leftInserter.ApplyBatch(keys, insertValues, keyLength, sortedIndices, new JoinWeightsMutator());
        }

        private async IAsyncEnumerable<StreamEventBatch> OnRecieveRight(StreamEventBatch msg, long time)
        {
            Debug.Assert(_leftTree != null);
            Debug.Assert(_eventsCounter != null);
            Debug.Assert(_rightInserter != null);
            Debug.Assert(_leftSearcher != null);

            var memoryManager = MemoryAllocator;

            List<Column> leftColumns = new List<Column>();
            PrimitiveList<int> foundOffsets = new PrimitiveList<int>(memoryManager);
            PrimitiveList<int> weights = new PrimitiveList<int>(memoryManager);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(memoryManager);

            for (int i = 0; i < _leftOutputColumns.Count; i++)
            {
                leftColumns.Add(Column.Create(memoryManager));
            }

            int keyLength = msg.Data.Weights.Count;
            ColumnRowReference[] keys = new ColumnRowReference[keyLength];
            JoinWeights[] insertValues = new JoinWeights[keyLength];

            for (int i = 0; i < keyLength; i++)
            {
                keys[i] = new ColumnRowReference()
                {
                    referenceBatch = msg.Data.EventBatchData,
                    RowIndex = i
                };
                insertValues[i] = new JoinWeights()
                {
                    weight = msg.Data.Weights[i],
                    joinWeight = 0 
                };
            }

            var sortedIndices = _rightInserter.SortAndGetIndices(keys, keyLength);

            var leftSearcher = _leftTree.CreateBulkSearcher(_searchLeftComparer);
            await leftSearcher.Start(keys, keyLength, sortedIndices);

            bool emitRightAlways = _mergeJoinRelation.Type == JoinType.Right || _mergeJoinRelation.Type == JoinType.Outer;

            while (await leftSearcher.MoveNextLeaf())
            {
                var leafNode = leftSearcher.CurrentLeaf;
                var pageKeyStorage = leafNode.keys;
                var pageValues = leafNode.values;
                bool pageUpdated = false;

                var results = leftSearcher.CurrentResults;
                for (int r = 0; r < results.Count; r++)
                {
                    var result = results[r];
                    var keyIndex = result.KeyIndex;
                    int weight = msg.Data.Weights[keyIndex];

                    int lowerBound = result.LowerBound < 0 ? ~result.LowerBound : result.LowerBound;
                    int upperBound = result.UpperBound;

                    for (int k = lowerBound; k <= upperBound; k++)
                    {
                        if (_postCondition != null)
                        {
                            if (!_postCondition(pageKeyStorage._data, k, keys[keyIndex].referenceBatch, keys[keyIndex].RowIndex))
                            {
                                continue;
                            }
                        }

                        var joinStorageValue = pageValues.Get(k);
                        int outWeight = joinStorageValue.weight * weight;
                        insertValues[keyIndex].joinWeight += outWeight;

                        for (int z = 0; z < leftColumns.Count; z++)
                        {
                            pageKeyStorage._data.Columns[_leftOutputColumns[z]].GetValueAt(k, _dataValueContainer, default);
                            leftColumns[z].Add(_dataValueContainer);
                        }
                        foundOffsets.Add(keyIndex);
                        iterations.Add(msg.Data.Iterations[keyIndex]);
                        weights.Add(outWeight);

                        if (_mergeJoinRelation.Type == JoinType.Left || _mergeJoinRelation.Type == JoinType.Outer)
                        {
                            pageUpdated = true;
                            if (joinStorageValue.joinWeight == 0)
                            {
                                for (int z = 0; z < leftColumns.Count; z++)
                                {
                                    pageKeyStorage._data.Columns[_leftOutputColumns[z]].GetValueAt(k, _dataValueContainer, default);
                                    leftColumns[z].Add(_dataValueContainer);
                                }
                                foundOffsets.Add(keyLength);
                                weights.Add(-joinStorageValue.weight);
                                iterations.Add(msg.Data.Iterations[keyIndex]);
                            }

                            joinStorageValue.joinWeight += outWeight;

                            if (joinStorageValue.joinWeight == 0)
                            {
                                for (int z = 0; z < leftColumns.Count; z++)
                                {
                                    pageKeyStorage._data.Columns[_leftOutputColumns[z]].GetValueAt(k, _dataValueContainer, default);
                                    leftColumns[z].Add(_dataValueContainer);
                                }
                                foundOffsets.Add(keyLength);
                                weights.Add(joinStorageValue.weight);
                                iterations.Add(msg.Data.Iterations[keyIndex]);
                            }
                            
                            pageValues.Update(k, joinStorageValue);
                        }

                        if (foundOffsets.Count >= MaxRowSize)
                        {
                            var outputBatch = BuildOutputBatch(msg, foundOffsets, weights, iterations, leftColumns, null, false);
                            _eventsCounter.Add(outputBatch.Data.Weights.Count);
                            yield return outputBatch;
                            ResetOutputLists(ref foundOffsets, ref weights, ref iterations, leftColumns, null);
                        }
                    }
                }

                if (pageUpdated)
                {
                    var bTree = (BPlusTree<ColumnRowReference, JoinWeights, ColumnKeyStorageContainer, JoinWeightsValueContainer>)_leftTree;
                    var isFull = bTree.m_stateClient.AddOrUpdate(leafNode.Id, leafNode);
                    if (isFull)
                    {
                        await bTree.m_stateClient.WaitForNotFullAsync();
                    }
                }
            }

            if (emitRightAlways)
            {
                for (int i = 0; i < keyLength; i++)
                {
                    if (insertValues[i].joinWeight == 0)
                    {
                        foundOffsets.Add(i);
                        iterations.Add(msg.Data.Iterations[i]);
                        weights.Add(msg.Data.Weights[i]);
                        for (int z = 0; z < leftColumns.Count; z++)
                        {
                            leftColumns[z].Add(NullValue.Instance);
                        }

                        if (foundOffsets.Count >= MaxRowSize)
                        {
                            var outputBatch = BuildOutputBatch(msg, foundOffsets, weights, iterations, leftColumns, null, false);
                            _eventsCounter.Add(outputBatch.Data.Weights.Count);
                            yield return outputBatch;
                            ResetOutputLists(ref foundOffsets, ref weights, ref iterations, leftColumns, null);
                        }
                    }
                }
            }

            if (foundOffsets.Count > 0)
            {
                var outputBatch = BuildOutputBatch(msg, foundOffsets, weights, iterations, leftColumns, null, false);
                _eventsCounter.Add(outputBatch.Data.Weights.Count);
                yield return outputBatch;
            }
            else
            {
                for (int z = 0; z < leftColumns.Count; z++)
                {
                    leftColumns[z].Dispose();
                }
                foundOffsets.Dispose();
                weights.Dispose();
                iterations.Dispose();
            }



            await _rightInserter.ApplyBatch(keys, insertValues, keyLength, sortedIndices, new JoinWeightsMutator());
        }

        private StreamEventBatch BuildOutputBatch(StreamEventBatch msg, PrimitiveList<int> foundOffsets, PrimitiveList<int> weights, PrimitiveList<uint> iterations, List<Column>? leftColumns, List<Column>? rightColumns, bool isLeft)
        {
            IColumn[] outputColumns = new IColumn[_leftOutputColumns.Count + _rightOutputColumns.Count];
            bool shouldDisposeOffsets = true;
            if (isLeft)
            {
                if (_leftOutputColumns.Count > 0)
                {
                    for (int i = 0; i < _leftOutputColumns.Count; i++)
                    {
                        outputColumns[_leftOutputIndices[i]] = ColumnWithOffset.CreateFlattened(msg.Data.EventBatchData.Columns[_leftOutputColumns[i]], foundOffsets, true, MemoryAllocator, out var usedOffset);
                        if (usedOffset)
                        {
                            shouldDisposeOffsets = false;
                        }
                    }
                }
                Debug.Assert(rightColumns != null);
                for (int i = 0; i < rightColumns.Count; i++)
                {
                    outputColumns[_rightOutputIndices[i]] = rightColumns[i];
                }
            }
            else
            {
                Debug.Assert(leftColumns != null);
                for (int i = 0; i < leftColumns.Count; i++)
                {
                    outputColumns[_leftOutputIndices[i]] = leftColumns[i];
                }
                if (_rightOutputColumns.Count > 0)
                {
                    for (int i = 0; i < _rightOutputColumns.Count; i++)
                    {
                        outputColumns[_rightOutputIndices[i]] = ColumnWithOffset.CreateFlattened(msg.Data.EventBatchData.Columns[_rightOutputColumns[i]], foundOffsets, true, MemoryAllocator, out var usedOffset);
                        if (usedOffset)
                        {
                            shouldDisposeOffsets = false;
                        }
                    }
                }
            }
            if (shouldDisposeOffsets)
            {
                foundOffsets.Dispose();
            }
            return new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(outputColumns)));
        }

        private void ResetOutputLists(ref PrimitiveList<int> foundOffsets, ref PrimitiveList<int> weights, ref PrimitiveList<uint> iterations, List<Column>? leftColumns, List<Column>? rightColumns)
        {
            var memoryManager = MemoryAllocator;
            foundOffsets = new PrimitiveList<int>(memoryManager);
            weights = new PrimitiveList<int>(memoryManager);
            iterations = new PrimitiveList<uint>(memoryManager);

            if (leftColumns != null)
            {
                for (int l = 0; l < leftColumns.Count; l++)
                {
                    leftColumns[l] = Column.Create(memoryManager);
                }
            }
            if (rightColumns != null)
            {
                for (int l = 0; l < rightColumns.Count; l++)
                {
                    rightColumns[l] = Column.Create(memoryManager);
                }
            }
        }

        protected override async Task InitializeOrRestore(IStateManagerClient stateManagerClient)
        {
            Logger.InitializingMergeJoinOperator(StreamName, Name);

#if DEBUG_WRITE
            if (!Directory.Exists("debugwrite"))
            {
                var dir = Directory.CreateDirectory("debugwrite");
            }
            if (allInput != null)
            {
                allInput.WriteLine("Restart");
            }
            else
            {
                allInput = File.CreateText($"debugwrite/{StreamName}-{Name}.all.txt");
                leftInput = File.CreateText($"debugwrite/{StreamName}-{Name}.left.txt");
                rightInput = File.CreateText($"debugwrite/{StreamName}-{Name}.right.txt");
            }
#endif

            if (_eventsCounter == null)
            {
                _eventsCounter = Metrics.CreateCounter<long>("events");
            }
            if (_eventsProcessed == null)
            {
                _eventsProcessed = Metrics.CreateCounter<long>("events_processed");
            }

            _leftTree = await stateManagerClient.GetOrCreateTree("left",
                new BPlusTreeOptions<ColumnRowReference, JoinWeights, ColumnKeyStorageContainer, JoinWeightsValueContainer>()
                {
                    Comparer = _leftInsertComparer,
                    KeySerializer = new ColumnStoreSerializer(_mergeJoinRelation.Left.OutputLength, MemoryAllocator),
                    ValueSerializer = new JoinWeightsSerializer(MemoryAllocator),
                    UseByteBasedPageSizes = true,
                    MemoryAllocator = MemoryAllocator
                });
            _leftInserter = _leftTree.CreateBulkInserter();
            _leftSearcher = _leftTree.CreateBulkSearcher(_searchLeftComparer); 
            _rightTree = await stateManagerClient.GetOrCreateTree("right",
                new BPlusTreeOptions<ColumnRowReference, JoinWeights, ColumnKeyStorageContainer, JoinWeightsValueContainer>()
                {
                    Comparer = _rightInsertComparer,
                    KeySerializer = new ColumnStoreSerializer(_mergeJoinRelation.Right.OutputLength, MemoryAllocator),
                    ValueSerializer = new JoinWeightsSerializer(MemoryAllocator),
                    UseByteBasedPageSizes = true,
                    MemoryAllocator = MemoryAllocator
                });
            _rightInserter = _rightTree.CreateBulkInserter();
            _rightSearcher = _rightTree.CreateBulkSearcher(_searchRightComparer);
        }
    }
}
