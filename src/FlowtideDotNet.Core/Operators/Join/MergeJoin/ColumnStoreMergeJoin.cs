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
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Storage.Utils;

namespace FlowtideDotNet.Core.Operators.Join.MergeJoin
{
    internal class ColumnStoreMergeJoin : MultipleInputVertex<StreamEventBatch, JoinState>
    {
        protected IBPlusTree<ColumnRowReference, JoinWeights, ColumnKeyStorageContainer, JoinWeightsValueContainer>? _leftTree;
        protected IBPlusTree<ColumnRowReference, JoinWeights, ColumnKeyStorageContainer, JoinWeightsValueContainer>? _rightTree;

        private readonly MergeJoinRelation _mergeJoinRelation;
        private MergeJoinInsertComparer _leftInsertComparer;
        private MergeJoinInsertComparer _rightInsertComparer;
        private MergeJoinSearchComparer _searchLeftComparer;
        private MergeJoinSearchComparer _searchRightComparer;

        private List<int> _leftOutputColumns;
        private List<int> _rightOutputColumns;

        private List<int> _leftOutputIndices;
        private List<int> _rightOutputIndices;

        // Metrics
        private ICounter<long>? _eventsCounter;
        private ICounter<long>? _eventsProcessed;

        private IBPlusTreeIterator<ColumnRowReference, JoinWeights, ColumnKeyStorageContainer, JoinWeightsValueContainer>? _leftIterator;
        private IBPlusTreeIterator<ColumnRowReference, JoinWeights, ColumnKeyStorageContainer, JoinWeightsValueContainer>? _rightIterator;

        protected readonly Func<EventBatchData, int, EventBatchData, int, bool>? _postCondition;

        private readonly DataValueContainer _dataValueContainer;

        private const int MaxRowSize = 100;
        private const int MaxCacheMisses = 1;

#if DEBUG_WRITE
        // Debug data
        private StreamWriter allInput;
        private StreamWriter leftInput;
        private StreamWriter rightInput;
        private StreamWriter outputWriter;
#endif

        public ColumnStoreMergeJoin(MergeJoinRelation mergeJoinRelation, FunctionsRegister functionsRegister, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(2, executionDataflowBlockOptions)
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

        public override string DisplayName => "Merge Join";

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override async Task<JoinState?> OnCheckpoint()
        {
#if DEBUG_WRITE
            allInput.WriteLine("Checkpoint");
            await allInput.FlushAsync();
#endif
            _leftIterator!.Reset();

            _rightIterator!.Reset();

            await _leftTree!.Commit();
            await _rightTree!.Commit();
            return new JoinState();
        }

        private async IAsyncEnumerable<StreamEventBatch> OnRecieveLeft(StreamEventBatch msg, long time)
        {
            Debug.Assert(_eventsCounter != null);
            Debug.Assert(_rightIterator != null);
            var memoryManager = MemoryAllocator;
            //using var it = _rightTree!.CreateIterator();
            List<Column> rightColumns = new List<Column>();
            PrimitiveList<int> foundOffsets = new PrimitiveList<int>(memoryManager);
            PrimitiveList<int> weights = new PrimitiveList<int>(memoryManager);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(memoryManager);

            var startCacheMisses = GetCacheMisses();
            var batchStartTime = ValueStopwatch.StartNew();
            for (int i = 0; i < _rightOutputColumns.Count; i++)
            {
                rightColumns.Add(Column.Create(memoryManager)); 
            }
            for (int i = 0; i < msg.Data.Weights.Count; i++)
            {
                var columnReference = new ColumnRowReference()
                {
                    referenceBatch = msg.Data.EventBatchData,
                    RowIndex = i
                };

                await _rightIterator.Seek(in columnReference, _searchRightComparer);
                int weight = msg.Data.Weights[i];
                int joinWeight = 0;
                if (!_searchRightComparer.noMatch)
                {
                    bool firstPage = true;
                    
                    await foreach(var page in _rightIterator)
                    {
                        var pageKeyStorage = page.Keys;
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
                                if (!_postCondition(columnReference.referenceBatch, columnReference.RowIndex, pageKeyStorage._data, k))
                                {
                                    _searchRightComparer.end = int.MinValue;
                                    break;
                                }
                            }
                            int outputWeight = page.Values.Get(k).weight * msg.Data.Weights[i];
                            joinWeight += outputWeight;
                            for (int z = 0; z < rightColumns.Count; z++)
                            {
                                pageKeyStorage!._data.Columns[_rightOutputColumns[z]].GetValueAt(k, _dataValueContainer, default);
                                rightColumns[z].Add(_dataValueContainer);
                            }
                            foundOffsets.Add(i);
                            iterations.Add(msg.Data.Iterations[i]);
                            weights.Add(outputWeight);

                            var newCacheMisses = GetCacheMisses();
                            var deltaCacheMisses = newCacheMisses - startCacheMisses;
                            var elapsedMilli = batchStartTime.GetElapsedTime().TotalMilliseconds;
                            // Check if we have more than 100 elements, if so we must yield the batch
                            if (foundOffsets.Count >= MaxRowSize || deltaCacheMisses > MaxCacheMisses || elapsedMilli > 10)
                            {
                                startCacheMisses = newCacheMisses;
                                IColumn[] outputColumns = new IColumn[_leftOutputColumns.Count + rightColumns.Count];
                                if (_leftOutputColumns.Count > 0)
                                {
                                    for (int l = 0; l < _leftOutputColumns.Count; l++)
                                    {
                                        outputColumns[_leftOutputIndices[l]] = new ColumnWithOffset(msg.Data.EventBatchData.Columns[_leftOutputColumns[l]], foundOffsets, true);
                                    }
                                }
                                else
                                {
                                    foundOffsets.Dispose();
                                }
                                for (int l = 0; l < rightColumns.Count; l++)
                                {
                                    outputColumns[_rightOutputIndices[l]] = rightColumns[l];
                                }

                                var outputBatch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(outputColumns)));

                                if (outputBatch.Data.Count > 102)
                                {

                                }
#if DEBUG_WRITE
                                foreach (var o in outputBatch.Events)
                                {
                                    outputWriter.WriteLine($"{o.Weight} {o.ToJson()}");
                                }
                                await outputWriter.FlushAsync();
#endif
                                _eventsCounter.Add(outputBatch.Data.Weights.Count);
                                yield return outputBatch;
                                
                                // Reset all lists
                                foundOffsets = new PrimitiveList<int>(MemoryAllocator);
                                weights = new PrimitiveList<int>(MemoryAllocator);
                                iterations = new PrimitiveList<uint>(MemoryAllocator);
                                for (int l = 0; l < rightColumns.Count; l++)
                                {
                                    rightColumns[l] = Column.Create(MemoryAllocator);
                                }
                                batchStartTime = ValueStopwatch.StartNew();
                            }
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
                    weights.Add(weight);
                    for (int z = 0; z < rightColumns.Count; z++)
                    {
                        rightColumns[z].Add(NullValue.Instance);
                    }
                }
                var insertWeights = new JoinWeights() { weight = weight, joinWeight = joinWeight };
                await _leftTree!.RMWNoResult(in columnReference, in insertWeights, (input, current, found) =>
                {
                    if (found)
                    {
                        current!.weight += input!.weight;
                        current.joinWeight += input.joinWeight;
                        if (current.weight == 0)
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
                IColumn[] outputColumns = new IColumn[_leftOutputColumns.Count + rightColumns.Count];
                if (_leftOutputColumns.Count > 0)
                {
                    for (int i = 0; i < _leftOutputColumns.Count; i++)
                    {
                        outputColumns[_leftOutputIndices[i]] = new ColumnWithOffset(msg.Data.EventBatchData.Columns[_leftOutputColumns[i]], foundOffsets, true);
                    }
                }
                else
                {
                    foundOffsets.Dispose();
                }
                
                for (int i = 0; i < rightColumns.Count; i++)
                {
                    outputColumns[_rightOutputIndices[i]] = rightColumns[i];
                }

                var outputBatch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(outputColumns)));

                if (outputBatch.Data.Count > 102)
                {

                }
#if DEBUG_WRITE
                foreach (var o in outputBatch.Events)
                {
                    outputWriter.WriteLine($"{o.Weight} {o.ToJson()}");
                }
                await outputWriter.FlushAsync();
#endif
                _eventsCounter.Add(outputBatch.Data.Weights.Count);
                yield return outputBatch;
            }
            else
            {
                for (int i = 0; i < rightColumns.Count; i++)
                {
                    rightColumns[i].Dispose();
                }
                foundOffsets.Dispose();
                weights.Dispose();
                iterations.Dispose();
            }
            _rightIterator.Reset();
        }

        private long GetCacheMisses()
        {
            return _leftTree!.CacheMisses + _rightTree!.CacheMisses;
        }

        private async IAsyncEnumerable<StreamEventBatch> OnRecieveRight(StreamEventBatch msg, long time)
        {
            Debug.Assert(_eventsCounter != null);
            Debug.Assert(_leftIterator != null);
            var memoryManager = MemoryAllocator;
            List<Column> leftColumns = new List<Column>();
            PrimitiveList<int> foundOffsets = new PrimitiveList<int>(memoryManager);
            PrimitiveList<int> weights = new PrimitiveList<int>(memoryManager);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(memoryManager);

            long startCacheMisses = GetCacheMisses();
            var batchStartTime = ValueStopwatch.StartNew();

            for (int i = 0; i < _leftOutputColumns.Count; i++)
            {
                leftColumns.Add(Column.Create(memoryManager));
            }
            for (int i = 0; i < msg.Data.Weights.Count; i++)
            {
                var columnReference = new ColumnRowReference()
                {
                    referenceBatch = msg.Data.EventBatchData,
                    RowIndex = i
                };

                await _leftIterator.Seek(in columnReference, _searchLeftComparer);

                int weight = msg.Data.Weights[i];
                int joinWeight = 0;
                if (!_searchLeftComparer.noMatch)
                {
                
                    bool firstPage = true;
                    
                    await foreach (var page in _leftIterator)
                    {
                        bool pageUpdated = false;
                        var pageKeyStorage = page.Keys;
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
                                if (!_postCondition(pageKeyStorage._data, k, columnReference.referenceBatch, columnReference.RowIndex))
                                {
                                    _searchLeftComparer.end = int.MinValue;
                                    break;
                                }
                            }
                            pageUpdated = RecieveRightHandleElement(
                                in i, 
                                in k, 
                                in weight, 
                                in msg, 
                                in foundOffsets, 
                                in weights, 
                                in iterations, 
                                page.Values, 
                                in pageKeyStorage!, 
                                in leftColumns, 
                                ref joinWeight);

                            var newCacheMisses = GetCacheMisses();
                            var deltaCacheMisses = newCacheMisses - startCacheMisses;

                            var elapsedMilli = batchStartTime.GetElapsedTime().TotalMilliseconds;
                            // Check if we have more than 100 elements, if so we must yield the batch
                            if (foundOffsets.Count >= MaxRowSize || deltaCacheMisses > MaxCacheMisses || elapsedMilli > 10)
                            {
                                startCacheMisses = newCacheMisses;
                                IColumn[] outputColumns = new IColumn[leftColumns.Count + _rightOutputColumns.Count];
                                for (int l = 0; l < leftColumns.Count; l++)
                                {
                                    outputColumns[_leftOutputIndices[l]] = leftColumns[l];
                                }
                                if (_rightOutputColumns.Count > 0)
                                {
                                    for (int l = 0; l < _rightOutputColumns.Count; l++)
                                    {
                                        outputColumns[_rightOutputIndices[l]] = new ColumnWithOffset(msg.Data.EventBatchData.Columns[_rightOutputColumns[l]], foundOffsets, true);
                                    }
                                }
                                else
                                {
                                    foundOffsets.Dispose();
                                }
                                var outputBatch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(outputColumns)));

                                if (outputBatch.Data.Count > 102)
                                {

                                }
#if DEBUG_WRITE
                                foreach (var o in outputBatch.Events)
                                {
                                    outputWriter.WriteLine($"{o.Weight} {o.ToJson()}");
                                }
                                await outputWriter.FlushAsync();
#endif
                                _eventsCounter.Add(outputBatch.Data.Weights.Count);
                                yield return outputBatch;

                                // Reset all lists
                                foundOffsets = new PrimitiveList<int>(MemoryAllocator);
                                weights = new PrimitiveList<int>(MemoryAllocator);
                                iterations = new PrimitiveList<uint>(MemoryAllocator);
                                for (int l = 0; l < leftColumns.Count; l++)
                                {
                                    leftColumns[l] = Column.Create(MemoryAllocator);
                                }
                            }
                            
                        }
                        if (pageUpdated)
                        {
                            await page.SavePage(false);
                        }
                        if (_searchLeftComparer.end < (page.Keys.Count - 1))
                        {
                            break;
                        }
                    }
                }
                var insertWeights = new JoinWeights() { weight = weight, joinWeight = joinWeight };
                await _rightTree!.RMWNoResult(in columnReference, in insertWeights, (input, current, found) =>
                {
                    if (found)
                    {
                        current!.weight += input!.weight;
                        current.joinWeight += input.joinWeight;
                        if (current.weight == 0)
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
                IColumn[] outputColumns = new IColumn[leftColumns.Count + _rightOutputColumns.Count];
                for (int i = 0; i < leftColumns.Count; i++)
                {
                    outputColumns[_leftOutputIndices[i]] = leftColumns[i];
                }
                if (_rightOutputColumns.Count > 0)
                {
                    for (int i = 0; i < _rightOutputColumns.Count; i++)
                    {
                        outputColumns[_rightOutputIndices[i]] = new ColumnWithOffset(msg.Data.EventBatchData.Columns[_rightOutputColumns[i]], foundOffsets, true);
                    }
                }
                else
                {
                    // Dipsose offsets since it was not required
                    foundOffsets.Dispose();
                }
                
                var outputBatch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(outputColumns)));

                if (outputBatch.Data.Count > 102)
                {

                }

                _eventsCounter.Add(outputBatch.Data.Weights.Count);
#if DEBUG_WRITE
                foreach (var o in outputBatch.Events)
                {
                    outputWriter.WriteLine($"{o.Weight} {o.ToJson()}");
                }
                await outputWriter.FlushAsync();
#endif

                yield return outputBatch;
            }
            else
            {
                for (int i = 0; i < leftColumns.Count; i++)
                {
                    leftColumns[i].Dispose();
                }
                foundOffsets.Dispose();
                weights.Dispose();
                iterations.Dispose();
            }
            _leftIterator.Reset();
        }

        // This method exist since its not possible to get by ref in an async method.
        private bool RecieveRightHandleElement(
            in int i,
            in int k, 
            in int weight,
            in StreamEventBatch msg,
            in PrimitiveList<int> foundOffsets,
            in PrimitiveList<int> weights,
            in PrimitiveList<uint> iterations,
            in IValueContainer<JoinWeights> values,
            in ColumnKeyStorageContainer pageKeyStorage,
            in List<Column> leftColumns,
            ref int joinWeight)
        {
            bool pageUpdated = false;
            ref var joinStorageValue = ref values.GetRef(k);
            int outputWeight = joinStorageValue.weight * weight;
            joinWeight += outputWeight;
            for (int z = 0; z < leftColumns.Count; z++)
            {
                pageKeyStorage._data.Columns[_leftOutputColumns[z]].GetValueAt(k, _dataValueContainer, default);
                leftColumns[z].Add(_dataValueContainer);
            }
            foundOffsets.Add(i);
            iterations.Add(msg.Data.Iterations[i]);
            weights.Add(outputWeight);

            if (_mergeJoinRelation.Type == JoinType.Left)
            {
                pageUpdated = true;
                if (joinStorageValue.joinWeight == 0)
                {
                    // If it was zero before, we must emit a left with right null to negate previous value

                    // TODO: Can optimize here since we are copying the same value from left two times.
                    // If offsets where also used on these values, we could just copy the offset
                    for (int z = 0; z < leftColumns.Count; z++)
                    {
                        pageKeyStorage!._data.Columns[_leftOutputColumns[z]].GetValueAt(k, _dataValueContainer, default);
                        leftColumns[z].Add(_dataValueContainer);
                    }
                    foundOffsets.Add(msg.Data.Weights.Count);
                    weights.Add(-joinStorageValue.weight);
                    iterations.Add(msg.Data.Iterations[i]);
                }

                joinStorageValue.joinWeight += outputWeight;

                if (joinStorageValue.joinWeight == 0)
                {
                    // Became 0 this time, must emit a left with right null
                    for (int z = 0; z < leftColumns.Count; z++)
                    {
                        pageKeyStorage!._data.Columns[_leftOutputColumns[z]].GetValueAt(k, _dataValueContainer, default);
                        leftColumns[z].Add(_dataValueContainer);
                    }
                    foundOffsets.Add(msg.Data.Weights.Count);
                    weights.Add(joinStorageValue.weight);
                    iterations.Add(msg.Data.Iterations[i]);
                }
            }
            return pageUpdated;
        }

        public override IAsyncEnumerable<StreamEventBatch> OnRecieve(int targetId, StreamEventBatch msg, long time)
        {
#if DEBUG_WRITE
            allInput.WriteLine("New batch");
            foreach (var e in msg.Events)
            {
                allInput.WriteLine($"{targetId}, {e.Weight} {e.ToJson()}");
            }
            if (targetId == 0)
            {
                foreach (var e in msg.Events)
                {
                    leftInput.WriteLine($"{e.Weight} {e.ToJson()}");
                }
                leftInput.Flush();
            }
            else
            {
                foreach (var e in msg.Events)
                {
                    rightInput.WriteLine($"{e.Weight} {e.ToJson()}");
                }
                rightInput.Flush();
            }
            
            allInput.Flush();
#endif
            Debug.Assert(_eventsProcessed != null, nameof(_eventsProcessed));
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

        protected override async Task InitializeOrRestore(JoinState? state, IStateManagerClient stateManagerClient)
        {
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
                outputWriter = File.CreateText($"debugwrite/{StreamName}-{Name}.output.txt");
            }
#endif
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
                new BPlusTreeOptions<ColumnRowReference, JoinWeights, ColumnKeyStorageContainer, JoinWeightsValueContainer>()
                {
                    Comparer = _leftInsertComparer,
                    KeySerializer = new ColumnStoreSerializer(_mergeJoinRelation.Left.OutputLength, MemoryAllocator),
                    ValueSerializer = new JoinWeightsSerializer(MemoryAllocator),
                    UseByteBasedPageSizes = true
                });
            _rightTree = await stateManagerClient.GetOrCreateTree("right",
                new BPlusTreeOptions<ColumnRowReference, JoinWeights, ColumnKeyStorageContainer, JoinWeightsValueContainer>()
                {
                    Comparer = _rightInsertComparer,
                    KeySerializer = new ColumnStoreSerializer(_mergeJoinRelation.Right.OutputLength, MemoryAllocator),
                    ValueSerializer = new JoinWeightsSerializer(MemoryAllocator),
                    UseByteBasedPageSizes = true
                });

            _leftIterator = _leftTree.CreateIterator();
            _rightIterator = _rightTree.CreateIterator();
        }
    }
}
