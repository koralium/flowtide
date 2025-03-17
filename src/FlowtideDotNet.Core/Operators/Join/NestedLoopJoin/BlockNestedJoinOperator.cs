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
using FlowtideDotNet.Base.Vertices.MultipleInput;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Core.Operators.Join.MergeJoin;
using FlowtideDotNet.Core.Utils;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Join.NestedLoopJoin
{
    internal class BlockNestedJoinOperator : MultipleInputVertex<StreamEventBatch>
    {
        private readonly JoinRelation _joinRelation;
        protected readonly Func<EventBatchData, int, EventBatchData, int, bool> _condition;
        private ICounter<long>? _eventsProcessed;
        private ICounter<long>? _eventsOutCounter;
        protected IBPlusTree<ColumnRowReference, JoinWeights, ColumnKeyStorageContainer, JoinWeightsValueContainer>? _leftTree;
        protected IBPlusTree<ColumnRowReference, JoinWeights, ColumnKeyStorageContainer, JoinWeightsValueContainer>? _rightTree;

        protected IBPlusTree<ColumnRowReference, JoinWeights, ColumnKeyStorageContainer, JoinWeightsValueContainer>? _leftTemporary;
        protected IBPlusTree<ColumnRowReference, JoinWeights, ColumnKeyStorageContainer, JoinWeightsValueContainer>? _rightTemporary;

        private List<int> _leftOutputColumns;
        private List<int> _rightOutputColumns;

        private List<int> _leftOutputIndices;
        private List<int> _rightOutputIndices;

#if DEBUG_WRITE
        // Debug data
        private StreamWriter? allInput;
        private StreamWriter? outputWriter;
#endif


        public BlockNestedJoinOperator(JoinRelation joinRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(2, executionDataflowBlockOptions)
        {
            this._joinRelation = joinRelation;

            if (joinRelation.Expression == null)
            {
                throw new InvalidOperationException("Join relation must have an expression");
            }

            _condition = ColumnBooleanCompiler.CompileTwoInputs(joinRelation.Expression, functionsRegister, joinRelation.Left.OutputLength);

            (_leftOutputColumns, _leftOutputIndices) = GetOutputColumns(joinRelation, 0, joinRelation.Left.OutputLength);
            (_rightOutputColumns, _rightOutputIndices) = GetOutputColumns(joinRelation, joinRelation.Left.OutputLength, joinRelation.Right.OutputLength);

            GetOutputColumns(joinRelation, 0, joinRelation.Left.OutputLength);
        }


        public override string DisplayName => "Nested Loop Join";

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
            Debug.Assert(_leftTree != null, nameof(_leftTree));
            Debug.Assert(_rightTree != null, nameof(_rightTree));

#if DEBUG_WRITE
            allInput!.WriteLine("Checkpoint");
            await allInput.FlushAsync();
            outputWriter!.WriteLine("Checkpoint");
            await outputWriter.FlushAsync();
#endif

            await _leftTree.Commit();
            await _rightTree.Commit();
        }

        protected override async IAsyncEnumerable<StreamEventBatch> OnWatermark(Watermark watermark)
        {
            Debug.Assert(_leftTree != null, nameof(_leftTree));
            Debug.Assert(_rightTree != null, nameof(_rightTree));
            Debug.Assert(_leftTemporary != null, nameof(_leftTemporary));
            Debug.Assert(_rightTemporary != null, nameof(_rightTemporary));
            Debug.Assert(_eventsOutCounter != null, nameof(_eventsOutCounter));

            PrimitiveList<int> weights = new PrimitiveList<int>(MemoryAllocator);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);
            IColumn[] columns = new IColumn[_joinRelation.OutputLength];

            for (int i = 0; i < _joinRelation.OutputLength; i++)
            {
                columns[i] = Column.Create(MemoryAllocator);
            }

            using var rightTempIterator = _rightTemporary.CreateIterator();

            using var leftPersistentIterator = _leftTree.CreateIterator();
            await leftPersistentIterator.SeekFirst();

            await foreach (var leftPage in leftPersistentIterator)
            {
                bool leftPageModified = false;
                var leftPageValues = leftPage.Values;
                var leftColumns = leftPage.Keys._data.Columns;

                // Must always seek the first page in the iterator
                await rightTempIterator.SeekFirst();
                await foreach (var rightTmpPage in rightTempIterator)
                {
                    bool rightModified = false;
                    var rightPageValues = rightTmpPage.Values;
                    var rightColumns = rightTmpPage.Keys._data.Columns;

                    for (int leftIndex = 0; leftIndex < leftPage.Keys.Count; leftIndex++)
                    {
                        for (int rightIndex = 0; rightIndex < rightTmpPage.Keys.Count; rightIndex++)
                        {
                            if (_condition(leftPage.Keys._data, leftIndex, rightTmpPage.Keys._data, rightIndex))
                            {
                                HandleLeftJoin(
                                    in leftIndex,
                                    in rightIndex,
                                    in leftPageValues,
                                    in rightPageValues,
                                    ref leftPageModified,
                                    ref rightModified,
                                    in weights,
                                    in iterations,
                                    in columns,
                                    in leftColumns,
                                    in rightColumns
                                    );
                            }
                        }
                    }

                    if (rightModified)
                    {
                        await rightTmpPage.SavePage(false);
                    }
                }
                if (leftPageModified)
                {
                    await leftPage.SavePage(false);
                }
            }

            await rightTempIterator.SeekFirst();
            // Insert all right temporary values into the right persitent tree
            await foreach (var rightTmpPage in rightTempIterator)
            {
                for (int rightIndex = 0; rightIndex < rightTmpPage.Keys.Count; rightIndex++)
                {
                    var rowRef = new ColumnRowReference() { referenceBatch = rightTmpPage.Keys._data, RowIndex = rightIndex };
                    var value = rightTmpPage.Values.Get(rightIndex);
                    var op = await _rightTree.RMWNoResult(rowRef, value, (input, current, exist) =>
                    {
                        if (exist)
                        {
                            current!.weight += input!.weight;
                            current!.joinWeight += input!.joinWeight;
                            if (current.weight == 0)
                            {
                                return (default, GenericWriteOperation.Delete);
                            }
                            return (current, GenericWriteOperation.Upsert);
                        }
                        return (input, GenericWriteOperation.Upsert);
                    });

                    if (value.joinWeight == 0 && (_joinRelation.Type == JoinType.Right || _joinRelation.Type == JoinType.Outer))
                    {
                        // Output null values here
                        weights.Add(value.weight);
                        iterations.Add(0);
                        for (int i = 0; i < _rightOutputColumns.Count; i++)
                        {
                            var outputColumn = columns[_rightOutputIndices[i]];
                            outputColumn.InsertRangeFrom(outputColumn.Count, rightTmpPage.Keys._data.Columns[_rightOutputColumns[i]], rightIndex, 1);
                        }
                        for (int i = 0; i < _leftOutputIndices.Count; i++)
                        {
                            columns[_leftOutputIndices[i]].Add(NullValue.Instance);
                        }
                    }
                }
            }
            await _rightTemporary.Clear();

            using var leftTempIterator = _leftTemporary.CreateIterator();

            using var rightPersistentIterator = _rightTree.CreateIterator();
            await rightPersistentIterator.SeekFirst();

            await foreach (var rightPage in rightPersistentIterator)
            {
                bool rightModified = false;
                var rightPageValues = rightPage.Values;
                var rightColumns = rightPage.Keys._data.Columns;

                await leftTempIterator.SeekFirst();

                await foreach (var leftTempPage in leftTempIterator)
                {
                    bool leftPageModified = false;
                    var leftPageValues = leftTempPage.Values;
                    var leftColumns = leftTempPage.Keys._data.Columns;

                    for (int leftIndex = 0; leftIndex < leftTempPage.Keys.Count; leftIndex++)
                    {
                        for (int rightIndex = 0; rightIndex < rightPage.Keys.Count; rightIndex++)
                        {
                            if (_condition(leftTempPage.Keys._data, leftIndex, rightPage.Keys._data, rightIndex))
                            {
                                HandleLRightsideJoin(
                                    in leftIndex,
                                    in rightIndex,
                                    in leftPageValues,
                                    in rightPageValues,
                                    ref leftPageModified,
                                    ref rightModified,
                                    in weights,
                                    in iterations,
                                    in columns,
                                    in leftColumns,
                                    in rightColumns
                                    );
                            }
                        }
                    }

                    if (leftPageModified)
                    {
                        await leftTempPage.SavePage(false);
                    }
                }

                if (rightModified)
                {
                    await rightPage.SavePage(false);
                }
            }

            await leftTempIterator.SeekFirst();

            // Insert all left temporary values into the left persitent tree
            await foreach (var leftTmpPage in leftTempIterator)
            {
                for (int leftIndex = 0; leftIndex < leftTmpPage.Keys.Count; leftIndex++)
                {
                    var rowRef = new ColumnRowReference() { referenceBatch = leftTmpPage.Keys._data, RowIndex = leftIndex };
                    var value = leftTmpPage.Values.Get(leftIndex);
                    var op = await _leftTree.RMWNoResult(rowRef, value, (input, current, exist) =>
                    {
                        if (exist)
                        {
                            current!.weight += input!.weight;
                            current!.joinWeight += input!.joinWeight;
                            if (current.weight == 0)
                            {
                                return (default, GenericWriteOperation.Delete);
                            }
                            return (current, GenericWriteOperation.Upsert);
                        }
                        return (input, GenericWriteOperation.Upsert);
                    });

                    if (value.joinWeight == 0 && (_joinRelation.Type == JoinType.Left || _joinRelation.Type == JoinType.Outer))
                    {
                        // Output null values here
                        weights.Add(value.weight);
                        iterations.Add(0);
                        for (int i = 0; i < _leftOutputColumns.Count; i++)
                        {
                            var outputColumn = columns[_leftOutputIndices[i]];
                            outputColumn.InsertRangeFrom(outputColumn.Count, leftTmpPage.Keys._data.Columns[_leftOutputColumns[i]], leftIndex, 1);
                        }
                        for (int i = 0; i < _rightOutputIndices.Count; i++)
                        {
                            columns[_rightOutputIndices[i]].Add(NullValue.Instance);
                        }
                    }
                }
            }

            await _leftTemporary.Clear();

            _eventsOutCounter.Add(weights.Count);

            var outputBatch = new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)));

#if DEBUG_WRITE
            allInput!.WriteLine("Watermark");
            await allInput.FlushAsync();
            outputWriter!.WriteLine("Watermark");
            foreach (var o in outputBatch.Events)
            {
                outputWriter!.WriteLine($"{o.Weight} {o.ToJson()}");
            }
            await outputWriter!.FlushAsync();
#endif

            yield return outputBatch;
        }

        private static (List<int> incomingIndices, List<int> outgoingIndex) GetOutputColumns(JoinRelation joinRelation, int relative, int maxSize)
        {
            List<int> columns = new List<int>();
            List<int> outgoingIndices = new List<int>();
            if (joinRelation.EmitSet)
            {
                for (int i = 0; i < joinRelation.Emit.Count; i++)
                {
                    var index = joinRelation.Emit[i];
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
                for (int i = 0; i < joinRelation.OutputLength - relative; i++)
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

        private void HandleLeftJoin(
            ref readonly int leftIndex,
            ref readonly int rightIndex,
            ref readonly JoinWeightsValueContainer leftValues,
            ref readonly JoinWeightsValueContainer rightValues,
            ref bool leftModified,
            ref bool rightModified,
            ref readonly PrimitiveList<int> weights,
            ref readonly PrimitiveList<uint> iterations,
            ref readonly IColumn[] columns,
            ref readonly IReadOnlyList<IColumn> leftColumns,
            ref readonly IReadOnlyList<IColumn> rightColumns)
        {
            ref var leftWeights = ref leftValues.GetRef(leftIndex);
            ref var rightWeights = ref rightValues.GetRef(rightIndex);
            var outputWeight = leftWeights.weight * rightValues.Get(rightIndex).weight;

            // Add output here
            weights.Add(outputWeight);
            iterations.Add(0);
            for (int i = 0; i < _leftOutputColumns.Count; i++)
            {
                var outputColumn = columns[_leftOutputIndices[i]];
                outputColumn.InsertRangeFrom(outputColumn.Count, leftColumns[_leftOutputColumns[i]], leftIndex, 1);
            }
            for (int i = 0; i < _rightOutputColumns.Count; i++)
            {
                var outputColumn = columns[_rightOutputIndices[i]];
                outputColumn.InsertRangeFrom(outputColumn.Count, rightColumns[_rightOutputColumns[i]], rightIndex, 1);
            }

            // Check if it previously was left values with null right
            var previousJoinWeight = leftWeights.joinWeight;
            rightWeights.joinWeight += outputWeight;
            leftWeights.joinWeight += outputWeight;

            if (_joinRelation.Type == JoinType.Left || _joinRelation.Type == JoinType.Outer)
            {
                // Check if it was previously a left values null right, then output a negation
                if (previousJoinWeight == 0 && leftWeights.joinWeight > 0)
                {
                    // Output a negation
                    weights.Add(-leftWeights.weight);
                    iterations.Add(0);
                    for (int i = 0; i < _leftOutputColumns.Count; i++)
                    {
                        var outputColumn = columns[_leftOutputIndices[i]];
                        outputColumn.InsertRangeFrom(outputColumn.Count, leftColumns[_leftOutputColumns[i]], leftIndex, 1);
                    }
                    for (int i = 0; i < _rightOutputIndices.Count; i++)
                    {
                        columns[_rightOutputIndices[i]].Add(NullValue.Instance);
                    }
                }
                // Went back to be a join with left values and null right
                if (previousJoinWeight > 0 && leftWeights.joinWeight == 0)
                {
                    weights.Add(leftWeights.weight);
                    iterations.Add(0);
                    for (int i = 0; i < _leftOutputColumns.Count; i++)
                    {
                        var outputColumn = columns[_leftOutputIndices[i]];
                        outputColumn.InsertRangeFrom(outputColumn.Count, leftColumns[_leftOutputColumns[i]], leftIndex, 1);
                    }
                    for (int i = 0; i < _rightOutputIndices.Count; i++)
                    {
                        columns[_rightOutputIndices[i]].Add(NullValue.Instance);
                    }
                }
            }

            rightModified = true;
            leftModified = true;
        }

        private void HandleLRightsideJoin(
            ref readonly int leftIndex,
            ref readonly int rightIndex,
            ref readonly JoinWeightsValueContainer leftValues,
            ref readonly JoinWeightsValueContainer rightValues,
            ref bool leftModified,
            ref bool rightModified,
            ref readonly PrimitiveList<int> weights,
            ref readonly PrimitiveList<uint> iterations,
            ref readonly IColumn[] columns,
            ref readonly IReadOnlyList<IColumn> leftColumns,
            ref readonly IReadOnlyList<IColumn> rightColumns)
        {
            ref var leftWeights = ref leftValues.GetRef(leftIndex);
            ref var rightWeights = ref rightValues.GetRef(rightIndex);
            var outputWeight = leftWeights.weight * rightValues.Get(rightIndex).weight;

            // Add output here
            weights.Add(outputWeight);
            iterations.Add(0);
            for (int i = 0; i < _leftOutputColumns.Count; i++)
            {
                var outputColumn = columns[_leftOutputIndices[i]];
                outputColumn.InsertRangeFrom(outputColumn.Count, leftColumns[_leftOutputColumns[i]], leftIndex, 1);
            }
            for (int i = 0; i < _rightOutputColumns.Count; i++)
            {
                var outputColumn = columns[_rightOutputIndices[i]];
                outputColumn.InsertRangeFrom(outputColumn.Count, rightColumns[_rightOutputColumns[i]], rightIndex, 1);
            }

            // Check if it previously was left values with null right
            var previousJoinWeight = rightWeights.joinWeight;
            rightWeights.joinWeight += outputWeight;
            leftWeights.joinWeight += outputWeight;

            if (_joinRelation.Type == JoinType.Right || _joinRelation.Type == JoinType.Outer)
            {
                // Check if it was previously a left values null right, then output a negation
                if (previousJoinWeight == 0 && rightWeights.joinWeight > 0)
                {
                    // Output a negation
                    weights.Add(-rightWeights.weight);
                    iterations.Add(0);
                    for (int i = 0; i < _rightOutputColumns.Count; i++)
                    {
                        var outputColumn = columns[_rightOutputIndices[i]];
                        outputColumn.InsertRangeFrom(outputColumn.Count, rightColumns[_rightOutputColumns[i]], rightIndex, 1);
                    }
                    for (int i = 0; i < _leftOutputIndices.Count; i++)
                    {
                        columns[_leftOutputIndices[i]].Add(NullValue.Instance);
                    }
                }
                // Went back to be a join with left values and null right
                if (previousJoinWeight > 0 && rightWeights.joinWeight == 0)
                {
                    weights.Add(rightWeights.weight);
                    iterations.Add(0);
                    for (int i = 0; i < _rightOutputColumns.Count; i++)
                    {
                        var outputColumn = columns[_rightOutputIndices[i]];
                        outputColumn.InsertRangeFrom(outputColumn.Count, rightColumns[_rightOutputColumns[i]], rightIndex, 1);
                    }
                    for (int i = 0; i < _leftOutputIndices.Count; i++)
                    {
                        columns[_leftOutputIndices[i]].Add(NullValue.Instance);
                    }
                }
            }

            rightModified = true;
            leftModified = true;
        }

        public override async IAsyncEnumerable<StreamEventBatch> OnRecieve(int targetId, StreamEventBatch msg, long time)
        {
            Debug.Assert(_leftTemporary != null, nameof(_leftTemporary));
            Debug.Assert(_rightTemporary != null, nameof(_rightTemporary));
            Debug.Assert(_eventsProcessed != null, nameof(_eventsProcessed));
            _eventsProcessed.Add(msg.Events.Count);

#if DEBUG_WRITE
            allInput!.WriteLine("New batch");
            foreach (var e in msg.Events)
            {
                allInput!.WriteLine($"{targetId}, {e.Weight} {e.ToJson()}");
            }
            allInput!.Flush();
#endif

            if (targetId == 0)
            {
                for (int i = 0; i < msg.Data.Weights.Count; i++)
                {
                    var rowRef = new ColumnRowReference() { referenceBatch = msg.Data.EventBatchData, RowIndex = i };
                    await _leftTemporary.RMWNoResult(rowRef, new JoinWeights() { joinWeight = 0, weight = msg.Data.Weights[i] }, (input, current, exist) =>
                    {
                        if (exist)
                        {
                            current.weight += input.weight;

                            if (current.weight == 0)
                            {
                                return (default, GenericWriteOperation.Delete);
                            }
                            return (current, GenericWriteOperation.Upsert);
                        }
                        return (input, GenericWriteOperation.Upsert);
                    });
                }
                yield break;
            }
            else if (targetId == 1)
            {
                for (int i = 0; i < msg.Data.Weights.Count; i++)
                {
                    var rowRef = new ColumnRowReference() { referenceBatch = msg.Data.EventBatchData, RowIndex = i };
                    await _rightTemporary.RMWNoResult(rowRef, new JoinWeights() { joinWeight = 0, weight = msg.Data.Weights[i] }, (input, current, exist) =>
                    {
                        if (exist)
                        {
                            current.weight += input.weight;

                            if (current.weight == 0)
                            {
                                return (default, GenericWriteOperation.Delete);
                            }
                            return (current, GenericWriteOperation.Upsert);
                        }
                        return (input, GenericWriteOperation.Upsert);
                    });
                }
                yield break;
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        protected override async Task InitializeOrRestore(IStateManagerClient stateManagerClient)
        {
            Logger.BlockNestedLoopInUse(StreamName, Name);

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
                outputWriter = File.CreateText($"debugwrite/{StreamName}-{Name}.output.txt");
            }
#endif


            _leftTree = await stateManagerClient.GetOrCreateTree("left",
                new BPlusTreeOptions<ColumnRowReference, JoinWeights, ColumnKeyStorageContainer, JoinWeightsValueContainer>()
                {
                    Comparer = new ColumnComparer(_joinRelation.Left.OutputLength),
                    KeySerializer = new ColumnStoreSerializer(_joinRelation.Left.OutputLength, MemoryAllocator),
                    ValueSerializer = new JoinWeightsSerializer(MemoryAllocator),
                    UseByteBasedPageSizes = true,
                    MemoryAllocator = MemoryAllocator
                });
            _rightTree = await stateManagerClient.GetOrCreateTree("right",
                new BPlusTreeOptions<ColumnRowReference, JoinWeights, ColumnKeyStorageContainer, JoinWeightsValueContainer>()
                {
                    Comparer = new ColumnComparer(_joinRelation.Right.OutputLength),
                    KeySerializer = new ColumnStoreSerializer(_joinRelation.Right.OutputLength, MemoryAllocator),
                    ValueSerializer = new JoinWeightsSerializer(MemoryAllocator),
                    UseByteBasedPageSizes = true,
                    MemoryAllocator = MemoryAllocator
                });

            _leftTemporary = await stateManagerClient.GetOrCreateTree("left_tmp",
                new BPlusTreeOptions<ColumnRowReference, JoinWeights, ColumnKeyStorageContainer, JoinWeightsValueContainer>()
                {
                    Comparer = new ColumnComparer(_joinRelation.Left.OutputLength),
                    KeySerializer = new ColumnStoreSerializer(_joinRelation.Left.OutputLength, MemoryAllocator),
                    ValueSerializer = new JoinWeightsSerializer(MemoryAllocator),
                    UseByteBasedPageSizes = true,
                    MemoryAllocator = MemoryAllocator
                });
            _rightTemporary = await stateManagerClient.GetOrCreateTree("right_tmp",
                new BPlusTreeOptions<ColumnRowReference, JoinWeights, ColumnKeyStorageContainer, JoinWeightsValueContainer>()
                {
                    Comparer = new ColumnComparer(_joinRelation.Right.OutputLength),
                    KeySerializer = new ColumnStoreSerializer(_joinRelation.Right.OutputLength, MemoryAllocator),
                    ValueSerializer = new JoinWeightsSerializer(MemoryAllocator),
                    UseByteBasedPageSizes = true,
                    MemoryAllocator = MemoryAllocator
                });

            if (_eventsProcessed == null)
            {
                _eventsProcessed = Metrics.CreateCounter<long>("events_processed");
            }
            if (_eventsOutCounter == null)
            {
                _eventsOutCounter = Metrics.CreateCounter<long>("events");
            }
        }
    }
}
