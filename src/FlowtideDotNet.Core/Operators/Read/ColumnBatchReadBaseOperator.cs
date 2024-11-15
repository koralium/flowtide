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

using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Operators.Normalization;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.CustomProtobuf;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using static SqlParser.Ast.DataType;
using static SqlParser.Ast.Expression;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Base;

namespace FlowtideDotNet.Core.Operators.Read
{

    public record struct ColumnReadEvent(EventBatchWeighted BatchData, long Watermark);

    public record struct DeltaReadEvent(EventBatchWeighted? BatchData, Watermark? Watermark);

    internal class ColumnBatchReadState
    {
        public bool InitialSent { get; set; }
    }

    internal abstract class ColumnBatchReadBaseOperator<TState> : ReadBaseOperator<TState>
        where TState: ColumnBatchReadState
    {
        private bool _initialSent;
        private readonly ReadRelation _readRelation;
        private readonly Func<EventBatchData, int, bool>? _filter;
        private List<int>? _primaryKeyColumns;
        private List<int>? _otherColumns;
        private List<int>? _emitList;
        private IBPlusTree<ColumnRowReference, ColumnRowReference, NormalizeKeyStorage, NormalizeValueStorage>? _fullLoadTempTree;
        private IBPlusTree<ColumnRowReference, ColumnRowReference, NormalizeKeyStorage, NormalizeValueStorage>? _persistentTree;
        private IBPlusTree<ColumnRowReference, int, NormalizeKeyStorage, PrimitiveListValueContainer<int>>? _deleteTree;

        public ColumnBatchReadBaseOperator(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions options) : base(options)
        {
            this._readRelation = readRelation;

            if (readRelation.Filter != null)
            {
                _filter = ColumnBooleanCompiler.Compile(readRelation.Filter, functionsRegister);
            }
        }

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override Task OnTrigger(string triggerName, object? state)
        {
            throw new NotImplementedException();
        }

        protected override async Task InitializeOrRestore(long restoreTime, TState? state, IStateManagerClient stateManagerClient)
        {
            if (state != null)
            {
                _initialSent = state.InitialSent;
            }
            else
            {
                _initialSent = false;
            }

            _primaryKeyColumns = await GetPrimaryKeyColumns();
            _otherColumns = [];
            _emitList = [];

            if (_readRelation.EmitSet)
            {
                _emitList = _readRelation.Emit;
                for (int i = 0; i < _readRelation.Emit.Count; i++)
                {
                    if (!_primaryKeyColumns.Contains(_readRelation.Emit[i]))
                    {
                        _otherColumns.Add(_readRelation.Emit[i]);
                    }
                }
            }
            else
            {
                _emitList = new List<int>();
                for (int i = 0; i < _readRelation.OutputLength; i++)
                {
                    _emitList.Add(i);
                    if (!_primaryKeyColumns.Contains(i))
                    {
                        _otherColumns.Add(i);
                    }
                }
            }

            _fullLoadTempTree = await stateManagerClient.GetOrCreateTree("full_load_temp", 
                new BPlusTreeOptions<ColumnRowReference, ColumnRowReference, NormalizeKeyStorage, NormalizeValueStorage>()
                {
                    Comparer = new NormalizeTreeComparer(_primaryKeyColumns),
                    KeySerializer = new NormalizeKeyStorageSerializer(_primaryKeyColumns, MemoryAllocator),
                    ValueSerializer = new NormalizeValueSerializer(_otherColumns, MemoryAllocator),
                    MemoryAllocator = MemoryAllocator
                });
            _persistentTree = await stateManagerClient.GetOrCreateTree("persistent",
                new BPlusTreeOptions<ColumnRowReference, ColumnRowReference, NormalizeKeyStorage, NormalizeValueStorage>()
                {
                    Comparer = new NormalizeTreeComparer(_primaryKeyColumns),
                    KeySerializer = new NormalizeKeyStorageSerializer(_primaryKeyColumns, MemoryAllocator),
                    ValueSerializer = new NormalizeValueSerializer(_otherColumns, MemoryAllocator),
                    MemoryAllocator = MemoryAllocator
                });
            _deleteTree = await stateManagerClient.GetOrCreateTree("delete",
                new BPlusTreeOptions<ColumnRowReference, int, NormalizeKeyStorage, PrimitiveListValueContainer<int>>()
                {
                    Comparer = new NormalizeTreeComparer(_primaryKeyColumns),
                    KeySerializer = new NormalizeKeyStorageSerializer(_primaryKeyColumns, MemoryAllocator),
                    ValueSerializer = new PrimitiveListValueContainerSerializer<int>(MemoryAllocator),
                    MemoryAllocator = MemoryAllocator
                });
        }

        protected override async Task<TState> OnCheckpoint(long checkpointTime)
        {
            Debug.Assert(_persistentTree != null);
            await _persistentTree.Commit();
            var state = await Checkpoint(checkpointTime).ConfigureAwait(false);
            state.InitialSent = _initialSent;
            return state;
        }

        protected abstract Task<TState> Checkpoint(long checkpointTime);

        protected abstract IAsyncEnumerable<ColumnReadEvent> FullLoad();

        protected abstract IAsyncEnumerable<DeltaReadEvent> DeltaLoad(long lastWatermark);

        protected abstract ValueTask<List<int>> GetPrimaryKeyColumns();

        private int CompareRowReference(ColumnRowReference x, ColumnRowReference y)
        {
            Debug.Assert(_otherColumns != null);
            for (int i = 0; i < _otherColumns.Count; i++)
            {
                var compareResult = DataValueComparer.Instance.Compare(
                    x.referenceBatch.Columns[_otherColumns[i]].GetValueAt(x.RowIndex, default),
                    y.referenceBatch.Columns[i].GetValueAt(y.RowIndex, default));

                if (compareResult != 0)
                {
                    return compareResult;
                }
            }
            return 0;
        }

        private static async IAsyncEnumerable<ColumnRowReference> IteratePerRow(IBPlusTreeIterator<ColumnRowReference, ColumnRowReference, NormalizeKeyStorage, NormalizeValueStorage> iterator)
        {
            await foreach (var page in iterator)
            {
                foreach (var kv in page)
                {
                    yield return kv.Key;
                }
            }
        }

        private async Task DoDeltaLoad(IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(_persistentTree != null);
            Debug.Assert(_otherColumns != null);
            Debug.Assert(_emitList != null);
            Debug.Assert(_primaryKeyColumns != null);

            Watermark? lastWatermark = default;
            bool sentData = false;
            await output.EnterCheckpointLock();

            await foreach (var e in DeltaLoad(0))
            {
                if (e.BatchData != null)
                {
                    PrimitiveList<int> toEmitOffsets = new PrimitiveList<int>(MemoryAllocator);
                    PrimitiveList<int> weights = new PrimitiveList<int>(MemoryAllocator);
                    PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);
                    PrimitiveList<int> deleteBatchKeyOffsets = new PrimitiveList<int>(MemoryAllocator);

                    List<IColumn> deleteBatchColumns = new List<IColumn>();

                    for (int i = 0; i < _otherColumns.Count; i++)
                    {
                        deleteBatchColumns.Add(Column.Create(MemoryAllocator));
                    }

                    for (int i = 0; i < e.BatchData.Count; i++)
                    {
                        var weight = e.BatchData.Weights[i];

                        var rowReference = new ColumnRowReference() { referenceBatch = e.BatchData.EventBatchData, RowIndex = i };
                        if (weight < 0) 
                        {
                            // Delete operation
                            await _persistentTree.RMWNoResult(rowReference, rowReference, (input, current, exists) =>
                            {
                                if (exists)
                                {
                                    deleteBatchKeyOffsets.Add(input.RowIndex);
                                    for (int k = 0; k < _otherColumns.Count; k++)
                                    {
                                        deleteBatchColumns[k].Add(current.referenceBatch.Columns[k].GetValueAt(current.RowIndex, default));
                                    }
                                    return (current, GenericWriteOperation.Delete);
                                }
                                return (input, GenericWriteOperation.None);
                            });
                        }
                        else if (weight > 0)
                        {
                            await _persistentTree.RMWNoResult(rowReference, rowReference, (input, current, exists) =>
                            {
                                if (exists)
                                {
                                    bool updated = false;
                                    if (_filter == null || _filter(input.referenceBatch, input.RowIndex))
                                    {
                                        weights.Add(1);
                                        iterations.Add(0);
                                        toEmitOffsets.Add(input.RowIndex);
                                        updated = true;
                                    }
                                    deleteBatchKeyOffsets.Add(input.RowIndex);
                                    for (int k = 0; k < _otherColumns.Count; k++)
                                    {
                                        deleteBatchColumns[k].Add(current.referenceBatch.Columns[k].GetValueAt(current.RowIndex, default));
                                    }
                                    return (input, updated ? GenericWriteOperation.Upsert : GenericWriteOperation.Delete);
                                }
                                // Does not exist
                                if (_filter == null || _filter(input.referenceBatch, input.RowIndex))
                                {
                                    toEmitOffsets.Add(input.RowIndex);
                                    return (input, GenericWriteOperation.Upsert);
                                }
                                return (input, GenericWriteOperation.None);
                            });
                        }
                    }

                    // Emit new data
                    if (weights.Count > 0)
                    {
                        sentData = true;
                        IColumn[] columns = new IColumn[_emitList.Count];

                        for (int k = 0; k < _emitList.Count; k++)
                        {
                            columns[k] = new ColumnWithOffset(e.BatchData.EventBatchData.Columns[_emitList[k]], toEmitOffsets, false);
                        }
                        // Send out the data
                        await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns))));
                    }
                    else
                    {
                        weights.Dispose();
                        iterations.Dispose();
                        toEmitOffsets.Dispose();
                    }

                    // Emit deleted data
                    if (deleteBatchKeyOffsets.Count > 0)
                    {
                        sentData = true;
                        PrimitiveList<int> deleteWeights = new PrimitiveList<int>(MemoryAllocator);
                        PrimitiveList<uint> deleteIterations = new PrimitiveList<uint>(MemoryAllocator);

                        for (int i = 0; i < deleteBatchKeyOffsets.Count; i++)
                        {
                            deleteWeights.Add(-1);
                            deleteIterations.Add(0);
                        }

                        IColumn[] deleteColumns = new IColumn[_readRelation.OutputLength];
                        for (int i = 0; i < _primaryKeyColumns.Count; i++)
                        {
                            var emitIndex = _emitList.IndexOf(_primaryKeyColumns[i]);
                            if (emitIndex >= 0)
                            {
                                deleteColumns[emitIndex] = new ColumnWithOffset(e.BatchData.EventBatchData.Columns[_primaryKeyColumns[i]], deleteBatchKeyOffsets, false);
                            }
                        }
                        for (int i = 0; i < _otherColumns.Count; i++)
                        {
                            var emitIndex = _emitList.IndexOf(_otherColumns[i]);
                            if (emitIndex >= 0)
                            {
                                deleteColumns[emitIndex] = deleteBatchColumns[i];
                            }
                        }
                        var outputBatch = new StreamEventBatch(new EventBatchWeighted(deleteWeights, deleteIterations, new EventBatchData(deleteColumns)));
                        await output.SendAsync(outputBatch);
                    }
                    else
                    {
                        deleteBatchKeyOffsets.Dispose();
                    }
                }

                if (e.Watermark != null)
                {
                    lastWatermark = e.Watermark;
                }
            }

            if (lastWatermark != null)
            {
                sentData = true;
                await output.SendWatermark(lastWatermark);
            }
            output.ExitCheckpointLock();

            if (sentData)
            {
                // Schedule checkpoint after a watermark
                ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
            }
        }

        protected async Task DoFullLoad(IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(_fullLoadTempTree != null);
            Debug.Assert(_persistentTree != null);
            Debug.Assert(_otherColumns != null);
            Debug.Assert(_deleteTree != null);
            Debug.Assert(_emitList != null);
            Debug.Assert(_primaryKeyColumns != null);

            // Lock checkpointing until the full load is complete
            await output.EnterCheckpointLock();

            await foreach (var columnReadEvent in FullLoad())
            {
                PrimitiveList<int> toEmitOffsets = new PrimitiveList<int>(MemoryAllocator);
                PrimitiveList<int> weights = new PrimitiveList<int>(MemoryAllocator);
                PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);
                PrimitiveList<int> deleteBatchKeyOffsets = new PrimitiveList<int>(MemoryAllocator);

                List<IColumn> deleteBatchColumns = new List<IColumn>();

                for (int i = 0; i < _otherColumns.Count; i++)
                {
                    deleteBatchColumns.Add(Column.Create(MemoryAllocator));
                }

                for (int i = 0; i < columnReadEvent.BatchData.Count; i++)
                {
                    if (columnReadEvent.BatchData.Weights[i] < 0)
                    {
                        throw new NotSupportedException("Full load does not support deletions");
                    }
                    var columnRef = new ColumnRowReference() { referenceBatch = columnReadEvent.BatchData.EventBatchData, RowIndex = i };
                    await _fullLoadTempTree.Upsert(columnRef, columnRef);

                    await _persistentTree.RMWNoResult(columnRef, columnRef, (input, current, exists) =>
                    {
                        if (exists)
                        {
                            // TODO: Check that the data has changed
                            if (CompareRowReference(input, current) != 0)
                            {
                                bool updated = false;
                                if (_filter == null || _filter(input.referenceBatch, input.RowIndex))
                                {
                                    weights.Add(1);
                                    iterations.Add(0);
                                    toEmitOffsets.Add(input.RowIndex);
                                    updated = true;
                                }

                                deleteBatchKeyOffsets.Add(input.RowIndex);
                                for (int k = 0; k < _otherColumns.Count; k++)
                                {
                                    deleteBatchColumns[k].Add(current.referenceBatch.Columns[k].GetValueAt(current.RowIndex, default));
                                }
                                return (input, updated ? GenericWriteOperation.Upsert : GenericWriteOperation.Delete);
                            }
                            return (input, GenericWriteOperation.None);
                        }

                        if (_filter == null || _filter(input.referenceBatch, input.RowIndex))
                        {
                            toEmitOffsets.Add(input.RowIndex);
                            return (input, GenericWriteOperation.Upsert);
                        }
                        return (input, GenericWriteOperation.None);
                    });
                }

                if (weights.Count > 0)
                {
                    IColumn[] columns = new IColumn[_emitList.Count];

                    for (int k = 0; k < _emitList.Count; k++)
                    {
                        columns[k] = new ColumnWithOffset(columnReadEvent.BatchData.EventBatchData.Columns[_emitList[k]], toEmitOffsets, false);
                    }
                    // Send out the data
                    await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns))));
                }
                else
                {
                    weights.Dispose();
                    iterations.Dispose();
                    toEmitOffsets.Dispose();
                }

                if (deleteBatchKeyOffsets.Count > 0)
                {
                    PrimitiveList<int> deleteWeights = new PrimitiveList<int>(MemoryAllocator);
                    PrimitiveList<uint> deleteIterations = new PrimitiveList<uint>(MemoryAllocator);

                    for (int i = 0; i < deleteBatchKeyOffsets.Count; i++)
                    {
                        deleteWeights.Add(-1);
                        deleteIterations.Add(0);
                    }

                    IColumn[] deleteColumns = new IColumn[_readRelation.OutputLength];
                    for (int i = 0; i < _primaryKeyColumns.Count; i++)
                    {
                        var emitIndex = _emitList.IndexOf(_primaryKeyColumns[i]);
                        if (emitIndex >= 0)
                        {
                            deleteColumns[emitIndex] = new ColumnWithOffset(columnReadEvent.BatchData.EventBatchData.Columns[_primaryKeyColumns[i]], deleteBatchKeyOffsets, false);
                        }
                    }
                    for (int i = 0; i < _otherColumns.Count; i++)
                    {
                        var emitIndex = _emitList.IndexOf(_otherColumns[i]);
                        if (emitIndex >= 0)
                        {
                            deleteColumns[emitIndex] = deleteBatchColumns[i];
                        }
                    }
                    var outputBatch = new StreamEventBatch(new EventBatchWeighted(deleteWeights, deleteIterations, new EventBatchData(deleteColumns)));
                    await output.SendAsync(outputBatch);
                }
                else
                {
                    deleteBatchKeyOffsets.Dispose();
                }
            }

            var tmpIterator = _fullLoadTempTree.CreateIterator();
            var persistentIterator = _persistentTree.CreateIterator();
            await tmpIterator.SeekFirst();
            await persistentIterator.SeekFirst();

            var tmpEnumerator = IteratePerRow(tmpIterator).GetAsyncEnumerator();
            var persistentEnumerator = IteratePerRow(persistentIterator).GetAsyncEnumerator();

            var hasNew = await tmpEnumerator.MoveNextAsync();
            var hasOld = await persistentEnumerator.MoveNextAsync();

            // Go through both trees and find deletions
            while (hasNew || hasOld)
            {
                int comparison = hasNew && hasOld ? CompareRowReference(tmpEnumerator.Current, persistentEnumerator.Current) : 0;

                // If there is no more old data, then we are done
                if (!hasOld)
                {
                    break;
                }
                if (hasNew && comparison < 0)
                {
                    hasNew = await tmpEnumerator.MoveNextAsync();
                }
                else if (!hasNew || comparison > 0)
                {
                    // Deletion
                    await _deleteTree.Upsert(persistentEnumerator.Current, 1);
                    hasOld = await persistentEnumerator.MoveNextAsync();
                }
                else
                {
                    hasNew = await tmpEnumerator.MoveNextAsync();
                    hasOld = await persistentEnumerator.MoveNextAsync();
                }
            }

            await _fullLoadTempTree.Clear();

            await OutputDeletedRowsFromFullLoad(output);

            // TODO: Output watermark

            // Exit the checkpoint lock
            output.ExitCheckpointLock();

            // Schedule a checkpoint
            ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
        }

        private async Task OutputDeletedRowsFromFullLoad(IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(_deleteTree != null);
            Debug.Assert(_otherColumns != null);
            Debug.Assert(_persistentTree != null);
            Debug.Assert(_primaryKeyColumns != null);

            PrimitiveList<int> weights = new PrimitiveList<int>(MemoryAllocator);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);

            IColumn[] deleteBatchColumns = new IColumn[_readRelation.OutputLength];

            for (int i = 0; i < _readRelation.OutputLength; i++)
            {
                deleteBatchColumns[i] = Column.Create(MemoryAllocator);
            }

            var deleteIterator = _deleteTree.CreateIterator();
            await deleteIterator.SeekFirst();

            await foreach (var page in deleteIterator)
            {
                foreach (var kv in page)
                {
                    // Go through the deletions and delete them from the persistent tree
                    var operation = await _persistentTree.RMWNoResult(kv.Key, default, (input, current, exists) =>
                    {
                        if (exists)
                        {
                            // Output delete event
                            for (int k = 0; k < _otherColumns.Count; k++)
                            {
                                deleteBatchColumns[_otherColumns[k]].Add(current.referenceBatch.Columns[k].GetValueAt(current.RowIndex, default));
                            }
                            return (current, GenericWriteOperation.Delete);
                        }
                        return (current, GenericWriteOperation.None);
                    });

                    if (operation == GenericWriteOperation.Delete)
                    {
                        weights.Add(-1);
                        iterations.Add(0);
                        // If it is a delete, add the key values as well
                        for (int i = 0; i < _primaryKeyColumns.Count; i++)
                        {
                            deleteBatchColumns[_primaryKeyColumns[i]].Add(kv.Key.referenceBatch.Columns[i].GetValueAt(kv.Key.RowIndex, default));
                        }
                    }
                    if (weights.Count >= 100)
                    {
                        await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(deleteBatchColumns))));

                        // Reset
                        weights = new PrimitiveList<int>(MemoryAllocator);
                        iterations = new PrimitiveList<uint>(MemoryAllocator);
                        for (int i = 0; i < _readRelation.OutputLength; i++)
                        {
                            deleteBatchColumns[i] = Column.Create(MemoryAllocator);
                        }
                    }
                }
            }

            if (weights.Count > 0)
            {
                await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(deleteBatchColumns))));
            }
            else
            {
                weights.Dispose();
                iterations.Dispose();

                for (int i = 0; i < _readRelation.OutputLength; i++)
                {
                    deleteBatchColumns[i].Dispose();
                }
            }

            await _deleteTree.Clear();
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            if (!_initialSent)
            {
                // Only do full load if we have not done it before
                await DoFullLoad(output);
                _initialSent = true;
            }
        }
    }
}
