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
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Core.Operators.LateMaterialization;
using FlowtideDotNet.Core.Operators.Normalization;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Relations;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Read
{

    public record struct ColumnReadEvent(EventBatchWeighted BatchData, long Watermark);

    public record struct DeltaReadEvent(EventBatchWeighted? BatchData, Watermark? Watermark);

    public abstract class ColumnBatchReadBaseOperator : ReadBaseOperator, IRowStoreHostVertex
    {
        private RowStoreHandle? _rowStoreHandle;

        public const string DeltaLoadTriggerName = "delta_load";
        public const string FullLoadTriggerName = "full_load";
        private readonly string _deltaloadWithTableTriggerName;
        private readonly string _fullLoadWithTableTriggerName;

        private IObjectState<bool>? _initialSent;
        private readonly ReadRelation _readRelation;
        private readonly Func<EventBatchData, int, bool>? _filter;
        private List<int>? _primaryKeyColumns;
        private List<int>? _otherColumns;
        private List<int>? _emitList;
        private List<int>? _sortedEmitList;
        private IBPlusTree<ColumnRowReference, ColumnRowReference, NormalizeKeyStorage, NormalizeValueStorage>? _fullLoadTempTree;
        private IBPlusTree<ColumnRowReference, ColumnRowReference, NormalizeKeyStorage, NormalizeValueStorage>? _persistentTree;
        private IBPlusTree<ColumnRowReference, int, NormalizeKeyStorage, PrimitiveListValueContainer<int>>? _deleteTree;
        private Task? _deltaLoadTask;
        private object _taskLock = new object();

        private IColumn[]? _deleteTreeToPersistentColumns;
        private EventBatchData? _deleteTreeTopersistentBatch;
        private ICounter<long>? _eventsCounter;
        private bool _waitingForFullLoad;
        private WatermarkOutputMode _watermarkOutputMode;
        // Set when the relation stores columns behind references
        private RowStoreHost? _host;
        private RowStoreKeySet? _hostedTempKeys;
        private RowStoreKeySet? _hostedDeleteKeys;

        public ColumnBatchReadBaseOperator(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions options) : base(options)
        {
            this._readRelation = readRelation;
            _deltaloadWithTableTriggerName = $"{DeltaLoadTriggerName}_{readRelation.NamedTable.DotSeperated}";
            _fullLoadWithTableTriggerName = $"{FullLoadTriggerName}_{readRelation.NamedTable.DotSeperated}";

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
            if (triggerName == _deltaloadWithTableTriggerName)
            {
                triggerName = DeltaLoadTriggerName;
            }
            else if (triggerName == _fullLoadWithTableTriggerName)
            {
                triggerName = FullLoadTriggerName;
            }
            switch (triggerName)
            {
                case DeltaLoadTriggerName:
                    lock (_taskLock)
                    {
                        if (_deltaLoadTask == null)
                        {
                            _deltaLoadTask = RunTask(DeltaLoadTrigger)
                                .ContinueWith((task) =>
                                {
                                    lock (_taskLock)
                                    {
                                        if (_waitingForFullLoad)
                                        {
                                            _deltaLoadTask = RunTask(FullLoadTrigger)
                                              .ContinueWith((t) =>
                                              {
                                                  lock (_taskLock)
                                                  {
                                                      // Full load just happened, so we reset the flag
                                                      _waitingForFullLoad = false;
                                                      _deltaLoadTask = default;
                                                  }
                                              });
                                            _waitingForFullLoad = false;
                                        }
                                        else
                                        {
                                            _deltaLoadTask = default;
                                        }
                                    }
                                });
                        }
                        return Task.CompletedTask;
                    }
                case FullLoadTriggerName:
                    lock (_taskLock)
                    {
                        if (_deltaLoadTask == null)
                        {
                            _deltaLoadTask = RunTask(FullLoadTrigger)
                                .ContinueWith((task) =>
                                {
                                    lock (_taskLock)
                                    {
                                        // Full load just happened, so we reset the flag
                                        _waitingForFullLoad = false;
                                        _deltaLoadTask = default;
                                    }
                                });
                        }
                        else
                        {
                            _waitingForFullLoad = true;
                        }
                        return Task.CompletedTask;
                    }
            }
            return Task.CompletedTask;
        }

        protected virtual async Task DeltaLoadTrigger(IngressOutput<StreamEventBatch> output, object? state)
        {
            await DoDeltaLoad(output);
        }

        protected virtual async Task FullLoadTrigger(IngressOutput<StreamEventBatch> output, object? state)
        {
            await DoFullLoad(output);
        }

        private const string WatermarkModeKey = "WATERMARK_OUTPUT_MODE";

        private WatermarkOutputMode GetWatermarkOutputMode()
        {
            if (_readRelation.Hint.Optimizations.Properties.TryGetValue(WatermarkModeKey, out var mode))
            {
                if (WatermarkOutputModeHelper.TryParseWatermarkOutputMode(mode, out var parsedMode))
                {
                    return parsedMode;
                }
                Logger.LogWarning("Unknown watermark output mode: {mode}", mode);
                return WatermarkOutputMode.AfterAllData;
            }
            return WatermarkOutputMode.AfterAllData;
        }


        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            _initialSent = await stateManagerClient.GetOrCreateObjectStateAsync<bool>("initial_sent");

            _readRelation.Hint.Optimizations.LogOnUnknownOptimization(Logger, WatermarkModeKey);
            _watermarkOutputMode = GetWatermarkOutputMode();

            if (_eventsCounter == null)
            {
                _eventsCounter = this.Metrics.CreateCounter<long>("events");
            }

            _primaryKeyColumns = await GetPrimaryKeyColumns();
            _otherColumns = [];
            _emitList = [];
            _deleteTreeToPersistentColumns = new IColumn[_readRelation.BaseSchema.Names.Count];
            _deleteTreeTopersistentBatch = new EventBatchData(_deleteTreeToPersistentColumns);

            if (_readRelation.EmitSet)
            {
                _emitList = _readRelation.Emit;
                _sortedEmitList = new List<int>(_readRelation.Emit);
                _sortedEmitList.Sort();
                for (int i = 0; i < _readRelation.Emit.Count; i++)
                {
                    if (!_primaryKeyColumns.Contains(_readRelation.Emit[i]))
                    {
                        _otherColumns.Add(_readRelation.Emit[i]);
                        _deleteTreeToPersistentColumns[i] = new AlwaysNullColumn();
                    }
                }
            }
            else
            {
                _emitList = new List<int>();
                for (int i = 0; i < _readRelation.OutputLengthWithoutRowStore; i++)
                {
                    _emitList.Add(i);
                    if (!_primaryKeyColumns.Contains(i))
                    {
                        _otherColumns.Add(i);
                        _deleteTreeToPersistentColumns[i] = new AlwaysNullColumn();
                    }
                }
            }

            List<int> deleteTreeKeys = new List<int>();
            for (int i = 0; i < _primaryKeyColumns.Count; i++)
            {
                deleteTreeKeys.Add(i);
            }

            if (_rowStoreHandle != null)
            {
                // A fetch that is reading the store ends first
                await _rowStoreHandle.CloseAsync();
            }
            _host?.Dispose();
            _host = null;
            _hostedTempKeys = null;
            _hostedDeleteKeys = null;
            var hostLayout = RowStoreHost.ReadLayout(_readRelation.RowStore, _readRelation.BaseSchema.Names.Count, _primaryKeyColumns, _emitList);
            if (hostLayout != null)
            {
                // Rows are stored behind references, every tree call is a batch
                _host = await RowStoreHost.CreateAsync(stateManagerClient, "persistent_keyed", "rows", hostLayout, new RowStoreOptions() { Mode = AppendOnly ? RowStoreMode.Append : RowStoreMode.Keyed, Heap = RowHeapOptions ?? new RowHeapOptions() { WriteQueueCapacity = _rowStoreHandle?.WriteQueuePages ?? 16 } }, MemoryAllocator);
                if (!_host.AppendOnly)
                {
                    _hostedTempKeys = await RowStoreKeySet.CreateAsync(stateManagerClient, "full_load_temp", _primaryKeyColumns.Count, MemoryAllocator);
                    _hostedDeleteKeys = await RowStoreKeySet.CreateAsync(stateManagerClient, "delete_keys", _primaryKeyColumns.Count, MemoryAllocator);
                }
                // Fetches may read the store from here on
                _rowStoreHandle?.Open(_host);
                return;
            }
            _fullLoadTempTree = await stateManagerClient.GetOrCreateTree("full_load_temp",
                new BPlusTreeOptions<ColumnRowReference, ColumnRowReference, NormalizeKeyStorage, NormalizeValueStorage>()
                {
                    Comparer = new NormalizeTreeComparer(_primaryKeyColumns),
                    KeySerializer = new NormalizeKeyStorageSerializer(_primaryKeyColumns, MemoryAllocator),
                    // Only its keys are read
                    ValueSerializer = new NormalizeValueSerializer(new List<int>(), MemoryAllocator),
                    MemoryAllocator = MemoryAllocator,
                    UseByteBasedPageSizes = true
                });
            _persistentTree = await stateManagerClient.GetOrCreateTree("persistent",
                new BPlusTreeOptions<ColumnRowReference, ColumnRowReference, NormalizeKeyStorage, NormalizeValueStorage>()
                {
                    Comparer = new NormalizeTreeComparer(_primaryKeyColumns),
                    KeySerializer = new NormalizeKeyStorageSerializer(_primaryKeyColumns, MemoryAllocator),
                    ValueSerializer = new NormalizeValueSerializer(_otherColumns, MemoryAllocator),
                    MemoryAllocator = MemoryAllocator,
                    UseByteBasedPageSizes = true
                });
            _deleteTree = await stateManagerClient.GetOrCreateTree("delete",
                new BPlusTreeOptions<ColumnRowReference, int, NormalizeKeyStorage, PrimitiveListValueContainer<int>>()
                {
                    Comparer = new NormalizeTreeComparer(deleteTreeKeys),
                    KeySerializer = new NormalizeKeyStorageSerializer(deleteTreeKeys, MemoryAllocator),
                    ValueSerializer = new PrimitiveListValueContainerSerializer<int>(MemoryAllocator),
                    MemoryAllocator = MemoryAllocator,
                    UseByteBasedPageSizes = true
                });
        }

        protected override async Task OnCheckpoint(long checkpointTime)
        {
            Debug.Assert(_initialSent != null);
            if (_host != null)
            {
                await _host.BeginCheckpointAsync();
            }
            else
            {
                Debug.Assert(_persistentTree != null);
                await _persistentTree.Commit();
            }
            await Checkpoint(checkpointTime).ConfigureAwait(false);
            await _initialSent.Commit();
        }

        public override Task BeforeSaveCheckpoint()
        {
            // The sinks are done, retired rows go with this checkpoint
            if (_host != null)
            {
                return _host.CheckpointAsync();
            }
            return Task.CompletedTask;
        }

        public override async ValueTask DisposeAsync()
        {
            if (_rowStoreHandle != null)
            {
                await _rowStoreHandle.CloseAsync();
            }
            _host?.Dispose();
            _host = null;
            await base.DisposeAsync();
        }

        // Set by tests, the row store's page size and queue
        internal RowHeapOptions? RowHeapOptions { get; set; }

        void IRowStoreHostVertex.AttachRowStoreHandle(RowStoreHandle handle)
        {
            _rowStoreHandle = handle;
        }

        // Applies a batch, the caller holds its rent
        private async Task<bool> SendHosted(IngressOutput<StreamEventBatch> output, EventBatchWeighted data, bool zeroWeightIsRow)
        {
            Debug.Assert(_host != null);
            Debug.Assert(_emitList != null);

            var result = await _host.ApplyAsync(data.EventBatchData, data.Weights, _filter, zeroWeightIsRow);
            try
            {
                var (inserts, deletes) = _host.CreateBatches(data.EventBatchData, result, _emitList);
                return await SendBoth(output, inserts, deletes);
            }
            catch
            {
                // Stored and not sent, the checkpoint must not keep it
                _host.MarkFailed();
                throw;
            }
        }

        // Sends both batches, one that was not sent is released
        private async Task<bool> SendBoth(IngressOutput<StreamEventBatch> output, EventBatchWeighted? inserts, EventBatchWeighted? deletes)
        {
            Debug.Assert(_eventsCounter != null);
            bool sent = false;
            try
            {
                if (inserts != null)
                {
                    _eventsCounter.Add(inserts.Count);
                    await output.SendAsync(new StreamEventBatch(inserts));
                    inserts = null;
                    sent = true;
                }
                if (deletes != null)
                {
                    _eventsCounter.Add(deletes.Count);
                    await output.SendAsync(new StreamEventBatch(deletes));
                    deletes = null;
                    sent = true;
                }
            }
            finally
            {
                Release(inserts);
                Release(deletes);
            }
            return sent;
        }

        // Frees a batch nobody took, a taken one keeps its rents
        private static void Release(EventBatchWeighted? batch)
        {
            if (batch != null)
            {
                batch.Rent(1);
                batch.Return();
            }
        }

        private async Task DoDeltaLoadHosted(IngressOutput<StreamEventBatch> output)
        {
            await foreach (var e in DeltaLoad(output.EnterCheckpointLock, output.ExitCheckpointLock, output.CancellationToken))
            {
                if (e.BatchData != null)
                {
                    e.BatchData.Rent(1);
                    try
                    {
                        await SendHosted(output, e.BatchData, false);
                    }
                    finally
                    {
                        e.BatchData.Weights.Dispose();
                        e.BatchData.Iterations.Dispose();
                        e.BatchData.Return();
                    }
                }

                if (e.Watermark != null)
                {
                    await output.SendWatermark(e.Watermark);
                }

                ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
            }
        }

        // The key columns of a batch, in key order
        private EventBatchData HostedKeys(EventBatchData batch)
        {
            Debug.Assert(_primaryKeyColumns != null);
            var columns = new IColumn[_primaryKeyColumns.Count];
            for (int k = 0; k < columns.Length; k++)
            {
                columns[k] = batch.Columns[_primaryKeyColumns[k]];
            }
            return new EventBatchData(columns);
        }

        private Column[] CreateKeyColumns()
        {
            Debug.Assert(_primaryKeyColumns != null);
            var columns = new Column[_primaryKeyColumns.Count];
            for (int k = 0; k < columns.Length; k++)
            {
                columns[k] = Column.Create(MemoryAllocator);
            }
            return columns;
        }

        private static void DisposeColumns(Column[] columns)
        {
            for (int k = 0; k < columns.Length; k++)
            {
                columns[k].Dispose();
            }
        }

        private async Task DoFullLoadHosted(IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(_host != null);
            Debug.Assert(_initialSent != null);

            if (_host.AppendOnly && _initialSent.Value)
            {
                throw new InvalidOperationException($"Source '{_readRelation.NamedTable.DotSeperated}' is append-only and stores its rows behind references, it is loaded once.");
            }

            // Lock checkpointing until the full load is complete
            await output.EnterCheckpointLock();
            bool sentData = false;
            bool initialLoad = !_initialSent.Value;
            try
            {
                long lastWatermark = -1;
                long batchId = 0;

                await foreach (var columnReadEvent in FullLoad(output.CancellationToken))
                {
                    lastWatermark = columnReadEvent.Watermark;
                    var data = columnReadEvent.BatchData;
                    data.Rent(1);
                    try
                    {
                        for (int i = 0; i < data.Count; i++)
                        {
                            if (data.Weights[i] < 0)
                            {
                                throw new NotSupportedException("Full load does not support deletions");
                            }
                        }
                        if (_hostedTempKeys != null && _initialSent.Value)
                        {
                            await _hostedTempKeys.AddAsync(HostedKeys(data.EventBatchData), data.Count);
                        }
                        // A loaded row of weight zero is a row
                        if (await SendHosted(output, data, true))
                        {
                            sentData = true;
                            if (_watermarkOutputMode == WatermarkOutputMode.OnEachBatch)
                            {
                                await output.SendWatermark(CreateWatermark(lastWatermark, batchId));
                                batchId++;
                            }
                        }
                    }
                    finally
                    {
                        data.Weights.Dispose();
                        data.Iterations.Dispose();
                        data.Return();
                    }
                }

                if (_hostedTempKeys != null && _initialSent.Value)
                {
                    await FindDeletedKeysHosted();
                    await _hostedTempKeys.Clear();
                    sentData |= await OutputDeletedRowsFromFullLoadHosted(output);
                }

                if (sentData && lastWatermark == -1)
                {
                    lastWatermark = 1;
                }

                if (lastWatermark >= 0 && sentData)
                {
                    await output.SendWatermark(CreateWatermark(lastWatermark, batchId));
                }
                // Saved in the same checkpoint as the loaded rows
                _initialSent.Value = true;
            }
            catch
            {
                // Half a load is never saved
                _host.MarkFailed();
                throw;
            }
            finally
            {
                output.ExitCheckpointLock();
            }

            if (sentData || initialLoad)
            {
                ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
            }
        }

        // Keys the load lacked go to the delete keys
        private async Task FindDeletedKeysHosted()
        {
            Debug.Assert(_host != null);
            Debug.Assert(_hostedTempKeys != null);
            Debug.Assert(_hostedDeleteKeys != null);
            Debug.Assert(_primaryKeyColumns != null);

            const int BatchRows = 1024;
            var value = new DataValueContainer();
            var missing = CreateKeyColumns();
            int count = 0;
            try
            {
                using var tmpIterator = _hostedTempKeys.CreateIterator();
                using var storedIterator = _host.CreateKeyIterator();
                await tmpIterator.SeekFirst();
                await storedIterator.SeekFirst();

                var tmpEnumerator = IteratePerRow(tmpIterator).GetAsyncEnumerator();
                var storedEnumerator = IteratePerRow(storedIterator).GetAsyncEnumerator();
                var hasNew = await tmpEnumerator.MoveNextAsync();
                var hasOld = await storedEnumerator.MoveNextAsync();

                while (hasOld)
                {
                    int comparison = hasNew ? CompareKeys(tmpEnumerator.Current, storedEnumerator.Current) : 1;
                    if (comparison < 0)
                    {
                        hasNew = await tmpEnumerator.MoveNextAsync();
                    }
                    else if (comparison > 0)
                    {
                        var key = storedEnumerator.Current;
                        for (int k = 0; k < missing.Length; k++)
                        {
                            key.referenceBatch.Columns[k].GetValueAt(key.RowIndex, value, default);
                            missing[k].Add(value);
                        }
                        count++;
                        hasOld = await storedEnumerator.MoveNextAsync();
                        if (count == BatchRows)
                        {
                            await _hostedDeleteKeys.AddAsync(new EventBatchData(missing), count);
                            DisposeColumns(missing);
                            missing = CreateKeyColumns();
                            count = 0;
                        }
                    }
                    else
                    {
                        hasNew = await tmpEnumerator.MoveNextAsync();
                        hasOld = await storedEnumerator.MoveNextAsync();
                    }
                }
                if (count > 0)
                {
                    await _hostedDeleteKeys.AddAsync(new EventBatchData(missing), count);
                }
            }
            finally
            {
                DisposeColumns(missing);
            }
        }

        // Removes the delete keys a page at a time
        private async Task<bool> OutputDeletedRowsFromFullLoadHosted(IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(_host != null);
            Debug.Assert(_hostedDeleteKeys != null);
            Debug.Assert(_emitList != null);

            bool sentData = false;
            var value = new DataValueContainer();
            using (var deleteIterator = _hostedDeleteKeys.CreateIterator())
            {
                await deleteIterator.SeekFirst();
                await foreach (var page in deleteIterator)
                {
                    var keys = CreateKeyColumns();
                    try
                    {
                        int count = 0;
                        foreach (var kv in page)
                        {
                            for (int k = 0; k < keys.Length; k++)
                            {
                                kv.Key.referenceBatch.Columns[k].GetValueAt(kv.Key.RowIndex, value, default);
                                keys[k].Add(value);
                            }
                            count++;
                        }
                        var result = _host.CreateResult();
                        EventBatchWeighted? deletes;
                        try
                        {
                            await _host.RemoveAsync(new EventBatchData(keys), count, result);
                            deletes = _host.CreateDeleteBatch(keys, result, _emitList);
                        }
                        catch
                        {
                            // The rows may be gone and were not sent
                            _host.MarkFailed();
                            result.Dispose();
                            throw;
                        }
                        try
                        {
                            sentData |= await SendBoth(output, null, deletes);
                        }
                        catch
                        {
                            _host.MarkFailed();
                            throw;
                        }
                    }
                    finally
                    {
                        DisposeColumns(keys);
                    }
                }
            }
            await _hostedDeleteKeys.Clear();
            return sentData;
        }

        protected abstract Task Checkpoint(long checkpointTime);

        protected abstract IAsyncEnumerable<ColumnReadEvent> FullLoad(CancellationToken cancellationToken, CancellationToken enumeratorCancellationToken = default);

        /// <summary>
        /// Called when loading delta, the enterCheckpointLock must be called by the the source to make sure that the checkpoint lock is held.
        /// The lock is released after the data has been sent out completely on the stream.
        /// </summary>
        /// <param name="lastWatermark"></param>
        /// <param name="EnterCheckpointLock"></param>
        /// <returns></returns>
        protected abstract IAsyncEnumerable<DeltaReadEvent> DeltaLoad(Func<Task> EnterCheckpointLock, Action ExitCheckpointLock, CancellationToken cancellationToken, CancellationToken enumeratorCancellationToken = default);

        protected abstract ValueTask<List<int>> GetPrimaryKeyColumns();

        /// <summary>
        /// True when the source only adds rows.
        /// </summary>
        protected virtual bool AppendOnly => false;

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

        private int CompareKeys(ColumnRowReference x, ColumnRowReference y)
        {
            Debug.Assert(_primaryKeyColumns != null);
            for (int i = 0; i < _primaryKeyColumns.Count; i++)
            {
                var compareResult = DataValueComparer.Instance.Compare(
                    x.referenceBatch.Columns[i].GetValueAt(x.RowIndex, default),
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

        protected async Task DoDeltaLoad(IngressOutput<StreamEventBatch> output)
        {
            if (_host != null)
            {
                await DoDeltaLoadHosted(output);
                return;
            }
            Debug.Assert(_persistentTree != null);
            Debug.Assert(_otherColumns != null);
            Debug.Assert(_emitList != null);
            Debug.Assert(_primaryKeyColumns != null);
            Debug.Assert(_eventsCounter != null);

            await foreach (var e in DeltaLoad(output.EnterCheckpointLock, output.ExitCheckpointLock, output.CancellationToken))
            {
                if (e.BatchData != null)
                {
                    e.BatchData.Rent(1);
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
                                    // The filter may read columns that are not stored
                                    bool passes = _filter == null || _filter(input.referenceBatch, input.RowIndex);
                                    if (passes && CompareRowReference(input, current) == 0)
                                    {
                                        return (current, GenericWriteOperation.None);
                                    }
                                    bool updated = false;
                                    if (passes)
                                    {
                                        weights.Add(1);
                                        iterations.Add(0);
                                        toEmitOffsets.Add(input.RowIndex);
                                        updated = true;
                                    }
                                    deleteBatchKeyOffsets.Add(input.RowIndex);
                                    for (int k = 0; k < _otherColumns.Count; k++)
                                    {
                                        var value = current.referenceBatch.Columns[k].GetValueAt(current.RowIndex, default);
                                        deleteBatchColumns[k].Add(value);
                                    }
                                    return (input, updated ? GenericWriteOperation.Upsert : GenericWriteOperation.Delete);
                                }
                                // Does not exist
                                if (_filter == null || _filter(input.referenceBatch, input.RowIndex))
                                {
                                    weights.Add(1);
                                    iterations.Add(0);
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
                        IColumn[] columns = new IColumn[_emitList.Count];
                        bool shouldDisposeOffsets = true;
                        for (int k = 0; k < _emitList.Count; k++)
                        {
                            columns[k] = ColumnWithOffset.CreateFlattened(e.BatchData.EventBatchData.Columns[_emitList[k]], toEmitOffsets, MemoryAllocator, out var offsetUsed);
                            if (offsetUsed)
                            {
                                shouldDisposeOffsets = false;
                            }
                        }
                        DisposeUnusedColumns(e.BatchData.EventBatchData);
                        if (shouldDisposeOffsets)
                        {
                            toEmitOffsets.Dispose();
                        }
                        // Send out the data
                        _eventsCounter.Add(weights.Count);
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
                        PrimitiveList<int> deleteWeights = new PrimitiveList<int>(MemoryAllocator);
                        PrimitiveList<uint> deleteIterations = new PrimitiveList<uint>(MemoryAllocator);

                        for (int i = 0; i < deleteBatchKeyOffsets.Count; i++)
                        {
                            deleteWeights.Add(-1);
                            deleteIterations.Add(0);
                        }
                        bool shouldDisposeOffsets = true;
                        IColumn[] deleteColumns = new IColumn[_emitList.Count];
                        for (int i = 0; i < _primaryKeyColumns.Count; i++)
                        {
                            var emitIndex = _emitList.IndexOf(_primaryKeyColumns[i]);
                            if (emitIndex >= 0)
                            {
                                deleteColumns[emitIndex] = ColumnWithOffset.CreateFlattened(e.BatchData.EventBatchData.Columns[_primaryKeyColumns[i]], deleteBatchKeyOffsets, MemoryAllocator, out var offsetUsed);
                                if (offsetUsed)
                                {
                                    shouldDisposeOffsets = false;
                                }
                            }
                        }
                        if (shouldDisposeOffsets)
                        {
                            deleteBatchKeyOffsets.Dispose();
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
                        _eventsCounter.Add(deleteWeights.Count);
                        await output.SendAsync(outputBatch);
                    }
                    else
                    {
                        deleteBatchKeyOffsets.Dispose();
                    }

                    e.BatchData.Weights.Dispose();
                    e.BatchData.Iterations.Dispose();
                    e.BatchData.Return();
                }

                if (e.Watermark != null)
                {
                    await output.SendWatermark(e.Watermark);
                }

                ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
            }
        }

        private void DisposeUnusedColumns(EventBatchData batchData)
        {
            // If we emit all columns (no EmitSet), there is nothing to dispose.
            if (_sortedEmitList == null || _sortedEmitList.Count == batchData.Columns.Count)
            {
                return;
            }

            for (int k = 0, sortIndex = 0; k < batchData.Columns.Count; k++)
            {
                // Go through sorted emit list and check if the column is in the emit list, if not, we need to dispose it
                // Since its sorted we do a merge check
                if (sortIndex < _sortedEmitList.Count && _sortedEmitList[sortIndex] == k)
                {
                    sortIndex++;
                }
                else
                {
                    batchData.Columns[k].Dispose();
                }
            }
        }

        protected ValueTask<(bool found, ColumnRowReference value)> LookupRowValue(ColumnRowReference rowKeyReference)
        {
            if (_host != null)
            {
                return LookupRowValueHosted(rowKeyReference);
            }
            Debug.Assert(_persistentTree != null);
            return _persistentTree.GetValue(in rowKeyReference);
        }

        /// <summary>
        /// True when the caller disposes a looked up row's batch.
        /// </summary>
        protected bool LookupRowIsOwned => _host != null;

        private async ValueTask<(bool found, ColumnRowReference value)> LookupRowValueHosted(ColumnRowReference rowKeyReference)
        {
            Debug.Assert(_host != null);
            Debug.Assert(_otherColumns != null);
            Debug.Assert(_primaryKeyColumns != null);

            var columns = await _host.LookupAsync(rowKeyReference);
            if (columns == null)
            {
                return (false, default);
            }
            // The value is the other columns, as the tree gives them
            var value = new IColumn[_otherColumns.Count];
            var used = new bool[columns.Length];
            for (int i = 0; i < value.Length; i++)
            {
                // A column sent twice is two columns for the caller
                value[i] = used[_otherColumns[i]] ? columns[_otherColumns[i]].Copy(MemoryAllocator) : columns[_otherColumns[i]];
                used[_otherColumns[i]] = true;
            }
            for (int i = 0; i < columns.Length; i++)
            {
                if (!used[i])
                {
                    columns[i].Dispose();
                }
            }
            return (true, new ColumnRowReference() { referenceBatch = new EventBatchData(value), RowIndex = 0 });
        }

        private Watermark CreateWatermark(long watermarkValue, long batchId = 0)
        {
            var watermark = LongWatermarkValue.Create(watermarkValue);
            watermark.BatchID = batchId;
            return new Watermark(_readRelation.NamedTable.DotSeperated, watermark);
        }

        protected async Task DoFullLoad(IngressOutput<StreamEventBatch> output)
        {
            if (_host != null)
            {
                await DoFullLoadHosted(output);
                return;
            }
            Debug.Assert(_fullLoadTempTree != null);
            Debug.Assert(_persistentTree != null);
            Debug.Assert(_otherColumns != null);
            Debug.Assert(_deleteTree != null);
            Debug.Assert(_emitList != null);
            Debug.Assert(_primaryKeyColumns != null);
            Debug.Assert(_eventsCounter != null);
            Debug.Assert(_initialSent != null);

            // Lock checkpointing until the full load is complete
            await output.EnterCheckpointLock();

            long lastWatermark = -1;
            bool sentData = false;

            long batchId = 0;

            await foreach (var columnReadEvent in FullLoad(output.CancellationToken))
            {
                // Held until the inserts and the deletes are both sent
                columnReadEvent.BatchData.Rent(1);
                try
                {
                    PrimitiveList<int> toEmitOffsets = new PrimitiveList<int>(MemoryAllocator);
                    PrimitiveList<int> weights = new PrimitiveList<int>(MemoryAllocator);
                    PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);
                    PrimitiveList<int> deleteBatchKeyOffsets = new PrimitiveList<int>(MemoryAllocator);

                    List<IColumn> deleteBatchColumns = new List<IColumn>();

                    lastWatermark = columnReadEvent.Watermark;

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

                        // Full load temp tree is only used to calculate deltas between full loads
                        // Initial time there is no requirement to save it
                        if (_initialSent.Value)
                        {
                            await _fullLoadTempTree.Upsert(columnRef, columnRef);
                        }

                        await _persistentTree.RMWNoResult(columnRef, columnRef, (input, current, exists) =>
                        {
                            if (exists)
                            {
                                // An equal row that fails the filter is taken back
                                bool passes = _filter == null || _filter(input.referenceBatch, input.RowIndex);
                                if (!passes || CompareRowReference(input, current) != 0)
                                {
                                    bool updated = false;
                                    if (passes)
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
                                weights.Add(1);
                                iterations.Add(0);
                                toEmitOffsets.Add(input.RowIndex);
                                return (input, GenericWriteOperation.Upsert);
                            }
                            return (input, GenericWriteOperation.None);
                        });
                    }

                    if (weights.Count > 0)
                    {
                        IColumn[] columns = new IColumn[_emitList.Count];
                        bool shouldDisposeOffsets = true;
                        for (int k = 0; k < _emitList.Count; k++)
                        {
                            columns[k] = ColumnWithOffset.CreateFlattened(columnReadEvent.BatchData.EventBatchData.Columns[_emitList[k]], toEmitOffsets, MemoryAllocator, out var offsetUsed);
                            if (offsetUsed)
                            {
                                shouldDisposeOffsets = false;
                            }
                        }
                        DisposeUnusedColumns(columnReadEvent.BatchData.EventBatchData);
                        if (shouldDisposeOffsets)
                        {
                            toEmitOffsets.Dispose();
                        }
                        // Send out the data
                        _eventsCounter.Add(weights.Count);
                        await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns))));
                        sentData = true;

                        if (_watermarkOutputMode == WatermarkOutputMode.OnEachBatch)
                        {
                            // If we are in ON_EACH_BATCH mode, we emit a watermark after each batch
                            await output.SendWatermark(CreateWatermark(lastWatermark, batchId));
                            batchId++;
                        }
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
                        bool shouldDisposeOffsets = true;
                        IColumn[] deleteColumns = new IColumn[_emitList.Count];
                        for (int i = 0; i < _primaryKeyColumns.Count; i++)
                        {
                            var emitIndex = _emitList.IndexOf(_primaryKeyColumns[i]);
                            if (emitIndex >= 0)
                            {
                                deleteColumns[emitIndex] = ColumnWithOffset.CreateFlattened(columnReadEvent.BatchData.EventBatchData.Columns[_primaryKeyColumns[i]], deleteBatchKeyOffsets, MemoryAllocator, out var offsetUsed);
                                if (offsetUsed)
                                {
                                    shouldDisposeOffsets = false;
                                }
                            }
                        }
                        if (shouldDisposeOffsets)
                        {
                            deleteBatchKeyOffsets.Dispose();
                        }
                        for (int i = 0; i < _otherColumns.Count; i++)
                        {
                            var emitIndex = _emitList.IndexOf(_otherColumns[i]);
                            if (emitIndex >= 0)
                            {
                                deleteColumns[emitIndex] = deleteBatchColumns[i];
                            }
                        }

                        _eventsCounter.Add(deleteWeights.Count);
                        var outputBatch = new StreamEventBatch(new EventBatchWeighted(deleteWeights, deleteIterations, new EventBatchData(deleteColumns)));
                        await output.SendAsync(outputBatch);
                        sentData = true;
                    }
                    else
                    {
                        deleteBatchKeyOffsets.Dispose();
                    }
                }
                finally
                {
                    columnReadEvent.BatchData.Weights.Dispose();
                    columnReadEvent.BatchData.Iterations.Dispose();
                    columnReadEvent.BatchData.Return();
                }
            }

            // We only compute delta between full loads after the initial batch
            // The initial batch does not have any existing data
            if (_initialSent.Value)
            {
                using var tmpIterator = _fullLoadTempTree.CreateIterator();
                using var persistentIterator = _persistentTree.CreateIterator();
                await tmpIterator.SeekFirst();
                await persistentIterator.SeekFirst();

                var tmpEnumerator = IteratePerRow(tmpIterator).GetAsyncEnumerator();
                var persistentEnumerator = IteratePerRow(persistentIterator).GetAsyncEnumerator();

                var hasNew = await tmpEnumerator.MoveNextAsync();
                var hasOld = await persistentEnumerator.MoveNextAsync();

                // Go through both trees and find deletions
                while (hasNew || hasOld)
                {
                    int comparison = hasNew && hasOld ? CompareKeys(tmpEnumerator.Current, persistentEnumerator.Current) : 0;

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

                sentData |= await OutputDeletedRowsFromFullLoad(output);
            }

            if (sentData && lastWatermark == -1)
            {
                // This happens if all existing data was deleted, a watermark of 1 is added for now
                lastWatermark = 1;
            }

            if (lastWatermark >= 0 && sentData)
            {
                await output.SendWatermark(CreateWatermark(lastWatermark, batchId));
            }

            // Exit the checkpoint lock
            output.ExitCheckpointLock();

            if (sentData || !_initialSent.Value)
            {
                // Schedule a checkpoint
                ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
            }
        }

        private async Task<bool> OutputDeletedRowsFromFullLoad(IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(_deleteTree != null);
            Debug.Assert(_otherColumns != null);
            Debug.Assert(_persistentTree != null);
            Debug.Assert(_primaryKeyColumns != null);
            Debug.Assert(_deleteTreeToPersistentColumns != null);
            Debug.Assert(_deleteTreeTopersistentBatch != null);
            Debug.Assert(_emitList != null);
            Debug.Assert(_eventsCounter != null);

            bool sentData = false;

            PrimitiveList<int> weights = new PrimitiveList<int>(MemoryAllocator);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);

            // Indexed by base schema, only the emitted slots are built since only they are sent.
            var isEmitted = new bool[_readRelation.BaseSchema.Names.Count];
            for (int k = 0; k < _emitList.Count; k++)
            {
                isEmitted[_emitList[k]] = true;
            }
            IColumn[] deleteBatchColumns = new IColumn[_readRelation.BaseSchema.Names.Count];
            CreateEmittedColumns();

            using var deleteIterator = _deleteTree.CreateIterator();
            await deleteIterator.SeekFirst();

            await foreach (var page in deleteIterator)
            {
                foreach (var kv in page)
                {
                    for (int k = 0; k < _primaryKeyColumns.Count; k++)
                    {
                        _deleteTreeToPersistentColumns[_primaryKeyColumns[k]] = kv.Key.referenceBatch.Columns[k];
                    }

                    var columnRef = new ColumnRowReference()
                    {
                        referenceBatch = _deleteTreeTopersistentBatch,
                        RowIndex = kv.Key.RowIndex
                    };
                    // Go through the deletions and delete them from the persistent tree
                    var operation = await _persistentTree.RMWNoResult(columnRef, default, (input, current, exists) =>
                    {
                        if (exists)
                        {
                            // Output delete event
                            for (int k = 0; k < _otherColumns.Count; k++)
                            {
                                if (isEmitted[_otherColumns[k]])
                                {
                                    deleteBatchColumns[_otherColumns[k]].Add(current.referenceBatch.Columns[k].GetValueAt(current.RowIndex, default));
                                }
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
                            if (isEmitted[_primaryKeyColumns[i]])
                            {
                                deleteBatchColumns[_primaryKeyColumns[i]].Add(kv.Key.referenceBatch.Columns[i].GetValueAt(kv.Key.RowIndex, default));
                            }
                        }
                    }
                    if (weights.Count >= 100)
                    {
                        IColumn[] columns = new IColumn[_emitList.Count];

                        for (int k = 0; k < _emitList.Count; k++)
                        {
                            columns[k] = deleteBatchColumns[_emitList[k]];
                        }

                        _eventsCounter.Add(weights.Count);
                        await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns))));
                        sentData = true;
                        // Reset
                        weights = new PrimitiveList<int>(MemoryAllocator);
                        iterations = new PrimitiveList<uint>(MemoryAllocator);
                        // The sent columns belong to the batch now, every emitted slot needs a new one.
                        CreateEmittedColumns();
                    }
                }
            }

            if (weights.Count > 0)
            {
                IColumn[] columns = new IColumn[_emitList.Count];

                for (int k = 0; k < _emitList.Count; k++)
                {
                    columns[k] = deleteBatchColumns[_emitList[k]];
                }
                _eventsCounter.Add(weights.Count);
                await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns))));
                sentData = true;
            }
            else
            {
                weights.Dispose();
                iterations.Dispose();

                for (int i = 0; i < deleteBatchColumns.Length; i++)
                {
                    if (isEmitted[i])
                    {
                        deleteBatchColumns[i].Dispose();
                    }
                }
            }

            await _deleteTree.Clear();
            return sentData;

            void CreateEmittedColumns()
            {
                for (int i = 0; i < deleteBatchColumns.Length; i++)
                {
                    if (isEmitted[i])
                    {
                        deleteBatchColumns[i] = Column.Create(MemoryAllocator);
                    }
                }
            }
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(_initialSent != null);
            if (!_initialSent.Value)
            {
                // Only do full load if we have not done it before
                await DoFullLoad(output);
                _initialSent.Value = true;
            }

            await RegisterTrigger(DeltaLoadTriggerName, DeltaLoadInterval);
            await RegisterTrigger(FullLoadTriggerName, FullLoadInterval);
            await RegisterTrigger(_deltaloadWithTableTriggerName);
            await RegisterTrigger(_fullLoadWithTableTriggerName);
        }

        protected virtual TimeSpan? DeltaLoadInterval { get; set; }

        protected virtual TimeSpan? FullLoadInterval { get; set; }

    }
}
