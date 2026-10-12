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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Relations;
using Microsoft.Extensions.Logging;
using Stowage;
using System.Diagnostics;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.DeltaLake.Internal
{
    internal class DeltaLakeSourceState
    {
        public long CurrentVersion { get; set; }
    }

    internal enum DeltaLoadOutcome
    {
        Loaded,
        Halted
    }

    internal class DeltaLakeSource : ReadBaseOperator
    {
        private const string DeltaLoadName = "delta_load";

        private readonly ReadRelation _readRelation;
        private readonly DeltaLakeOptions _options;
        private IObjectState<DeltaLakeSourceState>? _state;
        private DeltaTable? _table;
        private IDeltaFormatReader? _reader;
        private string _tableName;
        private IOPath _tableLoc;
        private Task? _deltaLoadTask;
        private object _deltaLoadLock = new object();

        private bool _hasCheckpointed = false;

        // Sticky for the run, the log no longer holds what this source needs
        private volatile bool _halted;
        private TruncationCheckSchedule? _truncationSchedule;
        // Set by skipped versions, a checkpoint has to persist them
        private volatile bool _skippedSinceCheckpoint;

        private IBPlusTree<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>? _changesTree;

        // Metrics
        private ICounter<long>? _eventsCounter;
        private ICounter<long>? _eventsProcessed;

        public DeltaLakeSource(ReadRelation readRelation, DeltaLakeOptions options, DataflowBlockOptions blockOptions) : base(blockOptions)
        {
            this._readRelation = readRelation;
            this._options = options;
            _tableName = string.Join("/", readRelation.NamedTable.Names);
            _tableLoc = _tableName;
        }

        public override string DisplayName => "DeltaLakeTable";

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override Task OnTrigger(string triggerName, object? state)
        {
            if (triggerName == DeltaLoadName)
            {
                lock (_deltaLoadLock)
                {
                    _deltaLoadTask ??= RunTask(LoadDelta)
                        .ContinueWith(task =>
                        {
                            lock (_deltaLoadLock)
                            {
                                _deltaLoadTask = null;
                            }
                        });
                }
            }
            return Task.CompletedTask;
        }

        private async Task<DeltaLoadOutcome> LoadCdcData(DeltaCommit deltaCommit, IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(_state?.Value != null);
            Debug.Assert(_reader != null);
            Debug.Assert(_eventsCounter != null);
            Debug.Assert(_eventsProcessed != null);

            await output.EnterCheckpointLock();

            // Batches are sent per file, so every file must exist before the first is sent, the first fails on open
            for (int i = 1; i < deltaCommit.CdcFiles.Count; i++)
            {
                var path = deltaCommit.CdcFiles[i].Path!;
                if (!await _options.StorageLocation.Exists(_tableLoc.Combine(path)))
                {
                    output.ExitCheckpointLock();
                    HaltOnMissingFile(path);
                    return DeltaLoadOutcome.Halted;
                }
            }

            var sent = false;
            try
            {
                foreach (var file in deltaCommit.CdcFiles)
                {
                    Debug.Assert(file.Path != null);
                    var batches = _reader.ReadCdcFile(_options.StorageLocation, _tableLoc, file.Path, file.PartitionValues, MemoryAllocator);

                    await foreach (var batch in batches)
                    {
                        PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);
                        iterations.InsertStaticRange(0, 0, (int)batch.count);

                        _eventsCounter.Add(batch.count);
                        _eventsProcessed.Add(batch.count);
                        sent = true;
                        await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(batch.weights, iterations, batch.data)));
                    }
                }
            }
            // Removed after the check: once rows were sent the failure rolls them back and the retry halts at the check
            catch (DeltaFileNotFoundException e) when (!sent)
            {
                output.ExitCheckpointLock();
                HaltOnMissingFile(e.Path);
                return DeltaLoadOutcome.Halted;
            }

            _hasCheckpointed = false;
            _state.Value.CurrentVersion = _state.Value.CurrentVersion + 1;
            await output.SendWatermark(new Base.Watermark(_tableName, LongWatermarkValue.Create(_state.Value.CurrentVersion)));
            ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
            output.ExitCheckpointLock();
            return DeltaLoadOutcome.Loaded;
        }

        /// <summary>
        /// Reads both added and deleted files and compute the delta between them
        /// </summary>
        /// <param name="deltaCommit"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        private async Task<DeltaLoadOutcome> LoadFileData(DeltaCommit deltaCommit, IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(_state?.Value != null);
            Debug.Assert(_changesTree != null);

            await output.EnterCheckpointLock();
            try
            {
                await BufferFileChanges(deltaCommit);
            }
            // Nothing was sent yet, the buffered changes are dropped
            catch (DeltaFileNotFoundException e)
            {
                await _changesTree.Clear();
                output.ExitCheckpointLock();
                HaltOnMissingFile(e.Path);
                return DeltaLoadOutcome.Halted;
            }
            await SendBufferedChanges(output);

            _hasCheckpointed = false;
            _state.Value.CurrentVersion = _state.Value.CurrentVersion + 1;
            await output.SendWatermark(new Base.Watermark(_tableName, LongWatermarkValue.Create(_state.Value.CurrentVersion)));
            ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
            output.ExitCheckpointLock();
            return DeltaLoadOutcome.Loaded;
        }

        // Reads both added and removed files into the changes tree, nothing is sent
        private async Task BufferFileChanges(DeltaCommit deltaCommit)
        {
            Debug.Assert(_reader != null);
            Debug.Assert(_changesTree != null);

            var deletionVectorFiles = deltaCommit.RemovedFiles.Select(x => x.Path).Intersect(deltaCommit.AddedFiles.Select(x => x.Path)).ToList();

            var addedFiles = deltaCommit.AddedFiles.Where(x => !deletionVectorFiles.Contains(x.Path)).ToList();
            var removedFiles = deltaCommit.RemovedFiles.Where(x => !deletionVectorFiles.Contains(x.Path)).ToList();

            foreach (var file in addedFiles)
            {
                if (!file.DataChange)
                {
                    continue;
                }
                Debug.Assert(file.Path != null);
                IDeleteVector? deleteVector;
                if (file.DeletionVector != null)
                {
                    deleteVector = await DeletionVectorReader.ReadDeletionVector(_options.StorageLocation, _tableLoc, file.DeletionVector);
                }
                else
                {
                    deleteVector = EmptyDeleteVector.Instance;
                }

                var batches = _reader.ReadDataFile(_options.StorageLocation, _tableLoc, file.Path, deleteVector, file.PartitionValues, MemoryAllocator);

                await foreach (var batch in batches)
                {
                    for (int i = 0; i < batch.count; i++)
                    {
                        await _changesTree.RMWNoResult(new ColumnRowReference() { referenceBatch = batch.data, RowIndex = i }, 1, (input, current, exists) =>
                        {
                            if (exists)
                            {
                                var newWeight = current + input;

                                if (newWeight == 0)
                                {
                                    return (0, GenericWriteOperation.Delete);
                                }

                                return (newWeight, GenericWriteOperation.Upsert);
                            }
                            else
                            {
                                return (input, GenericWriteOperation.Upsert);
                            }
                        });
                    }
                    batch.data.Dispose();
                }
            }

            foreach (var file in removedFiles)
            {
                if (!file.DataChange)
                {
                    continue;
                }
                Debug.Assert(file.Path != null);
                IDeleteVector? deleteVector;
                if (file.DeletionVector != null)
                {
                    deleteVector = await DeletionVectorReader.ReadDeletionVector(_options.StorageLocation, _tableLoc, file.DeletionVector);
                }
                else
                {
                    deleteVector = EmptyDeleteVector.Instance;
                }

                var batches = _reader.ReadDataFile(_options.StorageLocation, _tableLoc, file.Path, deleteVector, file.PartitionValues, MemoryAllocator);

                await foreach (var batch in batches)
                {
                    for (int i = 0; i < batch.count; i++)
                    {
                        await _changesTree.RMWNoResult(new ColumnRowReference() { referenceBatch = batch.data, RowIndex = i }, -1, (input, current, exists) =>
                        {
                            if (exists)
                            {
                                var newWeight = current + input;

                                if (newWeight == 0)
                                {
                                    return (0, GenericWriteOperation.Delete);
                                }

                                return (newWeight, GenericWriteOperation.Upsert);
                            }
                            else
                            {
                                return (input, GenericWriteOperation.Upsert);
                            }
                        });
                    }
                    batch.data.Dispose();
                }
            }

            foreach (var deletionVectorFile in deletionVectorFiles)
            {
                var addedFile = deltaCommit.AddedFiles.Single(x => x.Path == deletionVectorFile);
                var removedFile = deltaCommit.RemovedFiles.Single(x => x.Path == deletionVectorFile);

                if ((!addedFile.DataChange) && (!removedFile.DataChange))
                {
                    continue;
                }

                // Get the deletion vectors of the files
                IDeleteVector? addedFileDeleteVector;
                if (addedFile.DeletionVector != null)
                {
                    addedFileDeleteVector = await DeletionVectorReader.ReadDeletionVector(_options.StorageLocation, _tableLoc, addedFile.DeletionVector);
                }
                else
                {
                    addedFileDeleteVector = EmptyDeleteVector.Instance;
                }
                IDeleteVector? removedFileDeleteVector;
                if (removedFile.DeletionVector != null)
                {
                    removedFileDeleteVector = await DeletionVectorReader.ReadDeletionVector(_options.StorageLocation, _tableLoc, removedFile.DeletionVector);
                }
                else
                {
                    removedFileDeleteVector = EmptyDeleteVector.Instance;
                }

                // Get the differences between the deletion vectors, if a number is in the added file but not in the removed file, it should be a -1 weight
                // If it is in the removed file but not in the added file, it should be a 1 weight, this is like an insert
                var diffIterator = new DeletionVectorDiffIterator(removedFileDeleteVector, addedFileDeleteVector);
                var changedRowsIterator = _reader.ReadAddRemovedDataFile(_options.StorageLocation, _tableLoc, deletionVectorFile!, diffIterator, addedFile.PartitionValues, MemoryAllocator);

                await foreach (var batch in changedRowsIterator)
                {
                    // add them to the changes tree, since a row might have been added and removed in the same version, we need to keep track of the weight
                    // This removes any unneccessary duplicate rows.
                    for (int i = 0; i < batch.count; i++)
                    {
                        await _changesTree.RMWNoResult(new ColumnRowReference() { referenceBatch = batch.data, RowIndex = i }, batch.weights[i], (input, current, exists) =>
                        {
                            if (exists)
                            {
                                var newWeight = current + input;

                                if (newWeight == 0)
                                {
                                    return (0, GenericWriteOperation.Delete);
                                }

                                return (newWeight, GenericWriteOperation.Upsert);
                            }
                            else
                            {
                                return (input, GenericWriteOperation.Upsert);
                            }
                        });
                    }
                    batch.data.Dispose();
                    batch.weights.Dispose();
                }
            }
        }

        private async Task SendBufferedChanges(IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(_changesTree != null);
            Debug.Assert(_eventsCounter != null);
            Debug.Assert(_eventsProcessed != null);

            using var changesIterator = _changesTree.CreateIterator();
            await changesIterator.SeekFirst();

            await foreach (var page in changesIterator)
            {
                var pageColumns = page.Keys.Data.Columns;
                IColumn[] columns = new IColumn[pageColumns.Count];

                for (int i = 0; i < columns.Length; i++)
                {
                    columns[i] = pageColumns[i].Copy(MemoryAllocator);
                }

                var weights = page.Values.Data.Copy(MemoryAllocator);
                PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);
                iterations.InsertStaticRange(0, 0, weights.Count);

                _eventsCounter.Add(weights.Count);
                _eventsProcessed.Add(weights.Count);
                await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns))));
            }

            await _changesTree.Clear();
        }

        private async Task LoadDelta(IngressOutput<StreamEventBatch> output, object? state)
        {
            await LoadNewVersions(output);
            // Skipped versions send no watermark, a checkpoint still has to persist them
            if (_skippedSinceCheckpoint && !_halted)
            {
                ScheduleCheckpoint(_options.DeltaCheckInterval);
            }
        }

        private async Task LoadNewVersions(IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(_state?.Value != null);

            // Loop until we have read all new versions
            while (!_halted)
            {
                // If no checkpoint has been made and we want one version per checkpoint, return
                if (!_hasCheckpointed && _options.OneVersionPerCheckpoint)
                {
                    return;
                }

                var commitInfo = await DeltaTransactionReader.ReadVersionCommit(_options.StorageLocation, _tableLoc, _state.Value.CurrentVersion + 1);
                if (commitInfo == null)
                {
                    await CheckForTruncation();
                    return;
                }
                _truncationSchedule!.Loaded();

                if (!HasDataChange(commitInfo))
                {
                    _state.Value.CurrentVersion = _state.Value.CurrentVersion + 1;
                    _skippedSinceCheckpoint = true;
                    continue;
                }

                var outcome = commitInfo.CdcFiles.Count > 0 ? await LoadCdcData(commitInfo, output) : await LoadFileData(commitInfo, output);
                if (outcome == DeltaLoadOutcome.Halted)
                {
                    return;
                }
            }
        }

        // Change data actions are always dataChange=false, they still carry rows
        internal static bool HasDataChange(DeltaCommit commit)
        {
            return commit.CdcFiles.Count > 0 || commit.AddedFiles.Any(x => x.DataChange) || commit.RemovedFiles.Any(x => x.DataChange);
        }

        private async Task CheckForTruncation()
        {
            Debug.Assert(_state?.Value != null);
            Debug.Assert(_truncationSchedule != null);
            if (!_truncationSchedule.Miss())
            {
                return;
            }
            if (await NextCommitRemoved(_state.Value.CurrentVersion))
            {
                Halt($"commit {_state.Value.CurrentVersion + 1} is missing while later versions exist, the source fell behind the table's log retention");
            }
        }

        // A later version exists and the next commit is still absent after the listing, it can never come back
        private async Task<bool> NextCommitRemoved(long currentVersion)
        {
            var log = await DeltaTransactionReader.ListLog(_options.StorageLocation, _tableLoc);
            if (log.Head <= currentVersion && !log.Checkpoints.Any(x => x.Version > currentVersion))
            {
                return false;
            }
            return !await _options.StorageLocation.Exists(_tableLoc.Combine("_delta_log").Combine($"{currentVersion + 1:D20}.json"));
        }

        private void HaltOnMissingFile(string path)
        {
            Debug.Assert(_state?.Value != null);
            Halt($"file {path} of version {_state.Value.CurrentVersion + 1} is missing, it was removed (VACUUM) before the source read it");
        }

        private void Halt(string reason)
        {
            _halted = true;
            SetHealth(false);
            Logger.LogError("Delta Lake source for table {table} halted: {reason}. Reset the stream state to read the table again, the stream then sends every row of the table again.", _tableName, reason);
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            return Task.FromResult<IReadOnlySet<string>>(new HashSet<string>() { _tableName });
        }

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            // Vertices are reused on recovery
            _halted = false;
            _truncationSchedule = new TruncationCheckSchedule(_options.DeltaCheckInterval);
            _skippedSinceCheckpoint = false;
            _table = null;
            _reader = null;
            SetHealth(true);

            if (_eventsCounter == null)
            {
                _eventsCounter = Metrics.CreateCounter<long>("events");
            }
            if (_eventsProcessed == null)
            {
                _eventsProcessed = Metrics.CreateCounter<long>("events_processed");
            }

            _state = await stateManagerClient.GetOrCreateObjectStateAsync<DeltaLakeSourceState>("state");

            if (_state.Value == null)
            {
                _state.Value = new DeltaLakeSourceState()
                {
                    CurrentVersion = -1
                };
            }

            // Always fetch latest table to get all the latest column names
            var readOptions = new DeltaReadOptions() { SkipTombstones = true, Logger = Logger, ReportedCheckpointFailures = new HashSet<string>() };
            DeltaTable? latestTable = null;
            ExceptionDispatchInfo? unavailable = null;
            try
            {
                latestTable = await DeltaTransactionReader.ReadTable(_options.StorageLocation, _tableLoc, options: readOptions);
            }
            catch (DeltaVersionNotAvailableException e) when (_state.Value.CurrentVersion >= 0)
            {
                unavailable = ExceptionDispatchInfo.Capture(e);
            }

            if (unavailable != null)
            {
                // Every restart would fail the same way, a confirmed removed commit halts instead
                if (!await NextCommitRemoved(_state.Value.CurrentVersion))
                {
                    unavailable.Throw();
                }
                Halt($"commit {_state.Value.CurrentVersion + 1} is missing while later versions exist, the source fell behind the table's log retention");
            }
            else
            {
                if (latestTable == null)
                {
                    throw new InvalidOperationException($"Delta Lake Table {_tableName} does not exist");
                }
                // Only a first start reads a snapshot, a restore continues from its version
                if (_state.Value.CurrentVersion == -1)
                {
                    _table = _options.OneVersionPerCheckpoint ? await DeltaTransactionReader.ReadTable(_options.StorageLocation, _tableLoc, 0, readOptions) : latestTable;
                    if (_table == null)
                    {
                        throw new InvalidOperationException($"Delta Lake Table {_tableName} does not exist");
                    }
                }

                var reader = new ParquetSharpReader();
                reader.Initialize(latestTable, _readRelation.BaseSchema.Names);
                _reader = reader;
            }

            _changesTree = await stateManagerClient.GetOrCreateTree("changes", new BPlusTreeOptions<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>()
            {
                Comparer = new ColumnComparer(_readRelation.BaseSchema.Names.Count),
                KeySerializer = new ColumnStoreSerializer(_readRelation.BaseSchema.Names.Count, MemoryAllocator),
                MemoryAllocator = MemoryAllocator,
                ValueSerializer = new PrimitiveListValueContainerSerializer<int>(MemoryAllocator)
            });
        }

        protected override async Task OnCheckpoint(long checkpointTime)
        {
            Debug.Assert(_state != null);
            // Cleared before the commit, a skip after this point schedules another checkpoint
            _skippedSinceCheckpoint = false;
            await _state.Commit();
            _hasCheckpointed = true;
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(_state?.Value != null);
            Debug.Assert(_eventsCounter != null);
            Debug.Assert(_eventsProcessed != null);

            if (_state.Value.CurrentVersion == -1)
            {
                Debug.Assert(_table != null);
                Debug.Assert(_reader != null);
                await output.EnterCheckpointLock();
                foreach (var file in _table.AddFiles)
                {
                    Debug.Assert(file.Path != null);
                    IDeleteVector? deleteVector;
                    if (file.DeletionVector != null)
                    {
                        deleteVector = await DeletionVectorReader.ReadDeletionVector(_options.StorageLocation, _tableLoc, file.DeletionVector);
                    }
                    else
                    {
                        deleteVector = EmptyDeleteVector.Instance;
                    }

                    var batches = _reader.ReadDataFile(_options.StorageLocation, _tableLoc, file.Path, deleteVector, file.PartitionValues, MemoryAllocator);

                    await foreach (var batch in batches)
                    {
                        PrimitiveList<int> weights = new PrimitiveList<int>(MemoryAllocator);
                        PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);
                        weights.InsertStaticRange(0, 1, (int)batch.count);
                        iterations.InsertStaticRange(0, 0, (int)batch.count);

                        _eventsCounter.Add(batch.count);
                        _eventsProcessed.Add(batch.count);
                        await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, batch.data)));
                    }
                }

                _state.Value.CurrentVersion = _table.Version;
                // The snapshot is not needed after the first load
                _table = null;
                await output.SendWatermark(new Base.Watermark(_tableName, LongWatermarkValue.Create(_state.Value.CurrentVersion)));
                ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
                output.ExitCheckpointLock();
            }
            else
            {
                // Load delta if we have a current version
                await LoadDelta(output, default);
            }

            await RegisterTrigger(DeltaLoadName, _options.DeltaCheckInterval);
        }
    }
}
