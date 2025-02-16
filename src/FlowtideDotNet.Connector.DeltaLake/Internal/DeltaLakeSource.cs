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

using FASTER.core;
using FlowtideDotNet.Base.Vertices.Ingress;
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
using Stowage;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.DeltaLake.Internal
{
    internal class DeltaLakeSourceState
    {
        public long CurrentVersion { get; set; }
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

        private IBPlusTree<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>? _changesTree;

        public DeltaLakeSource(ReadRelation readRelation, DeltaLakeOptions options, DataflowBlockOptions blockOptions) : base(blockOptions)
        {
            this._readRelation = readRelation;
            this._options = options;
            _tableName = readRelation.NamedTable.DotSeperated;
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

        private async Task LoadCdcData(DeltaCommit deltaCommit, IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(_state?.Value != null);
            Debug.Assert(_reader != null);

            await output.EnterCheckpointLock();

            foreach(var file in deltaCommit.CdcFiles)
            {
                Debug.Assert(file.Path != null);
                var batches = _reader.ReadCdcFile(_options.StorageLocation, _tableLoc, file.Path, file.PartitionValues, MemoryAllocator);

                await foreach(var batch in batches)
                {
                    PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);
                    iterations.InsertStaticRange(0, 0, (int)batch.count);

                    await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(batch.weights, iterations, batch.data)));
                }
            }

            _hasCheckpointed = false;
            _state.Value.CurrentVersion = _state.Value.CurrentVersion + 1;
            await output.SendWatermark(new Base.Watermark(_tableName, _state.Value.CurrentVersion));
            ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
            output.ExitCheckpointLock();
        }

        /// <summary>
        /// Reads both added and deleted files and compute the delta between them
        /// </summary>
        /// <param name="deltaCommit"></param>
        /// <param name="output"></param>
        /// <returns></returns>
        private async Task LoadFileData(DeltaCommit deltaCommit, IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(_state?.Value != null);
            Debug.Assert(_reader != null);
            Debug.Assert(_changesTree != null);

            await output.EnterCheckpointLock();

            foreach (var file in deltaCommit.AddedFiles)
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
                }
            }

            foreach (var file in deltaCommit.RemovedFiles)
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
                }
            }

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

                await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns))));
            }

            await _changesTree.Clear();

            _hasCheckpointed = false;
            _state.Value.CurrentVersion = _state.Value.CurrentVersion + 1;
            await output.SendWatermark(new Base.Watermark(_tableName, _state.Value.CurrentVersion));
            ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
            output.ExitCheckpointLock();
        }

        private async Task LoadDelta(IngressOutput<StreamEventBatch> output, object? state)
        {
            Debug.Assert(_state?.Value != null);

            // Loop until we have read all new versions
            do
            {
                // If no checkpoint has been made and we want one version per checkpoint, return
                if (!_hasCheckpointed && _options.OneVersionPerCheckpoint)
                {
                    return;
                }

                DeltaCommit? commitInfo = default;

                // Loop to skip commits with no changes
                do
                {
                    commitInfo = await DeltaTransactionReader.ReadVersionCommit(_options.StorageLocation, _tableLoc, _state.Value.CurrentVersion + 1);

                    if (commitInfo == null)
                    {
                        return;
                    }

                    if (commitInfo.AddedFiles.Count > 0 || commitInfo.RemovedFiles.Count > 0 || commitInfo.CdcFiles.Count > 0)
                    {
                        break;
                    }
                    else
                    {
                        _state.Value.CurrentVersion = _state.Value.CurrentVersion + 1;
                    }
                } while (true);

                if (commitInfo.CdcFiles.Count > 0)
                {
                    await LoadCdcData(commitInfo, output);
                }
                else
                {
                    await LoadFileData(commitInfo, output);
                }
            } while (true);
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            return Task.FromResult<IReadOnlySet<string>>(new HashSet<string>() { _tableName });
        }

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            _state = await stateManagerClient.GetOrCreateObjectStateAsync<DeltaLakeSourceState>("state");

            if (_state.Value == null)
            {
                _state.Value = new DeltaLakeSourceState()
                {
                    CurrentVersion = -1
                };
            }

            var maxVersion = long.MaxValue;

            if (_state.Value.CurrentVersion != -1)
            {
                maxVersion = _state.Value.CurrentVersion;
            }

            if (_options.OneVersionPerCheckpoint)
            {
                maxVersion = 0;
            }

            _table = await DeltaTransactionReader.ReadTable(_options.StorageLocation, _tableLoc, maxVersion);

            var reader = new ParquetSharpReader();
            reader.Initialize(_table, _readRelation.BaseSchema.Names);
            _reader = reader;

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
            await _state.Commit();
            _hasCheckpointed = true;
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(_state?.Value != null);
            Debug.Assert(_table != null);
            Debug.Assert(_reader != null);
            if (_state.Value.CurrentVersion == -1)
            {
                await output.EnterCheckpointLock();
                foreach(var file in _table.AddFiles)
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

                    await foreach(var batch in batches)
                    {
                        PrimitiveList<int> weights = new PrimitiveList<int>(MemoryAllocator);
                        PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);
                        weights.InsertStaticRange(0, 1, (int)batch.count);
                        iterations.InsertStaticRange(0, 0, (int)batch.count);

                        await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, batch.data)));
                    }
                }

                _state.Value.CurrentVersion = _table.Version;
                await output.SendWatermark(new Base.Watermark(_tableName, _state.Value.CurrentVersion));
                ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
                output.ExitCheckpointLock();
            }

            await RegisterTrigger(DeltaLoadName, _options.DeltaCheckInterval);
        }
    }
}
