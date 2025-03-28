﻿// Licensed under the Apache License, Version 2.0 (the "License")
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
using FlowtideDotNet.Base.Utils;
using FlowtideDotNet.Core.Storage;
using FlowtideDotNet.Core.Utils;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Write
{
    public class SimpleWriteState : IStatefulWriteState
    {
        public long StorageSegmentId { get; set; }
        public bool SentInitialData { get; set; }
    }

    public class MetadataResult
    {
        public MetadataResult(IReadOnlyList<int> primaryKeyColumns)
        {
            PrimaryKeyColumns = primaryKeyColumns;
        }

        public IReadOnlyList<int> PrimaryKeyColumns { get; }
    }

    public enum ExecutionMode
    {
        OnCheckpoint = 0,
        OnWatermark = 1,
        /// <summary>
        /// Hybrid mode starts with on checkpoint and then switches to on watermark after initial data
        /// </summary>
        Hybrid = 2
    }

    public readonly struct SimpleChangeEvent
    {
        public SimpleChangeEvent(RowEvent row, bool isDeleted)
        {
            Row = row;
            IsDeleted = isDeleted;
        }

        public RowEvent Row { get; }

        public bool IsDeleted { get; }
    }

    /// <summary>
    /// Helper class to implement a write operator that groups values by primary keys.
    /// </summary>
    public abstract class SimpleGroupedWriteOperator : GroupedWriteBaseOperator
    {
        private MetadataResult? m_metadataResult;
        private IBPlusTree<RowEvent, int, ListKeyContainer<RowEvent>, ListValueContainer<int>>? m_modified;
        private bool m_hasModified;
        private readonly ExecutionMode m_executionMode;
        private IObjectState<SimpleWriteState>? _state;
        private Watermark? _latestWatermark;
        private ICounter<long>? _eventsProcessed;
        private IBPlusTree<RowEvent, int, ListKeyContainer<RowEvent>, ListValueContainer<int>>? m_existingData;

        protected SimpleGroupedWriteOperator(ExecutionMode executionMode, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            this.m_executionMode = executionMode;
        }

        protected override async Task Checkpoint(long checkpointTime)
        {
            Debug.Assert(_state?.Value != null);
            if (m_executionMode == ExecutionMode.OnCheckpoint ||
                (m_executionMode == ExecutionMode.Hybrid && !_state.Value.SentInitialData))
            {
                await SendData();
            }
            await _state.Commit();
        }

        private async Task SendData()
        {
            Debug.Assert(_latestWatermark != null);
            Debug.Assert(m_modified != null);
            if (m_hasModified)
            {
                var rowIterator = GetChangedRows();
                await UploadChanges(rowIterator, _latestWatermark, CancellationToken);
                await m_modified.Clear();
                m_hasModified = false;
            }
            if (_state!.Value!.SentInitialData == false)
            {
                await OnInitialDataSent();
                _state.Value.SentInitialData = true;
            }
        }

        private static async IAsyncEnumerable<KeyValuePair<RowEvent, int>> IteratePerRow(IBPlusTreeIterator<RowEvent, int, ListKeyContainer<RowEvent>, ListValueContainer<int>> iterator)
        {
            await foreach (var page in iterator)
            {
                foreach (var kv in page)
                {
                    yield return kv;
                }
            }
        }

        private async IAsyncEnumerable<SimpleChangeEvent> DeleteExistingData()
        {
            Debug.Assert(m_modified != null);
            Debug.Assert(m_existingData != null);
            Debug.Assert(PrimaryKeyComparer != null);

            var treeIterator = m_modified.CreateIterator();
            var existingIterator = m_existingData.CreateIterator();
            await treeIterator.SeekFirst();
            await existingIterator.SeekFirst();

            var tmpEnumerator = IteratePerRow(treeIterator).GetAsyncEnumerator();
            var persistentEnumerator = IteratePerRow(existingIterator).GetAsyncEnumerator();

            var hasNew = await tmpEnumerator.MoveNextAsync();
            var hasOld = await persistentEnumerator.MoveNextAsync();

            // Go through both trees and find deletions
            while (hasNew || hasOld)
            {
                int comparison = hasNew && hasOld ? PrimaryKeyComparer.Compare(tmpEnumerator.Current.Key, persistentEnumerator.Current.Key) : 0;

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
                    yield return new SimpleChangeEvent(new RowEvent(-1, 0, persistentEnumerator.Current.Key.RowData), true);
                    hasOld = await persistentEnumerator.MoveNextAsync();
                }
                else
                {
                    hasNew = await tmpEnumerator.MoveNextAsync();
                    hasOld = await persistentEnumerator.MoveNextAsync();
                }
            }
            await m_existingData.Clear();
        }

        protected virtual Task OnInitialDataSent()
        {
            return Task.CompletedTask;
        }

        protected abstract Task UploadChanges(IAsyncEnumerable<SimpleChangeEvent> rows, Watermark watermark, CancellationToken cancellationToken);

        private async IAsyncEnumerable<SimpleChangeEvent> GetChangedRows()
        {
            Debug.Assert(m_modified != null);
            var iterator = m_modified.CreateIterator();
            await iterator.SeekFirst();
            await foreach (var page in iterator)
            {

                foreach (var kv in page)
                {
                    var (rows, isDeleted) = await this.GetGroup(kv.Key);
                    if (rows.Count > 1)
                    {
                        var lastRow = rows.Last();
                        yield return new SimpleChangeEvent(lastRow, false);
                    }
                    else if (rows.Count == 1)
                    {
                        yield return new SimpleChangeEvent(rows[0], false);
                    }
                    else if (isDeleted)
                    {
                        yield return new SimpleChangeEvent(kv.Key, true);
                    }
                }
            }

            if (!_state!.Value!.SentInitialData &&
                FetchExistingData)
            {
                await foreach (var row in DeleteExistingData())
                {
                    yield return row;
                }
            }
        }

        protected virtual bool FetchExistingData => false;

        protected virtual IAsyncEnumerable<RowEvent> GetExistingData()
        {
            return new EmptyAsyncEnumerable<RowEvent>();
        }

        protected override Task OnWatermark(Watermark watermark)
        {
            Debug.Assert(_state?.Value != null);
            _latestWatermark = watermark;
            if (m_executionMode == ExecutionMode.OnWatermark ||
                (m_executionMode == ExecutionMode.Hybrid && _state.Value.SentInitialData))
            {
                return SendData();
            }
            return base.OnWatermark(watermark);
        }

        protected abstract Task<MetadataResult> SetupAndLoadMetadataAsync();

        protected override async ValueTask<IReadOnlyList<int>> GetPrimaryKeyColumns()
        {
            if (m_metadataResult == null)
            {
                m_metadataResult = await SetupAndLoadMetadataAsync();
            }
            return m_metadataResult.PrimaryKeyColumns;
        }

        protected ValueTask<(bool found, RowEvent key)> GetExistingDataRow(RowEvent e)
        {
            if (m_existingData != null && !_state!.Value!.SentInitialData && FetchExistingData)
            {
                return m_existingData.GetKey(e);
            }
            return ValueTask.FromResult((false, default(RowEvent)));
        }

        protected override async Task Initialize(long restoreTime, IStateManagerClient stateManagerClient)
        {
            Debug.Assert(PrimaryKeyComparer != null);
            if (m_metadataResult == null)
            {
                m_metadataResult = await SetupAndLoadMetadataAsync();
            }
            if (_eventsProcessed == null)
            {
                _eventsProcessed = Metrics.CreateCounter<long>("events_processed");
            }

            _state = await stateManagerClient.GetOrCreateObjectStateAsync<SimpleWriteState>("simple_write_state");
            if (_state.Value == null)
            {
                _state.Value = new SimpleWriteState()
                {
                    SentInitialData = false
                };
            }
            m_modified = await stateManagerClient.GetOrCreateTree("temporary",
                new BPlusTreeOptions<RowEvent, int, ListKeyContainer<RowEvent>, ListValueContainer<int>>()
                {
                    Comparer = new BPlusTreeListComparer<RowEvent>(PrimaryKeyComparer),
                    ValueSerializer = new ValueListSerializer<int>(new IntSerializer()),
                    KeySerializer = new KeyListSerializer<RowEvent>(new StreamEventBPlusTreeSerializer()),
                    MemoryAllocator = MemoryAllocator
                });
            await m_modified.Clear();

            if (FetchExistingData && !_state!.Value.SentInitialData)
            {
                // Create a tree to store existing data in the destination
                // This will be used to check written data to existing data if it should be removed from the destination.
                m_existingData = await stateManagerClient.GetOrCreateTree("existing_data",
                    new BPlusTreeOptions<RowEvent, int, ListKeyContainer<RowEvent>, ListValueContainer<int>>()
                    {
                        Comparer = new BPlusTreeListComparer<RowEvent>(PrimaryKeyComparer),
                        ValueSerializer = new ValueListSerializer<int>(new IntSerializer()),
                        KeySerializer = new KeyListSerializer<RowEvent>(new StreamEventBPlusTreeSerializer()),
                        MemoryAllocator = MemoryAllocator
                    });

                Logger.FetchingExistingDataInDataSource(StreamName, Name);
                await foreach (var row in GetExistingData())
                {
                    await m_existingData.Upsert(row, 1);
                }
                Logger.DoneFetchingExistingData(StreamName, Name);
            }
        }

        protected override async Task OnRecieve(StreamEventBatch msg, long time)
        {
            Debug.Assert(m_modified != null);
            Debug.Assert(_eventsProcessed != null);
            _eventsProcessed.Add(msg.Events.Count);

            foreach (var e in msg.Events)
            {
                // Add the row to permanent storage
                await this.Insert(e);
                m_hasModified = true;
                // Add the row to the modified storage to keep track on which rows where changed
                await m_modified.Upsert(e, 0);
            }
        }
    }
}
