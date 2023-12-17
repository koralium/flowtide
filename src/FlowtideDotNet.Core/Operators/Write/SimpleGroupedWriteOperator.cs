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
using FlowtideDotNet.Core.Storage;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Xml.Serialization;

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
        OnWatermark = 1
    }

    public readonly struct SimpleChangeEvent
    {
        public SimpleChangeEvent(StreamEvent row, bool isDeleted)
        {
            Row = row;
            IsDeleted = isDeleted;
        }

        public StreamEvent Row { get; }

        public bool IsDeleted { get; }
    }

    /// <summary>
    /// Helper class to implement a write operator that groups values by primary keys.
    /// </summary>
    public abstract class SimpleGroupedWriteOperator : GroupedWriteBaseOperator<SimpleWriteState>
    {
        private MetadataResult? m_metadataResult;
        private IBPlusTree<StreamEvent, int>? m_modified;
        private bool m_hasModified;
        private readonly ExecutionMode m_executionMode;
        private SimpleWriteState? _state;
        private Watermark _latestWatermark;

        protected SimpleGroupedWriteOperator(ExecutionMode executionMode, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            this.m_executionMode = executionMode;
        }

        protected override async Task<SimpleWriteState> Checkpoint(long checkpointTime)
        {
            if (m_executionMode == ExecutionMode.OnCheckpoint)
            {
                await SendData();
            }
            return new SimpleWriteState();
        }

        private async Task SendData()
        {
            Debug.Assert(m_modified != null);
            if (m_hasModified)
            {
                var rowIterator = GetChangedRows();
                await UploadChanges(rowIterator, _latestWatermark, CancellationToken);
                await m_modified.Clear();
                m_hasModified = false;
            }
            if (_state!.SentInitialData == false)
            {
                await OnInitialDataSent();
                _state.SentInitialData = true;
            }
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
        }

        protected override Task OnWatermark(Watermark watermark)
        {
            _latestWatermark = watermark;
            if (m_executionMode == ExecutionMode.OnWatermark)
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

        protected override async Task Initialize(long restoreTime, SimpleWriteState? state, IStateManagerClient stateManagerClient)
        {
            if (m_metadataResult == null)
            {
                m_metadataResult = await SetupAndLoadMetadataAsync();
            }
            if (state != null)
            {
                _state = state;
            }
            else
            {
                _state = new SimpleWriteState()
                {
                    SentInitialData = false
                };
            }
            m_modified = await stateManagerClient.GetOrCreateTree("temporary", new BPlusTreeOptions<StreamEvent, int>()
            {
                Comparer = PrimaryKeyComparer,
                ValueSerializer = new IntSerializer(),
                KeySerializer = new StreamEventBPlusTreeSerializer()
            });
            await m_modified.Clear();
        }

        protected override async Task OnRecieve(StreamEventBatch msg, long time)
        {
            Debug.Assert(m_modified != null);
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
