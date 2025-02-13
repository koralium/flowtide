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
using FlowtideDotNet.Base.Utils;
using FlowtideDotNet.Base.Vertices.Egress;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Write.Column
{
    public abstract class ColumnGroupedWriteOperator : EgressVertex<StreamEventBatch>
    {
        private readonly ExecutionMode m_executionMode;
        private readonly WriteRelation m_writeRelation;
        private IBPlusTree<ColumnRowReference, int, ColumnKeyStorageContainer, ListValueContainer<int>>? m_tree;
        private IBPlusTree<ColumnRowReference, int, ModifiedKeyStorage, ListValueContainer<int>>? m_modified;

        /// <summary>
        /// Tree used to store the existing data in the destination, it will be used to compare the existing data with the new data and create delete operations
        /// for those keys that does not exist in the new data.
        /// </summary>
        private IBPlusTree<ColumnRowReference, int, ModifiedKeyStorage, ListValueContainer<int>>? m_existingData;
        private bool m_hasModified;
        private Watermark? m_latestWatermark;
        private WriteTreeSearchComparer? m_writeTreeSearchComparer;
        private IObjectState<bool>? m_hasSentInitialData;
        private ExistingRowComparer? m_existingRowComparer;
        private IReadOnlyList<int>? m_primaryKeyColumns;
        private readonly IColumn[] _deleteBatchColumns;
        private readonly EventBatchData _deleteEventBatch;

        public ColumnGroupedWriteOperator(ExecutionMode executionMode, WriteRelation writeRelation, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            this.m_executionMode = executionMode;
            this.m_writeRelation = writeRelation;

            // Create the event batch used for delete, this is used to give the user the same amount of columns
            // if it is an upsert or a delete
            _deleteBatchColumns = new IColumn[writeRelation.OutputLength];
            for (int i = 0; i < _deleteBatchColumns.Length; i++)
            {
                _deleteBatchColumns[i] = AlwaysNullColumn.Instance;
            }
            _deleteEventBatch = new EventBatchData(_deleteBatchColumns);
        }

        protected virtual bool FetchExistingData => false;

        public IReadOnlyList<int> PrimaryKeyColumns => m_primaryKeyColumns ?? throw new InvalidOperationException("Must be called after initialize and restore");

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        protected abstract ValueTask<IReadOnlyList<int>> GetPrimaryKeyColumns();

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            m_hasSentInitialData = await stateManagerClient.GetOrCreateObjectStateAsync<bool>("initialDataSent");

            m_primaryKeyColumns = await GetPrimaryKeyColumns();

            m_tree = await stateManagerClient.GetOrCreateTree("output",
                new BPlusTreeOptions<ColumnRowReference, int, ColumnKeyStorageContainer, ListValueContainer<int>>()
                {
                    Comparer = new WriteTreeInsertComparer(m_primaryKeyColumns.Select(x => new KeyValuePair<int, ReferenceSegment?>(x, default)).ToList(), m_writeRelation.OutputLength),
                    ValueSerializer = new ValueListSerializer<int>(new IntSerializer()),
                    KeySerializer = new ColumnStoreSerializer(m_writeRelation.OutputLength, MemoryAllocator),
                    MemoryAllocator = MemoryAllocator,
                    UseByteBasedPageSizes = true
                });

            m_modified = await stateManagerClient.GetOrCreateTree("temporary",
                new BPlusTreeOptions<ColumnRowReference, int, ModifiedKeyStorage, ListValueContainer<int>>()
                {
                    Comparer = new ModifiedTreeComparer(m_primaryKeyColumns.ToList()),
                    ValueSerializer = new ValueListSerializer<int>(new IntSerializer()),
                    KeySerializer = new ModifiedKeyStorageSerializer(m_primaryKeyColumns.ToList(), MemoryAllocator),
                    MemoryAllocator = MemoryAllocator,
                    UseByteBasedPageSizes = true
                });
            await m_modified.Clear();

            List<KeyValuePair<int, ReferenceSegment?>> referenceColumns = new List<KeyValuePair<int, ReferenceSegment?>>();
            for (int i = 0; i < m_primaryKeyColumns.Count; i++)
            {
                referenceColumns.Add(new KeyValuePair<int, ReferenceSegment?>(i, default));
            }
            m_writeTreeSearchComparer = new WriteTreeSearchComparer(m_primaryKeyColumns.Select(x => new KeyValuePair<int, ReferenceSegment?>(x, default)).ToList(), referenceColumns);

            if (FetchExistingData && !m_hasSentInitialData.Value)
            {
                var primaryKeySelectorList = m_primaryKeyColumns.Select(x => new KeyValuePair<int, ReferenceSegment?>(x, default)).ToList();
                m_existingRowComparer = new ExistingRowComparer(primaryKeySelectorList, primaryKeySelectorList);
                m_existingData = await stateManagerClient.GetOrCreateTree("existing",
                new BPlusTreeOptions<ColumnRowReference, int, ModifiedKeyStorage, ListValueContainer<int>>()
                {
                    Comparer = new ModifiedTreeComparer(m_primaryKeyColumns.ToList()),
                    ValueSerializer = new ValueListSerializer<int>(new IntSerializer()),
                    KeySerializer = new ModifiedKeyStorageSerializer(m_primaryKeyColumns.ToList(), MemoryAllocator),
                    MemoryAllocator = MemoryAllocator,
                    UseByteBasedPageSizes = true
                });
                await UpsertExistingData();
            }
            
        }

        private async Task UpsertExistingData()
        {
            Debug.Assert(m_existingData != null);
            await foreach(var batch in GetExistingData())
            {
                for (int i = 0; i < batch.Count; i++)
                {
                    await m_existingData.Upsert(new ColumnRowReference() { referenceBatch = batch, RowIndex = i }, 1);
                }
            }
        }

        protected override Task OnWatermark(Watermark watermark)
        {
            Debug.Assert(m_hasSentInitialData != null);
            m_latestWatermark = watermark;
            if (m_executionMode == ExecutionMode.OnWatermark ||
                (m_executionMode == ExecutionMode.Hybrid && m_hasSentInitialData.Value))
            {
                return SendData();
            }
            return base.OnWatermark(watermark);
        }

        protected override async Task OnCheckpoint(long checkpointTime)
        {
            Debug.Assert(m_hasSentInitialData != null);
            if (m_executionMode == ExecutionMode.OnCheckpoint || (m_executionMode == ExecutionMode.Hybrid && !m_hasSentInitialData.Value))
            {
                await SendData();
            }
            Checkpoint(checkpointTime);
            await m_hasSentInitialData.Commit();
        }

        protected abstract void Checkpoint(long checkpointTime);

        private async IAsyncEnumerable<ColumnWriteOperation> GetChangedRows()
        {
            Debug.Assert(m_modified != null);
            Debug.Assert(m_tree != null);
            Debug.Assert(m_writeTreeSearchComparer != null);
            Debug.Assert(m_primaryKeyColumns != null);
            Debug.Assert(m_hasSentInitialData != null);

            using var modifiedIterator = m_modified.CreateIterator();
            await modifiedIterator.SeekFirst();
            using var treeIterator = m_tree.CreateIterator();

            await foreach (var page in modifiedIterator)
            {
                int pageCount = page.Keys._data.Count;

                for (int i = 0; i < pageCount; i++)
                {
                    // Search up the existing row for the changed primary key
                    await treeIterator.Seek(new ColumnRowReference() { referenceBatch = page.Keys._data, RowIndex = i }, m_writeTreeSearchComparer);

                    // Check that a match was made
                    if (!m_writeTreeSearchComparer.noMatch)
                    {
                        var enumerator = treeIterator.GetAsyncEnumerator();
                        await enumerator.MoveNextAsync();
                        var existingpage = enumerator.Current;
                        yield return new ColumnWriteOperation()
                        {
                            EventBatchData = existingpage.Keys._data,
                            Index = m_writeTreeSearchComparer.start,
                            IsDeleted = false
                        };
                    }
                    else
                    {
                        for (int k = 0; k < m_primaryKeyColumns.Count; k++)
                        {
                            _deleteBatchColumns[m_primaryKeyColumns[k]] = page.Keys._data.Columns[k];
                        }
                        yield return new ColumnWriteOperation()
                        {
                            EventBatchData = _deleteEventBatch,
                            Index = i,
                            IsDeleted = true
                        };
                    }
                }
            }

            if (!m_hasSentInitialData.Value &&
                FetchExistingData)
            {
                await foreach (var row in DeleteExistingData())
                {
                    yield return row;
                }
            }
        }

        private static async IAsyncEnumerable<ColumnRowReference> IteratePerRow(IBPlusTreeIterator<ColumnRowReference, int, ModifiedKeyStorage, ListValueContainer<int>> iterator)
        {
            await foreach (var page in iterator)
            {
                foreach (var kv in page)
                {
                    yield return kv.Key;
                }
            }
        }

        private async IAsyncEnumerable<ColumnWriteOperation> DeleteExistingData()
        {
            Debug.Assert(m_modified != null);
            Debug.Assert(m_existingData != null);
            Debug.Assert(m_existingRowComparer != null);
            Debug.Assert(m_primaryKeyColumns != null);

            using var treeIterator = m_modified.CreateIterator();
            using var existingIterator = m_existingData.CreateIterator();

            await treeIterator.SeekFirst();
            await existingIterator.SeekFirst();

            var tmpEnumerator = IteratePerRow(treeIterator).GetAsyncEnumerator();
            var persistentEnumerator = IteratePerRow(existingIterator).GetAsyncEnumerator();

            var hasNew = await tmpEnumerator.MoveNextAsync();
            var hasOld = await persistentEnumerator.MoveNextAsync();

            // Go through both trees and find deletions
            while (hasNew || hasOld)
            {
                int comparison = hasNew && hasOld ? m_existingRowComparer.CompareTo(tmpEnumerator.Current, persistentEnumerator.Current) : 0;

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
                    for (int i = 0; i < m_primaryKeyColumns.Count; i++)
                    {
                        _deleteBatchColumns[m_primaryKeyColumns[i]] = persistentEnumerator.Current.referenceBatch.Columns[i];
                    }
                    yield return new ColumnWriteOperation()
                    {
                        EventBatchData = _deleteEventBatch,
                        Index = persistentEnumerator.Current.RowIndex,
                        IsDeleted = true
                    };
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

        protected virtual IAsyncEnumerable<EventBatchData> GetExistingData()
        {
            return new EmptyAsyncEnumerable<EventBatchData>();
        }

        private async Task SendData()
        {
            Debug.Assert(m_modified != null);
            Debug.Assert(m_latestWatermark != null);
            Debug.Assert(m_hasSentInitialData != null);
            if (m_hasModified)
            {
                var changedRows = GetChangedRows();
                await UploadChanges(changedRows, m_latestWatermark, !m_hasSentInitialData.Value, CancellationToken);
                await m_modified.Clear();
                m_hasModified = false;
            }
            if (m_hasSentInitialData.Value == false)
            {
                await OnInitialDataSent();
                m_hasSentInitialData.Value = true;
            }
        }

        protected virtual Task OnInitialDataSent()
        {
            return Task.CompletedTask;
        }

        protected abstract Task UploadChanges(IAsyncEnumerable<ColumnWriteOperation> rows, Watermark watermark, bool isInitialData, CancellationToken cancellationToken);

        protected override async Task OnRecieve(StreamEventBatch msg, long time)
        {
            Debug.Assert(m_tree != null);
            Debug.Assert(m_modified != null);

            var batch = msg.Data.EventBatchData;
            var weights = msg.Data.Weights;
            for (int i = 0; i < msg.Data.Weights.Count; i++)
            {
                m_hasModified = true;
                var rowReference = new ColumnRowReference() { referenceBatch = batch, RowIndex = i };
                await m_tree.RMWNoResult(in rowReference, weights[i], (input, current, found) =>
                {
                    if (found)
                    {
                        current += input;
                        if (current == 0)
                        {
                            return (0, GenericWriteOperation.Delete);
                        }
                        return (current, GenericWriteOperation.Upsert);
                    }
                    return (input, GenericWriteOperation.Upsert);
                });
                
                await m_modified.Upsert(in rowReference, 1);
            }
        }
    }
}
