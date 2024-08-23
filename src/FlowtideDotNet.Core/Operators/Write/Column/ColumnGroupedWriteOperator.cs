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
using FlowtideDotNet.Base.Vertices.Egress;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Storage;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Write.Column
{
    public abstract class ColumnGroupedWriteOperator<TState> : EgressVertex<StreamEventBatch, TState>
        where TState : ColumnWriteState
    {
        private readonly ExecutionMode _executionMode;
        private readonly WriteRelation _writeRelation;
        private IBPlusTree<ColumnRowReference, int, ColumnKeyStorageContainer, ListValueContainer<int>>? _tree;
        private IBPlusTree<ColumnRowReference, int, ModifiedKeyStorage, ListValueContainer<int>>? m_modified;

        public ColumnGroupedWriteOperator(ExecutionMode executionMode, WriteRelation writeRelation, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            this._executionMode = executionMode;
            this._writeRelation = writeRelation;
        }

        public override string DisplayName => throw new NotImplementedException();

        public override Task Compact()
        {
            throw new NotImplementedException();
        }

        public override Task DeleteAsync()
        {
            throw new NotImplementedException();
        }

        protected abstract ValueTask<IReadOnlyList<int>> GetPrimaryKeyColumns();

        protected override async Task InitializeOrRestore(long restoreTime, TState? state, IStateManagerClient stateManagerClient)
        {
            var primaryKeyColumns = await GetPrimaryKeyColumns();

            _tree = await stateManagerClient.GetOrCreateTree("output",
                new BPlusTreeOptions<ColumnRowReference, int, ColumnKeyStorageContainer, ListValueContainer<int>>()
                {
                    Comparer = new WriteExistingInsertComparer(primaryKeyColumns.Select(x => new KeyValuePair<int, ReferenceSegment?>(x, default)).ToList(), _writeRelation.OutputLength),
                    ValueSerializer = new ValueListSerializer<int>(new IntSerializer()),
                    KeySerializer = new ColumnStoreSerializer(_writeRelation.OutputLength)
                });

            m_modified = await stateManagerClient.GetOrCreateTree("temporary",
                new BPlusTreeOptions<ColumnRowReference, int, ModifiedKeyStorage, ListValueContainer<int>>()
                {
                    Comparer = new ModifiedTreeComparer(primaryKeyColumns.ToList()),
                    ValueSerializer = new ValueListSerializer<int>(new IntSerializer()),
                    KeySerializer = new ModifiedKeyStorageSerializer(primaryKeyColumns.ToList())
                });
            await m_modified.Clear();
        }

        protected override async Task<TState> OnCheckpoint(long checkpointTime)
        {
            if (_executionMode == ExecutionMode.OnCheckpoint)
            {
                await SendData();
            }
            throw new NotImplementedException();
        }

        private async IAsyncEnumerable<ColumnWriteOperation> GetChangedRows()
        {
            var modifiedIterator = m_modified.CreateIterator();
            await modifiedIterator.SeekFirst();
            var treeIterator = _tree.CreateIterator();

            await foreach (var page in modifiedIterator)
            {
                int pageCount = page.Keys._data.Count;

                for (int i = 0; i < pageCount; i++)
                {
                    // Search up the existing row for the changed primary key
                    await treeIterator.Seek(new ColumnRowReference() { referenceBatch = page.Keys._data, RowIndex = i });
                    
                    // Check that a match was made
                    
                }
            }

            yield break;
        }

        private async Task SendData()
        {
            var modifiedIterator = m_modified.CreateIterator();
            await modifiedIterator.SeekFirst();

            await foreach(var page in modifiedIterator)
            {

            }
        }

        protected override async Task OnRecieve(StreamEventBatch msg, long time)
        {
            var batch = msg.Data.EventBatchData;
            var weights = msg.Data.Weights;
            for (int i = 0; i < msg.Data.Weights.Count; i++)
            {
                var rowReference = new ColumnRowReference() { referenceBatch = batch, RowIndex = i };
                await _tree.RMW(in rowReference, weights[i], (input, current, found) =>
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
