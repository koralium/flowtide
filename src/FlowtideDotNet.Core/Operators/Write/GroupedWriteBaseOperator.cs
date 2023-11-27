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

using FlowtideDotNet.Base.Vertices.Egress;
using System.Threading.Tasks.Dataflow;
using FlowtideDotNet.Core.Compute.Group;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Storage.Serializers;
using System.Diagnostics;

namespace FlowtideDotNet.Core.Operators.Write
{
    /// <summary>
    /// An egress operator that groups rows by key.
    /// 
    /// </summary>
    /// <typeparam name="TState"></typeparam>
    public abstract class GroupedWriteBaseOperator<TState> : EgressVertex<StreamEventBatch, TState>
        where TState: IStatefulWriteState
    {
        private IBPlusTree<GroupedStreamEvent, int>? _tree;
        private Func<GroupedStreamEvent, GroupedStreamEvent, int>? _comparer;
        private IComparer<RowEvent>? _streamEventComparer;

        public GroupedWriteBaseOperator(ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
        }

        protected abstract ValueTask<IReadOnlyList<int>> GetPrimaryKeyColumns();

        protected IComparer<RowEvent>? PrimaryKeyComparer => _streamEventComparer;

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        protected async ValueTask Insert(RowEvent streamEvent)
        {
            Debug.Assert(_tree != null, nameof(_tree));
            var groupedEvent = new GroupedStreamEvent(0, streamEvent.RowData);
            await _tree.RMW(groupedEvent, streamEvent.Weight, (input, current, found) =>
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
        }

        protected async ValueTask<(IReadOnlyList<RowEvent> rows, bool isDeleted)> GetGroup(RowEvent streamEvent)
        {
            var iterator = _tree!.CreateIterator();
            var seekEvent = new GroupedStreamEvent(1, streamEvent.RowData);
            await iterator.Seek(seekEvent);

            List<RowEvent> events = new List<RowEvent>();

            bool breakAll = false;
            await foreach(var page in iterator)
            {
                foreach(var kv in page)
                {
                    if (_comparer!(kv.Key, seekEvent) != 0)
                    {
                        breakAll = true;
                        break;
                    }
                    if (kv.Value > 0)
                    {
                        
                        var ev = new RowEvent(kv.Value, 0, kv.Key.RowData);
                        events.Add(ev);
                    }
                }
                if (breakAll)
                {
                    break;
                }
            }

            if (events.Count == 0)
            {
                return (events, true);
            }
            return (events, false);
        }

        /// <summary>
        /// Add a new event to the storage and get out all rows that exist on the same keys.
        /// This allows grouping operations to happen.
        /// </summary>
        /// <param name="streamEvent"></param>
        /// <returns></returns>
        protected async ValueTask<(IReadOnlyList<RowEvent> rows, bool isDeleted)> InsertAndGetGroup(RowEvent streamEvent)
        {
            await Insert(streamEvent);
            return await GetGroup(streamEvent);
        }

        protected override async Task InitializeOrRestore(long restoreTime, TState? state, IStateManagerClient stateManagerClient)
        {
            var primaryKeyColumns = await GetPrimaryKeyColumns();

            _streamEventComparer = new StreamEventComparer(GroupIndexCreator.CreateComparer<RowEvent>(primaryKeyColumns));
            _comparer = GroupIndexCreator.CreateComparer<GroupedStreamEvent>(primaryKeyColumns);

            _tree = await stateManagerClient.GetOrCreateTree("output", new BPlusTreeOptions<GroupedStreamEvent, int>() 
            { 
                Comparer = new GroupedStreamEventComparer(_comparer),
                ValueSerializer = new IntSerializer(),
                KeySerializer = new GroupedStreamEventBPlusTreeSerializer()
            });

            await Initialize(restoreTime, state, stateManagerClient);
        }

        protected abstract Task Initialize(long restoreTime, TState? state, IStateManagerClient stateManagerClient);

        protected override async Task<TState> OnCheckpoint(long checkpointTime)
        {
            Debug.Assert(_tree != null, nameof(_tree));
            await _tree.Commit();
            var newState = await Checkpoint(checkpointTime);
            return newState;
        }

        protected abstract Task<TState> Checkpoint(long checkpointTime);

        public override ValueTask DisposeAsync()
        {
            return base.DisposeAsync();
        }

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }
    }
}
