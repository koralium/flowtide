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
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.SqlServer;
using FlowtideDotNet.Storage.StateManager;
using FlexBuffers;
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.Acceptance
{
    public class TestWriteState : IStatefulWriteState
    {
        public bool InitialCheckpointDone { get; set; }
        public long StorageSegmentId { get; set; }
    }
    public class TestWriteOperator : GroupedWriteBaseOperator<TestWriteState>
    {
#if DEBUG_WRITE
        private StreamWriter allInput;
#endif
        private TestWriteState currentState;
        private SortedSet<StreamEvent> modified;
        List<int> primaryKeyIds;
        private readonly Func<IReadOnlyList<DataChange>, Task> onValueChange;
        private readonly WriteRelation writeRelation;

        public TestWriteOperator(Func<IReadOnlyList<DataChange>, Task> onValueChange, WriteRelation writeRelation, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            // Go through types and find not null fields.

            primaryKeyIds = new List<int>();
            primaryKeyIds.Add(0);

            this.onValueChange = onValueChange;
            this.writeRelation = writeRelation;
        }

        public override string DisplayName => "Write";

        protected override async Task<TestWriteState> Checkpoint(long checkpointTime)
        {
            await SendData();
            if (!currentState.InitialCheckpointDone)
            {
                // Send data
                

                currentState.InitialCheckpointDone = true;
            }
            return currentState;
        }

        private async Task SendData()
        {
            List<DataChange> output = new List<DataChange>();
            foreach (var m in modified)
            {
                Dictionary<string, FlxValue> key = new Dictionary<string, FlxValue>();
                for (int i = 0; i < primaryKeyIds.Count; i++)
                {
                    var primaryKeyIndex = primaryKeyIds[i];
                    key.Add(writeRelation.TableSchema.Names[primaryKeyIndex], m.GetColumn(primaryKeyIndex));
                }
                if (m.GetColumn(0).AsString.Equals("PILAN\\JENU"))
                {

                }
                var (rows, isDeleted) = await this.GetGroup(m);
                output.Add(new DataChange()
                {
                    Key = key,
                    IsDeleted = isDeleted,
                    Rows = rows,
                });
            }
            await onValueChange(output);
            modified.Clear();
        }

        protected override ValueTask<IReadOnlyList<int>> GetPrimaryKeyColumns()
        {
            return ValueTask.FromResult<IReadOnlyList<int> >(primaryKeyIds);
        }

        protected override Task Initialize(long restoreTime, TestWriteState? state, IStateManagerClient stateManagerClient)
        {
#if DEBUG_WRITE
            allInput = File.CreateText($"{Name}.all.txt");
#endif
            currentState = state;
            if (state == null)
            {
                currentState = new TestWriteState();
            }
            modified = new SortedSet<StreamEvent>(PrimaryKeyComparer);
            return Task.CompletedTask;
        }

        protected override async Task OnWatermark(Watermark watermark)
        {
            //if (currentState.InitialCheckpointDone)
            //{
            //    // Send data
            //    await SendData();
            //}
        }

        protected override async Task OnRecieve(StreamEventBatch msg, long time)
        {
            foreach (var e in msg.Events)
            {

                var primaryKeyValue = e.GetColumn(0);
                



                await this.Insert(e);


#if DEBUG_WRITE
                    allInput.WriteLine($"Input: {e.Weight} {e.Vector.ToJson}");
                    var group = await this.GetGroup(e);
                    foreach(var r in group.rows)
                    {
                        allInput.WriteLine(r.Vector.ToJson);
                    }
                    await allInput.FlushAsync();
#endif
                modified.Add(e);
            }
        }
    }
}
