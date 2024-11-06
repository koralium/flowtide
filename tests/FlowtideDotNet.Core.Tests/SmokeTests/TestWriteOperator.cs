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
using FlowtideDotNet.Storage.StateManager;
using FlexBuffers;
using FlowtideDotNet.Substrait.Relations;
using System.Text;
using System.Threading.Tasks.Dataflow;
using System.Text.Json;
using System.Globalization;
using System.Diagnostics;

namespace FlowtideDotNet.Core.Tests.SmokeTests
{
    internal class TestWriteState : IStatefulWriteState
    {
        public bool InitialCheckpointDone { get; set; }
        public long StorageSegmentId { get; set; }
    }
    internal class TestWriteOperator<T> : GroupedWriteBaseOperator<TestWriteState>
    {
        private TestWriteState? currentState;
        private SortedSet<RowEvent>? modified;
        List<int> primaryKeyIds;
        private readonly Func<List<T>, Task> onValueChange;
        private readonly WriteRelation writeRelation;
        private Dictionary<string, List<T>> currentData;

        public TestWriteOperator(List<int> primaryKeys, Func<List<T>, Task> onValueChange, WriteRelation writeRelation, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            currentData = new Dictionary<string, List<T>>();
            primaryKeyIds = primaryKeys;

            this.onValueChange = onValueChange;
            this.writeRelation = writeRelation;
        }

        public override string DisplayName => "Write";

        protected override async Task<TestWriteState> Checkpoint(long checkpointTime)
        {
            Debug.Assert(currentState != null, nameof(currentState));
            if (!currentState.InitialCheckpointDone)
            {
                // Send data
                await SendData();

                currentState.InitialCheckpointDone = true;
            }
            return currentState;
        }

        private T Deserialize(RowEvent ev)
        {
            StringBuilder jsonBuilder = new StringBuilder();
            jsonBuilder.AppendLine("{");
            CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
            for (int i = 0; i < ev.Length; i++)
            {
                var propName = writeRelation.TableSchema.Names[i];
                jsonBuilder.Append($"\"{propName}\": ");
                jsonBuilder.Append(ev.GetColumn(i).ToJson);
                if ((i + 1) < ev.Length)
                {
                    jsonBuilder.AppendLine(",");
                }
                else
                {
                    jsonBuilder.AppendLine();
                }
            }
            jsonBuilder.AppendLine("}");
            var str = jsonBuilder.ToString();
            JsonSerializerOptions options = new JsonSerializerOptions()
            {
                PropertyNameCaseInsensitive = true
            };
            options.Converters.Add(new DateTimeConverter());
            var val = JsonSerializer.Deserialize<T>(str, options);
            Debug.Assert(val != null, nameof(val));
            return val;
        }

        private async Task SendData()
        {
            Debug.Assert(modified != null, nameof(modified));

            foreach (var m in modified)
            {
                StringBuilder keyBuilder = new StringBuilder();
                Dictionary<string, FlxValue> key = new Dictionary<string, FlxValue>();
                for (int i = 0; i < primaryKeyIds.Count; i++)
                {
                    var primaryKeyIndex = primaryKeyIds[i];
                    keyBuilder.Append(m.GetColumn(primaryKeyIndex).ToJson);
                    if ((i+ 1) < primaryKeyIds.Count)
                    {
                        keyBuilder.Append("|");
                    }
                    key.Add(writeRelation.TableSchema.Names[primaryKeyIndex], m.GetColumn(primaryKeyIndex));
                }
                var (rows, isDeleted) = await this.GetGroup(m);

                if (rows.Count >= 1 && !isDeleted)
                {
                    List<T> newDataList = new List<T>();
                    foreach(var ev in rows)
                    {
                        newDataList.Add(Deserialize(ev));
                    }
                    //var ev = rows.First();
                    //var val = Deserialize(ev);
                    currentData[keyBuilder.ToString()] = newDataList;
                }
                else if (isDeleted)
                {
                    currentData.Remove(keyBuilder.ToString());
                }
            }
            await onValueChange(currentData.Values.SelectMany(x => x).ToList());
            modified.Clear();
        }

        protected override ValueTask<IReadOnlyList<int>> GetPrimaryKeyColumns()
        {
            return ValueTask.FromResult<IReadOnlyList<int>>(primaryKeyIds);
        }

        protected override Task Initialize(long restoreTime, TestWriteState? state, IStateManagerClient stateManagerClient)
        {
            currentState = state;
            if (state == null)
            {
                currentState = new TestWriteState();
            }
            modified = new SortedSet<RowEvent>(PrimaryKeyComparer);
            return Task.CompletedTask;
        }

        protected override async Task OnWatermark(Watermark watermark)
        {
            Debug.Assert(currentState != null, nameof(currentState));

            if (currentState.InitialCheckpointDone)
            {
                // Send data
                await SendData();
            }
        }

        protected override async Task OnRecieve(StreamEventBatch msg, long time)
        {
            Debug.Assert(modified != null, nameof(modified));

            foreach (var e in msg.Events)
            {
                var primaryKeyValue = e.GetColumn(0);
                await this.Insert(e);
                modified.Add(e);
            }
        }
    }
}
