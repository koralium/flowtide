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
using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.AcceptanceTests.Internal
{
    internal class MockDataSourceState
    {
        public int LatestOffset { get; set; }
    }
    internal class MockDataSourceOperator : ReadBaseOperator
    {
        private readonly ReadRelation readRelation;
        private readonly MockDatabase mockDatabase;
        private HashSet<string> _watermarkNames;
        private MockTable _table;
        private IObjectState<MockDataSourceState>? _state;
        private BatchConverter _batchConverter;

        public MockDataSourceOperator(ReadRelation readRelation, MockDatabase mockDatabase, DataflowBlockOptions options) : base(options)
        {
            this.readRelation = readRelation;
            this.mockDatabase = mockDatabase;

            _table = mockDatabase.GetTable(readRelation.NamedTable.DotSeperated);

            _watermarkNames = new HashSet<string>() { readRelation.NamedTable.DotSeperated };
            _batchConverter = BatchConverter.GetBatchConverter(_table.Type, readRelation.BaseSchema.Names);
        }

        public override string DisplayName => "Mock data source";

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        private async Task FetchChanges(IngressOutput<StreamEventBatch> output, object? state)
        {
            Debug.Assert(_state?.Value != null);
            await output.EnterCheckpointLock();
            var (operations, fetchedOffset) = _table.GetOperations(_state.Value.LatestOffset);
            bool sentData = false;


            PrimitiveList<int> weights = new PrimitiveList<int>(MemoryAllocator);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);
            Column[] columns = new Column[readRelation.OutputLength];

            for (int i = 0; i < readRelation.OutputLength; i++)
            {
                columns[i] = new Column(MemoryAllocator);
            }

            foreach (var operation in operations)
            {
                _batchConverter.AppendToColumns(operation.Object, columns);

                iterations.Add(1);
                if (operation.IsDelete)
                {
                    weights.Add(-1);
                }
                else
                {
                    weights.Add(1);
                }

                if (weights.Count > 100)
                {
                    sentData = true;
                    await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns))));

                    columns = new Column[readRelation.OutputLength];
                    for (int i = 0; i < readRelation.OutputLength; i++)
                    {
                        columns[i] = new Column(MemoryAllocator);
                    }
                    weights = new PrimitiveList<int>(MemoryAllocator);
                    iterations = new PrimitiveList<uint>(MemoryAllocator);
                }
            }

            if (weights.Count > 0)
            {
                sentData = true;
                await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns))));
            }
            else
            {
                weights.Dispose();
                iterations.Dispose();
                foreach (var column in columns)
                {
                    column.Dispose();
                }
            }
            _state.Value.LatestOffset = fetchedOffset;

            if (sentData)
            {
                await output.SendWatermark(new Base.Watermark(readRelation.NamedTable.DotSeperated, LongWatermarkValue.Create(fetchedOffset)));
                this.ScheduleCheckpoint(TimeSpan.FromMilliseconds(200));
            }

            output.ExitCheckpointLock();
        }

        public override Task OnTrigger(string triggerName, object? state)
        {
            if (triggerName == "changes")
            {
                RunTask(FetchChanges);
            }
            else if (triggerName == "crash")
            {
                RunTask((output, state) => throw new CrashException("crash"));
            }
            return Task.CompletedTask;
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            return Task.FromResult<IReadOnlySet<string>>(_watermarkNames);
        }

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            _state = await stateManagerClient.GetOrCreateObjectStateAsync<MockDataSourceState>("mock_data_source_state");
            if (_state.Value == null)
            {
                _state.Value = new MockDataSourceState();
            }
            await RegisterTrigger("crash");
        }

        protected override async Task OnCheckpoint(long checkpointTime)
        {
            Debug.Assert(_state?.Value != null);
            await _state.Commit();
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(_state?.Value != null);
            await output.EnterCheckpointLock();
            var (operations, fetchedOffset) = _table.GetOperations(_state.Value.LatestOffset);

            PrimitiveList<int> weights = new PrimitiveList<int>(MemoryAllocator);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);
            Column[] columns = new Column[readRelation.OutputLength];

            for (int i = 0; i < readRelation.OutputLength; i++)
            {
                columns[i] = new Column(MemoryAllocator);
            }

            foreach (var operation in operations)
            {
                _batchConverter.AppendToColumns(operation.Object, columns);

                iterations.Add(1);
                if (operation.IsDelete)
                {
                    weights.Add(-1);
                }
                else
                {
                    weights.Add(1);
                }

                if (weights.Count > 100)
                {
                    await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns))));

                    columns = new Column[readRelation.OutputLength];
                    for (int i = 0; i < readRelation.OutputLength; i++)
                    {
                        columns[i] = new Column(MemoryAllocator);
                    }
                    weights = new PrimitiveList<int>(MemoryAllocator);
                    iterations = new PrimitiveList<uint>(MemoryAllocator);
                }
            }

            if (weights.Count > 0)
            {
                await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns))));
                await output.SendWatermark(new Base.Watermark(readRelation.NamedTable.DotSeperated, LongWatermarkValue.Create(fetchedOffset)));
            }
            else
            {
                weights.Dispose();
                iterations.Dispose();
                foreach (var column in columns)
                {
                    column.Dispose();
                }
            }
            _state.Value.LatestOffset = fetchedOffset;
            
            output.ExitCheckpointLock();
            await this.RegisterTrigger("changes", TimeSpan.FromMilliseconds(50));
            this.ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
        }
    }
}
