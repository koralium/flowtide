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

using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using Microsoft.VisualStudio.TestPlatform.Utilities;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.AcceptanceTests.Internal
{
    internal class MockDataSourceState
    {
        public int LatestOffset { get; set; }
    }
    internal class MockDataSourceOperator : ReadBaseOperator<MockDataSourceState>
    {
        private readonly ReadRelation readRelation;
        private readonly MockDatabase mockDatabase;
        private HashSet<string> _watermarkNames;
        private MockTable _table;
        private int _lastestOffset;

        public MockDataSourceOperator(ReadRelation readRelation, MockDatabase mockDatabase, DataflowBlockOptions options) : base(options)
        {
            this.readRelation = readRelation;
            this.mockDatabase = mockDatabase;

            _table = mockDatabase.GetTable(readRelation.NamedTable.DotSeperated);

            _watermarkNames = new HashSet<string>() { readRelation.NamedTable.DotSeperated };
        }

        public override string DisplayName => "Mock data source";

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        private async Task FetchChanges(IngressOutput<StreamEventBatch> output, object? state)
        {
            await output.EnterCheckpointLock();
            var (operations, fetchedOffset) = _table.GetOperations(_lastestOffset);
            bool sentData = false;
            List<RowEvent> o = new List<RowEvent>();
            foreach (var operation in operations)
            {
                o.Add(MockTable.ToStreamEvent(operation, readRelation.BaseSchema.Names));

                if (o.Count > 100)
                {
                    sentData = true;
                    await output.SendAsync(new StreamEventBatch(o));
                    o = new List<RowEvent>();
                }
            }

            if (o.Count > 0)
            {
                sentData = true;
                await output.SendAsync(new StreamEventBatch(o));
            }
            _lastestOffset = fetchedOffset;

            if (sentData)
            {
                await output.SendWatermark(new Base.Watermark(readRelation.NamedTable.DotSeperated, fetchedOffset));
                this.ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
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
                RunTask((output, state) => throw new Exception("crash"));
            }
            return Task.CompletedTask;
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            return Task.FromResult<IReadOnlySet<string>>(_watermarkNames);
        }

        protected override Task InitializeOrRestore(long restoreTime, MockDataSourceState? state, IStateManagerClient stateManagerClient)
        {
            if (state != null)
            {
                _lastestOffset = state.LatestOffset;
            }
            RegisterTrigger("crash");
            return Task.CompletedTask;
        }

        protected override Task<MockDataSourceState> OnCheckpoint(long checkpointTime)
        {
            return Task.FromResult(new MockDataSourceState() { LatestOffset = _lastestOffset });
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            await output.EnterCheckpointLock();
            var (operations, fetchedOffset) = _table.GetOperations(_lastestOffset);

            List<RowEvent> o = new List<RowEvent>();
            foreach(var operation in operations)
            {
                o.Add(MockTable.ToStreamEvent(operation, readRelation.BaseSchema.Names));
                //o.Add(new StreamEvent(1, 0, operation.Vector));

                if (o.Count > 100)
                {
                    await output.SendAsync(new StreamEventBatch(o));
                    o = new List<RowEvent>();
                }
            }

            if (o.Count > 0)
            {
                await output.SendAsync(new StreamEventBatch(o));
            }
            _lastestOffset = fetchedOffset;
            await output.SendWatermark(new Base.Watermark(readRelation.NamedTable.DotSeperated, fetchedOffset));
            output.ExitCheckpointLock();
            await this.RegisterTrigger("changes", TimeSpan.FromMilliseconds(50));
            this.ScheduleCheckpoint(TimeSpan.FromMilliseconds(100));
        }
    }
}
