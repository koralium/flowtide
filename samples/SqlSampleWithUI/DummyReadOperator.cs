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
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;

namespace SqlSampleWithUI
{
    public class DummyReadFactory : RegexConnectorSourceFactory
    {
        public DummyReadFactory(string regexPattern) : base(regexPattern)
        {
        }

        public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
        {
            return new DummyReadOperator(dataflowBlockOptions);
        }
    }

    public class DummyReadOperator : ReadBaseOperator<object>
    {
        public DummyReadOperator(DataflowBlockOptions options) : base(options)
        {
        }

        public override string DisplayName => "Dummy read";

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override Task OnTrigger(string triggerName, object? state)
        {
            return Task.CompletedTask;
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            return Task.FromResult<IReadOnlySet<string>>(new HashSet<string>() { "dummy" });
        }

        protected override Task InitializeOrRestore(long restoreTime, object? state, IStateManagerClient stateManagerClient)
        {
            return Task.CompletedTask;
        }

        protected override Task<object> OnCheckpoint(long checkpointTime)
        {
            return Task.FromResult(new object());
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            
            for (int i = 0; i < 1_000_000; i++)
            {
                await output.EnterCheckpointLock();
                var memoryManager = MemoryAllocator;
                IColumn[] columns = new IColumn[16];
                PrimitiveList<int> weights = new PrimitiveList<int>(memoryManager);
                PrimitiveList<uint> iterations = new PrimitiveList<uint>(memoryManager);

                
                for (int b = 0; b < 16; b++)
                {
                    columns[b] = Column.Create(memoryManager);
                }

                for (int k = 0; k < 100; k++)
                {
                    weights.Add(1);
                    iterations.Add(0);
                    for (int z = 0; z < 16; z++)
                    {
                        columns[z].Add(new Int64Value((i * 100) + k));
                    }
                }
                await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns))));
                output.ExitCheckpointLock();
                
            }
            ScheduleCheckpoint(TimeSpan.FromSeconds(1));
        }
    }
}
