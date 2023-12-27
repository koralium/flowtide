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
using System.Threading.Tasks.Dataflow;

namespace MonitoringAzureMonitor
{
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

                List<RowEvent> o = new List<RowEvent>();
                for (int k = 0; k < 100; k++)
                {
                    o.Add(RowEvent.Create(1, 0, b =>
                    {
                        for (int z = 0; z < 16; z++)
                        {
                            b.Add(123);
                        }
                    }));
                }
                await output.SendAsync(new StreamEventBatch(o));
                output.ExitCheckpointLock();
                ScheduleCheckpoint(TimeSpan.FromSeconds(1));
            }
        }
    }
}
