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
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;

namespace MonitoringPrometheus
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
        private int _watermarkCounter = 1;
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
            if (triggerName == "on_check")
            {
                RunTask(SendChanges);
            }
            return Task.CompletedTask;
        }

        private async Task SendChanges(IngressOutput<StreamEventBatch> output, object? state)
        {
            await output.EnterCheckpointLock();

            List<RowEvent> o = new List<RowEvent>();
            for (int k = 0; k < 1; k++)
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
            await output.SendWatermark(new FlowtideDotNet.Base.Watermark("dummy", _watermarkCounter++));
            output.ExitCheckpointLock();
            ScheduleCheckpoint(TimeSpan.FromSeconds(1));
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
            for (int i = 0; i < 1_000; i++)
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
                await output.SendWatermark(new FlowtideDotNet.Base.Watermark("dummy", _watermarkCounter++));
                output.ExitCheckpointLock();
                ScheduleCheckpoint(TimeSpan.FromSeconds(1));
                await RegisterTrigger("on_check", TimeSpan.FromSeconds(5));
            }
        }
    }
}
