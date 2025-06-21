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
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.Failure
{
    internal class TestIngressFactory : RegexConnectorSourceFactory
    {
        public TestIngressFactory(string regexPattern) : base(regexPattern)
        {
        }

        public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
        {
            return new TestIngress(dataflowBlockOptions);
        }
    }

    internal class TestIngress : ReadBaseOperator
    {
        public TestIngress(DataflowBlockOptions options) : base(options)
        {
        }

        public override string DisplayName => "Read";

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override Task OnTrigger(string triggerName, object? state)
        {
            // Check change data here
            return Task.CompletedTask;
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            HashSet<string> watermarks = new HashSet<string>()
            {
                "test"
            };
            return Task.FromResult<IReadOnlySet<string>>(watermarks);
        }

        protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            return Task.CompletedTask;
        }

        protected override Task<object> OnCheckpoint(long checkpointTime)
        {
            return Task.FromResult(new object());
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            await output.EnterCheckpointLock();
            var streamEvent = RowEvent.Create(1, 0, b =>
            {
                b.Add("namn1");
                b.Add("namn2");
                b.Add(90210);
            });
            await output.SendAsync(new StreamEventBatch(new List<RowEvent>()
            {
                streamEvent
            }, 3));
            await output.SendWatermark(new FlowtideDotNet.Base.Watermark("test", LongWatermarkValue.Create(1)));
            output.ExitCheckpointLock();
            await this.RegisterTrigger("on_check", TimeSpan.FromSeconds(5));
            this.ScheduleCheckpoint(TimeSpan.FromSeconds(1));
        }
    }
}
