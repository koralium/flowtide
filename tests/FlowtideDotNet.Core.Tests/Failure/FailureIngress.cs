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
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.Failure
{
    internal class FailureIngressFactory : RegexConnectorSourceFactory
    {
        public FailureIngressFactory(string regexPattern) : base(regexPattern)
        {
        }

        public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
        {
            return new FailureIngress(dataflowBlockOptions);
        }
    }

    internal class FailureIngress : ReadBaseOperator<object>
    {
        public FailureIngress(DataflowBlockOptions options) : base(options)
        {
        }

        public override string DisplayName => "FailureIngress";

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
            HashSet<string> watermarks = new HashSet<string>()
            {
                "test"
            };
            return Task.FromResult<IReadOnlySet<string>>(watermarks);
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
            ScheduleCheckpoint(TimeSpan.FromSeconds(1));
            await output.EnterCheckpointLock();

            _ = RunTask( async (output, o) =>
            {
                await Task.Delay(TimeSpan.FromSeconds(10));
            });

            throw new Exception();

            output.ExitCheckpointLock();
        }
    }
}
