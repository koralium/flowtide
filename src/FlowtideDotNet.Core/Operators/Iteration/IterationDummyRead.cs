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
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.StateManager;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Iteration
{
    /// <summary>
    /// Dummy read operator that will only be used if there is no input on ingress point on the iteration operator.
    /// This will send out checkpoint events to the iteration operator.
    /// </summary>
    internal class IterationDummyRead : ReadBaseOperator
    {
        private readonly HashSet<string> watermarkNames = new HashSet<string>();
        public IterationDummyRead(DataflowBlockOptions options) : base(options)
        {
        }

        public override string DisplayName => "Iteration Empty Read";

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
            return Task.FromResult<IReadOnlySet<string>>(watermarkNames);
        }

        protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            return Task.CompletedTask;
        }

        protected override Task OnCheckpoint(long checkpointTime)
        {
            return Task.CompletedTask;
        }
        
        protected override Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            return Task.CompletedTask;
        }
    }
}
