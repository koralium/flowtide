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

using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Storage.StateManager;
using System.Threading.Tasks.Dataflow;

namespace SqlSampleWithUI
{
    public class DummyWriteOperator : WriteBaseOperator<object>
    {
        public DummyWriteOperator(ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
        }

        public override string DisplayName => "Dummy write";

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        protected override Task InitializeOrRestore(long restoreTime, object? state, IStateManagerClient stateManagerClient)
        {
            return Task.CompletedTask;
        }

        protected override Task<object> OnCheckpoint(long checkpointTime)
        {
            return Task.FromResult(new object());
        }

        protected override Task OnRecieve(StreamEventBatch msg, long time)
        {
            return Task.CompletedTask;
        }
    }
}
