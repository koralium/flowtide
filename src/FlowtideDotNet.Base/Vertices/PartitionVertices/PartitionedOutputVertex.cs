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

using FlowtideDotNet.Base.Vertices.MultipleInput;
using FlowtideDotNet.Storage.StateManager;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Base.Vertices.PartitionVertices
{
    public class PartitionedOutputVertex<T, TState> : MultipleInputVertex<T, TState>
    {
        public PartitionedOutputVertex(int targetCount, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(targetCount, executionDataflowBlockOptions)
        {
        }

        public override string DisplayName => "Partioned Output";

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override Task<TState?> OnCheckpoint()
        {
            return Task.FromResult(default(TState));
        }

        public override async IAsyncEnumerable<T> OnRecieve(int targetId, T msg, long time)
        {
            yield return msg;
        }

        protected override Task InitializeOrRestore(TState? state, IStateManagerClient stateManagerClient)
        {
            return Task.CompletedTask;
        }
    }
}
