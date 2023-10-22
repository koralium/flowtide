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

using FlowtideDotNet.Base.Vertices.Unary;
using FlowtideDotNet.Core.Storage;
using FlowtideDotNet.Storage.StateManager;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.StatefulUnary
{
    /// <summary>
    /// Operator that helps implement Unary stateful operators
    /// </summary>
    internal abstract class StatefulUnaryOperator : UnaryVertex<StreamEventBatch, object?>
    {
        private IStorageTree<StreamEvent>? _storageTree;

        public StatefulUnaryOperator(ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
        }

        protected IStorageTree<StreamEvent> Storage => _storageTree!;

        public override async Task<object?> OnCheckpoint()
        {
            await _storageTree!.Checkpoint();
            return null;
        }

        public override Task Compact()
        {
            return _storageTree!.Compact();
        }

        protected override Task InitializeOrRestore(object? state, IStateManagerClient stateManagerClient)
        {
            _storageTree = ZoneTreeCreator.CreateTree(StreamName, Name, "state", 0);

            return Task.CompletedTask;
        }
    }
}
