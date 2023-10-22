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
using FlowtideDotNet.Core.Storage;
using FlowtideDotNet.Storage.StateManager;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.StatefulBinary
{
    public abstract class StatefulBinaryOperator : MultipleInputVertex<StreamEventBatch, object>
    {
        private IStorageTree<StreamEvent>? _leftStorageTree;
        private IStorageTree<StreamEvent>? _rightStorageTree;
        public StatefulBinaryOperator(ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(2, executionDataflowBlockOptions)
        {
        }

        protected IStorageTree<StreamEvent> Input1Storage => _leftStorageTree!;

        protected IStorageTree<StreamEvent> Input2Storage => _rightStorageTree!;

        public override Task Compact()
        {
            var leftTask = _leftStorageTree!.Compact();
            var rightTask = _rightStorageTree!.Compact();
            return Task.WhenAll(leftTask, rightTask);
        }

        public override async Task<object> OnCheckpoint()
        {
            var leftTask = _leftStorageTree!.Checkpoint();
            var rightTask = _rightStorageTree!.Checkpoint();
            await Task.WhenAll(leftTask, rightTask);
            return null;
        }

        protected abstract IAsyncEnumerable<StreamEventBatch> OnInput1(StreamEventBatch streamEventBatch, long time);

        protected abstract IAsyncEnumerable<StreamEventBatch> OnInput2(StreamEventBatch streamEventBatch, long time);

        public override IAsyncEnumerable<StreamEventBatch> OnRecieve(int targetId, StreamEventBatch msg, long time)
        {
            if (targetId == 0)
            {
                return OnInput1(msg, time);
            }
            else if (targetId == 1)
            {
                return OnInput2(msg, time);
            }
            else
            {
                throw new NotSupportedException("Unpexpected target id");
            }
        }

        protected override Task InitializeOrRestore(object? state, IStateManagerClient stateManagerClient)
        {
            _leftStorageTree = ZoneTreeCreator.CreateTree(StreamName, Name, "left", 0);
            _rightStorageTree = ZoneTreeCreator.CreateTree(StreamName, Name, "right", 0);
            return Task.CompletedTask;
        }
    }
}
