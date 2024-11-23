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
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Set
{
    internal class ColumnSetOperator<TStruct> : MultipleInputVertex<StreamEventBatch, object>
    {
        public ColumnSetOperator(SetRelation setRelation, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(setRelation.Inputs.Count, executionDataflowBlockOptions)
        {
        }

        public override string DisplayName => throw new NotImplementedException();

        public override Task Compact()
        {
            throw new NotImplementedException();
        }

        public override Task DeleteAsync()
        {
            throw new NotImplementedException();
        }

        public override Task<object?> OnCheckpoint()
        {
            throw new NotImplementedException();
        }

        public override IAsyncEnumerable<StreamEventBatch> OnRecieve(int targetId, StreamEventBatch msg, long time)
        {
            throw new NotImplementedException();
        }

        protected override Task InitializeOrRestore(object? state, IStateManagerClient stateManagerClient)
        {
            throw new NotImplementedException();
        }
    }
}
