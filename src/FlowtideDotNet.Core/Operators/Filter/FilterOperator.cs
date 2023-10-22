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
using FlowtideDotNet.Core.Operators.Filter.Internal;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging;
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Filter
{
    internal class FilterOperator : UnaryVertex<StreamEventBatch, object?>
    {
        public override string DisplayName => "Filter";

        private readonly IFilterImplementation _filterImplementation;
        public FilterOperator(FilterRelation filterRelation, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            var containsDate = new ContainsDateFilterVisitor().Visit(filterRelation.Condition, null);

            if (!containsDate)
            {
                _filterImplementation = new NormalFilterImpl(filterRelation);
            }
            else
            {
                _filterImplementation = new DateFilterLoopAll(filterRelation);
            }
        }

        public override Task Compact()
        {
            return _filterImplementation.Compact();
        }

        public override Task<object?> OnCheckpoint()
        {
            return _filterImplementation.OnCheckpoint();
        }

        public override IAsyncEnumerable<StreamEventBatch> OnTrigger(string name, object? state)
        {
            return _filterImplementation.OnTrigger(name, state);
        }

        public override IAsyncEnumerable<StreamEventBatch> OnRecieve(StreamEventBatch msg, long time)
        {
            return _filterImplementation.OnRecieve(msg, time);
        }

        protected override Task InitializeOrRestore(object? state, IStateManagerClient stateManagerClient)
        {
            Logger.LogInformation("Initializing filter operator.");
            return _filterImplementation.InitializeOrRestore(StreamName, Name, RegisterTrigger, state);
        }

        public override Task DeleteAsync()
        {
            return _filterImplementation.DeleteAsync();
        }
    }
}
