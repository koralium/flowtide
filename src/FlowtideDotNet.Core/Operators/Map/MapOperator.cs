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
using FlowtideDotNet.Storage.StateManager;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Map
{
    public class MapOperator : UnaryVertex<StreamEventBatch, object?>
    {
        private readonly Schema outputSchema;
        private readonly Func<RowEvent, RowEvent> mapFunction;
        private int count;

        public override string DisplayName => "Map";

        public MapOperator(
            Schema outputSchema,
            Func<RowEvent, RowEvent> mapFunction, 
            ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            this.outputSchema = outputSchema;
            this.mapFunction = mapFunction;
        }

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override Task<object?> OnCheckpoint()
        {
            return Task.FromResult<object?>(null);
        }

        public override async IAsyncEnumerable<StreamEventBatch> OnRecieve(StreamEventBatch msg, long time)
        {
            List<RowEvent> events = new List<RowEvent>();
            
            foreach (var b in msg.Events)
            {
                events.Add(mapFunction(b));
            }

            StreamEventBatch streamEventBatch = new StreamEventBatch(outputSchema, events);
            yield return streamEventBatch;
        }

        protected override Task InitializeOrRestore(object? state, IStateManagerClient stateManagerClient)
        {
            return Task.CompletedTask;
        }
    }
}
