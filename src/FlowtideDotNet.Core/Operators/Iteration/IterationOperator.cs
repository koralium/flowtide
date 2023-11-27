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
using FlowtideDotNet.Base.Vertices.FixedPoint;
using FlowtideDotNet.Storage.StateManager;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Iteration
{
    internal class IterationState
    {

    }
    internal class IterationOperator : FixedPointVertex<StreamEventBatch, IterationState>
    {
        public IterationOperator(ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
        }

        public override string DisplayName => "Iteration Operator";

        protected override Task InitializeOrRestore(IterationState? state, IStateManagerClient stateManagerClient)
        {
            return Task.CompletedTask;
        }

        protected override async IAsyncEnumerable<KeyValuePair<int, StreamMessage<StreamEventBatch>>> OnFeedbackRecieve(StreamEventBatch data, long time)
        {
            // At this time, send data to egress and to loop after each feedback.
            List<RowEvent> loopOutput = new List<RowEvent>();
            List<RowEvent> egressOutput = new List<RowEvent>();
            foreach (var streamEvent in data.Events)
            {
                // Increase iteration counter for the loop output.
                loopOutput.Add(new RowEvent(streamEvent.Weight, streamEvent.Iteration + 1, streamEvent.RowData));
                // Reset iteration counter for the egress output.
                egressOutput.Add(new RowEvent(streamEvent.Weight, 0, streamEvent.RowData));
            }

            yield return new KeyValuePair<int, StreamMessage<StreamEventBatch>>(0, new StreamMessage<StreamEventBatch>(new StreamEventBatch(null, egressOutput), time));
            yield return new KeyValuePair<int, StreamMessage<StreamEventBatch>>(1, new StreamMessage<StreamEventBatch>(new StreamEventBatch(null, loopOutput), time));
        }

        protected override async IAsyncEnumerable<KeyValuePair<int, StreamMessage<StreamEventBatch>>> OnIngressRecieve(StreamEventBatch data, long time)
        {
            yield return new KeyValuePair<int, StreamMessage<StreamEventBatch>>(0, new StreamMessage<StreamEventBatch>(data, time));
            yield return new KeyValuePair<int, StreamMessage<StreamEventBatch>>(1, new StreamMessage<StreamEventBatch>(data, time));
        }
    }
}
