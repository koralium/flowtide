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
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Base.Vertices.FixedPoint;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Iteration
{
    internal class IterationState
    {

    }
    internal class IterationOperator : FixedPointVertex<StreamEventBatch, IterationState>
    {
        private readonly IterationRelation _iterationRelation;
        private ICounter<long>? _eventsProcessed;
        private readonly Func<RowEvent, bool>? _expression;
        public IterationOperator(IterationRelation iterationRelation, FunctionsRegister functionsRegister, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            this._iterationRelation = iterationRelation;
            if (iterationRelation.SkipIterateCondition != null)
            {
                _expression = BooleanCompiler.Compile<RowEvent>(iterationRelation.SkipIterateCondition, functionsRegister);
            }
            
        }

        public override string DisplayName => "Iteration Operator";

        public override bool NoReadSourceInLoop()
        {
            var visitor = new ReadSourceLocateVisitor();
            visitor.Visit(_iterationRelation, new object());
            return visitor.ReadExists;
        }

        protected override Task InitializeOrRestore(IterationState? state, IStateManagerClient stateManagerClient)
        {
            if (_eventsProcessed == null)
            {
                _eventsProcessed = Metrics.CreateCounter<long>("events_processed");
            }
            return Task.CompletedTask;
        }

        protected override async IAsyncEnumerable<KeyValuePair<int, StreamMessage<StreamEventBatch>>> OnFeedbackRecieve(StreamEventBatch data, long time)
        {
            Debug.Assert(_eventsProcessed != null);
            _eventsProcessed.Add(data.Events.Count);
            // At this time, send data to egress and to loop after each feedback.
            List<RowEvent> loopOutput = new List<RowEvent>();
            List<RowEvent> egressOutput = new List<RowEvent>();
            foreach (var streamEvent in data.Events)
            {
                // Stop iteration if the max iteration is reached.
                if (_iterationRelation.MaxIterations != null && streamEvent.Iteration >= _iterationRelation.MaxIterations.Value)
                {
                    continue;
                }
                // Increase iteration counter for the loop output.
                if (_expression == null || !_expression(streamEvent))
                {
                    loopOutput.Add(new RowEvent(streamEvent.Weight, streamEvent.Iteration + 1, streamEvent.RowData));
                }
                
                // Reset iteration counter for the egress output.
                egressOutput.Add(new RowEvent(streamEvent.Weight, 0, streamEvent.RowData));
            }

            if (egressOutput.Count > 0)
            {
                yield return new KeyValuePair<int, StreamMessage<StreamEventBatch>>(0, new StreamMessage<StreamEventBatch>(new StreamEventBatch(egressOutput), time));
            }
            if (loopOutput.Count > 0)
            {
                yield return new KeyValuePair<int, StreamMessage<StreamEventBatch>>(1, new StreamMessage<StreamEventBatch>(new StreamEventBatch(loopOutput), time));
            }
        }

        protected override async IAsyncEnumerable<KeyValuePair<int, StreamMessage<StreamEventBatch>>> OnIngressRecieve(StreamEventBatch data, long time)
        {
            Debug.Assert(_eventsProcessed != null);
            _eventsProcessed.Add(data.Events.Count);
            yield return new KeyValuePair<int, StreamMessage<StreamEventBatch>>(0, new StreamMessage<StreamEventBatch>(data, time));
            yield return new KeyValuePair<int, StreamMessage<StreamEventBatch>>(1, new StreamMessage<StreamEventBatch>(data, time));
        }
    }
}
