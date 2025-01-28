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
using FlowtideDotNet.Base.Utils;
using FlowtideDotNet.Base.Vertices.FixedPoint;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Iteration
{
    internal class IterationState
    {

    }

    internal class ColumnIterationOperator : FixedPointVertex<StreamEventBatch, IterationState>
    {
        private readonly IterationRelation _iterationRelation;
        private ICounter<long>? _eventsProcessed;
        private readonly Func<EventBatchData, int, bool>? _expression;

        public ColumnIterationOperator(IterationRelation iterationRelation, FunctionsRegister functionsRegister, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            this._iterationRelation = iterationRelation;
            if (iterationRelation.SkipIterateCondition != null)
            {
                _expression = ColumnBooleanCompiler.Compile(iterationRelation.SkipIterateCondition, functionsRegister);
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

        protected override IAsyncEnumerable<KeyValuePair<int, StreamMessage<StreamEventBatch>>> OnFeedbackRecieve(StreamEventBatch data, long time)
        {
            Debug.Assert(_eventsProcessed != null);
            _eventsProcessed.Add(data.Data.Count);

            PrimitiveList<int> offsetsEgress = new PrimitiveList<int>(MemoryAllocator);
            PrimitiveList<int> offsetsLoop = new PrimitiveList<int>(MemoryAllocator);

            PrimitiveList<uint> iterationsLoop = new PrimitiveList<uint>(MemoryAllocator);
            PrimitiveList<uint> iterationsEgress = new PrimitiveList<uint>(MemoryAllocator);

            PrimitiveList<int> weightsEgress = new PrimitiveList<int>(MemoryAllocator);
            PrimitiveList<int> weightsLoop = new PrimitiveList<int>(MemoryAllocator);


            for (int i = 0; i < data.Data.Count; i++)
            {
                if (_iterationRelation.MaxIterations != null && data.Data.Iterations[i] >= _iterationRelation.MaxIterations.Value)
                {
                    continue;
                }
                // Increase iteration counter for the loop output.
                if (_expression == null || !_expression(data.Data.EventBatchData, i))
                {
                    iterationsLoop.Add(data.Data.Iterations[i] + 1);
                    offsetsLoop.Add(i);
                    weightsLoop.Add(data.Data.Weights[i]);
                }
                iterationsEgress.Add(0);
                offsetsEgress.Add(i);
                weightsEgress.Add(data.Data.Weights[i]);
            }

            List<KeyValuePair<int, StreamMessage<StreamEventBatch>>> output = new List<KeyValuePair<int, StreamMessage<StreamEventBatch>>>(2);
            if (offsetsEgress.Count > 0)
            {
                IColumn[] egressColumns = new IColumn[data.Data.EventBatchData.Columns.Count];

                for (int i = 0; i < data.Data.EventBatchData.Columns.Count; i++)
                {
                    egressColumns[i] = new ColumnWithOffset(data.Data.EventBatchData.Columns[i], offsetsEgress, false);
                }

                var egressBatch = new EventBatchWeighted(weightsEgress, iterationsEgress, new EventBatchData(egressColumns));
                output.Add(new KeyValuePair<int, StreamMessage<StreamEventBatch>>(0, new StreamMessage<StreamEventBatch>(new StreamEventBatch(egressBatch), time)));
            }
            else
            {
                offsetsEgress.Dispose();
                iterationsEgress.Dispose();
                weightsEgress.Dispose();
            }
            if (offsetsLoop.Count > 0)
            {
                IColumn[] loopColumns = new IColumn[data.Data.EventBatchData.Columns.Count];

                for (int i = 0; i < data.Data.EventBatchData.Columns.Count; i++)
                {
                    loopColumns[i] = new ColumnWithOffset(data.Data.EventBatchData.Columns[i], offsetsLoop, false);
                }

                var loopBatch = new EventBatchWeighted(weightsLoop, iterationsLoop, new EventBatchData(loopColumns));
                output.Add(new KeyValuePair<int, StreamMessage<StreamEventBatch>>(1, new StreamMessage<StreamEventBatch>(new StreamEventBatch(loopBatch), time)));
            }
            else
            {
                offsetsLoop.Dispose();
                iterationsLoop.Dispose();
                weightsLoop.Dispose();
            }
            if (output.Count > 0)
            {
                return output.ToAsyncEnumerable();
            }
            return EmptyAsyncEnumerable<KeyValuePair<int, StreamMessage<StreamEventBatch>>>.Instance;
        }

        protected override IAsyncEnumerable<KeyValuePair<int, StreamMessage<StreamEventBatch>>> OnIngressRecieve(StreamEventBatch data, long time)
        {
            Debug.Assert(_eventsProcessed != null);
            _eventsProcessed.Add(data.Data.Count);
            KeyValuePair<int, StreamMessage<StreamEventBatch>>[] output =
            [
                new KeyValuePair<int, StreamMessage<StreamEventBatch>>(0, new StreamMessage<StreamEventBatch>(data, time)),
                new KeyValuePair<int, StreamMessage<StreamEventBatch>>(1, new StreamMessage<StreamEventBatch>(data, time)),
            ];
            return output.ToAsyncEnumerable();
        }
    }
}
