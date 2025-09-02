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
using FlowtideDotNet.Base.Vertices.PartitionVertices;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Storage.StateManager;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Partition
{
    internal class PartitionOperator : PartitionVertex<StreamEventBatch>
    {
        private readonly int targetNumber;
        private readonly Func<RowEvent, uint> _partitionFunction;
        private List<RowEvent>?[] outputs;
        private ICounter<long>? _eventsProcessed;

        public PartitionOperator(PartitionOperatorOptions options, FunctionsRegister functionsRegister, int targetNumber, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(targetNumber, executionDataflowBlockOptions)
        {
            this.targetNumber = targetNumber;
            _partitionFunction = HashCompiler.CompileGetHashCode(options.Expressions, functionsRegister);
            outputs = new List<RowEvent>[targetNumber];
        }

        public override string DisplayName => "Partition";

        protected override Task InitializeOrRestore(long restoreVersion, IStateManagerClient stateManagerClient)
        {
            if (_eventsProcessed == null)
            {
                _eventsProcessed = Metrics.CreateCounter<long>("events_processed");
            }
            return Task.CompletedTask;
        }

        protected override IAsyncEnumerable<KeyValuePair<int, StreamMessage<StreamEventBatch>>> PartitionData(StreamEventBatch data, long time)
        {
            Debug.Assert(_eventsProcessed != null);
            _eventsProcessed.Add(data.Events.Count);
            int columnCount = 0;
            foreach (var e in data.Events)
            {
                columnCount = e.Length;
                var hash = _partitionFunction(e);
                var partitionId = hash % targetNumber;
                if (outputs[partitionId] == null)
                {
                    outputs[partitionId] = new List<RowEvent>();
                }
                outputs[partitionId]!.Add(e);
            }

            List<KeyValuePair<int, StreamMessage<StreamEventBatch>>> output = new List<KeyValuePair<int, StreamMessage<StreamEventBatch>>>();
            for (int i = 0; i < targetNumber; i++)
            {
                if (outputs[i] != null)
                {
                    output.Add(new KeyValuePair<int, StreamMessage<StreamEventBatch>>(i, new StreamMessage<StreamEventBatch>(new StreamEventBatch(outputs[i]!, columnCount), time)));
                    outputs[i] = null;
                }
            }
            if (output.Count > 0)
            {
                return output.ToAsyncEnumerable();
            }
            return EmptyAsyncEnumerable<KeyValuePair<int, StreamMessage<StreamEventBatch>>>.Instance;
        }
    }
}
