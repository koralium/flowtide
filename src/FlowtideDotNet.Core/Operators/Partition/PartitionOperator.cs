﻿// Licensed under the Apache License, Version 2.0 (the "License")
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
using FlowtideDotNet.Base.Vertices.PartitionVertices;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Storage.StateManager;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Partition
{
    internal class PartitionOperator : PartitionVertex<StreamEventBatch, object>
    {
        private readonly int targetNumber;
        private readonly Func<StreamEvent, uint> _partitionFunction;
        private List<StreamEvent>?[] outputs;

        public PartitionOperator(PartitionOperatorOptions options, FunctionsRegister functionsRegister, int targetNumber, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(targetNumber, executionDataflowBlockOptions)
        {
            this.targetNumber = targetNumber;
            _partitionFunction = HashCompiler.CompileGetHashCode(options.Expressions, functionsRegister);
            outputs = new List<StreamEvent>[targetNumber];
        }

        public override string DisplayName => "Partition";

        protected override Task InitializeOrRestore(object? state, IStateManagerClient stateManagerClient)
        {
            return Task.CompletedTask;
        }

        protected override async IAsyncEnumerable<KeyValuePair<int, StreamMessage<StreamEventBatch>>> PartitionData(StreamEventBatch data, long time)
        {
            foreach(var e in data.Events)
            {
                var hash = _partitionFunction(e);
                var partitionId = hash % targetNumber;
                if (outputs[partitionId] == null)
                {
                    outputs[partitionId] = new List<StreamEvent>();
                }
                outputs[partitionId]!.Add(e);
            }
            for (int i = 0; i < targetNumber; i++)
            {
                if (outputs[i] != null)
                {
                    yield return new KeyValuePair<int, StreamMessage<StreamEventBatch>>(i, new StreamMessage<StreamEventBatch>(new StreamEventBatch(null, outputs[i]!), time));
                    outputs[i] = null;
                }
            }
        }
    }
}
