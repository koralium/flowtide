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

using FlowtideDotNet.Base.Utils;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Filter
{
    internal class ColumnFilterOperator : UnaryVertex<StreamEventBatch>
    {
        private readonly FilterRelation _filterRelation;
        private readonly Func<EventBatchData, int, bool> _filter;

        public ColumnFilterOperator(FilterRelation filterRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(executionDataflowBlockOptions)
        {
            _filterRelation = filterRelation;
            _filter = ColumnBooleanCompiler.Compile(filterRelation.Condition, functionsRegister);
        }

        public override string DisplayName => "Filter";

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
            return Task.FromResult<object?>(default);
        }

        public override IAsyncEnumerable<StreamEventBatch> OnRecieve(StreamEventBatch msg, long time)
        {

            PrimitiveList<int> offsets = new PrimitiveList<int>(MemoryAllocator);
            PrimitiveList<int> weights = new PrimitiveList<int>(MemoryAllocator);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);

            var data = msg.Data;
            for (int i = 0; i < data.Count; i++)
            {
                if (_filter(data.EventBatchData, i))
                {
                    weights.Add(data.Weights[i]);
                    iterations.Add(data.Iterations[i]);
                    offsets.Add(i);
                }
            }

            if (_filterRelation.EmitSet)
            {
                bool shouldDisposeOffset = true;
                var outputColumns = new IColumn[_filterRelation.OutputLength];
                for (int i = 0; i < _filterRelation.Emit.Count; i++)
                {
                    var emitIndex = _filterRelation.Emit[i];
                    outputColumns[i] = ColumnWithOffset.CreateFlattened(data.EventBatchData.Columns[emitIndex], offsets, false, MemoryAllocator, out var offsetUsed);
                    if (offsetUsed)
                    {
                        shouldDisposeOffset = false;
                    }
                }
                if (shouldDisposeOffset)
                {
                    offsets.Dispose();
                }

                var outputData = new EventBatchData(outputColumns);
                return new SingleAsyncEnumerable<StreamEventBatch>(new StreamEventBatch(new EventBatchWeighted(weights, iterations, outputData)));
            }
            else
            {
                bool shouldDisposeOffset = true;
                var outputColumns = new IColumn[_filterRelation.OutputLength];
                for (int i = 0; i < data.EventBatchData.Columns.Count; i++)
                {
                    outputColumns[i] = ColumnWithOffset.CreateFlattened(data.EventBatchData.Columns[i], offsets, false, MemoryAllocator, out var offsetUsed);
                    if (offsetUsed)
                    {
                        shouldDisposeOffset = false;
                    }
                }
                if (shouldDisposeOffset)
                {
                    offsets.Dispose();
                }
                var outputData = new EventBatchData(outputColumns);
                return new SingleAsyncEnumerable<StreamEventBatch>(new StreamEventBatch(new EventBatchWeighted(weights, iterations, outputData)));
            }
        }

        protected override Task InitializeOrRestore(IStateManagerClient stateManagerClient)
        {
            return Task.CompletedTask;
        }
    }
}
