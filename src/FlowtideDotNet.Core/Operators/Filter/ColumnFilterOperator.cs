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
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.Memory;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.Filter
{
    internal class ColumnFilterOperator : UnaryVertex<StreamEventBatch, object?>
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

        public override async IAsyncEnumerable<StreamEventBatch> OnRecieve(StreamEventBatch msg, long time)
        {

            PrimitiveList<int>[] offsets = new PrimitiveList<int>[_filterRelation.OutputLength];
            PrimitiveList<int> weights = new PrimitiveList<int>(MemoryAllocator);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);

            for (int i = 0; i < offsets.Length; i++)
            {
                offsets[i] = new PrimitiveList<int>(MemoryAllocator);
            }

            var data = msg.Data;
            for (int i = 0; i < data.Count; i++)
            {
                if (_filter(data.EventBatchData, i))
                {
                    weights.Add(data.Weights[i]);
                    iterations.Add(data.Iterations[i]);
                    for (int j = 0; j < offsets.Length; j++)
                    {
                        offsets[j].Add(i);
                    }
                }
            }

            if (_filterRelation.EmitSet)
            {
                var outputColumns = new IColumn[_filterRelation.OutputLength];
                for (int i = 0; i < _filterRelation.Emit.Count; i++)
                {
                    var emitIndex = _filterRelation.Emit[i];
                    outputColumns[i] = new ColumnWithOffset(data.EventBatchData.Columns[emitIndex], offsets[i], false);
                }

                var outputData = new EventBatchData(outputColumns);
                yield return new StreamEventBatch(new EventBatchWeighted(weights, iterations, outputData));
            }
            else
            {
                var outputColumns = new IColumn[_filterRelation.OutputLength];
                for (int i = 0; i < data.EventBatchData.Columns.Count; i++)
                {
                    outputColumns[i] = new ColumnWithOffset(data.EventBatchData.Columns[i], offsets[i], false);
                }
                var outputData = new EventBatchData(outputColumns);
                yield return new StreamEventBatch(new EventBatchWeighted(weights, iterations, outputData));
            }
        }

        protected override Task InitializeOrRestore(object? state, IStateManagerClient stateManagerClient)
        {
            return Task.CompletedTask;
        }
    }
}
