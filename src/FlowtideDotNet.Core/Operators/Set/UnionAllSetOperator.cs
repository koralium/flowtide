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
using FlowtideDotNet.Base.Vertices.MultipleInput;
using FlowtideDotNet.Core.ColumnStore;
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
    /// <summary>
    /// Special set operator that combines the results of two or more queries.
    /// This requires no storage so its a seperate class.
    /// </summary>
    internal class UnionAllSetOperator : MultipleInputVertex<StreamEventBatch, SetOperatorState>
    {
        private readonly SetRelation _setRelation;

        private readonly int _inputLength;
        private readonly int[]? _emit;
        public UnionAllSetOperator(SetRelation setRelation, ExecutionDataflowBlockOptions executionDataflowBlockOptions) : base(setRelation.Inputs.Count, executionDataflowBlockOptions)
        {
            _setRelation = setRelation;

            if (setRelation.Inputs.Count <= 0)
            {
                throw new ArgumentException("Set operator must have at least one input");
            }

            _inputLength = setRelation.Inputs[0].OutputLength;
            for (int i = 0; i < setRelation.Inputs.Count; i++)
            {
                if (setRelation.Inputs[i].OutputLength != _inputLength)
                {
                    throw new ArgumentException("All inputs must have the same length");
                }
            }

            if (setRelation.EmitSet)
            {
                // Check if length differs
                bool difference = _inputLength != setRelation.OutputLength;

                if (!difference)
                {
                    // Check if any order is different
                    for (int i = 0; i < setRelation.Emit.Count; i++)
                    {
                        if (setRelation.Emit[i] != i)
                        {
                            difference = true;
                            break;
                        }
                    }
                }
                
                if (difference)
                {
                    _emit = setRelation.Emit.ToArray();
                }
            }
        }

        public override string DisplayName => "Set(UnionAll)";

        public override Task Compact()
        {
            return Task.CompletedTask;
        }

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override Task<SetOperatorState?> OnCheckpoint()
        {
            return Task.FromResult<SetOperatorState?>(new SetOperatorState());
        }

        public override IAsyncEnumerable<StreamEventBatch> OnRecieve(int targetId, StreamEventBatch msg, long time)
        {
            if (_emit == null)
            {
                return new SingleAsyncEnumerable<StreamEventBatch>(msg);
            }
            else
            {
                IColumn[] columns = new IColumn[_emit.Length];

                for (int i = 0; i < _emit.Length; i++)
                {
                    columns[i] = msg.Data.EventBatchData.Columns[_emit[i]];
                }

                var newBatch = new EventBatchWeighted(msg.Data.Weights, msg.Data.Iterations, new EventBatchData(columns));
                return new SingleAsyncEnumerable<StreamEventBatch>(new StreamEventBatch(newBatch));
            }
        }

        protected override Task InitializeOrRestore(SetOperatorState? state, IStateManagerClient stateManagerClient)
        {
            return Task.CompletedTask;
        }
    }
}
