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

using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Base.Utils;
using FlowtideDotNet.Base.Vertices.Unary;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.TableFunction
{
    /// <summary>
    /// Operator that handles table functions when used in a join.
    /// This is used with UNNEST as an example when joining it with the original data.
    /// </summary>
    internal class TableFunctionJoinOperator : UnaryVertex<StreamEventBatch, object?>
    {
        private readonly Substrait.Relations.TableFunctionRelation _tableFunctionRelation;
        private readonly Func<RowEvent, IEnumerable<RowEvent>> _func;
        private readonly Func<RowEvent, RowEvent, bool>? _joinCondition;
        private readonly IRowData _rightNullData;

        private ICounter<long>? _eventsCounter;
        private ICounter<long>? _eventsProcessed;

        public TableFunctionJoinOperator(
            Substrait.Relations.TableFunctionRelation tableFunctionRelation, 
            FunctionsRegister functionsRegister, 
            ExecutionDataflowBlockOptions executionDataflowBlockOptions) 
            : base(executionDataflowBlockOptions)
        {
            this._tableFunctionRelation = tableFunctionRelation;

            if (_tableFunctionRelation.Input == null)
            {
                throw new InvalidOperationException("Table function must have an input when used in a join");
            }

            _func = TableFunctionCompiler.CompileWithArg(tableFunctionRelation.TableFunction, functionsRegister);
            if (tableFunctionRelation.JoinCondition != null)
            {
                // Create a boolean filter function that takes both the left and right side of the join
                _joinCondition = BooleanCompiler.Compile<RowEvent>(tableFunctionRelation.JoinCondition, functionsRegister, _tableFunctionRelation.Input.OutputLength);
            }

            _rightNullData = RowEvent.Create(0, 0, v =>
            {
                for (int i = 0; i < tableFunctionRelation.TableFunction.TableSchema.Names.Count; i++)
                {
                    v.AddNull();
                }
            }).RowData;
        }

        public override string DisplayName => "TableFunction";

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

        public override IAsyncEnumerable<StreamEventBatch> OnRecieve(StreamEventBatch msg, long time)
        {
            Debug.Assert(_eventsProcessed != null);
            Debug.Assert(_eventsCounter != null);

            _eventsProcessed.Add(msg.Events.Count);
            List<RowEvent> output = new List<RowEvent>();
            foreach(var e in msg.Events)
            {
                var newRows = _func(e);

                bool emittedAny = false;
                foreach (var newRow in newRows)
                {
                    if (_joinCondition == null || _joinCondition(e, newRow))
                    {
                        emittedAny = true;
                        output.Add(new RowEvent(
                            e.Weight * newRow.Weight,
                            e.Iteration,
                            ArrayRowData.Create(e.RowData, newRow.RowData, _tableFunctionRelation.Emit)));
                    }
                }

                if (_tableFunctionRelation.Type == JoinType.Left &&
                    !emittedAny)
                {
                    output.Add(new RowEvent(
                            e.Weight,
                            e.Iteration,
                            ArrayRowData.Create(e.RowData, _rightNullData, _tableFunctionRelation.Emit)));
                }
            }

            _eventsCounter.Add(output.Count);
            return new SingleAsyncEnumerable<StreamEventBatch>(new StreamEventBatch(output, _tableFunctionRelation.OutputLength));
        }

        protected override Task InitializeOrRestore(object? state, IStateManagerClient stateManagerClient)
        {
            if (_eventsCounter == null)
            {
                _eventsCounter = Metrics.CreateCounter<long>("events");
            }
            if (_eventsProcessed == null)
            {
                _eventsProcessed = Metrics.CreateCounter<long>("events_processed");
            }
            return Task.CompletedTask;
        }
    }
}
