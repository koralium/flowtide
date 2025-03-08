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
using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.TableFunction
{
    internal class TableFunctionReadOperator : IngressVertex<StreamEventBatch>
    {
        private IReadOnlySet<string>? _watermarkNames;
        private readonly TableFunctionRelation _tableFunctionRelation;
        private readonly IFunctionsRegister _functionsRegister;
        private Func<EventBatchData, int, IEnumerable<EventBatchWeighted>>? _func;
        private IObjectState<TableFunctionReadState>? _state;

        private ICounter<long>? _eventsCounter;
        private ICounter<long>? _eventsProcessed;

        public TableFunctionReadOperator(TableFunctionRelation tableFunctionRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions options) : base(options)
        {
            _tableFunctionRelation = tableFunctionRelation;
            _functionsRegister = functionsRegister;
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

        public override Task OnTrigger(string triggerName, object? state)
        {
            return Task.CompletedTask;
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            if (_watermarkNames != null)
            {
                return Task.FromResult(_watermarkNames);
            }
            throw new InvalidOperationException("Get watermarks called before initialize");
        }

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            _state = await stateManagerClient.GetOrCreateObjectStateAsync<TableFunctionReadState>("table_func_state");
            if (_state.Value == null)
            {
                _state.Value = new TableFunctionReadState()
                {
                    HasSentInitial = false
                };
            }
            if (_eventsCounter == null)
            {
                _eventsCounter = Metrics.CreateCounter<long>("events");
            }
            if (_eventsProcessed == null)
            {
                _eventsProcessed = Metrics.CreateCounter<long>("events_processed");
            }

            _watermarkNames = new HashSet<string>() { Name };

            var compileResult = ColumnTableFunctionCompiler.CompileWithArg(_tableFunctionRelation.TableFunction, _functionsRegister, MemoryAllocator);
            _func = compileResult.Function;
        }

        protected override async Task OnCheckpoint(long checkpointTime)
        {
            Debug.Assert(_state != null);
            await _state.Commit();
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(_func != null);
            Debug.Assert(_eventsCounter != null);
            Debug.Assert(_eventsProcessed != null);
            Debug.Assert(_state?.Value != null);

            if (!_state.Value.HasSentInitial)
            {
                var batches = _func(new EventBatchData(Array.Empty<IColumn>()), 0);


                foreach(var batch in batches)
                {
                    _eventsCounter.Add(batch.Count);
                    _eventsProcessed.Add(batch.Count);
                    await output.SendAsync(new StreamEventBatch(batch));
                }

                await output.SendWatermark(new Base.Watermark(Name, 1));
                ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
                _state.Value.HasSentInitial = true;
            }
        }
    }
}
