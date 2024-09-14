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

using FlexBuffers;
using FlowtideDotNet.Base.Metrics;
using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Operators.VirtualTable;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.TableFunction
{
    internal class TableFunctionReadOperator : IngressVertex<StreamEventBatch, TableFunctionReadState>
    {
        private IReadOnlySet<string>? _watermarkNames;
        private readonly Func<RowEvent, IEnumerable<RowEvent>> _func;
        private readonly TableFunctionRelation _tableFunctionRelation;
        private bool _hasSentInitial = false;
        private RowEvent _emptyRow;

        private ICounter<long>? _eventsCounter;
        private ICounter<long>? _eventsProcessed;

        public TableFunctionReadOperator(TableFunctionRelation tableFunctionRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions options) : base(options)
        {
            _func = TableFunctionCompiler.CompileWithArg(tableFunctionRelation.TableFunction, functionsRegister);
            _emptyRow = new RowEvent();
            this._tableFunctionRelation = tableFunctionRelation;
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

        protected override Task InitializeOrRestore(long restoreTime, TableFunctionReadState? state, IStateManagerClient stateManagerClient)
        {
            if (_eventsCounter == null)
            {
                _eventsCounter = Metrics.CreateCounter<long>("events");
            }
            if (_eventsProcessed == null)
            {
                _eventsProcessed = Metrics.CreateCounter<long>("events_processed");
            }

            _watermarkNames = new HashSet<string>() { Name };

            if (state != null)
            {
                _hasSentInitial = state.HasSentInitial;
            }

            return Task.CompletedTask;
        }

        protected override Task<TableFunctionReadState> OnCheckpoint(long checkpointTime)
        {
            return Task.FromResult(new TableFunctionReadState()
            {
                HasSentInitial = _hasSentInitial
            });
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(_eventsCounter != null);
            Debug.Assert(_eventsProcessed != null);

            if (!_hasSentInitial)
            {
                List<RowEvent> outputEvents = new List<RowEvent>();

                var rows = _func(_emptyRow);

                outputEvents.AddRange(rows);

                // Add metrics
                _eventsCounter.Add(outputEvents.Count);
                _eventsProcessed.Add(outputEvents.Count);

                // Send the events
                await output.SendAsync(new StreamEventBatch(outputEvents, _tableFunctionRelation.OutputLength));
                await output.SendWatermark(new Base.Watermark(Name, 1));
                ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
                _hasSentInitial = true;
            }
        }
    }
}
