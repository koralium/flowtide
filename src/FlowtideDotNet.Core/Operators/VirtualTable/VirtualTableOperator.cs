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

using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.VirtualTable
{
    internal class VirtualTableOperator : IngressVertex<StreamEventBatch>
    {
        private readonly VirtualTableReadRelation virtualTableReadRelation;
        private readonly IFunctionsRegister functionsRegister;
        private IReadOnlySet<string>? watermarkNames;
        private IObjectState<VirtualTableState>? _state;
        private int[] _emitList;
        public override string DisplayName => "Virtual Table";

        public VirtualTableOperator(VirtualTableReadRelation virtualTableReadRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions options) : base(options)
        {
            this.virtualTableReadRelation = virtualTableReadRelation;
            this.functionsRegister = functionsRegister;

            if (virtualTableReadRelation.EmitSet)
            {
                _emitList = virtualTableReadRelation.Emit.ToArray();
            }
            else
            {
                _emitList = new int[virtualTableReadRelation.OutputLength];
                for (int i = 0; i < virtualTableReadRelation.OutputLength; i++)
                {
                    _emitList[i] = i;
                }
            }
        }

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
            if (watermarkNames != null)
            {
                return Task.FromResult(watermarkNames);
            }
            throw new InvalidOperationException("Get watermarks called before initialize");
        }

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            watermarkNames = new HashSet<string>() { Name };

            _state = await stateManagerClient.GetOrCreateObjectStateAsync<VirtualTableState>("virtual_table_state");
            if (_state.Value == null)
            {
                _state.Value = new VirtualTableState()
                {
                    HasSentInitial = false
                };
            }

        }

        protected override async Task OnCheckpoint(long checkpointTime)
        {
            Debug.Assert(_state != null);
            await _state.Commit();
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(_state?.Value != null);
            if (_state.Value.HasSentInitial)
            {
                return;
            }
            await output.EnterCheckpointLock();
            var emptyBatch = new EventBatchData(Array.Empty<IColumn>());
            Column[] columns = new Column[virtualTableReadRelation.OutputLength];
            PrimitiveList<int> weights = new PrimitiveList<int>(MemoryAllocator);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);

            for (int i = 0; i < virtualTableReadRelation.OutputLength; i++)
            {
                columns[i] = Column.Create(MemoryAllocator);
            }

            foreach (var row in virtualTableReadRelation.Values.Expressions)
            {
                weights.Add(1);
                iterations.Add(0);
                for (int i = 0; i < _emitList.Length; i++)
                {
                    var compiledColumnProjection = ColumnProjectCompiler.Compile(row.Fields[_emitList[i]], functionsRegister);
                    compiledColumnProjection(emptyBatch, 0, columns[i]);
                }
            }

            var outputBatch = new EventBatchData(columns);
            await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, outputBatch)));
            await output.SendWatermark(new Base.Watermark(Name, 1));
            _state.Value.HasSentInitial = true;
            output.ExitCheckpointLock();
            ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
        }
    }
}
