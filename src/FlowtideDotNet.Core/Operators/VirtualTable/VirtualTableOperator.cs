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
using FlowtideDotNet.Storage.StateManager;
using FlexBuffers;
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Operators.VirtualTable
{
    internal class VirtualTableOperator : IngressVertex<StreamEventBatch, VirtualTableState>
    {
        private readonly VirtualTableReadRelation virtualTableReadRelation;
        private IReadOnlySet<string>? watermarkNames;
        private bool hasSentInitial = false;

        public override string DisplayName => "Virtual Table";

        public VirtualTableOperator(VirtualTableReadRelation virtualTableReadRelation, DataflowBlockOptions options) : base(options)
        {
            this.virtualTableReadRelation = virtualTableReadRelation;
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

        protected override Task InitializeOrRestore(long restoreTime, VirtualTableState? state, IStateManagerClient stateManagerClient)
        {
            watermarkNames = new HashSet<string>() { Name };

            if (state != null)
            {
                hasSentInitial = state.HasSentInitial;
            }
            
            return Task.CompletedTask;
        }

        protected override Task<VirtualTableState> OnCheckpoint(long checkpointTime)
        {
            return Task.FromResult(new VirtualTableState()
            {
                HasSentInitial = hasSentInitial
            });
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            List<RowEvent> outputEvents = new List<RowEvent>();
            foreach(var row in virtualTableReadRelation.Values.JsonValues)
            {
                // TODO: Take payload from row and fill in actual values
                //var payload = JsonToFlexBufferConverter.Convert(row);

                var vectorPayload = FlexBufferBuilder.Vector(v =>
                {
                    // TODO: Implement
                });

                outputEvents.Add(new RowEvent(1, 0, new CompactRowData(vectorPayload)));
                
            }

            await output.SendAsync(new StreamEventBatch(outputEvents, virtualTableReadRelation.OutputLength));
            await output.SendWatermark(new Base.Watermark(Name, 1));
        }
    }
}
