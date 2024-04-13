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

using System.Text.Json;
using System.Threading.Tasks.Dataflow;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Sources.Generic.Internal
{
    internal class GenericReadOperator<T> : BatchableReadBaseOperator
        where T : class
    {
        private readonly GenericDataSourceAsync<T> genericDataSource;
        private readonly ObjectToRowEvent objectToRowEvent;

        public GenericReadOperator(ReadRelation readRelation, GenericDataSourceAsync<T> genericDataSource, IFunctionsRegister functionsRegister, DataflowBlockOptions options) : base(readRelation, functionsRegister, options)
        {
            this.genericDataSource = genericDataSource;
            objectToRowEvent = new ObjectToRowEvent(readRelation);
        }

        protected override async IAsyncEnumerable<BatchableReadEvent> DeltaLoad(long lastWatermark)
        {
            await foreach (var ev in genericDataSource.DeltaLoadAsync(lastWatermark))
            {
                var rowEvent = objectToRowEvent.Convert(ev.Value, ev.isDelete);
                yield return new BatchableReadEvent(ev.Key, rowEvent, ev.Watermark);
            }
        }

        protected override async IAsyncEnumerable<BatchableReadEvent> FullLoad()
        {
            // Read data, convert to json, create row event from the json and then send.
            await foreach (var ev in genericDataSource.FullLoadAsync())
            {
                var rowEvent = objectToRowEvent.Convert(ev.Value, ev.isDelete);
                yield return new BatchableReadEvent(ev.Key, rowEvent, ev.Watermark);
            }
        }

        protected override TimeSpan? GetFullLoadSchedule()
        {
            return genericDataSource.FullLoadInterval;
        }

        protected override TimeSpan? GetDeltaLoadTimeSpan()
        {
            return genericDataSource.DeltaLoadInterval;
        }
    }
}
