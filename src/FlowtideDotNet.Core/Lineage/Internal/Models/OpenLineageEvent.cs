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

using System.Text.Json.Serialization;

namespace FlowtideDotNet.Core.Lineage.Internal.Models
{
    internal class OpenLineageEvent
    {
        [JsonPropertyName("eventTime")]
        public DateTime EventTime { get; }

        [JsonPropertyName("producer")]
        public string Producer { get; }

        [JsonPropertyName("schemaURL")]
        public string SchemaURL => "https://openlineage.io/spec/0-0-1/OpenLineage.json";

        [JsonPropertyName("eventType")]
        public LineageEventType EventType { get; }

        [JsonPropertyName("run")]
        public LineageRun Run { get; }

        [JsonPropertyName("job")]
        public LineageJob Job { get; }

        [JsonPropertyName("inputs")]
        public IReadOnlyList<LineageInputTable> Inputs { get; }

        [JsonPropertyName("outputs")]
        public IReadOnlyList<LineageOutputTable> Outputs { get; }

        public OpenLineageEvent(
            DateTime eventTime,
            string producer,
            LineageEventType eventType,
            LineageRun run,
            LineageJob job,
            IReadOnlyList<LineageInputTable> inputs, 
            IReadOnlyList<LineageOutputTable> outputs)
        {
            EventTime = eventTime;
            Producer = producer;
            EventType = eventType;
            Run = run;
            Job = job;
            Inputs = inputs;
            Outputs = outputs;
        }

        public OpenLineageEvent ChangeEventType(LineageEventType eventType)
        {
            return new OpenLineageEvent(DateTime.UtcNow, Producer, eventType, Run, Job, Inputs, Outputs);
        }
    }
}
