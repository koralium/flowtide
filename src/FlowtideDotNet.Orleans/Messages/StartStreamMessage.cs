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

namespace FlowtideDotNet.Orleans.Messages
{
    [GenerateSerializer]
    [Immutable]
    public class StartStreamMessage
    {
        [Id(1)]
        public string StreamName { get; }

        /// <summary>
        /// The final distributed plan as serialized Substrait JSON. The plan is prepared once,
        /// centrally, by the stream grain - from SQL text or from a user provided plan - so
        /// every substream grain runs the exact same plan. Each grain deserializes its own
        /// instance, building a stream mutates the plan in place so an instance must never be
        /// shared.
        /// </summary>
        [Id(2)]
        public string PlanJson { get; }

        /// <summary>
        /// Name of the substream to start
        /// If null, the plan should not contain substreams and it will run the entire plan.
        /// </summary>
        [Id(3)]
        public string? SubstreamName { get; }

        // Id(4) retired: was the substream count, used when substream grains rebuilt the plan
        // from SQL text themselves. The plan arrives already distributed now.

        public StartStreamMessage(string streamName, string planJson, string? substreamName)
        {
            StreamName = streamName;
            PlanJson = planJson;
            SubstreamName = substreamName;
        }
    }
}
