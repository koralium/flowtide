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

namespace FlowtideDotNet.Orleans.Grains
{
    [GenerateSerializer]
    public class SubStreamGrainStorage
    {
        [Id(1)]
        public string? StreamName { get; set; }

        // Id(2) retired: was the SQL text, from when substream grains rebuilt plans from SQL
        // themselves. Id(4) retired: was the substream count used by that rebuild.

        /// <summary>
        /// Name of the substream to start
        /// If null, the plan should not contain substreams and it will run the entire plan.
        /// </summary>
        [Id(3)]
        public string? SubstreamName { get; set; }

        /// <summary>
        /// The final distributed plan as serialized Substrait JSON, prepared centrally by the
        /// stream grain. The plan is deserialized fresh whenever the stream is built - after
        /// a grain activation or a keep alive restart - since building a stream mutates the
        /// plan instance in place.
        /// </summary>
        [Id(5)]
        public string? PlanJson { get; set; }
    }
}
