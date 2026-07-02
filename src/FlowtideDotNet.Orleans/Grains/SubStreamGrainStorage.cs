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

        /// <summary>
        /// The SQL text of the stream, the plan is rebuilt from the SQL text when the grain activates.
        /// </summary>
        [Id(2)]
        public string? SqlText { get; set; }

        /// <summary>
        /// Name of the substream to start
        /// If null, the plan should not contain substreams and it will run the entire plan.
        /// </summary>
        [Id(3)]
        public string? SubstreamName { get; set; }

        /// <summary>
        /// If set, the plan is split automatically into this many substreams when the SQL text
        /// does not contain substream statements.
        /// </summary>
        [Id(4)]
        public int? SubstreamCount { get; set; }
    }
}
