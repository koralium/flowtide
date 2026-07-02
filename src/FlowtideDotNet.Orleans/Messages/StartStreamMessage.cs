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
        /// The SQL text of the stream. Each substream grain builds the plan from the SQL text
        /// itself, plans are never sent between grains.
        /// </summary>
        [Id(2)]
        public string SqlText { get; }

        /// <summary>
        /// Name of the substream to start
        /// If null, the plan should not contain substreams and it will run the entire plan.
        /// </summary>
        [Id(3)]
        public string? SubstreamName { get; }

        /// <summary>
        /// If set, the plan is split automatically into this many substreams when the SQL text
        /// does not contain substream statements.
        /// </summary>
        [Id(4)]
        public int? SubstreamCount { get; }

        public StartStreamMessage(string streamName, string sqlText, string? substreamName, int? substreamCount = null)
        {
            StreamName = streamName;
            SqlText = sqlText;
            SubstreamName = substreamName;
            SubstreamCount = substreamCount;
        }
    }
}
