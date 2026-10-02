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

using Confluent.Kafka;
using FlowtideDotNet.Core.Lineage;

namespace FlowtideDotNet.OpenLineage.Kafka
{
    /// <summary>
    /// Options for sending OpenLineage events to a Kafka topic
    /// </summary>
    public class OpenLineageKafkaOptions : OpenLineageOptions
    {
        /// <summary>
        /// Configuration of the Kafka producer, required
        /// </summary>
        public ProducerConfig? ProducerConfig { get; set; }

        /// <summary>
        /// Topic the events are written to, required
        /// </summary>
        public string? TopicName { get; set; }

        /// <summary>
        /// Key for every message, defaults to run:{job namespace}/{job name}
        /// </summary>
        public string? MessageKey { get; set; }
    }
}
