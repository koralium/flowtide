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
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Connector.Kafka
{
    public class FlowtideKafkaSinkOptions
    {
        public required ProducerConfig ProducerConfig { get; set; }

        public required IFlowtideKafkaKeySerializer KeySerializer { get; set; }

        public required IFlowtideKafkaValueSerializer ValueSerializer { get; set; }


        public ConsumerConfig? FetchExistingConfig { get; set; }

        /// <summary>
        /// If set will fetch existing data before starting which will be used to compare the
        /// result of the stream against.
        /// 
        /// If set, FetchExistingValueDeserializer must also be set.
        /// </summary>
        public IFlowtideKafkaKeyDeserializer? FetchExistingKeyDeserializer { get; set; }

        /// <summary>
        /// If set, FetchExistingKeyDeserializer must also be set.
        /// </summary>
        public IFlowtideKafkaDeserializer? FetchExistingValueDeserializer { get; set; }

        /// <summary>
        /// A method that gets called before events are sent to kafka.
        /// It is possible here to remove or add events from the list before they are sent to kafka.
        /// 
        /// One use case is to look up sent events in a database and remove them from the list if they ere already sent.
        /// </summary>
        public Func<List<KeyValuePair<byte[], byte[]?>>, Task>? EventProcessor { get; set; }

        public Func<IProducer<byte[], byte[]?>, WriteRelation, string, Task>? OnInitialDataSent { get; set; }
    }
}
