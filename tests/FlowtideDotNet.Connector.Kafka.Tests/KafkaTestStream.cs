﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Engine;

namespace FlowtideDotNet.Connector.Kafka.Tests
{
    internal class KafkaTestStream : FlowtideTestStream
    {
        private readonly KafkaFixture kafkaFixture;
        private readonly string topic;
        private readonly string testName;
        private readonly bool fetchExisting;

        public KafkaTestStream(KafkaFixture kafkaFixture, string topic, string testName, bool fetchExisting) : base(testName)
        {
            this.kafkaFixture = kafkaFixture;
            this.topic = topic;
            this.testName = testName;
            this.fetchExisting = fetchExisting;
        }

        protected override void AddReadResolvers(IConnectorManager factory)
        {
            factory.AddKafkaSource("*", new FlowtideKafkaSourceOptions()
            {
                ConsumerConfig = kafkaFixture.GetConsumerConfig(testName),
                KeyDeserializer = new FlowtidekafkaStringKeyDeserializer(),
                ValueDeserializer = new FlowtideKafkaUpsertJsonDeserializer()
            });
        }

        protected override void AddWriteResolvers(IConnectorManager factory)
        {
            var opt = new FlowtideKafkaSinkOptions()
            {
                KeySerializer = new FlowtideKafkaStringKeySerializer(),
                ProducerConfig = kafkaFixture.GetProducerConfig(),
                ValueSerializer = new FlowtideKafkaUpsertJsonSerializer(),
                EventProcessor = (events) =>
                {
                    return Task.CompletedTask;
                },
                OnInitialDataSent = (producer, writeRel, topicName) =>
                {
                    return Task.CompletedTask;
                }
            };
            if (fetchExisting)
            {
                opt.FetchExistingConfig = kafkaFixture.GetConsumerConfig(testName);
                opt.FetchExistingValueDeserializer = new FlowtideKafkaUpsertJsonDeserializer();
                opt.FetchExistingKeyDeserializer = new FlowtidekafkaStringKeyDeserializer();
            }

            factory.AddKafkaSink("*", opt);
        }
    }
}
