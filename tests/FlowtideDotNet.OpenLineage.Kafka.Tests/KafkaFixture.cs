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
using Confluent.Kafka.Admin;
using Testcontainers.Kafka;

namespace FlowtideDotNet.OpenLineage.Kafka.Tests
{
    public class KafkaFixture : IAsyncLifetime
    {
        private readonly KafkaContainer _container;
        public KafkaFixture()
        {
            _container = new KafkaBuilder("confluentinc/cp-kafka:7.5.12")
                .Build();
        }

        public ProducerConfig GetProducerConfig()
        {
            return new ProducerConfig()
            {
                BootstrapServers = _container.GetBootstrapAddress()
            };
        }

        public ConsumerConfig GetConsumerConfig(string groupId)
        {
            return new ConsumerConfig()
            {
                BootstrapServers = _container.GetBootstrapAddress(),
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };
        }

        public async Task CreateTopic(string topic)
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig() { BootstrapServers = _container.GetBootstrapAddress() }).Build();
            await adminClient.CreateTopicsAsync([new TopicSpecification() { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }]);
        }

        public async Task DisposeAsync()
        {
            await _container.DisposeAsync();
        }

        public async Task InitializeAsync()
        {
            await _container.StartAsync();
        }
    }
}
