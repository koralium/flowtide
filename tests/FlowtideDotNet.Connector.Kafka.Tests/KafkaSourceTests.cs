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

namespace FlowtideDotNet.Connector.Kafka.Tests
{
    public class KafkaSourceTests : IClassFixture<KafkaFixture>
    {
        private readonly KafkaFixture kafkaFixture;

        public KafkaSourceTests(KafkaFixture kafkaFixture)
        {
            this.kafkaFixture = kafkaFixture;
        }

        [Fact]
        public async Task TestIngestData()
        {
            var producer = new ProducerBuilder<string, string>(kafkaFixture.GetProducerConfig()).Build();

            var topic = "test-topic";

            var jsonData = @"
                {
                    ""firstName"": ""testFirst"",
                    ""lastName"": ""testLast"",
                    ""other"": ""hello""
                }
            ";

            await producer.ProduceAsync(topic, new Message<string, string>()
            {
                Key = "key",
                Value = jsonData
            });

            // Create the output topic
            await new AdminClientBuilder(new AdminClientConfig(kafkaFixture.GetConfig())).Build().CreateTopicsAsync(new List<TopicSpecification>() { new TopicSpecification() { Name = "output", NumPartitions = 1, ReplicationFactor = 1 } });

            var consumer = new ConsumerBuilder<string, string>(kafkaFixture.GetConsumerConfig("testkafka")).Build();

            KafkaTestStream kafkaTestStream = new KafkaTestStream(kafkaFixture, topic, "testkafka", false);

            await kafkaTestStream.StartStream(@"
                CREATE TABLE [test-topic] (
                    _key,
                    firstName,
                    lastName
                );

                INSERT INTO output
                SELECT 
                    _key,
                    firstName,
                    lastName
                FROM [test-topic]
            ");


            consumer.Subscribe("output");

            var msg1 = consumer.Consume();

            Assert.Equal("key", msg1.Message.Key);
            Assert.Equal("{\"firstName\":\"testFirst\",\"lastName\":\"testLast\"}", msg1.Message.Value);

            var jsonData2 = @"
                {
                    ""firstName"": ""testFirst2"",
                    ""lastName"": ""testLast2""  
                }
            ";

            await producer.ProduceAsync(topic, new Message<string, string>()
            {
                Key = "key2",
                Value = jsonData2
            });

            var msg2 = consumer.Consume();

            Assert.Equal("key2", msg2.Message.Key);
            Assert.Equal("{\"firstName\":\"testFirst2\",\"lastName\":\"testLast2\"}", msg2.Message.Value);

            await producer.ProduceAsync(topic, new Message<string, string>()
            {
                Key = "key"
            });

            var msg3 = consumer.Consume();

            Assert.Equal("key", msg3.Message.Key);
            Assert.Null(msg3.Message.Value);
        }

        [Fact]
        public async Task TestWithExistingData()
        {
            var producer = new ProducerBuilder<string, string>(kafkaFixture.GetProducerConfig()).Build();

            var topic = "test-topic2";

            var jsonData = @"{""firstName"":""testFirst"",""lastName"":""testLast""}";

            await producer.ProduceAsync(topic, new Message<string, string>()
            {
                Key = "key",
                Value = jsonData
            });

            // Create the output topic
            await new AdminClientBuilder(new AdminClientConfig(kafkaFixture.GetConfig())).Build().CreateTopicsAsync(new List<TopicSpecification>() { new TopicSpecification() { Name = "output2", NumPartitions = 1, ReplicationFactor = 1 } });


            await producer.ProduceAsync("output2", new Message<string, string>()
            {
                Key = "key",
                Value = jsonData
            });

            await producer.ProduceAsync("output2", new Message<string, string>()
            {
                Key = "key2",
                Value = @"{""firstName"":""testSecond"",""lastName"":""testSecond""}"
            });


            var consumer = new ConsumerBuilder<string, string>(kafkaFixture.GetConsumerConfig("testwithexistingdata")).Build();

            KafkaTestStream kafkaTestStream = new KafkaTestStream(kafkaFixture, topic, "testwithexistingdata", true);

            await kafkaTestStream.StartStream(@"
                CREATE TABLE [test-topic2] (
                    _key,
                    firstName,
                    lastName
                );

                INSERT INTO output2
                SELECT 
                    _key,
                    firstName,
                    lastName
                FROM [test-topic2]
            ");


            consumer.Subscribe("output2");

            var msg1 = consumer.Consume();

            Assert.Equal("key", msg1.Message.Key);
            Assert.Equal("{\"firstName\":\"testFirst\",\"lastName\":\"testLast\"}", msg1.Message.Value);

            var msg2 = consumer.Consume();
            var msg3 = consumer.Consume();
            Assert.Equal("key2", msg3.Message.Key);
            Assert.Null(msg3.Message.Value);
        }

        [Fact]
        public async Task TestFetchExistingWithNoData()
        {
            var producer = new ProducerBuilder<string, string>(kafkaFixture.GetProducerConfig()).Build();

            var topic = "test-topic3";

            var jsonData = @"{""firstName"":""testFirst"",""lastName"":""testLast""}";

            await producer.ProduceAsync(topic, new Message<string, string>()
            {
                Key = "key",
                Value = jsonData
            });

            // Create the output topic
            await new AdminClientBuilder(new AdminClientConfig(kafkaFixture.GetConfig())).Build().CreateTopicsAsync(new List<TopicSpecification>() { new TopicSpecification() { Name = "output3", NumPartitions = 1, ReplicationFactor = 1 } });


            var consumer = new ConsumerBuilder<string, string>(kafkaFixture.GetConsumerConfig("Testfetchexistingwithnodata")).Build();

            KafkaTestStream kafkaTestStream = new KafkaTestStream(kafkaFixture, topic, "TestFetchExistingWithNoData", true);

            await kafkaTestStream.StartStream(@"
                CREATE TABLE [test-topic3] (
                    _key,
                    firstName,
                    lastName
                );

                INSERT INTO output3
                SELECT 
                    _key,
                    firstName,
                    lastName
                FROM [test-topic3]
            ");


            consumer.Subscribe("output3");

            var msg1 = consumer.Consume();

            Assert.Equal("key", msg1.Message.Key);
            Assert.Equal("{\"firstName\":\"testFirst\",\"lastName\":\"testLast\"}", msg1.Message.Value);
        }
    }
}