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
                    ""lastName"": ""testLast""  
                }
            ";

            await producer.ProduceAsync(topic, new Message<string, string>()
            {
                Key = "key",
                Value = jsonData
            });

            // Create the output topic
            await new AdminClientBuilder(new AdminClientConfig(kafkaFixture.GetConfig())).Build().CreateTopicsAsync(new List<TopicSpecification>() { new TopicSpecification() { Name = "output", NumPartitions = 1, ReplicationFactor = 1 } });

            var consumer = new ConsumerBuilder<string, string>(kafkaFixture.GetConsumerConfig()).Build();

            KafkaTestStream kafkaTestStream = new KafkaTestStream(kafkaFixture, topic, "testkafka");

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
    }
}