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
        public async Task TestIngestDataStartEmpty()
        {
            var producer = new ProducerBuilder<string, string>(kafkaFixture.GetProducerConfig()).Build();

            var topic = "test-topic";

            var jsonData = @"
                {
                    ""firstName"": ""testFirst"",
                    ""lastName"": ""testLast""  
                }
            ";

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
                Key = "key2"
            });

            var msg3 = consumer.Consume();

            Assert.Equal("key2", msg3.Message.Key);
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
    }
}