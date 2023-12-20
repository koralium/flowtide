using Confluent.Kafka;
using FlowtideDotNet.Connector.Kafka.Extensions;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Substrait.Sql;
using Xunit;

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

            var status = await producer.ProduceAsync(topic, new Message<string, string>()
            {
                Key = "key",
                Value = jsonData
            });

            KafkaTestStream kafkaTestStream = new KafkaTestStream(kafkaFixture, topic, "testkafka");

            await kafkaTestStream.StartStream(@"
                CREATE TABLE kafkasource (
                    _key,
                    firstName,
                    lastName
                );

                INSERT INTO output
                SELECT 
                    _key,
                    firstName,
                    lastName
                FROM kafkasource
            ");

            await kafkaTestStream.WaitForUpdate();

            kafkaTestStream.AssertCurrentDataEqual(new[] { new { key = "key", firstName = "testFirst", lastName = "testLast" } });

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

            await kafkaTestStream.WaitForUpdate();

            kafkaTestStream.AssertCurrentDataEqual(new[] { 
                new { key = "key", firstName = "testFirst", lastName = "testLast" },
                new { key = "key2", firstName = "testFirst2", lastName = "testLast2" },
            });

            await producer.ProduceAsync(topic, new Message<string, string>()
            {
                Key = "key"
            });

            await kafkaTestStream.WaitForUpdate();

            kafkaTestStream.AssertCurrentDataEqual(new[] {
                new { key = "key2", firstName = "testFirst2", lastName = "testLast2" },
            });
        }
    }
}