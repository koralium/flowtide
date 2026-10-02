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
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.TestFramework;
using System.Diagnostics;
using System.Text.Json;

namespace FlowtideDotNet.OpenLineage.Kafka.Tests
{
    public class User
    {
        public int UserKey { get; set; }

        public string? Name { get; set; }
    }

    public class OpenLineageKafkaTests : IClassFixture<KafkaFixture>
    {
        private readonly KafkaFixture _kafkaFixture;

        public OpenLineageKafkaTests(KafkaFixture kafkaFixture)
        {
            _kafkaFixture = kafkaFixture;
        }

        [Fact]
        public async Task RunEventsAreSentWithRunKey()
        {
            var runId = Guid.NewGuid();
            var messages = await RunStreamAndConsumeEvents("runkeystream", new OpenLineageKafkaOptions()
            {
                ProducerConfig = _kafkaFixture.GetProducerConfig(),
                TopicName = "lineage-runkey",
                RunId = runId,
                IncludeSchema = true
            });

            var events = messages.Select(x => JsonDocument.Parse(x.Value).RootElement).ToList();
            Assert.Equal(["START", "RUNNING", "COMPLETE"], events.Select(x => x.GetProperty("eventType").GetString()));
            Assert.All(messages, x => Assert.Equal("run:flowtide/runkeystream", x.Key));

            Assert.All(events, x =>
            {
                Assert.Equal("flowtide", x.GetProperty("job").GetProperty("namespace").GetString());
                Assert.Equal("runkeystream", x.GetProperty("job").GetProperty("name").GetString());
                Assert.Equal(runId, x.GetProperty("run").GetProperty("runId").GetGuid());
                Assert.Equal("users", x.GetProperty("inputs")[0].GetProperty("name").GetString());

                var output = x.GetProperty("outputs")[0];
                Assert.Equal("output", output.GetProperty("name").GetString());
                Assert.Equal(2, output.GetProperty("facets").GetProperty("schema").GetProperty("fields").GetArrayLength());
            });
        }

        [Fact]
        public async Task MessageKeyReplacesDefaultKey()
        {
            var messages = await RunStreamAndConsumeEvents("messagekeystream", new OpenLineageKafkaOptions()
            {
                ProducerConfig = _kafkaFixture.GetProducerConfig(),
                TopicName = "lineage-messagekey",
                MessageKey = "mykey"
            });

            Assert.All(messages, x => Assert.Equal("mykey", x.Key));
        }

        private async Task<List<Message<string, string>>> RunStreamAndConsumeEvents(string streamName, OpenLineageKafkaOptions options)
        {
            await _kafkaFixture.CreateTopic(options.TopicName!);
            using var consumer = new ConsumerBuilder<string, string>(_kafkaFixture.GetConsumerConfig(streamName)).Build();
            consumer.Subscribe(options.TopicName);

            var connectorManager = new ConnectorManager();
            connectorManager.AddTestDataTable("users", TestDataTable.Create(new User() { UserKey = 1, Name = "user1" }));
            connectorManager.AddTestDataSink("output", new TestDataSink());

            var sqlPlanBuilder = new SqlPlanBuilder();
            foreach (var tableProvider in connectorManager.GetTableProviders())
            {
                sqlPlanBuilder.AddTableProvider(tableProvider);
            }
            sqlPlanBuilder.Sql("INSERT INTO output SELECT UserKey, Name FROM users");

            using var persistentStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
            var stream = new FlowtideBuilder(streamName)
                .AddPlan(sqlPlanBuilder.GetPlan())
                .AddConnectorManager(connectorManager)
                .WithStateOptions(new StateManagerOptions() { PersistentStorage = persistentStorage })
                .WithOpenLineageKafka(options)
                .Build();

            await stream.StartAsync();
            var messages = Consume(consumer, 2);

            await stream.StopAsync();
            messages.AddRange(Consume(consumer, 1));

            await stream.DisposeAsync();
            return messages;
        }

        private static List<Message<string, string>> Consume(IConsumer<string, string> consumer, int count)
        {
            var messages = new List<Message<string, string>>();
            var stopwatch = Stopwatch.StartNew();
            while (messages.Count < count)
            {
                Assert.True(stopwatch.Elapsed < TimeSpan.FromMinutes(1), $"Received {messages.Count} of {count} lineage events");
                var result = consumer.Consume(TimeSpan.FromSeconds(1));
                if (result != null)
                {
                    messages.Add(result.Message);
                }
            }
            return messages;
        }
    }
}
