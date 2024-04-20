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
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.Kafka.Tests.Debezium.Postgres
{
    public class DebeziumPostgresFixture : IAsyncLifetime
    {
        private INetwork _network;
        private IContainer _zookeeperContainer;
        private IContainer _kafkaContainer;
        private IContainer _postgresContainer;
        private IContainer _connectContainer;

        public DebeziumPostgresFixture()
        {
            _network = new NetworkBuilder()
                //.WithName("debezium-network")
                .Build();
            _zookeeperContainer = new DotNet.Testcontainers.Builders.ContainerBuilder()
                .WithImage("quay.io/debezium/zookeeper:2.1")
                //.WithName("zookeeper")
                .WithPortBinding(2181, 2181)
                .WithNetwork(_network)
                .WithNetworkAliases("zookeeper")
                .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Started AdminServer"))
                .Build();

            _kafkaContainer = new DotNet.Testcontainers.Builders.ContainerBuilder()
                .WithImage("docker.io/bitnami/kafka:3.3")
                //s.WithName("kafka")
                .WithPortBinding(9093, 9093)
                .WithEnvironment("KAFKA_BROKER_ID", "1")
                .WithEnvironment("KAFKA_CFG_ZOOKEEPER_CONNECT", "zookeeper:2181")
                .WithEnvironment("ALLOW_PLAINTEXT_LISTENER", "yes")
                .WithEnvironment("KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP", "CLIENT:PLAINTEXT,EXTERNAL:PLAINTEXT")
                .WithEnvironment("KAFKA_CFG_LISTENERS", "CLIENT://:9092,EXTERNAL://:9093")
                .WithEnvironment("KAFKA_CFG_ADVERTISED_LISTENERS", "CLIENT://kafka:9092,EXTERNAL://localhost:9093")
                .WithEnvironment("KAFKA_CFG_INTER_BROKER_LISTENER_NAME", "CLIENT")
                .WithNetwork(_network)
                .WithNetworkAliases("kafka")
                .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("\\[KafkaServer id=\\d+\\] started"))
                .Build();

            _postgresContainer = new DotNet.Testcontainers.Builders.ContainerBuilder()
                .WithImage("quay.io/debezium/example-postgres:2.1")
                .WithName("postgres")
                .WithPortBinding(5432, 5432)
                .WithEnvironment("POSTGRES_USER", "postgres")
                .WithEnvironment("POSTGRES_PASSWORD", "postgres")
                .WithEnvironment("POSTGRES_DB", "postgres")
                .WithNetwork(_network)
                .WithNetworkAliases("postgres")
                .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("listening on IPv4 address \"0.0.0.0\", port 5432"))
                .Build();

            _connectContainer = new DotNet.Testcontainers.Builders.ContainerBuilder()
                .WithImage("quay.io/debezium/connect:2.1")
                .WithPortBinding(8083, true)
                .WithEnvironment("BOOTSTRAP_SERVERS", "kafka:9092")
                .WithEnvironment("GROUP_ID", "1")
                .WithEnvironment("CONFIG_STORAGE_TOPIC", "my_connect_configs")
                .WithEnvironment("OFFSET_STORAGE_TOPIC", "my_connect_offsets")
                .WithEnvironment("STATUS_STORAGE_TOPIC", "my_connect_statuses")
                .WithNetwork(_network)
                .WithNetworkAliases("connect")
                .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Kafka Connect started"))
                .Build();
        }

        public async Task DisposeAsync()
        {
            await _connectContainer.DisposeAsync();
            await _postgresContainer.DisposeAsync();
            await _kafkaContainer.DisposeAsync();
            await _zookeeperContainer.DisposeAsync();
            await _network.DisposeAsync();
        }

        public async Task InitializeAsync()
        {
            await _zookeeperContainer.StartAsync();
            await _kafkaContainer.StartAsync();
            await _postgresContainer.StartAsync();
            await _connectContainer.StartAsync();

            var logs = await _connectContainer.GetLogsAsync();
            HttpClient httpClient = new HttpClient();
        }

        public async Task ListenOnTables(string schemaIncludeList, string topicPrefix)
        {
            HttpClient httpClient = new HttpClient();
            var content = new StringContent(@"
            {
                ""name"": """ + schemaIncludeList + @"-connector"",
                ""config"": {
                    ""connector.class"": ""io.debezium.connector.postgresql.PostgresConnector"",
                    ""tasks.max"": ""1"",
                    ""database.hostname"": ""postgres"",
                    ""database.port"": ""5432"",
                    ""database.user"": ""postgres"",
                    ""database.password"": ""postgres"",
                    ""database.dbname"": ""postgres"",
                    ""topic.prefix"": """ + topicPrefix + @""",
                    ""schema.include.list"": """ + schemaIncludeList + @"""
                }
            }
            ", Encoding.UTF8, "application/json");

            var port = _connectContainer.GetMappedPublicPort(8083);
            var resp = await httpClient.PostAsync($"http://localhost:{port}/connectors", content);
            var resultContent = await resp.Content.ReadAsStringAsync();
            Assert.Equal(HttpStatusCode.Created, resp.StatusCode);

            string statusResult = string.Empty;
            do
            {
                var statusResponse = await httpClient.GetAsync($"http://localhost:{port}/connectors/{schemaIncludeList}-connector/status");
                statusResult = await statusResponse.Content.ReadAsStringAsync();
                Assert.Equal(HttpStatusCode.OK, statusResponse.StatusCode);
                await Task.Delay(10);
            } while (!statusResult.Contains("\"state\":\"RUNNING\""));
        }

        public ConsumerConfig GetConsumerConfig(string groupId)
        {
            return new ConsumerConfig()
            {
                BootstrapServers = $"localhost:{_kafkaContainer.GetMappedPublicPort(9093)}",
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };
        }
    }
}
