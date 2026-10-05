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

using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Analysis;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;
using Elastic.Transport;
using FlowtideDotNet.AcceptanceTests.Entities;
using FlowtideDotNet.Connector.CosmosDB.Tests;
using FlowtideDotNet.Connector.ElasticSearch.Exceptions;
using System.Text.Json;

namespace FlowtideDotNet.Connector.ElasticSearch.Tests
{
    public class SinkTests : IClassFixture<ElasticSearchFixture>
    {
        private const string InsertUsersSql = @"
            INSERT INTO testindex
            SELECT
                UserKey as _id,
                FirstName,
                LastName,
                UserKey as pk
            FROM users
            ";

        private readonly ElasticSearchFixture elasticSearchFixture;

        public SinkTests(ElasticSearchFixture elasticSearchFixture)
        {
            this.elasticSearchFixture = elasticSearchFixture;

            ElasticsearchClient elasticClient = new ElasticsearchClient(elasticSearchFixture.GetConnectionSettings());
            // Delete the concrete index first, it removes the testindex alias
            elasticClient.Indices.DeleteAsync("testindex_v1").Wait();
            elasticClient.Indices.DeleteAsync("testindex").Wait();
        }

        private static async Task WaitForDocument(ElasticsearchTestStream stream, ElasticsearchClient elasticClient, string id)
        {
            for (int i = 0; i < 1000; i++)
            {
                await stream.SchedulerTick();
                var resp = await elasticClient.Transport.GetAsync<StringResponse>("testindex/_doc/" + id);
                if (resp.ApiCallDetails.HttpStatusCode == 200)
                {
                    return;
                }
                await Task.Delay(10);
            }
            Assert.Fail($"Document '{id}' was not written to elasticsearch");
        }

        private static async Task<JsonElement> GetFlatSettings(ElasticsearchClient elasticClient)
        {
            var resp = await elasticClient.Transport.GetAsync<StringResponse>("testindex/_settings?flat_settings=true");
            Assert.Equal(200, resp.ApiCallDetails.HttpStatusCode);
            using var doc = JsonDocument.Parse(resp.Body);
            return doc.RootElement.GetProperty("testindex").GetProperty("settings").Clone();
        }

        private static async Task<Properties> GetProperties(ElasticsearchClient elasticClient)
        {
            var mappingInfo = await elasticClient.Indices.GetMappingAsync<User>(b => b.Indices("testindex"));
            return mappingInfo.Mappings["testindex"].Mappings.Properties!;
        }

        private static async Task<int?> GetIndexStatus(ElasticsearchClient elasticClient)
        {
            var resp = await elasticClient.Transport.GetAsync<StringResponse>("testindex");
            return resp.ApiCallDetails.HttpStatusCode;
        }

        [Fact]
        public async Task TestInsert()
        {
            await using ElasticsearchTestStream stream = new ElasticsearchTestStream(elasticSearchFixture, "TestInsert");
            stream.Generate();
            await stream.StartStream(@"
            INSERT INTO testindex
            SELECT
                UserKey as _id,
                FirstName as firstName,
                LastName as lastName,
                BirthDate as birthDate
            FROM users
            ");

            ElasticsearchClient elasticClient = new ElasticsearchClient(elasticSearchFixture.GetConnectionSettings());

            var lastUser = stream.Users.Last();
            bool success = false;
            StringResponse? stringResponse;
            do
            {
                await stream.SchedulerTick();
                stringResponse = await elasticClient.Transport.GetAsync<StringResponse>("testindex/_doc/" + lastUser.UserKey.ToString());
                success = stringResponse.ApiCallDetails.HttpStatusCode == 200;
                await Task.Delay(10);
            } while (!success);

            var resp = await elasticClient.GetSourceAsync<User>(lastUser.UserKey.ToString(), g => g.Index("testindex"));

            var mappingInfo = await elasticClient.Indices.GetMappingAsync<User>(b => b.Indices("testindex"));
            var birthDateField = mappingInfo.Mappings["testindex"].Mappings.Properties!["birthDate"];
            Assert.Equal("date", birthDateField.Type);
            Assert.Equal(lastUser.BirthDate!.Value, resp.Source.BirthDate!.Value.ToUniversalTime(), TimeSpan.FromMilliseconds(1));
        }

        [Fact]
        public async Task TestInsertWithUpdate()
        {
            await using ElasticsearchTestStream stream = new ElasticsearchTestStream(elasticSearchFixture, "TestInsertWithUpdate");
            stream.Generate();
            await stream.StartStream(@"
            INSERT INTO testindex
            SELECT
                UserKey as _id,
                FirstName,
                LastName,
                UserKey as pk
            FROM users
            ");

            ElasticsearchClient elasticClient = new ElasticsearchClient(elasticSearchFixture.GetConnectionSettings());

            var lastUser = stream.Users.Last();
            bool success = false;
            do
            {
                await stream.SchedulerTick();
                var resp = await elasticClient.Transport.GetAsync<StringResponse>("testindex/_doc/" + lastUser.UserKey.ToString());
                success = resp.ApiCallDetails.HttpStatusCode == 200;
                await Task.Delay(10);
            } while (!success);

            stream.Generate();

            lastUser = stream.Users.Last();

            success = false;
            do
            {
                await stream.SchedulerTick();
                var resp = await elasticClient.Transport.GetAsync<StringResponse>("testindex/_doc/" + lastUser.UserKey.ToString());
                success = resp.ApiCallDetails.HttpStatusCode == 200;
                await Task.Delay(10);
            } while (!success);

            stream.DeleteUser(lastUser);

            success = false;
            do
            {
                await stream.SchedulerTick();
                var resp = await elasticClient.Transport.GetAsync<StringResponse>("testindex/_doc/" + lastUser.UserKey.ToString());
                success = resp.ApiCallDetails.HttpStatusCode == 404;
                await Task.Delay(10);
            } while (!success);
        }

        [Fact]
        public async Task TestInitialDataSent()
        {
            bool calledOnInitialDataSent = false;
            await using ElasticsearchTestStream stream = new ElasticsearchTestStream(
                elasticSearchFixture,
                "TestInitialDataSent",
                onInitialDataSent: (client, writeRelation, indexName) =>
                {
                    calledOnInitialDataSent = true;
                    return Task.CompletedTask;
                });
            stream.Generate();
            await stream.StartStream(@"
            INSERT INTO testindex
            SELECT
                UserKey as _id,
                FirstName,
                LastName,
                UserKey as pk
            FROM users
            ");

            ElasticsearchClient elasticClient = new ElasticsearchClient(elasticSearchFixture.GetConnectionSettings());

            var lastUser = stream.Users.Last();
            bool success = false;
            do
            {
                await stream.SchedulerTick();
                var resp = await elasticClient.Transport.GetAsync<StringResponse>("testindex/_doc/" + lastUser.UserKey.ToString());
                success = resp.ApiCallDetails.HttpStatusCode == 200;
                await Task.Delay(10);
            } while (!success);

            int testCount = 0;
            while (calledOnInitialDataSent == false)
            {
                Assert.True(testCount < 100);
                testCount++;
                await stream.SchedulerTick();
                await Task.Delay(10);
            }
        }

        [Fact]
        public async Task TestOnDataSent()
        {
            bool calledOnDataSent = false;
            await using ElasticsearchTestStream stream = new ElasticsearchTestStream(
                elasticSearchFixture,
                "TestOnDataSent",
                onDataSent: (client, writeRelation, indexName, watermark) =>
                {
                    calledOnDataSent = true;
                    return Task.CompletedTask;
                });
            stream.Generate();
            await stream.StartStream(@"
            INSERT INTO testindex
            SELECT
                UserKey as _id,
                FirstName,
                LastName,
                UserKey as pk
            FROM users
            ");

            ElasticsearchClient elasticClient = new ElasticsearchClient(elasticSearchFixture.GetConnectionSettings());

            var lastUser = stream.Users.Last();
            bool success = false;
            do
            {
                await stream.SchedulerTick();
                var resp = await elasticClient.Transport.GetAsync<StringResponse>("testindex/_doc/" + lastUser.UserKey.ToString());
                success = resp.ApiCallDetails.HttpStatusCode == 200;
                await Task.Delay(10);
            } while (!success);

            int testCount = 0;
            while (calledOnDataSent == false)
            {
                Assert.True(testCount < 100);
                testCount++;
                await stream.SchedulerTick();
                await Task.Delay(10);
            }
        }

        [Fact]
        public async Task TestInsertWithCustomMappingIndexDoesNotExist()
        {
            await using ElasticsearchTestStream stream = new ElasticsearchTestStream(elasticSearchFixture, "TestInsertWithCustomMappingIndexDoesNotExist", (properties) =>
            {
                properties["FirstName"] = new KeywordProperty();
            });
            stream.Generate();
            await stream.StartStream(@"
            INSERT INTO testindex
            SELECT
                UserKey as _id,
                FirstName,
                LastName,
                UserKey as pk
            FROM users
            ");

            ElasticsearchClient elasticClient = new ElasticsearchClient(elasticSearchFixture.GetConnectionSettings());

            var lastUser = stream.Users.Last();
            bool success = false;
            do
            {
                var resp = await elasticClient.Transport.GetAsync<StringResponse>("testindex/_doc/" + lastUser.UserKey.ToString());
                success = resp.ApiCallDetails.HttpStatusCode == 200;
                await Task.Delay(10);
            } while (!success);

            var properties = await GetProperties(elasticClient);
            Assert.IsType<KeywordProperty>(properties["FirstName"]);
        }

        [Fact]
        public async Task TestInsertWithCustomMappingIndexExistsWithNoMappings()
        {
            ElasticsearchClient elasticClient = new ElasticsearchClient(elasticSearchFixture.GetConnectionSettings());
            await elasticClient.Indices.CreateAsync("testindex");
            await using ElasticsearchTestStream stream = new ElasticsearchTestStream(elasticSearchFixture, "TestInsertWithCustomMappingIndexExistsWithNoMappings", (properties) =>
            {
                properties["FirstName"] = new KeywordProperty();
            });
            stream.Generate();
            await stream.StartStream(@"
            INSERT INTO testindex
            SELECT
                UserKey as _id,
                FirstName,
                LastName,
                UserKey as pk
            FROM users
            ");

            var lastUser = stream.Users.Last();
            bool success = false;
            do
            {
                var resp = await elasticClient.Transport.GetAsync<StringResponse>("testindex/_doc/" + lastUser.UserKey.ToString());
                success = resp.ApiCallDetails.HttpStatusCode == 200;
                await Task.Delay(10);
            } while (!success);
        }

        [Fact]
        public async Task TestInsertWithCustomMappingIndexExistsWithMappings()
        {
            Properties props = new Properties
            {
                { "FirstName", new KeywordProperty() }
            };
            ElasticsearchClient elasticClient = new ElasticsearchClient(elasticSearchFixture.GetConnectionSettings());
            await elasticClient.Indices.CreateAsync("testindex", c => c.Mappings(m => m.Properties(props)));
            await using ElasticsearchTestStream stream = new ElasticsearchTestStream(elasticSearchFixture, "TestInsertWithCustomMappingIndexExistsWithMappings", (properties) =>
            {
                properties["FirstName"] = new KeywordProperty();
            });
            stream.Generate();
            await stream.StartStream(@"
            INSERT INTO testindex
            SELECT
                UserKey as _id,
                FirstName,
                LastName,
                UserKey as pk
            FROM users
            ");

            var lastUser = stream.Users.Last();
            bool success = false;
            do
            {
                var resp = await elasticClient.Transport.GetAsync<StringResponse>("testindex/_doc/" + lastUser.UserKey.ToString());
                success = resp.ApiCallDetails.HttpStatusCode == 200;
                await Task.Delay(10);
            } while (!success);

        }

        [Fact]
        public async Task TestInsertWithCustomMappingIndexExistsWithMappingsCollision()
        {
            ElasticsearchClient elasticClient = new ElasticsearchClient(elasticSearchFixture.GetConnectionSettings());
            await elasticClient.Indices.DeleteAsync("testindex");
            Properties props = new Properties
            {
                { "FirstName", new TextProperty() }
            };
            await elasticClient.Indices.CreateAsync("testindex", c => c.Mappings(m => m.Properties(props)));
            await using ElasticsearchTestStream stream = new ElasticsearchTestStream(elasticSearchFixture, "TestInsertWithCustomMappingIndexExistsWithMappingsCollision", (properties) =>
            {
                properties["FirstName"] = new KeywordProperty();
            });
            stream.Generate();

            var ex = await Assert.ThrowsAsync<FlowtideElasticsearchResponseException>(async () =>
            {
                await stream.StartStream(@"
                    INSERT INTO testindex
                    SELECT
                        UserKey as _id,
                        FirstName,
                        LastName,
                        UserKey as pk
                    FROM users
                    ");
            });
            Assert.Contains("put mapping", ex.Message);
        }

        [Fact]
        public async Task TestOnIndexCreationAppliesSettingsAnalyzerAndAlias()
        {
            Properties? customProperties = null;
            int hookCalls = 0;
            await using ElasticsearchTestStream stream = new ElasticsearchTestStream(
                elasticSearchFixture,
                "TestOnIndexCreationAppliesSettingsAnalyzerAndAlias",
                customMapping: (properties) =>
                {
                    customProperties = properties;
                    properties["FirstName"] = new TextProperty { Analyzer = "folding" };
                    properties["LastName"] = new KeywordProperty { Normalizer = "lc" };
                },
                configureOptions: (options) =>
                {
                    options.OnIndexCreation = (ctx) =>
                    {
                        hookCalls++;
                        Assert.Equal("testindex", ctx.IndexName);
                        Assert.Equal("testindex", ctx.WriteRelation.NamedObject.DotSeperated);
                        Assert.Same(customProperties, ctx.Request.Mappings!.Properties);

                        ctx.Request.Settings = new IndexSettings
                        {
                            NumberOfShards = 2,
                            NumberOfReplicas = 0,
                            RefreshInterval = "5s",
                            Analysis = new IndexSettingsAnalysis
                            {
                                Analyzers = new Analyzers
                                {
                                    { "folding", new CustomAnalyzer("standard") { Filter = new List<string> { "lowercase", "asciifolding" } } }
                                },
                                Normalizers = new Normalizers
                                {
                                    { "lc", new CustomNormalizer { Filter = new List<string> { "lowercase" } } }
                                }
                            }
                        };
                        ctx.Request.Aliases = new Dictionary<Name, Alias>
                        {
                            ["testindex_alias"] = new Alias()
                        };
                        return Task.CompletedTask;
                    };
                });
            stream.Generate();
            await stream.StartStream(InsertUsersSql);

            ElasticsearchClient elasticClient = new ElasticsearchClient(elasticSearchFixture.GetConnectionSettings());

            Assert.Equal(1, hookCalls);

            var settings = await GetFlatSettings(elasticClient);
            Assert.Equal("2", settings.GetProperty("index.number_of_shards").GetString());
            Assert.Equal("0", settings.GetProperty("index.number_of_replicas").GetString());
            Assert.Equal("5s", settings.GetProperty("index.refresh_interval").GetString());
            Assert.Equal("standard", settings.GetProperty("index.analysis.analyzer.folding.tokenizer").GetString());

            var aliasResponse = await elasticClient.Transport.GetAsync<StringResponse>("testindex/_alias/testindex_alias");
            Assert.Equal(200, aliasResponse.ApiCallDetails.HttpStatusCode);

            var properties = await GetProperties(elasticClient);
            Assert.Equal("folding", Assert.IsType<TextProperty>(properties["FirstName"]).Analyzer);
            Assert.Equal("lc", Assert.IsType<KeywordProperty>(properties["LastName"]).Normalizer);

            await WaitForDocument(stream, elasticClient, stream.Users.Last().UserKey.ToString());
        }

        [Fact]
        public async Task TestOnIndexCreationNotCalledWhenIndexExists()
        {
            ElasticsearchClient elasticClient = new ElasticsearchClient(elasticSearchFixture.GetConnectionSettings());
            await elasticClient.Indices.CreateAsync("testindex");

            bool hookCalled = false;
            await using ElasticsearchTestStream stream = new ElasticsearchTestStream(
                elasticSearchFixture,
                "TestOnIndexCreationNotCalledWhenIndexExists",
                configureOptions: (options) =>
                {
                    options.OnIndexCreation = (ctx) =>
                    {
                        hookCalled = true;
                        ctx.Request.Settings = new IndexSettings { NumberOfShards = 3 };
                        return Task.CompletedTask;
                    };
                });
            stream.Generate();
            await stream.StartStream(InsertUsersSql);

            Assert.False(hookCalled);
            var settings = await GetFlatSettings(elasticClient);
            Assert.Equal("1", settings.GetProperty("index.number_of_shards").GetString());

            await WaitForDocument(stream, elasticClient, stream.Users.Last().UserKey.ToString());
        }

        [Fact]
        public async Task TestOnIndexCreationInvalidSettingsLeavesNoIndex()
        {
            await using ElasticsearchTestStream stream = new ElasticsearchTestStream(
                elasticSearchFixture,
                "TestOnIndexCreationInvalidSettingsLeavesNoIndex",
                customMapping: (properties) =>
                {
                    properties["FirstName"] = new KeywordProperty();
                },
                configureOptions: (options) =>
                {
                    options.OnIndexCreation = (ctx) =>
                    {
                        ctx.Request.Settings = new IndexSettings { NumberOfShards = 0 };
                        return Task.CompletedTask;
                    };
                });
            stream.Generate();

            var ex = await Assert.ThrowsAsync<FlowtideElasticsearchResponseException>(() => stream.StartStream(InsertUsersSql));
            Assert.Contains("create index", ex.Message);
            Assert.Contains("testindex", ex.Message);
            Assert.NotNull(ex.Response.ElasticsearchServerError);

            ElasticsearchClient elasticClient = new ElasticsearchClient(elasticSearchFixture.GetConnectionSettings());
            Assert.Equal(404, await GetIndexStatus(elasticClient));
        }

        [Fact]
        public async Task TestOnIndexCreationHookExceptionPropagates()
        {
            await using ElasticsearchTestStream stream = new ElasticsearchTestStream(
                elasticSearchFixture,
                "TestOnIndexCreationHookExceptionPropagates",
                configureOptions: (options) =>
                {
                    options.OnIndexCreation = (ctx) =>
                    {
                        throw new InvalidOperationException("boom");
                    };
                });
            stream.Generate();

            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => stream.StartStream(InsertUsersSql));
            Assert.Equal("boom", ex.Message);

            ElasticsearchClient elasticClient = new ElasticsearchClient(elasticSearchFixture.GetConnectionSettings());
            Assert.Equal(404, await GetIndexStatus(elasticClient));
        }

        [Fact]
        public async Task TestOnIndexCreationIndexCreatedConcurrently()
        {
            await using ElasticsearchTestStream stream = new ElasticsearchTestStream(
                elasticSearchFixture,
                "TestOnIndexCreationIndexCreatedConcurrently",
                customMapping: (properties) =>
                {
                    properties["FirstName"] = new KeywordProperty();
                },
                configureOptions: (options) =>
                {
                    options.OnIndexCreation = async (ctx) =>
                    {
                        // Simulate another process creating the index first
                        var createResponse = await ctx.Client.Indices.CreateAsync(ctx.IndexName);
                        Assert.True(createResponse.IsValidResponse);
                        ctx.Request.Settings = new IndexSettings { NumberOfShards = 2 };
                    };
                });
            stream.Generate();
            await stream.StartStream(InsertUsersSql);

            ElasticsearchClient elasticClient = new ElasticsearchClient(elasticSearchFixture.GetConnectionSettings());

            var settings = await GetFlatSettings(elasticClient);
            Assert.Equal("1", settings.GetProperty("index.number_of_shards").GetString());

            var properties = await GetProperties(elasticClient);
            Assert.IsType<KeywordProperty>(properties["FirstName"]);

            await WaitForDocument(stream, elasticClient, stream.Users.Last().UserKey.ToString());
        }

        [Fact]
        public async Task TestOnIndexCreationReplacedTypeMappingKeepsCustomMappings()
        {
            await using ElasticsearchTestStream stream = new ElasticsearchTestStream(
                elasticSearchFixture,
                "TestOnIndexCreationReplacedTypeMappingKeepsCustomMappings",
                customMapping: (properties) =>
                {
                    properties["FirstName"] = new KeywordProperty();
                },
                configureOptions: (options) =>
                {
                    options.OnIndexCreation = (ctx) =>
                    {
                        ctx.Request.Mappings = new TypeMapping { Dynamic = DynamicMapping.False };
                        return Task.CompletedTask;
                    };
                });
            stream.Generate();
            await stream.StartStream(InsertUsersSql);

            ElasticsearchClient elasticClient = new ElasticsearchClient(elasticSearchFixture.GetConnectionSettings());

            var mappingInfo = await elasticClient.Indices.GetMappingAsync<User>(b => b.Indices("testindex"));
            var typeMapping = mappingInfo.Mappings["testindex"].Mappings;
            Assert.Equal(DynamicMapping.False, typeMapping.Dynamic);
            Assert.IsType<KeywordProperty>(typeMapping.Properties!["FirstName"]);

            await WaitForDocument(stream, elasticClient, stream.Users.Last().UserKey.ToString());
        }

        [Fact]
        public async Task TestAliasAsIndexNameFailsWithoutCallingHook()
        {
            ElasticsearchClient elasticClient = new ElasticsearchClient(elasticSearchFixture.GetConnectionSettings());
            var createResponse = await elasticClient.Indices.CreateAsync(new CreateIndexRequest("testindex_v1")
            {
                Aliases = new Dictionary<Name, Alias>
                {
                    ["testindex"] = new Alias()
                }
            });
            Assert.True(createResponse.IsValidResponse);

            try
            {
                bool hookCalled = false;
                await using ElasticsearchTestStream stream = new ElasticsearchTestStream(
                    elasticSearchFixture,
                    "TestAliasAsIndexNameFailsWithoutCallingHook",
                    configureOptions: (options) =>
                    {
                        options.OnIndexCreation = (ctx) =>
                        {
                            hookCalled = true;
                            return Task.CompletedTask;
                        };
                    });
                stream.Generate();

                var ex = await Assert.ThrowsAsync<NotSupportedException>(() => stream.StartStream(InsertUsersSql));
                Assert.Contains("testindex", ex.Message);
                Assert.False(hookCalled);
            }
            finally
            {
                await elasticClient.Indices.DeleteAsync("testindex_v1");
            }
        }

        [Fact]
        public async Task TestGetIndexErrorDoesNotCallHook()
        {
            bool hookCalled = false;
            await using ElasticsearchTestStream stream = new ElasticsearchTestStream(
                elasticSearchFixture,
                "TestGetIndexErrorDoesNotCallHook",
                configureOptions: (options) =>
                {
                    options.ConnectionSettings = () => new ElasticsearchClientSettings(new Uri("http://localhost:1"));
                    options.OnIndexCreation = (ctx) =>
                    {
                        hookCalled = true;
                        return Task.CompletedTask;
                    };
                });
            stream.Generate();

            var ex = await Assert.ThrowsAsync<FlowtideElasticsearchResponseException>(() => stream.StartStream(InsertUsersSql));
            Assert.Contains("get index", ex.Message);
            Assert.NotNull(ex.InnerException);
            Assert.False(hookCalled);
        }
    }
}
