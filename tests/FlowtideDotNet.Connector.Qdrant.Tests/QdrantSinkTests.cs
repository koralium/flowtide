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

using FlowtideDotNet.AcceptanceTests.Entities;
using Qdrant.Client.Grpc;
using System.Diagnostics;

namespace FlowtideDotNet.Connector.Qdrant.Tests
{
    public class QdrantSinkTests : IClassFixture<QdrantFixture>
    {
        private readonly QdrantFixture _qdrantFixture;
        private readonly FakeEmbeddingsGenerator _generator;

        public QdrantSinkTests(QdrantFixture qdrantFixture)
        {
            _qdrantFixture = qdrantFixture;
            _generator = new FakeEmbeddingsGenerator();
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task TestInsert(bool wait)
        {
            var name = nameof(TestInsert) + wait;
            var options = new QdrantSinkOptions
            {
                QdrantChannelFunc = _qdrantFixture.ChannelFunc,
                CollectionName = name,
                Wait = wait,
            };

            await CreateCollection(name);

            var stream = new QdrantSinkStream(name, options, _generator, null);

            var numberOfProjects = 4;

            stream.Generate(numberOfProjects);

            await stream.StartStream(@"
                INSERT INTO output
                SELECT 
                    projectKey AS id,
                    name as vector_string
                FROM projects
            ");

            var scroll = await WaitForScrollWithPoints(name, numberOfProjects);

            Assert.NotNull(scroll);
            Assert.NotEmpty(scroll.Result);
            Assert.Equal(numberOfProjects, scroll.Result.Count);
        }

        [Theory]
        [InlineData(100)]
        [InlineData(500)]
        [InlineData(2000)]
        [InlineData(5000)]
        public async Task TestBigInsert(int count)
        {
            var name = nameof(TestInsert) + count;
            var options = new QdrantSinkOptions
            {
                QdrantChannelFunc = _qdrantFixture.ChannelFunc,
                CollectionName = name,
            };

            await CreateCollection(name);

            var stream = new QdrantSinkStream(name, options, _generator, null);

            var numberOfProjects = count;

            stream.Generate(numberOfProjects);

            await stream.StartStream(@"
                INSERT INTO output
                SELECT 
                    projectKey AS id,
                    name as vector_string
                FROM projects
            ");

            var scroll = await WaitForScrollWithPoints(name, numberOfProjects, timeoutSeconds: 30, onDelay: stream.SchedulerTick, withPayload: false);

            Assert.NotNull(scroll);
            Assert.NotEmpty(scroll.Result);
            Assert.Equal(numberOfProjects, scroll.Result.Count);
        }

        [Theory]
        [InlineData(100)]
        [InlineData(500)]
        [InlineData(2000)]
        [InlineData(5000)]
        public async Task TestBigInsertAndUpdate(int count)
        {
            var name = nameof(TestBigInsertAndUpdate) + count;
            var options = new QdrantSinkOptions
            {
                QdrantChannelFunc = _qdrantFixture.ChannelFunc,
                CollectionName = name,
            };

            await CreateCollection(name);

            var stream = new QdrantSinkStream(name, options, _generator, null);

            var numberOfProjects = count;

            stream.Generate(numberOfProjects);

            await stream.StartStream(@"
                INSERT INTO output
                SELECT 
                    projectKey AS id,
                    name as vector_string,
                    projectNumber as project_number
                FROM projects
            ");

            await WaitForScrollWithPoints(
                name,
                numberOfProjects,
                timeoutSeconds: 30,
                onDelay:
                stream.SchedulerTick,
                withPayload: false);

            var targetFilter = $"__flowtide_id:{stream.Projects[stream.Projects.Count - 1].ProjectKey}";

            foreach (var project in stream.Projects.ToList())
            {
                project.ProjectNumber = Guid.NewGuid().ToString();
                stream.AddOrUpdateProject(project);
            }

            var scrollFilter = new Filter
            {
                Must =
                {
                    new Condition
                    {
                        Field = new FieldCondition
                        {
                            Key = "flowtide",
                            Match = new Match
                            {
                                Keyword = targetFilter
                            }
                        },
                    }
                }
            };

            var scroll = await WaitForScrollWithPoints(
                name,
                timeoutSeconds: 30,
                filter: scrollFilter,
                waitUntilPayloadCondition: HasProjectNumber,
                onDelay: stream.SchedulerTick,
                withPayload: true);

            Assert.NotNull(scroll);
            Assert.NotEmpty(scroll.Result);

            bool HasProjectNumber(KeyValuePair<string, Value> payload)
            {
                return payload.Key == options.QdrantPayloadDataPropertyName
                    && payload.Value.StructValue != null
                    && payload.Value.StructValue.Fields.Any(s =>
                        s.Key == "project_number" && Guid.TryParse(s.Value.StringValue, out _)
                    );
            }
        }

        [Theory]
        [InlineData(QdrantPayloadUpdateMode.SetPayload)]
        [InlineData(QdrantPayloadUpdateMode.OverwritePayload)]
        public async Task TestUpdateWithChangedRow(QdrantPayloadUpdateMode qdrantPayloadUpdateMode)
        {
            var name = nameof(TestUpdateWithChangedRow) + qdrantPayloadUpdateMode;
            var options = new QdrantSinkOptions
            {
                QdrantChannelFunc = _qdrantFixture.ChannelFunc,
                CollectionName = name,
                QdrantPayloadUpdateMode = qdrantPayloadUpdateMode
            };

            await CreateCollection(name);

            var stream = new QdrantSinkStream(name, options, _generator, null);

            var numberOfProjects = 1;

            stream.Generate(numberOfProjects);

            await stream.StartStream(@"
                INSERT INTO output
                SELECT 
                    projectKey AS id,
                    'abc' as vector_string,
                    name as project_name,
                    projectNumber as project_number
                FROM projects
            ");

            await WaitForScrollWithPoints(name, numberOfProjects);

            var project = new Project
            {
                CompanyId = "1",
                Name = "Project 1",
                ProjectNumber = "abc-1",
                ProjectKey = 999
            };

            stream.AddOrUpdateProject(project);

            numberOfProjects++;
            await WaitForScrollWithPoints(name, numberOfProjects, onDelay: stream.SchedulerTick);

            project.ProjectNumber = "abc-2";
            stream.AddOrUpdateProject(project);

            var scroll = await WaitForScrollWithPoints(
                name,
                waitUntilPayloadCondition: HasProjectNumber,
                onDelay: stream.SchedulerTick);

            Assert.NotNull(scroll);
            Assert.NotEmpty(scroll.Result);
            Assert.Equal(numberOfProjects, scroll.Result.Count);

            bool HasProjectNumber(KeyValuePair<string, Value> payload)
            {
                return payload.Key == options.QdrantPayloadDataPropertyName
                    && payload.Value.StructValue != null
                    && payload.Value.StructValue.Fields.Any(s =>
                        s.Key == "project_number"
                        && s.Value.StringValue == project.ProjectNumber
                    );
            }
        }

        [Theory]
        [InlineData(QdrantPayloadUpdateMode.SetPayload)]
        [InlineData(QdrantPayloadUpdateMode.OverwritePayload)]
        public async Task TestUpdateWithMultipleChangedRows(QdrantPayloadUpdateMode qdrantPayloadUpdateMode)
        {
            var name = nameof(TestUpdateWithMultipleChangedRows) + qdrantPayloadUpdateMode;
            var options = new QdrantSinkOptions
            {
                QdrantChannelFunc = _qdrantFixture.ChannelFunc,
                CollectionName = name,
                QdrantPayloadUpdateMode = qdrantPayloadUpdateMode
            };

            await CreateCollection(name);

            var stream = new QdrantSinkStream(name, options, _generator, null);

            var projectA = new Project
            {
                CompanyId = "1",
                Name = "ProjectA",
                ProjectNumber = "a",
                ProjectKey = 1
            };

            var projectB = new Project
            {
                CompanyId = "1",
                Name = "ProjectB",
                ProjectNumber = "b",
                ProjectKey = 2
            };

            stream.AddOrUpdateProject(projectA);
            stream.AddOrUpdateProject(projectB);

            await stream.StartStream(@"
                INSERT INTO output
                SELECT 
                    projectKey AS id,
                    'abc' as vector_string,
                    name as project_name,
                    projectNumber as project_number
                FROM projects
            ");

            await WaitForScrollWithPoints(name, 2, onDelay: stream.SchedulerTick);

            projectA.ProjectNumber = "a1";
            projectB.ProjectNumber = "b1";

            stream.AddOrUpdateProject(projectA);
            stream.AddOrUpdateProject(projectB);

            var scroll = await WaitForScrollWithPoints(name,
                waitUntilPayloadCondition: HasProjectNumber,
                onDelay: stream.SchedulerTick);

            Assert.NotNull(scroll);
            Assert.NotEmpty(scroll.Result);
            Assert.Equal(2, scroll.Result.Count);

            bool HasProjectNumber(KeyValuePair<string, Value> payload)
            {
                return payload.Key == options.QdrantPayloadDataPropertyName
                    && payload.Value.StructValue != null
                    && payload.Value.StructValue.Fields.Any(s =>
                        s.Key == "project_number"
                        && (s.Value.StringValue == projectA.ProjectNumber || s.Value.StringValue == projectB.ProjectNumber)
                    );
            }
        }

        [Fact]
        public async Task TestSetPayloadKeepsCustomProperties()
        {
            var name = nameof(TestSetPayloadKeepsCustomProperties);
            var options = new QdrantSinkOptions
            {
                QdrantChannelFunc = _qdrantFixture.ChannelFunc,
                CollectionName = name,
                QdrantPayloadUpdateMode = QdrantPayloadUpdateMode.SetPayload
            };

            await CreateCollection(name);

            var stream = new QdrantSinkStream(name, options, _generator, null);

            var project = new Project
            {
                CompanyId = "1",
                Name = "ProjectA",
                ProjectNumber = "a",
                ProjectKey = 1
            };

            stream.AddOrUpdateProject(project);

            await stream.StartStream(@"
                INSERT INTO output
                SELECT 
                    projectKey AS id,
                    'abc' as vector_string,
                    name as project_name
                FROM projects
            ");

            var scroll = await WaitForScrollWithPoints(name);
            var point = scroll.Result[0];

            var client = _qdrantFixture.GetClient();
            var result = await client.SetPayloadAsync(name,
                new Dictionary<string, Value>
                {
                    { "my_extra_field", "my_value" }
                },
                ids: [Guid.Parse(point.Id.Uuid)]);


            Assert.Equal(UpdateStatus.Completed, result.Status);

            project.Name = "updated_name";
            stream.AddOrUpdateProject(project);
            scroll = await WaitForScrollWithPoints(
                name,
                waitUntilPayloadCondition: HasProjectName,
                onDelay: stream.SchedulerTick);

            point = scroll.Result[0];

            Assert.NotNull(scroll);
            Assert.NotEmpty(scroll.Result);
            Assert.Equal("my_value", point.Payload.First(s => s.Key == "my_extra_field").Value.StringValue);

            bool HasProjectName(KeyValuePair<string, Value> payload)
            {
                return payload.Key == options.QdrantPayloadDataPropertyName
                    && payload.Value.StructValue != null
                    && payload.Value.StructValue.Fields.Any(s =>
                        s.Key == "project_name"
                        && (s.Value.StringValue == project.Name)
                    );
            }
        }

        [Fact]
        public async Task OverwritePayloadRemovesCustomProperties()
        {
            var name = nameof(OverwritePayloadRemovesCustomProperties);
            var options = new QdrantSinkOptions
            {
                QdrantChannelFunc = _qdrantFixture.ChannelFunc,
                CollectionName = name,
                QdrantPayloadUpdateMode = QdrantPayloadUpdateMode.OverwritePayload
            };

            await CreateCollection(name);

            var stream = new QdrantSinkStream(name, options, _generator, null);

            var project = new Project
            {
                CompanyId = "1",
                Name = "ProjectA",
                ProjectNumber = "a",
                ProjectKey = 1
            };

            stream.AddOrUpdateProject(project);

            await stream.StartStream(@"
                INSERT INTO output
                SELECT 
                    projectKey AS id,
                    'abc' as vector_string,
                    name as project_name
                FROM projects
            ");

            var scroll = await WaitForScrollWithPoints(name);
            var point = scroll.Result[0];

            var client = _qdrantFixture.GetClient();
            var result = await client.SetPayloadAsync(name,
                new Dictionary<string, Value>
                {
                    { "my_extra_field", "my_value" }
                },
                ids: [Guid.Parse(point.Id.Uuid)]);


            Assert.Equal(UpdateStatus.Completed, result.Status);

            project.Name = "updated_name";
            stream.AddOrUpdateProject(project);

            scroll = await WaitForScrollWithPoints(
                name,
                waitUntilPayloadCondition: HasProjectName,
                onDelay: stream.SchedulerTick);

            point = scroll.Result[0];

            Assert.NotNull(scroll);
            Assert.NotEmpty(scroll.Result);
            Assert.DoesNotContain(point.Payload, s => s.Key == "my_extra_field");

            bool HasProjectName(KeyValuePair<string, Value> payload)
            {
                return payload.Key == options.QdrantPayloadDataPropertyName
                    && payload.Value.StructValue != null
                    && payload.Value.StructValue.Fields.Any(s =>
                        s.Key == "project_name"
                        && (s.Value.StringValue == project.Name)
                    );
            }
        }

        [Fact]
        public async Task TestUpdateVector()
        {
            var name = nameof(TestUpdateVector);
            var options = new QdrantSinkOptions
            {
                QdrantChannelFunc = _qdrantFixture.ChannelFunc,
                CollectionName = name,
            };

            await CreateCollection(name);

            var stream = new QdrantSinkStream(name, options, _generator, null);

            var project = new Project
            {
                CompanyId = "1",
                Name = "Project 1",
                ProjectNumber = "abc-1",
                ProjectKey = 999
            };

            stream.AddOrUpdateProject(project);

            await stream.StartStream(@"
                INSERT INTO output
                SELECT 
                    projectKey AS id,
                    name as vector_string
                FROM projects
            ");

            var initialScroll = await WaitForScrollWithPoints(name, withVector: true);
            var initialPoint = initialScroll.Result[0];

            project.Name = "updated_project";
            stream.AddOrUpdateProject(project);

            var updatedScroll = await WaitForScrollWithPoints(
                name,
                withVector: true,
                waitUntilPayloadCondition: HasVectorText,
                onDelay: stream.SchedulerTick);
            var updatedPoint = updatedScroll.Result[0];

            Assert.NotNull(updatedScroll);
            Assert.NotEqual(updatedPoint.Vectors, initialPoint.Vectors);

            bool HasVectorText(KeyValuePair<string, Value> payload)
            {
                return payload.Key == options.QdrantPayloadDataPropertyName
                    && payload.Value.StructValue != null
                    && payload.Value.StructValue.Fields.Any(s =>
                        s.Key == options.QdrantVectorTextPropertyName
                        && s.Value.StringValue == project.Name
                    );
            }
        }

        [Fact]
        public async Task TestDelete()
        {
            var name = nameof(TestDelete);
            var options = new QdrantSinkOptions
            {
                QdrantChannelFunc = _qdrantFixture.ChannelFunc,
                CollectionName = name,
            };

            await CreateCollection(name);

            var stream = new QdrantSinkStream(name, options, _generator, null);

            stream.Generate(1);

            var user = stream.Users[0];

            await stream.StartStream(@"
                INSERT INTO output
                SELECT 
                    userKey AS id,
                    'a' as vector_string
                FROM users
            ");

            var scroll = await WaitForScrollWithPoints(name, 1);

            Assert.NotNull(scroll);
            Assert.NotEmpty(scroll.Result);

            stream.DeleteUser(user);

            scroll = await WaitForScrollWithPoints(name, 0, onDelay: stream.SchedulerTick);

            Assert.NotNull(scroll);
            Assert.Empty(scroll.Result);
        }

        [Fact]
        public async Task TestInsertWithListUnderOwnKey()
        {
            var name = nameof(TestInsertWithListUnderOwnKey);
            var options = new QdrantSinkOptions
            {
                QdrantChannelFunc = _qdrantFixture.ChannelFunc,
                CollectionName = name,
                QdrantStoreListsUnderOwnKey = true
            };

            await CreateCollection(name);

            var stream = new QdrantSinkStream(name, options, _generator, null);

            var numberOfProjects = 1;

            stream.Generate(numberOfProjects);

            await stream.StartStream(@"
                INSERT INTO qdrant
                SELECT 
                    projectKey AS id,
                    name AS vector_string,
                    list('key1', 'key2', 'd', 0) AS my_list
                FROM projects
            ");

            var scroll = await WaitForScrollWithPoints(name);
            var point = scroll.Result[0];
            var listPayload = point.Payload.First(s => s.Key == "my_list");

            Assert.NotNull(scroll);
            Assert.NotEmpty(scroll.Result);
            Assert.Equal(numberOfProjects, scroll.Result.Count);
            Assert.NotNull(listPayload.Value.ListValue);
            Assert.NotEmpty(listPayload.Value.ListValue.Values);
        }

        [Fact]
        public async Task TestInsertWithStructUnderOwnKey()
        {
            var name = nameof(TestInsertWithStructUnderOwnKey);
            var options = new QdrantSinkOptions
            {
                QdrantChannelFunc = _qdrantFixture.ChannelFunc,
                CollectionName = name,
                QdrantStoreMapsUnderOwnKey = true
            };

            await CreateCollection(name);

            var stream = new QdrantSinkStream(name, options, _generator, null);

            var numberOfProjects = 1;

            stream.Generate(numberOfProjects);

            await stream.StartStream(@"
                INSERT INTO output
                SELECT 
                    projectKey AS id,
                    name AS vector_string,
                    named_struct('key1', 'b', 'key2', 'd') AS extra_info
                FROM projects
            ");

            var scroll = await WaitForScrollWithPoints(name);
            var point = scroll.Result[0];
            var structData = point.Payload.First(s => s.Key == "extra_info");

            Assert.NotNull(scroll);
            Assert.NotEmpty(scroll.Result);
            Assert.Equal(numberOfProjects, scroll.Result.Count);
            Assert.Equal(2, structData.Value.StructValue.Fields.Count);
            Assert.Equal("b", structData.Value.StructValue.Fields.First(s => s.Key == "key1").Value.StringValue);
            Assert.Equal("d", structData.Value.StructValue.Fields.First(s => s.Key == "key2").Value.StringValue);
        }

        [Fact]
        public async Task TestInsertWithMapsUnderOwnKey()
        {
            var name = nameof(TestInsertWithMapsUnderOwnKey);
            var options = new QdrantSinkOptions
            {
                QdrantChannelFunc = _qdrantFixture.ChannelFunc,
                CollectionName = name,
                QdrantStoreMapsUnderOwnKey = true
            };

            await CreateCollection(name);

            var stream = new QdrantSinkStream(name, options, _generator, null);

            var numberOfProjects = 1;

            stream.Generate(numberOfProjects);

            await stream.StartStream(@"
                INSERT INTO output
                SELECT 
                    projectKey AS id,
                    name AS vector_string,
                    map('key1', 'b', 'key2', 'd') AS extra_info
                FROM projects
            ");

            var scroll = await WaitForScrollWithPoints(name);
            var point = scroll.Result[0];
            var mapData = point.Payload.First(s => s.Key == "extra_info");

            Assert.NotNull(scroll);
            Assert.NotEmpty(scroll.Result);
            Assert.Equal(numberOfProjects, scroll.Result.Count);
            Assert.Equal(2, mapData.Value.StructValue.Fields.Count);
            Assert.Equal("b", mapData.Value.StructValue.Fields.First(s => s.Key == "key1").Value.StringValue);
            Assert.Equal("d", mapData.Value.StructValue.Fields.First(s => s.Key == "key2").Value.StringValue);
        }

        [Fact]
        public async Task CustomPayloadDataName()
        {
            var name = nameof(CustomPayloadDataName);
            var options = new QdrantSinkOptions
            {
                QdrantChannelFunc = _qdrantFixture.ChannelFunc,
                CollectionName = name,
                QdrantPayloadDataPropertyName = "my_stuff",
            };

            await CreateCollection(name);

            var stream = new QdrantSinkStream(name, options, _generator, null);

            var numberOfProjects = 1;

            stream.Generate(numberOfProjects);

            await stream.StartStream(@"
                INSERT INTO output
                SELECT 
                    projectKey AS id,
                    name AS vector_string,
                    1 AS my_secret
                FROM projects
            ");

            var scroll = await WaitForScrollWithPoints(name);
            var point = scroll.Result[0];
            var payloadData = point.Payload.First(s => s.Key == options.QdrantPayloadDataPropertyName);

            Assert.True(payloadData.Value.StructValue.Fields.Count > 0);
            Assert.Contains(payloadData.Value.StructValue.Fields, s => s.Key == "my_secret");
        }

        [Fact]
        public async Task CustomIdColumnName()
        {
            var name = nameof(CustomIdColumnName);
            var options = new QdrantSinkOptions
            {
                QdrantChannelFunc = _qdrantFixture.ChannelFunc,
                CollectionName = name,
                IdColumnName = "my_id",
            };

            await CreateCollection(name);

            var stream = new QdrantSinkStream(name, options, _generator, null);

            var numberOfProjects = 1;

            stream.Generate(numberOfProjects);

            await stream.StartStream(@"
                INSERT INTO output
                SELECT 
                    projectKey AS my_id,
                    name AS vector_string
                FROM projects
            ");

            var scroll = await WaitForScrollWithPoints(name);

            Assert.NotNull(scroll);
            Assert.NotEmpty(scroll.Result);
            Assert.Equal(numberOfProjects, scroll.Result.Count);
        }

        [Fact]
        public async Task CustomVectorStringColumnName()
        {
            var name = nameof(CustomVectorStringColumnName);
            var options = new QdrantSinkOptions
            {
                QdrantChannelFunc = _qdrantFixture.ChannelFunc,
                CollectionName = name,
                VectorStringColumnName = "my_vector_string",
            };

            await CreateCollection(name);

            var stream = new QdrantSinkStream(name, options, _generator, null);

            var numberOfProjects = 1;

            stream.Generate(numberOfProjects);

            await stream.StartStream(@"
                INSERT INTO output
                SELECT 
                    projectKey AS id,
                    name AS my_vector_string
                FROM projects
            ");

            var scroll = await WaitForScrollWithPoints(name);

            Assert.NotNull(scroll);
            Assert.NotEmpty(scroll.Result);
            Assert.Equal(numberOfProjects, scroll.Result.Count);
        }


        [Fact]
        public async Task ExcludeTextInPayload()
        {
            var name = nameof(ExcludeTextInPayload);
            var options = new QdrantSinkOptions
            {
                QdrantChannelFunc = _qdrantFixture.ChannelFunc,
                CollectionName = name,
                QdrantIncludeVectorTextInPayload = false
            };

            await CreateCollection(name);

            var stream = new QdrantSinkStream(name, options, _generator, null);

            var numberOfProjects = 1;

            stream.Generate(numberOfProjects);

            await stream.StartStream(@"
                INSERT INTO output
                SELECT 
                    projectKey AS id,
                    name AS vector_string
                FROM projects
            ");

            var scroll = await WaitForScrollWithPoints(name);
            var point = scroll.Result[0];
            var payloadData = point.Payload.First(s => s.Key == options.QdrantPayloadDataPropertyName);

            Assert.NotNull(scroll);
            Assert.NotEmpty(scroll.Result);
            Assert.Equal(numberOfProjects, scroll.Result.Count);
            Assert.Equal(Value.KindOneofCase.StructValue, payloadData.Value.KindCase);
            Assert.DoesNotContain(payloadData.Value.StructValue.Fields, s => s.Key == options.QdrantVectorTextPropertyName);
        }

        [Fact]
        public async Task CustomVectorTextPropertyName()
        {
            var name = nameof(CustomVectorTextPropertyName);
            var options = new QdrantSinkOptions
            {
                QdrantChannelFunc = _qdrantFixture.ChannelFunc,
                CollectionName = name,
                QdrantVectorTextPropertyName = "my_vector_text",
            };

            await CreateCollection(name);

            var stream = new QdrantSinkStream(name, options, _generator, null);

            var numberOfProjects = 1;

            stream.Generate(numberOfProjects);

            await stream.StartStream(@"
                INSERT INTO output
                SELECT 
                    projectKey AS id,
                    name AS vector_string
                FROM projects
            ");

            var scroll = await WaitForScrollWithPoints(name);
            var point = scroll.Result[0];
            var payloadData = point.Payload.First(s => s.Key == options.QdrantPayloadDataPropertyName);

            Assert.NotNull(scroll);
            Assert.NotEmpty(scroll.Result);
            Assert.Equal(numberOfProjects, scroll.Result.Count);
            Assert.Equal(Value.KindOneofCase.StructValue, payloadData.Value.KindCase);
            Assert.Contains(payloadData.Value.StructValue.Fields, s => s.Key == options.QdrantVectorTextPropertyName);
        }

        [Fact]
        public async Task OnInitialize()
        {
            var name = nameof(OnInitialize);
            var onInitializeCalled = false;

            var options = new QdrantSinkOptions
            {
                QdrantChannelFunc = _qdrantFixture.ChannelFunc,
                CollectionName = name,
                OnInitialize = (state, client) =>
                {
                    onInitializeCalled = true;
                    Assert.NotNull(state);
                    return Task.CompletedTask;
                }
            };

            await CreateCollection(name);

            var stream = new QdrantSinkStream(name, options, _generator, null);

            var numberOfProjects = 1;

            stream.Generate(numberOfProjects);

            await stream.StartStream(@"
                INSERT INTO output
                SELECT 
                    projectKey AS id,
                    name AS vector_string
                FROM projects
            ");

            Assert.True(onInitializeCalled);
        }

        [Fact]
        public async Task OnInitializeModifyCollectionName()
        {
            var name = nameof(OnInitializeModifyCollectionName);
            var modifiedName = name + 1;

            var options = new QdrantSinkOptions
            {
                QdrantChannelFunc = _qdrantFixture.ChannelFunc,
                CollectionName = name,
                OnInitialize = async (state, client) =>
                {
                    state.CollectionName = modifiedName;
                    await client.CreateCollectionAsync(state.CollectionName, new VectorParams
                    {
                        Distance = Distance.Cosine,
                        Size = (ulong)_generator.Size,
                    });
                }
            };

            var stream = new QdrantSinkStream(name, options, _generator, null);

            var numberOfProjects = 1;

            stream.Generate(numberOfProjects);

            await stream.StartStream(@"
                INSERT INTO output
                SELECT 
                    projectKey AS id,
                    name AS vector_string
                FROM projects
            ");

            var client = _qdrantFixture.GetClient();
            var exists = await client.CollectionExistsAsync(modifiedName);
            var scroll = await WaitForScrollWithPoints(modifiedName, numberOfProjects);

            Assert.True(exists);
            Assert.NotNull(scroll);
            Assert.Equal(numberOfProjects, scroll.Result.Count);
        }

        [Fact]
        public async Task ExtraDataIsRetainedBetweenDelegates()
        {
            var name = nameof(ExtraDataIsRetainedBetweenDelegates);

            var onInitializeCalled = false;
            var onInitialDataSentCalled = false;
            var onChangesDoneCalled = false;

            var options = new QdrantSinkOptions
            {
                QdrantChannelFunc = _qdrantFixture.ChannelFunc,
                CollectionName = name,
                OnInitialize = (state, client) =>
                {
                    onInitializeCalled = true;
                    state.ExtraData.Add("my_key", "my_value");
                    return Task.CompletedTask;
                },
                OnInitialDataSent = (state, client) =>
                {
                    onInitialDataSentCalled = true;
                    Assert.True(state.ExtraData.ContainsKey("my_key"));
                    Assert.Equal("my_value", state.ExtraData["my_key"]);
                    return Task.CompletedTask;
                },
                OnChangesDone = (state, client) =>
                {
                    onChangesDoneCalled = true;
                    Assert.True(state.ExtraData.ContainsKey("my_key"));
                    Assert.Equal("my_value", state.ExtraData["my_key"]);
                    return Task.CompletedTask;
                }
            };

            await CreateCollection(name);
            var stream = new QdrantSinkStream(name, options, _generator, null);

            var numberOfProjects = 1;

            stream.Generate(numberOfProjects);

            await stream.StartStream(@"
                INSERT INTO output
                SELECT 
                    projectKey AS id,
                    name AS vector_string
                FROM projects
            ");

            await WaitForScrollWithPoints(name, timeoutSeconds: 50);

            Assert.True(onInitializeCalled);
            Assert.True(onInitialDataSentCalled);
            Assert.True(onChangesDoneCalled);
        }

        [Fact]
        public async Task OnInitialDataSent()
        {
            var name = nameof(OnInitialDataSent);
            var onInitialDataSentCalled = false;

            var options = new QdrantSinkOptions
            {
                QdrantChannelFunc = _qdrantFixture.ChannelFunc,
                CollectionName = name,
                OnInitialDataSent = (state, client) =>
                {
                    onInitialDataSentCalled = true;
                    Assert.NotNull(state);
                    return Task.CompletedTask;
                },
            };

            await CreateCollection(name);

            var stream = new QdrantSinkStream(name, options, _generator, null);

            var numberOfProjects = 1;

            stream.Generate(numberOfProjects);

            await stream.StartStream(@"
                INSERT INTO output
                SELECT 
                    projectKey AS id,
                    name AS vector_string
                FROM projects
            ");

            await WaitForScrollWithPoints(name);

            Assert.True(onInitialDataSentCalled);
        }

        [Fact]
        public async Task OnChangesDone()
        {
            var name = nameof(OnChangesDone);
            var onchangesDoneCalled = false;
            var startDate = DateTimeOffset.UtcNow;

            var options = new QdrantSinkOptions
            {
                QdrantChannelFunc = _qdrantFixture.ChannelFunc,
                CollectionName = name,
                OnChangesDone = (state, client) =>
                {
                    onchangesDoneCalled = true;
                    Assert.NotNull(state);
                    Assert.True(state.LastUpdate >= startDate);
                    return Task.CompletedTask;
                },
            };

            await CreateCollection(name);

            var stream = new QdrantSinkStream(name, options, _generator, null);

            var numberOfProjects = 1;

            stream.Generate(numberOfProjects);

            await stream.StartStream(@"
                INSERT INTO output
                SELECT 
                    projectKey AS id,
                    name AS vector_string
                FROM projects
            ");

            await WaitForScrollWithPoints(name);

            Assert.True(onchangesDoneCalled);
        }

        [Fact]
        public async Task AllPointsGetSamePayloadFromChunking()
        {
            var name = nameof(AllPointsGetSamePayloadFromChunking);
            var options = new QdrantSinkOptions
            {
                QdrantChannelFunc = _qdrantFixture.ChannelFunc,
                CollectionName = name,
            };

            await CreateCollection(name);
            var stream = new QdrantSinkStream(name, options, _generator, new EvenPartsChunker(2));

            var numberOfProjects = 1;
            stream.Generate(numberOfProjects);
            await stream.StartStream(@"
                INSERT INTO output
                SELECT 
                    projectKey AS id,
                    'Whatever1Whatever2' AS vector_string,
                    'abc' AS my_property
                FROM projects
            ");

            var scroll = await WaitForScrollWithPoints(name);

            Assert.NotNull(scroll);
            Assert.NotEmpty(scroll.Result);
            Assert.All(scroll.Result, scroll =>
            {
                Assert.Equal(2, scroll.Payload.Count);
                Assert.Contains(scroll.Payload, s => s.Key == options.QdrantPayloadDataPropertyName);
                Assert.Contains(
                    scroll.Payload.First(s => s.Key == options.QdrantPayloadDataPropertyName).Value.StructValue.Fields,
                    s => s.Key == "my_property" && s.Value.StringValue == "abc"
                );
            });
        }

        [Fact]
        public async Task AllChunkPointsAreDeleted()
        {
            var name = nameof(AllChunkPointsAreDeleted);
            var options = new QdrantSinkOptions
            {
                QdrantChannelFunc = _qdrantFixture.ChannelFunc,
                CollectionName = name,
            };

            await CreateCollection(name);
            var stream = new QdrantSinkStream(name, options, _generator, new EvenPartsChunker(2));

            stream.GenerateUsers(1);

            var user = stream.Users[0];

            await stream.StartStream(@"
                INSERT INTO output
                SELECT 
                    userKey AS id,
                    'Whatever1Whatever2' AS vector_string
                FROM users
            ");

            // each chunk is a point, EvenPartsChunker ensures we get two chunks
            var scroll = await WaitForScrollWithPoints(name, waitUntilNumberOfPoints: 2);
            Assert.NotNull(scroll);
            Assert.NotEmpty(scroll.Result);

            stream.DeleteUser(user);

            scroll = await WaitForScrollWithPoints(name, waitUntilNumberOfPoints: 0, onDelay: stream.SchedulerTick);

            Assert.NotNull(scroll);
            Assert.Empty(scroll.Result);
        }

        [Fact]
        public async Task OldExtraChunksAreRemovedOnUpdate()
        {
            var name = nameof(OldExtraChunksAreRemovedOnUpdate);
            var options = new QdrantSinkOptions
            {
                QdrantChannelFunc = _qdrantFixture.ChannelFunc,
                CollectionName = name,
            };

            await CreateCollection(name);
            var stream = new QdrantSinkStream(name, options, _generator, new EveryCharChunker());

            var project = new Project
            {
                CompanyId = "1",
                Name = "abc",
                ProjectNumber = "abc",
                ProjectKey = 999
            };

            stream.AddOrUpdateProject(project);

            await stream.StartStream(@"
                INSERT INTO output
                SELECT 
                    projectKey AS id,
                    name AS vector_string
                FROM projects
            ");

            // each chunk is a point, EvenPartsChunker ensures we get two chunks
            var scroll = await WaitForScrollWithPoints(name, waitUntilNumberOfPoints: 3);
            Assert.NotNull(scroll);
            Assert.Equal(3, scroll.Result.Count);

            project.Name = "ab";

            scroll = await WaitForScrollWithPoints(name, waitUntilNumberOfPoints: 2, onDelay: stream.SchedulerTick);

            Assert.NotNull(scroll);
            Assert.Equal(2, scroll.Result.Count);
        }

        [Fact]
        public async Task VersionIsAddedToFlowtidePayload()
        {
            var name = nameof(VersionIsAddedToFlowtidePayload);
            var options = new QdrantSinkOptions
            {
                QdrantChannelFunc = _qdrantFixture.ChannelFunc,
                CollectionName = name,
            };

            await CreateCollection(name);
            var stream = new QdrantSinkStream(name, options, _generator, new EvenPartsChunker(2));

            var numberOfProjects = 1;
            stream.Generate(numberOfProjects);

            var version = "my_version";
            await stream.StartStream(@"
                INSERT INTO output
                SELECT 
                    projectKey AS id,
                    'abc' AS vector_string
                FROM projects
            ", version: version);

            var scroll = await WaitForScrollWithPoints(name);
            var point = scroll.Result[0];

            Assert.NotNull(scroll);
            Assert.NotEmpty(scroll.Result);
            Assert.Contains(point.Payload.Values,
                s => s.ListValue != null &&
                s.ListValue.Values.Any(
                    x => x.StringValue.Contains(version)
                    )
                );
        }

        [Fact]
        public async Task MissingIdPropertyFromPlanThrows()
        {
            var collection = nameof(MissingIdPropertyFromPlanThrows);
            var options = new QdrantSinkOptions
            {
                QdrantChannelFunc = _qdrantFixture.ChannelFunc,
                CollectionName = collection
            };

            var stream = new QdrantSinkStream(collection, options, _generator, null);


            var exception = await Assert.ThrowsAnyAsync<NotSupportedException>(() => stream.StartStream(@"
                INSERT INTO output
                SELECT 
                    projectKey AS not_id,
                    name as vector_string
                FROM projects
            "));

            Assert.NotNull(exception);
            Assert.Contains(options.IdColumnName, exception.Message);
            Assert.Contains(nameof(options.IdColumnName), exception.Message);
        }

        [Fact]
        public async Task MissingVectorStringFromPlanThrows()
        {
            var collection = nameof(MissingVectorStringFromPlanThrows);
            var options = new QdrantSinkOptions
            {
                QdrantChannelFunc = _qdrantFixture.ChannelFunc,
                CollectionName = collection
            };

            var stream = new QdrantSinkStream(collection, options, _generator, null);

            var exception = await Assert.ThrowsAnyAsync<NotSupportedException>(() => stream.StartStream(@"
                INSERT INTO output
                SELECT 
                    projectKey AS id,
                    name as not_vector_string
                FROM projects
            "));

            Assert.NotNull(exception);
            Assert.Contains(options.VectorStringColumnName, exception.Message);
            Assert.Contains(nameof(options.VectorStringColumnName), exception.Message);
        }

        private async Task CreateCollection(string name)
        {
            var client = _qdrantFixture.GetClient();
            await client.CreateCollectionAsync(name, new VectorParams
            {
                Distance = Distance.Cosine,
                Size = (ulong)_generator.Size,
            });
        }

        private async Task<ScrollResponse> WaitForScrollWithPoints(
            string collection,
            int? waitUntilNumberOfPoints = null,
            Func<KeyValuePair<string, Value>, bool>? waitUntilPayloadCondition = null,
            int timeoutSeconds = 5,
            Func<Task>? onDelay = null,
            bool withPayload = true,
            bool withVector = false,
            Filter? filter = null)
        {
            var client = _qdrantFixture.GetClient();
            ScrollResponse? scroll = null;
            var timer = new Stopwatch();
            timer.Start();

            var limit = TimeSpan.FromSeconds(timeoutSeconds);

            while (scroll == null)
            {
                var attempt = await client.ScrollAsync(collection,
                    filter,
                    limit: waitUntilNumberOfPoints > 0 ? (uint)waitUntilNumberOfPoints : 10,
                    payloadSelector: new WithPayloadSelector
                    {
                        Enable = withPayload
                    },
                    vectorsSelector: withVector
                    );

                if (waitUntilPayloadCondition != null)
                {
                    foreach (var payload in attempt.Result.SelectMany(s => s.Payload))
                    {
                        if (waitUntilPayloadCondition(payload))
                        {
                            scroll = attempt;
                        }
                    }
                }
                else if (waitUntilNumberOfPoints.HasValue)
                {
                    if (attempt.Result.Count == waitUntilNumberOfPoints)
                    {
                        scroll = attempt;
                    }
                }
                else
                {
                    if (attempt.Result.Count > 0)
                    {
                        scroll = attempt;
                    }
                }

                await Task.Delay(10);
                if (onDelay != null)
                {
                    await onDelay();
                }

                if (timer.Elapsed > limit)
                {
                    throw new InvalidOperationException($"Timeout waiting for scroll with points in collection {collection}");
                }
            }

            return scroll;
        }
    }
}
