using FlowtideDotNet.Connector.OpenFGA.Internal;
using OpenFga.Sdk.Client;
using OpenFga.Sdk.Client.Model;
using OpenFga.Sdk.Model;
using System.Text.Json;

namespace FlowtideDotNet.Connector.OpenFGA.Tests
{
    public class OpenFGATests : IClassFixture<OpenFGAFixture>
    {
        private readonly OpenFGAFixture openFGAFixture;

        public OpenFGATests(OpenFGAFixture openFGAFixture)
        {
            this.openFGAFixture = openFGAFixture;
        }

        [Fact]
        public async Task TestSimpleModel()
        {
            var config = openFGAFixture.Configuration;

            var model = @"
            {
              ""schema_version"": ""1.1"",
              ""type_definitions"": [
                {
                  ""type"": ""user"",
                  ""relations"": {},
                  ""metadata"": null
                },
                {
                  ""type"": ""doc"",
                  ""relations"": {
                    ""member"": {
                      ""this"": {}
                    }
                  },
                  ""metadata"": {
                    ""relations"": {
                      ""member"": {
                        ""directly_related_user_types"": [
                          {
                            ""type"": ""user""
                          }
                        ]
                      }
                    }
                  }
                }
              ]
            }";
            var client = new OpenFgaClient(config);

            var createStoreResponse = await client.CreateStore(new ClientCreateStoreRequest()
            {
                Name = "teststore1"
            });
            var authModelRequest = JsonSerializer.Deserialize<ClientWriteAuthorizationModelRequest>(model);
            
            var createModelResponse = await client.WriteAuthorizationModel(authModelRequest, new ClientWriteOptions() { StoreId = createStoreResponse.Id });

            var conf = openFGAFixture.Configuration;
            conf.StoreId = createStoreResponse.Id;
            conf.AuthorizationModelId = createModelResponse.AuthorizationModelId;
            var stream = new OpenFgaTestStream("test", conf);
            stream.Generate(100);
            await stream.StartStream(@"
                INSERT INTO openfga
                SELECT 
                    'user:' || o.userkey as user,
                    'member' as relation,
                    'doc:' || o.orderkey as object
                FROM orders o
            ");

            var postClient = new OpenFgaClient(conf);
            while (true)
            {
                var readResp = await postClient.Read(options: new ClientReadOptions() { PageSize = 100});
                if (readResp.Tuples.Count == 100)
                {
                    break;
                }
                await Task.Delay(10);
            }

            var firstOrder = stream.Orders[0];
            var checkResponse = await postClient.Check(new ClientCheckRequest()
            {
                User = $"user:{firstOrder.UserKey}",
                Relation = "member",
                Object = $"doc:{firstOrder.OrderKey}"
            });
            var checkResponse2 = await postClient.Check(new ClientCheckRequest()
            {
                User = $"user:9000",
                Relation = "member",
                Object = $"doc:{firstOrder.OrderKey}"
            });
        }

        

        [Fact]
        public async Task TestReadTuples()
        {
            var config = openFGAFixture.Configuration;

            var model = @"
            {
              ""schema_version"": ""1.1"",
              ""type_definitions"": [
                {
                  ""type"": ""user"",
                  ""relations"": {},
                  ""metadata"": null
                },
                {
                  ""type"": ""doc"",
                  ""relations"": {
                    ""member"": {
                      ""this"": {}
                    }
                  },
                  ""metadata"": {
                    ""relations"": {
                      ""member"": {
                        ""directly_related_user_types"": [
                          {
                            ""type"": ""user""
                          }
                        ]
                      }
                    }
                  }
                }
              ]
            }";
            var client = new OpenFgaClient(config);

            var createStoreResponse = await client.CreateStore(new ClientCreateStoreRequest()
            {
                Name = "teststore2"
            });
            var authModelRequest = JsonSerializer.Deserialize<ClientWriteAuthorizationModelRequest>(model);

            var createModelResponse = await client.WriteAuthorizationModel(authModelRequest, new ClientWriteOptions() { StoreId = createStoreResponse.Id });

            var conf = openFGAFixture.Configuration;
            conf.StoreId = createStoreResponse.Id;
            conf.AuthorizationModelId = createModelResponse.AuthorizationModelId;

            var addTupleClient = new OpenFgaClient(conf);

            for (int i = 0; i < 10; i++)
            {
                await addTupleClient.Write(new ClientWriteRequest()
                {
                    Writes = new List<ClientTupleKey>()
                {
                    new ClientTupleKey()
                    {
                        User = $"user:{i}",
                        Object = "doc:1",
                        Relation = "member"
                    }
                }
                });
            }
            

            var stream = new OpenFgaTestStream("testreadtuples", conf);
            stream.Generate(100);
            await stream.StartStream(@"

                CREATE TABLE openfga (
                    user,
                    relation,
                    object
                );

                INSERT INTO testverify
                SELECT 
                    user,
                    relation,
                    object
                FROM openfga
            ");

            await stream.WaitForUpdate();
            var rows = stream.GetActualRowsAsVectors();

            await addTupleClient.Write(new ClientWriteRequest()
            {
                Writes = new List<ClientTupleKey>()
                {
                    new ClientTupleKey()
                    {
                        User = $"user:11",
                        Object = "doc:1",
                        Relation = "member"
                    }
                }
            });

            await stream.WaitForUpdate();
        }

        [Fact]
        public async Task TesObjectTypeFilter()
        {
            var config = openFGAFixture.Configuration;

            var model = @"
            {
              ""schema_version"": ""1.1"",
              ""type_definitions"": [
                {
                  ""type"": ""user"",
                  ""relations"": {},
                  ""metadata"": null
                },
                {
                  ""type"": ""doc"",
                  ""relations"": {
                    ""member"": {
                      ""this"": {}
                    }
                  },
                  ""metadata"": {
                    ""relations"": {
                      ""member"": {
                        ""directly_related_user_types"": [
                          {
                            ""type"": ""user""
                          }
                        ]
                      }
                    }
                  }
                }
              ]
            }";
            var client = new OpenFgaClient(config);

            var createStoreResponse = await client.CreateStore(new ClientCreateStoreRequest()
            {
                Name = "teststore3"
            });
            var authModelRequest = JsonSerializer.Deserialize<ClientWriteAuthorizationModelRequest>(model);

            var createModelResponse = await client.WriteAuthorizationModel(authModelRequest, new ClientWriteOptions() { StoreId = createStoreResponse.Id });

            var conf = openFGAFixture.Configuration;
            conf.StoreId = createStoreResponse.Id;
            conf.AuthorizationModelId = createModelResponse.AuthorizationModelId;

            var addTupleClient = new OpenFgaClient(conf);

            for (int i = 0; i < 10; i++)
            {
                await addTupleClient.Write(new ClientWriteRequest()
                {
                    Writes = new List<ClientTupleKey>()
                {
                    new ClientTupleKey()
                    {
                        User = $"user:{i}",
                        Object = "doc:1",
                        Relation = "member"
                    }
                }
                });
            }


            var stream = new OpenFgaTestStream("testobjecttypefilter", conf);
            stream.Generate(100);
            await stream.StartStream(@"

                CREATE TABLE openfga (
                    user,
                    relation,
                    object,
                    object_type
                );

                INSERT INTO testverify
                SELECT 
                    user,
                    relation,
                    object
                FROM openfga
                where object_type = 'doc'
            ");

            await stream.WaitForUpdate();
            var rows = stream.GetActualRowsAsVectors();

            await addTupleClient.Write(new ClientWriteRequest()
            {
                Writes = new List<ClientTupleKey>()
                {
                    new ClientTupleKey()
                    {
                        User = $"user:11",
                        Object = "doc:1",
                        Relation = "member"
                    }
                }
            });

            await stream.WaitForUpdate();
        }

        [Fact]
        public async Task TestReadParsedModel()
        {
            var config = openFGAFixture.Configuration;

            var model = @"
            {
              ""schema_version"": ""1.1"",
              ""type_definitions"": [
                {
                  ""type"": ""user"",
                  ""relations"": {},
                  ""metadata"": null
                },
                {
                  ""type"": ""group"",
                  ""relations"": {
                    ""parent"": {
                      ""this"": {}
                    },
                    ""member"": {
                      ""this"": {}
                    },
                    ""can_read"": {
                      ""union"": {
                        ""child"": [
                          {
                            ""computedUserset"": {
                              ""relation"": ""member""
                            }
                          },
                          {
                            ""tupleToUserset"": {
                              ""computedUserset"": {
                                ""relation"": ""can_read""
                              },
                              ""tupleset"": {
                                ""relation"": ""parent""
                              }
                            }
                          }
                        ]
                      }
                    }
                  },
                  ""metadata"": {
                    ""relations"": {
                      ""parent"": {
                        ""directly_related_user_types"": [
                          {
                            ""type"": ""group""
                          }
                        ]
                      },
                      ""member"": {
                        ""directly_related_user_types"": [
                          {
                            ""type"": ""user""
                          }
                        ]
                      },
                      ""can_read"": {
                        ""directly_related_user_types"": []
                      }
                    }
                  }
                },
                {
                  ""type"": ""doc"",
                  ""relations"": {
                    ""parent_group"": {
                      ""this"": {}
                    },
                    ""can_read"": {
                      ""tupleToUserset"": {
                        ""computedUserset"": {
                          ""relation"": ""can_read""
                        },
                        ""tupleset"": {
                          ""relation"": ""parent_group""
                        }
                      }
                    }
                  },
                  ""metadata"": {
                    ""relations"": {
                      ""parent_group"": {
                        ""directly_related_user_types"": [
                          {
                            ""type"": ""group""
                          }
                        ]
                      },
                      ""can_read"": {
                        ""directly_related_user_types"": []
                      }
                    }
                  }
                }
              ]
            }";
            var client = new OpenFgaClient(config);

            var createStoreResponse = await client.CreateStore(new ClientCreateStoreRequest()
            {
                Name = "teststore4"
            });
            var authModelRequest = JsonSerializer.Deserialize<ClientWriteAuthorizationModelRequest>(model);

            var createModelResponse = await client.WriteAuthorizationModel(authModelRequest, new ClientWriteOptions() { StoreId = createStoreResponse.Id });

            var conf = openFGAFixture.Configuration;
            conf.StoreId = createStoreResponse.Id;
            conf.AuthorizationModelId = createModelResponse.AuthorizationModelId;

            var addTupleClient = new OpenFgaClient(conf);

            await addTupleClient.Write(new ClientWriteRequest()
            {
                Writes = new List<ClientTupleKey>()
                {
                    new ClientTupleKey()
                    {
                        User = $"user:1",
                        Object = "group:1",
                        Relation = "member"
                    }
                }
            });

            await addTupleClient.Write(new ClientWriteRequest()
            {
                Writes = new List<ClientTupleKey>()
                {
                    new ClientTupleKey()
                    {
                        User = $"group:1",
                        Object = "doc:1",
                        Relation = "parent_group"
                    }
                }
            });

            await addTupleClient.Write(new ClientWriteRequest()
            {
                Writes = new List<ClientTupleKey>()
                {
                    new ClientTupleKey()
                    {
                        User = $"group:2",
                        Object = "group:1",
                        Relation = "parent"
                    }
                }
            });

            await addTupleClient.Write(new ClientWriteRequest()
            {
                Writes = new List<ClientTupleKey>()
                {
                    new ClientTupleKey()
                    {
                        User = $"user:2",
                        Object = "group:2",
                        Relation = "member"
                    }
                }
            });

            var stream = new OpenFgaTestStream("testreadparsedstream", conf);

            var parsedModel = JsonSerializer.Deserialize<AuthorizationModel>(model);
            var modelPlan = new FlowtideOpenFgaModelParser(parsedModel).Parse("doc", "can_read", "openfga");

            stream.SqlPlanBuilder.AddPlanAsView("authdata", modelPlan);

            stream.Generate(100);
            await stream.StartStream(@"
                INSERT INTO testverify
                SELECT 
                    user,
                    relation,
                    object
                FROM authdata
            ");

            await stream.WaitForUpdate();
            var rows = stream.GetActualRowsAsVectors();

            Assert.Equal(2, rows.Count);

            await addTupleClient.Write(new ClientWriteRequest()
            {
                Writes = new List<ClientTupleKey>()
                {
                    new ClientTupleKey()
                    {
                        User = $"user:3",
                        Object = "group:1",
                        Relation = "member"
                    }
                }
            });

            await addTupleClient.Write(new ClientWriteRequest()
            {
                Writes = new List<ClientTupleKey>()
                {
                    new ClientTupleKey()
                    {
                        User = $"user:4",
                        Object = "group:2",
                        Relation = "member"
                    }
                }
            });

            await stream.WaitForUpdate();
            var rows2 = stream.GetActualRowsAsVectors();

            Assert.Equal(4, rows2.Count);

            await addTupleClient.Write(new ClientWriteRequest()
            {
                Deletes = new List<ClientTupleKeyWithoutCondition>()
                {
                    new ClientTupleKeyWithoutCondition()
                    {
                        User = $"user:4",
                        Object = "group:2",
                        Relation = "member"
                    }
                }
            });

            await stream.WaitForUpdate();
            var rows3 = stream.GetActualRowsAsVectors();
            Assert.Equal(3, rows3.Count);
        }

        [Fact]
        public async Task TestParsedModelDirect()
        {
            var config = openFGAFixture.Configuration;

            var model = @"
            {
              ""schema_version"": ""1.1"",
              ""type_definitions"": [
                {
                  ""type"": ""user"",
                  ""relations"": {},
                  ""metadata"": null
                },
                {
                  ""type"": ""group"",
                  ""relations"": {
                    ""member"": {
                      ""this"": {}
                    }
                  },
                  ""metadata"": {
                    ""relations"": {
                      ""member"": {
                        ""directly_related_user_types"": [
                          {
                            ""type"": ""user""
                          }
                        ]
                      }
                    }
                  }
                }
              ]
            }";
            var client = new OpenFgaClient(config);

            var createStoreResponse = await client.CreateStore(new ClientCreateStoreRequest()
            {
                Name = "teststore5"
            });
            var authModelRequest = JsonSerializer.Deserialize<ClientWriteAuthorizationModelRequest>(model);

            var createModelResponse = await client.WriteAuthorizationModel(authModelRequest, new ClientWriteOptions() { StoreId = createStoreResponse.Id });

            var conf = openFGAFixture.Configuration;
            conf.StoreId = createStoreResponse.Id;
            conf.AuthorizationModelId = createModelResponse.AuthorizationModelId;

            var addTupleClient = new OpenFgaClient(conf);

            await addTupleClient.Write(new ClientWriteRequest()
            {
                Writes = new List<ClientTupleKey>()
                {
                    new ClientTupleKey()
                    {
                        User = $"user:1",
                        Object = "group:1",
                        Relation = "member"
                    }
                }
            });


            var stream = new OpenFgaTestStream("testreadparsedstream", conf);

            var parsedModel = JsonSerializer.Deserialize<AuthorizationModel>(model);
            var modelPlan = new FlowtideOpenFgaModelParser(parsedModel).Parse("group", "member", "openfga");

            stream.SqlPlanBuilder.AddPlanAsView("authdata", modelPlan);

            stream.Generate(100);
            await stream.StartStream(@"
                INSERT INTO testverify
                SELECT 
                    user,
                    relation,
                    object
                FROM authdata
            ");

            await stream.WaitForUpdate();
            var rows = stream.GetActualRowsAsVectors();

            Assert.Equal(2, rows.Count);
        }
    }
}