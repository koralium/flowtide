using FlowtideDotNet.Connector.OpenFGA.Internal;
using FlowtideDotNet.Substrait.Relations;
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
        public async Task TestInsertAlreadyExists()
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
                Name = "testalreadyexist"
            });
            var authModelRequest = JsonSerializer.Deserialize<ClientWriteAuthorizationModelRequest>(model);

            var createModelResponse = await client.WriteAuthorizationModel(authModelRequest, new ClientWriteOptions() { StoreId = createStoreResponse.Id });

            var conf = openFGAFixture.Configuration;
            conf.StoreId = createStoreResponse.Id;
            conf.AuthorizationModelId = createModelResponse.AuthorizationModelId;
            var stream = new OpenFgaTestStream("testalreadyexist", conf);
            stream.Generate(10);

            var addTupleClient = new OpenFgaClient(conf);


            await addTupleClient.Write(new ClientWriteRequest()
            {
                Writes = new List<ClientTupleKey>()
                {
                    new ClientTupleKey()
                    {
                        User = $"user:{stream.Orders[0].UserKey}",
                        Object = $"doc:{stream.Orders[0].OrderKey}",
                        Relation = "member"
                    }
                }
            });

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
                var readResp = await postClient.Read(options: new ClientReadOptions() { PageSize = 100 });
                if (readResp.Tuples.Count == 10)
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
                    user_id,
                    user_type,
                    relation,
                    object_id,
                    object_type
                );

                INSERT INTO testverify
                SELECT 
                    user_id,
                    user_type,
                    relation,
                    object_id,
                    object_type
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
                    user_id,
                    user_type,
                    relation,
                    object_id,
                    object_type
                );

                INSERT INTO testverify
                SELECT 
                    user_id,
                    relation,
                    object_id
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
                    user_type,
                    user_id
                    relation,
                    object_type,
                    object_id
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


            var stream = new OpenFgaTestStream("testparsedmodeldirect", conf);

            var parsedModel = JsonSerializer.Deserialize<AuthorizationModel>(model);
            var modelPlan = new FlowtideOpenFgaModelParser(parsedModel).Parse("group", "member", "openfga");

            stream.SqlPlanBuilder.AddPlanAsView("authdata", modelPlan);

            stream.Generate(100);
            await stream.StartStream(@"
                INSERT INTO testverify
                SELECT 
                    user_type,
                    user_id
                    relation,
                    object_type,
                    object_id
                FROM authdata
            ");

            await stream.WaitForUpdate();
            var rows = stream.GetActualRowsAsVectors();

            Assert.Equal(2, rows.Count);
        }

        [Fact]
        public async Task TestParsedModelWithAndWildcard()
        {
            //  model
            //    schema 1.1
            //
            //  type user
            //
            //  type role
            //    relations
            //      define can_read: [user:*]
            //
            //  type role_binding
            //    relations
            //      define role: [role]
            //        define user: [user]
            //        define can_read: user and can_read from role
            
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
                  ""type"": ""role"",
                  ""relations"": {
                    ""can_read"": {
                      ""this"": {}
                    }
                  },
                  ""metadata"": {
                    ""relations"": {
                      ""can_read"": {
                        ""directly_related_user_types"": [
                          {
                            ""type"": ""user"",
                            ""wildcard"": {}
                          }
                        ]
                      }
                    }
                  }
                },
                {
                  ""type"": ""role_binding"",
                  ""relations"": {
                    ""role"": {
                      ""this"": {}
                    },
                    ""user"": {
                      ""this"": {}
                    },
                    ""can_read"": {
                      ""intersection"": {
                        ""child"": [
                          {
                            ""computedUserset"": {
                              ""relation"": ""user""
                            }
                          },
                          {
                            ""tupleToUserset"": {
                              ""computedUserset"": {
                                ""relation"": ""can_read""
                              },
                              ""tupleset"": {
                                ""relation"": ""role""
                              }
                            }
                          }
                        ]
                      }
                    }
                  },
                  ""metadata"": {
                    ""relations"": {
                      ""role"": {
                        ""directly_related_user_types"": [
                          {
                            ""type"": ""role""
                          }
                        ]
                      },
                      ""user"": {
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
                }
              ]
            }";

            var client = new OpenFgaClient(config);

            var createStoreResponse = await client.CreateStore(new ClientCreateStoreRequest()
            {
                Name = "teststore6"
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
                        Object = "role_binding:1_1",
                        Relation = "user"
                    }
                }
            });
            await addTupleClient.Write(new ClientWriteRequest()
            {
                Writes = new List<ClientTupleKey>()
                {
                    new ClientTupleKey()
                    {
                        User = $"role:1",
                        Object = "role_binding:1_1",
                        Relation = "role"
                    }
                }
            });
            await addTupleClient.Write(new ClientWriteRequest()
            {
                Writes = new List<ClientTupleKey>()
                {
                    new ClientTupleKey()
                    {
                        User = $"user:*",
                        Object = "role:1",
                        Relation = "can_read"
                    }
                }
            });


            var stream = new OpenFgaTestStream("TestParsedModelWithAndWildcard", conf);

            var parsedModel = JsonSerializer.Deserialize<AuthorizationModel>(model);
            var modelPlan = new FlowtideOpenFgaModelParser(parsedModel).Parse("role_binding", "can_read", "openfga");

            stream.SqlPlanBuilder.AddPlanAsView("authdata", modelPlan);

            stream.Generate(100);
            await stream.StartStream(@"
                INSERT INTO testverify
                SELECT 
                    user_type,
                    user_id
                    relation,
                    object_type,
                    object_id
                FROM authdata
            ");

            await stream.WaitForUpdate();
            var rows = stream.GetActualRowsAsVectors();

            Assert.Equal(1, rows.Count);

            // Remove the can_read access from the role
            await addTupleClient.Write(new ClientWriteRequest()
            {
                Deletes = new List<ClientTupleKeyWithoutCondition>()
                {
                    new ClientTupleKeyWithoutCondition()
                    {
                        User = $"user:*",
                        Object = "role:1",
                        Relation = "can_read"
                    }
                }
            });
            await stream.WaitForUpdate();
            var rowsAfterDelete = stream.GetActualRowsAsVectors();
            await stream.WaitForUpdate();
        }
    }
}