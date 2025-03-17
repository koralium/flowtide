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

using OpenFga.Sdk.Client;
using OpenFga.Sdk.Client.Model;
using OpenFga.Sdk.Model;
using System.Diagnostics;
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
        public async Task TestInsert()
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
            Assert.NotNull(authModelRequest);
            var createModelResponse = await client.WriteAuthorizationModel(authModelRequest, new ClientWriteOptions() { StoreId = createStoreResponse.Id });

            var conf = openFGAFixture.Configuration;
            conf.StoreId = createStoreResponse.Id;
            conf.AuthorizationModelId = createModelResponse.AuthorizationModelId;
            var stream = new OpenFgaTestStream("test", conf, false, true);
            stream.Generate(100);
            await stream.StartStream(@"
                INSERT INTO openfga
                SELECT 
                    'user' as user_type,
                    o.userkey as user_id,
                    'member' as relation,
                    'doc' as object_type,
                    o.orderkey as object_id
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
            Debug.Assert(authModelRequest != null);

            var createModelResponse = await client.WriteAuthorizationModel(authModelRequest, new ClientWriteOptions() { StoreId = createStoreResponse.Id });

            var conf = openFGAFixture.Configuration;
            conf.StoreId = createStoreResponse.Id;
            conf.AuthorizationModelId = createModelResponse.AuthorizationModelId;
            var stream = new OpenFgaTestStream("testalreadyexist", conf, false, true);
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
                    'user' as user_type,
                    o.userkey as user_id,
                    'member' as relation,
                    'doc' as object_type,
                    o.orderkey as object_id
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

            await stream.HealthyFor(TimeSpan.FromSeconds(2));
        }

        [Fact]
        public async Task TestDeleteDoesNotExist()
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
                Name = "testdeletenotexist"
            });
            var authModelRequest = JsonSerializer.Deserialize<ClientWriteAuthorizationModelRequest>(model);
            Debug.Assert(authModelRequest != null);

            var createModelResponse = await client.WriteAuthorizationModel(authModelRequest, new ClientWriteOptions() { StoreId = createStoreResponse.Id });

            var conf = openFGAFixture.Configuration;
            conf.StoreId = createStoreResponse.Id;
            conf.AuthorizationModelId = createModelResponse.AuthorizationModelId;
            var stream = new OpenFgaTestStream("testdeletenotexist", conf, false, true);
            stream.Generate(10);

            await stream.StartStream(@"
                INSERT INTO openfga
                SELECT 
                    'user' as user_type,
                    o.userkey as user_id,
                    'member' as relation,
                    'doc' as object_type,
                    o.orderkey as object_id
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
            await postClient.Write(new ClientWriteRequest()
            {
                Deletes = new List<ClientTupleKeyWithoutCondition>()
                {
                    new ClientTupleKeyWithoutCondition()
                    {
                        User = $"user:{firstOrder.UserKey}",
                        Object = $"doc:{firstOrder.OrderKey}",
                        Relation = "member"
                    }
                }
            });
            stream.DeleteOrder(firstOrder);

            await stream.HealthyFor(TimeSpan.FromSeconds(2));
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
            Debug.Assert(authModelRequest != null);

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
            

            var stream = new OpenFgaTestStream("testreadtuples", conf, true, false);
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
            Debug.Assert(authModelRequest != null);

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


            var stream = new OpenFgaTestStream("testobjecttypefilter", conf, true, false);
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
            Debug.Assert(authModelRequest != null);

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

            var stream = new OpenFgaTestStream("testreadparsedstream", conf, true, false);

            var parsedModel = JsonSerializer.Deserialize<AuthorizationModel>(model);
            Debug.Assert(parsedModel != null);

            var modelPlan = OpenFgaToFlowtide.Convert(parsedModel, "doc", "can_read", "openfga");

            stream.SqlPlanBuilder.AddPlanAsView("authdata", modelPlan);

            stream.Generate(100);
            await stream.StartStream(@"
                INSERT INTO testverify
                SELECT 
                    user_type,
                    user_id,
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
            Debug.Assert(authModelRequest != null);

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


            var stream = new OpenFgaTestStream("testparsedmodeldirect", conf, true, false);

            var parsedModel = JsonSerializer.Deserialize<AuthorizationModel>(model);
            Debug.Assert(parsedModel != null);
            var modelPlan = OpenFgaToFlowtide.Convert(parsedModel, "group", "member", "openfga");

            stream.SqlPlanBuilder.AddPlanAsView("authdata", modelPlan);

            stream.Generate(100);
            await stream.StartStream(@"
                INSERT INTO testverify
                SELECT 
                    user_type,
                    user_id,
                    relation,
                    object_type,
                    object_id
                FROM authdata
            ");

            await stream.WaitForUpdate();
            var rows = stream.GetActualRowsAsVectors();

            Assert.Single(rows);
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
            Debug.Assert(authModelRequest != null);

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


            var stream = new OpenFgaTestStream("TestParsedModelWithAndWildcard", conf, true, false);

            var parsedModel = JsonSerializer.Deserialize<AuthorizationModel>(model);
            Debug.Assert(parsedModel != null);
            var modelPlan = OpenFgaToFlowtide.Convert(parsedModel, "role_binding", "can_read", "openfga");

            stream.SqlPlanBuilder.AddPlanAsView("authdata", modelPlan);

            stream.Generate(100);
            await stream.StartStream(@"
                INSERT INTO testverify
                SELECT 
                    user_type,
                    user_id,
                    relation,
                    object_type,
                    object_id
                FROM authdata
            ");

            await stream.WaitForUpdate();
            var rows = stream.GetActualRowsAsVectors();
            stream.AssertCurrentDataEqual(new[]
            {
                new { user_type = "user", user_id = "1", relation = "can_read", object_type = "role_binding", object_id = "1_1" }
            });

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
            Assert.Empty(rowsAfterDelete);
        }

        [Fact]
        public async Task TestReadParsedModelWithStopType()
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
                Name = "TestReadParsedModelWithStopType"
            });
            var authModelRequest = JsonSerializer.Deserialize<ClientWriteAuthorizationModelRequest>(model);
            Debug.Assert(authModelRequest != null);

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

            var stream = new OpenFgaTestStream("TestReadParsedModelWithStopType", conf, true, false);

            var parsedModel = JsonSerializer.Deserialize<AuthorizationModel>(model);
            Debug.Assert(parsedModel != null);
            var modelPlan = OpenFgaToFlowtide.Convert(parsedModel, "doc", "can_read", "openfga", "group");

            stream.SqlPlanBuilder.AddPlanAsView("authdata", modelPlan);

            //stream.Generate(100);
            await stream.StartStream(@"
                INSERT INTO testverify
                SELECT 
                    user_type,
                    user_id,
                    relation,
                    object_type,
                    object_id
                FROM authdata
            ");

            await stream.WaitForUpdate();
            var rows = stream.GetActualRowsAsVectors();

            stream.AssertCurrentDataEqual(new[]
            {
                new { user_type = "group", user_id = "1", relation = "can_read", object_type = "doc", object_id = "1" }
            });
        }

        [Fact]
        public async Task TestHashtagRelation()
        {
            //  model
            //    schema 1.1
            //
            //  type user
            //
            //  type group
            //    relations
            //      define member: [user]
            //
            //  type doc
            //    relations
            //      define can_read: [group#member]

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
                },
                {
                  ""type"": ""doc"",
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
                            ""type"": ""group"",
                            ""relation"": ""member""
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
                Name = "TestHashtagRelation"
            });
            var authModelRequest = JsonSerializer.Deserialize<ClientWriteAuthorizationModelRequest>(model);
            Assert.NotNull(authModelRequest);
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
                        User = $"group:1#member",
                        Object = "doc:1",
                        Relation = "can_read"
                    }
                }
            });

            var stream = new OpenFgaTestStream("TestHashtagRelation", conf, true, false);

            var parsedModel = JsonSerializer.Deserialize<AuthorizationModel>(model);
            Assert.NotNull(parsedModel);
            var modelPlan = OpenFgaToFlowtide.Convert(parsedModel, "doc", "can_read", "openfga", "group");

            stream.SqlPlanBuilder.AddPlanAsView("authdata", modelPlan);

            await stream.StartStream(@"
                INSERT INTO testverify
                SELECT 
                    user_type,
                    user_id,
                    relation,
                    object_type,
                    object_id
                FROM authdata
            ");

            await stream.WaitForUpdate();
            stream.AssertCurrentDataEqual(new[]
            {
                new { user_type = "user", user_id = "1", relation = "can_read", object_type = "doc", object_id = "1" }
            });
        }

        private static async IAsyncEnumerable<TupleKey> FetchRows(OpenFgaClient client)
        {
            var response = await client.Read(new ClientReadRequest());

            foreach (var v in response.Tuples)
            {
                yield return v.Key;
            }
        }

        [Fact]
        public async Task TestInsertDeleteExisting()
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
            Assert.NotNull(authModelRequest);
            var createModelResponse = await client.WriteAuthorizationModel(authModelRequest, new ClientWriteOptions() { StoreId = createStoreResponse.Id });

            var conf = openFGAFixture.Configuration;
            conf.StoreId = createStoreResponse.Id;
            conf.AuthorizationModelId = createModelResponse.AuthorizationModelId;

            var inesrtClient = new OpenFgaClient(conf);
            await inesrtClient.Write(new ClientWriteRequest()
            {
                Writes = new List<ClientTupleKey>()
                {
                    new ClientTupleKey()
                    {
                        User = "user:9999",
                        Relation = "member",
                        Object = "doc:9999"
                    }
                }
            });

            var stream = new OpenFgaTestStream("testinsertdeleteexisting", conf, false, true, FetchRows);
            stream.Generate(10);
            await stream.StartStream(@"
                INSERT INTO openfga
                SELECT 
                    'user' as user_type,
                    o.userkey as user_id,
                    'member' as relation,
                    'doc' as object_type,
                    o.orderkey as object_id
                FROM orders o
            ");

            var postClient = new OpenFgaClient(conf);

            ReadResponse? readResp = default;
            while (true)
            {
                readResp = await postClient.Read(options: new ClientReadOptions() { PageSize = 100 });
                if (readResp.Tuples.Count == 10)
                {
                    break;
                }
                await Task.Delay(10);
            }

            Assert.Equal(10, readResp.Tuples.Count);
        }
    }
}