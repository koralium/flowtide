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
                Name = "teststore"
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
        public async Task TestParseModelToView()
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
                  ""type"": ""organization_group"",
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
                            ""type"": ""organization_group""
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
                  ""type"": ""project"",
                  ""relations"": {
                    ""organisation"": {
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
                                ""relation"": ""organisation""
                              }
                            }
                          }
                        ]
                      }
                    }
                  },
                  ""metadata"": {
                    ""relations"": {
                      ""organisation"": {
                        ""directly_related_user_types"": [
                          {
                            ""type"": ""organization_group""
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
                }
              ]
            }";

            var parsedModel = JsonSerializer.Deserialize<AuthorizationModel>(model);
        }
    }
}