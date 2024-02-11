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

using FlowtideDotNet.Connector.OpenFGA.Internal;
using OpenFga.Sdk.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.OpenFGA.Tests
{
    public class ModelParseTests
    {
        [Fact]
        public void TestDirect()
        {
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

            var authModel = JsonSerializer.Deserialize<AuthorizationModel>(model);

            var parser = new FlowtideZanzibarConverter(authModel, new HashSet<string>());
            parser.Parse("group", "member");
        }

        [Fact]
        public void TestRecursion()
        {
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

            var authModel = JsonSerializer.Deserialize<AuthorizationModel>(model);

            var parser = new FlowtideZanzibarConverter(authModel, new HashSet<string>());
            parser.Parse("doc", "can_read");
        }

        [Fact]
        public void TestIntersectionAndWildcard()
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

            var authModel = JsonSerializer.Deserialize<AuthorizationModel>(model);

            var parser = new FlowtideZanzibarConverter(authModel, new HashSet<string>());
            parser.Parse("role_binding", "can_read");
        }

        [Fact]
        public void TestHashtag()
        {
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

            var authModel = JsonSerializer.Deserialize<AuthorizationModel>(model);

            var parser = new FlowtideZanzibarConverter(authModel, new HashSet<string>());
            parser.Parse("doc", "can_read");
        }
    }
}
