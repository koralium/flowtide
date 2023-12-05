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

using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Storage.DeviceFactories;
using FlowtideDotNet.Substrait.Conversion;
using FlowtideDotNet.Substrait;
using FASTER.core;
using Microsoft.Extensions.Logging;

namespace FlowtideDotNet.Core.Tests.Failure
{
    public class FailureTests
    {
        [Fact]
        public async Task TestEgressFailure()
        {
            var plan = @"
{
  ""extensionUris"": [{
    ""extensionUriAnchor"": 1,
    ""uri"": ""/functions_comparison.yaml""
  }],
  ""extensions"": [{
    ""extensionFunction"": {
      ""extensionUriReference"": 1,
      ""functionAnchor"": 0,
      ""name"": ""equal:any_any""
    }
  }],
  ""relations"": [{
    ""root"": {
      ""input"": {
        ""project"": {
          ""common"": {
            ""emit"": {
              ""outputMapping"": [6, 7]
            }
          },
          ""input"": {
            ""join"": {
              ""common"": {
                ""direct"": {
                }
              },
              ""left"": {
                ""project"": {
                  ""common"": {
                    ""emit"": {
                      ""outputMapping"": [3, 4, 5]
                    }
                  },
                  ""input"": {
                    ""filter"": {
                      ""common"": {
                        ""direct"": {
                        }
                      },
                      ""input"": {
                        ""read"": {
                          ""common"": {
                            ""direct"": {
                            }
                          },
                          ""baseSchema"": {
                            ""names"": [""FIRSTNAME"", ""LASTNAME"", ""ZIP""],
                            ""struct"": {
                              ""types"": [{
                                ""string"": {
                                  ""typeVariationReference"": 0,
                                  ""nullability"": ""NULLABILITY_REQUIRED""
                                }
                              }, {
                                ""string"": {
                                  ""typeVariationReference"": 0,
                                  ""nullability"": ""NULLABILITY_NULLABLE""
                                }
                              }, {
                                ""i32"": {
                                  ""typeVariationReference"": 0,
                                  ""nullability"": ""NULLABILITY_NULLABLE""
                                }
                              }],
                              ""typeVariationReference"": 0,
                              ""nullability"": ""NULLABILITY_REQUIRED""
                            }
                          },
                          ""namedTable"": {
                            ""names"": [""PERSONS""]
                          }
                        }
                      },
                      ""condition"": {
                        ""scalarFunction"": {
                          ""functionReference"": 0,
                          ""args"": [],
                          ""outputType"": {
                            ""bool"": {
                              ""typeVariationReference"": 0,
                              ""nullability"": ""NULLABILITY_NULLABLE""
                            }
                          },
                          ""arguments"": [{
                            ""value"": {
                              ""selection"": {
                                ""directReference"": {
                                  ""structField"": {
                                    ""field"": 2
                                  }
                                },
                                ""rootReference"": {
                                }
                              }
                            }
                          }, {
                            ""value"": {
                              ""literal"": {
                                ""i32"": 90210,
                                ""nullable"": false,
                                ""typeVariationReference"": 0
                              }
                            }
                          }],
                          ""options"": []
                        }
                      }
                    }
                  },
                  ""expressions"": [{
                    ""selection"": {
                      ""directReference"": {
                        ""structField"": {
                          ""field"": 1
                        }
                      },
                      ""rootReference"": {
                      }
                    }
                  }, {
                    ""selection"": {
                      ""directReference"": {
                        ""structField"": {
                          ""field"": 0
                        }
                      },
                      ""rootReference"": {
                      }
                    }
                  }, {
                    ""selection"": {
                      ""directReference"": {
                        ""structField"": {
                          ""field"": 2
                        }
                      },
                      ""rootReference"": {
                      }
                    }
                  }]
                }
              },
              ""right"": {
                ""read"": {
                  ""common"": {
                    ""direct"": {
                    }
                  },
                  ""baseSchema"": {
                    ""names"": [""FIRSTNAME"", ""LASTNAME"", ""ZIP""],
                    ""struct"": {
                      ""types"": [{
                        ""string"": {
                          ""typeVariationReference"": 0,
                          ""nullability"": ""NULLABILITY_REQUIRED""
                        }
                      }, {
                        ""string"": {
                          ""typeVariationReference"": 0,
                          ""nullability"": ""NULLABILITY_NULLABLE""
                        }
                      }, {
                        ""i32"": {
                          ""typeVariationReference"": 0,
                          ""nullability"": ""NULLABILITY_NULLABLE""
                        }
                      }],
                      ""typeVariationReference"": 0,
                      ""nullability"": ""NULLABILITY_REQUIRED""
                    }
                  },
                  ""namedTable"": {
                    ""names"": [""PERSONTABLE""]
                  }
                }
              },
              ""expression"": {
                ""scalarFunction"": {
                  ""functionReference"": 0,
                  ""args"": [],
                  ""outputType"": {
                    ""bool"": {
                      ""typeVariationReference"": 0,
                      ""nullability"": ""NULLABILITY_REQUIRED""
                    }
                  },
                  ""arguments"": [{
                    ""value"": {
                      ""selection"": {
                        ""directReference"": {
                          ""structField"": {
                            ""field"": 1
                          }
                        },
                        ""rootReference"": {
                        }
                      }
                    }
                  }, {
                    ""value"": {
                      ""selection"": {
                        ""directReference"": {
                          ""structField"": {
                            ""field"": 3
                          }
                        },
                        ""rootReference"": {
                        }
                      }
                    }
                  }],
                  ""options"": []
                }
              },
              ""type"": ""JOIN_TYPE_INNER""
            }
          },
          ""expressions"": [{
            ""selection"": {
              ""directReference"": {
                ""structField"": {
                  ""field"": 0
                }
              },
              ""rootReference"": {
              }
            }
          }, {
            ""selection"": {
              ""directReference"": {
                ""structField"": {
                  ""field"": 3
                }
              },
              ""rootReference"": {
              }
            }
          }]
        }
      },
      ""names"": [""LASTNAME"", ""FIRSTNAME""]
    }
  }],
  ""expectedTypeUrls"": []
}
";

            SubstraitDeserializer substraitDeserializer = new SubstraitDeserializer();
            var deserializedPlan = substraitDeserializer.Deserialize(plan);
            var convertedPlan = SubstraitToDifferentialCompute.Convert(deserializedPlan, true, "lastname");
            var optimizedPlan = PlanOptimizer.Optimize(convertedPlan);

            bool thrownOnce = false;
            int checkpointCount = 0;

            var tmpStorage = new InMemoryDeviceFactory();
            tmpStorage.Initialize("./data/tmp");
            FlowtideBuilder differentialComputeBuilder = new FlowtideBuilder("teststream")
                .WithStateOptions(new FlowtideDotNet.Storage.StateManager.StateManagerOptions()
                {
                    CachePageCount = 100,
                    CheckpointDir = "./data",
                    LogDevice = tmpStorage.Get(new FileDescriptor()
                    {
                        directoryName = "persistent",
                        fileName = "log"
                    }),
                    TemporaryStorageFactory = tmpStorage
                })
                .AddPlan(optimizedPlan)
                .AddReadWriteFactory(new TestFactory(
                    (readRel, opt) =>
                    {
                        return new TestIngress(opt);
                    },
                    (writeRel, opt) =>
                    {
                        return new FailureEgress(opt, new FailureEgressOptions()
                        {
                            OnCheckpoint = () =>
                            {
                                checkpointCount++;
                                if (!thrownOnce)
                                {
                                    thrownOnce = true;
                                    throw new Exception();
                                }
                            }
                        });
                    }
                    ));

            var stream = differentialComputeBuilder.Build();
            await stream.StartAsync();

            var cancelToken = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            while (!cancelToken.IsCancellationRequested)
            {
                if (checkpointCount == 2)
                {
                    break;
                }
            }
        }
    }
}
