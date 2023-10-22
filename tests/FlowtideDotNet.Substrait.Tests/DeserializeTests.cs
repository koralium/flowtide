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

namespace FlowtideDotNet.Substrait.Tests
{
    public class DeserializeTests
    {
        [Fact]
        public void IbisOutput()
        {
            string ibisOutput = @"
{
  ""extensionUris"": [
    {
      ""extensionUriAnchor"": 1,
      ""uri"": ""https://github.com/substrait-io/substrait/blob/main/extensions/functions_comparison.yaml""
    }
  ],
  ""extensions"": [
    {
      ""extensionFunction"": {
        ""extensionUriReference"": 1,
        ""functionAnchor"": 1,
        ""name"": ""equal:any_any""
      }
    }
  ],
  ""relations"": [
    {
      ""root"": {
        ""input"": {
          ""project"": {
            ""common"": {
              ""emit"": {
                ""outputMapping"": [
                  10,
                  11
                ]
              }
            },
            ""input"": {
              ""project"": {
                ""common"": {
                  ""emit"": {
                    ""outputMapping"": [
                      5,
                      6,
                      7,
                      8,
                      9,
                      10,
                      11,
                      12,
                      13,
                      14
                    ]
                  }
                },
                ""input"": {
                  ""join"": {
                    ""left"": {
                      ""read"": {
                        ""common"": {
                          ""direct"": {}
                        },
                        ""baseSchema"": {
                          ""names"": [
                            ""a"",
                            ""b"",
                            ""c"",
                            ""d"",
                            ""e""
                          ],
                          ""struct"": {
                            ""types"": [
                              {
                                ""string"": {
                                  ""nullability"": ""NULLABILITY_NULLABLE""
                                }
                              },
                              {
                                ""fp64"": {
                                  ""nullability"": ""NULLABILITY_NULLABLE""
                                }
                              },
                              {
                                ""i32"": {
                                  ""nullability"": ""NULLABILITY_NULLABLE""
                                }
                              },
                              {
                                ""i64"": {
                                  ""nullability"": ""NULLABILITY_NULLABLE""
                                }
                              },
                              {
                                ""i64"": {
                                  ""nullability"": ""NULLABILITY_NULLABLE""
                                }
                              }
                            ],
                            ""nullability"": ""NULLABILITY_REQUIRED""
                          }
                        },
                        ""namedTable"": {
                          ""names"": [
                            ""t""
                          ]
                        }
                      }
                    },
                    ""right"": {
                      ""read"": {
                        ""common"": {
                          ""direct"": {}
                        },
                        ""baseSchema"": {
                          ""names"": [
                            ""a"",
                            ""b"",
                            ""c"",
                            ""d"",
                            ""e""
                          ],
                          ""struct"": {
                            ""types"": [
                              {
                                ""string"": {
                                  ""nullability"": ""NULLABILITY_NULLABLE""
                                }
                              },
                              {
                                ""fp64"": {
                                  ""nullability"": ""NULLABILITY_NULLABLE""
                                }
                              },
                              {
                                ""i32"": {
                                  ""nullability"": ""NULLABILITY_NULLABLE""
                                }
                              },
                              {
                                ""i64"": {
                                  ""nullability"": ""NULLABILITY_NULLABLE""
                                }
                              },
                              {
                                ""i64"": {
                                  ""nullability"": ""NULLABILITY_NULLABLE""
                                }
                              }
                            ],
                            ""nullability"": ""NULLABILITY_REQUIRED""
                          }
                        },
                        ""namedTable"": {
                          ""names"": [
                            ""t""
                          ]
                        }
                      }
                    },
                    ""expression"": {
                      ""scalarFunction"": {
                        ""functionReference"": 1,
                        ""outputType"": {
                          ""bool"": {
                            ""nullability"": ""NULLABILITY_NULLABLE""
                          }
                        },
                        ""arguments"": [
                          {
                            ""value"": {
                              ""selection"": {
                                ""directReference"": {
                                  ""structField"": {}
                                },
                                ""rootReference"": {}
                              }
                            }
                          },
                          {
                            ""value"": {
                              ""selection"": {
                                ""directReference"": {
                                  ""structField"": {}
                                },
                                ""rootReference"": {}
                              }
                            }
                          }
                        ]
                      }
                    },
                    ""type"": ""JOIN_TYPE_LEFT""
                  }
                },
                ""expressions"": [
                  {
                    ""selection"": {
                      ""directReference"": {
                        ""structField"": {}
                      },
                      ""rootReference"": {}
                    }
                  },
                  {
                    ""selection"": {
                      ""directReference"": {
                        ""structField"": {
                          ""field"": 1
                        }
                      },
                      ""rootReference"": {}
                    }
                  },
                  {
                    ""selection"": {
                      ""directReference"": {
                        ""structField"": {
                          ""field"": 2
                        }
                      },
                      ""rootReference"": {}
                    }
                  },
                  {
                    ""selection"": {
                      ""directReference"": {
                        ""structField"": {
                          ""field"": 3
                        }
                      },
                      ""rootReference"": {}
                    }
                  },
                  {
                    ""selection"": {
                      ""directReference"": {
                        ""structField"": {
                          ""field"": 4
                        }
                      },
                      ""rootReference"": {}
                    }
                  },
                  {
                    ""selection"": {
                      ""directReference"": {
                        ""structField"": {
                          ""field"": 5
                        }
                      },
                      ""rootReference"": {}
                    }
                  },
                  {
                    ""selection"": {
                      ""directReference"": {
                        ""structField"": {
                          ""field"": 6
                        }
                      },
                      ""rootReference"": {}
                    }
                  },
                  {
                    ""selection"": {
                      ""directReference"": {
                        ""structField"": {
                          ""field"": 7
                        }
                      },
                      ""rootReference"": {}
                    }
                  },
                  {
                    ""selection"": {
                      ""directReference"": {
                        ""structField"": {
                          ""field"": 8
                        }
                      },
                      ""rootReference"": {}
                    }
                  },
                  {
                    ""selection"": {
                      ""directReference"": {
                        ""structField"": {
                          ""field"": 9
                        }
                      },
                      ""rootReference"": {}
                    }
                  }
                ]
              }
            },
            ""expressions"": [
              {
                ""selection"": {
                  ""directReference"": {
                    ""structField"": {}
                  },
                  ""rootReference"": {}
                }
              },
              {
                ""selection"": {
                  ""directReference"": {
                    ""structField"": {
                      ""field"": 1
                    }
                  },
                  ""rootReference"": {}
                }
              }
            ]
          }
        },
        ""names"": [
          ""a"",
          ""b""
        ]
      }
    }
  ],
  ""version"": {
    ""minorNumber"": 32,
    ""producer"": ""ibis-substrait""
  }
}
";
            var deserializer = new SubstraitDeserializer();
            var plan = deserializer.Deserialize(ibisOutput);
            
        }
    }
}