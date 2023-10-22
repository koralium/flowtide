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
    public class ModifierTests
    {
        [Fact]
        public void TestPlanAsView()
        {
            var subPlan = @"
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
          ""filter"": {
            ""input"": {
              ""read"": {
                ""common"": {
                  ""direct"": {}
                },
                ""baseSchema"": {
                  ""names"": [
                    ""a""
                  ],
                  ""struct"": {
                    ""types"": [
                      {
                        ""string"": {
                          ""nullability"": ""NULLABILITY_NULLABLE""
                        }
                      }
                    ],
                    ""nullability"": ""NULLABILITY_REQUIRED""
                  }
                },
                ""namedTable"": {
                  ""names"": [
                    ""normaltable""
                  ]
                }
              }
            },
            ""condition"": {
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
                      ""literal"": {
                        ""string"": ""123""
                      }
                    }
                  }
                ]
              }
            }
          }
        },
        ""names"": [
          ""a""
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

            var rootPlan = @"
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
          ""join"": {
            ""left"": {
              ""read"": {
                ""common"": {
                  ""direct"": {}
                },
                ""baseSchema"": {
                  ""names"": [
                    ""b""
                  ],
                  ""struct"": {
                    ""types"": [
                      {
                        ""string"": {
                          ""nullability"": ""NULLABILITY_NULLABLE""
                        }
                      }
                    ],
                    ""nullability"": ""NULLABILITY_REQUIRED""
                  }
                },
                ""namedTable"": {
                  ""names"": [
                    ""roottable""
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
                    ""a""
                  ],
                  ""struct"": {
                    ""types"": [
                      {
                        ""string"": {
                          ""nullability"": ""NULLABILITY_NULLABLE""
                        }
                      }
                    ],
                    ""nullability"": ""NULLABILITY_REQUIRED""
                  }
                },
                ""namedTable"": {
                  ""names"": [
                    ""viewtable""
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
                          ""structField"": {
                            ""field"": 1
                          }
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
        ""names"": [
          ""b"",
          ""a""
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
            var sub = deserializer.Deserialize(subPlan);
            var root = deserializer.Deserialize(rootPlan);
            PlanModifier planModifier = new PlanModifier();
            planModifier.AddPlanAsView("viewtable", sub);
            planModifier.AddRootPlan(root);
            planModifier.WriteToTable("output");
            var modifiedPlan = planModifier.Modify();

        }
    }
}
