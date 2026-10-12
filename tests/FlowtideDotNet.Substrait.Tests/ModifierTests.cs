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

using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;

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
            // Ignore obsolete warning since the test is checking for the obsolete method
#pragma warning disable CS0618 // Type or member is obsolete
            planModifier.WriteToTable("output");
#pragma warning restore CS0618 // Type or member is obsolete
            var modifiedPlan = planModifier.Modify();

        }

        [Fact]
        public void CheckRelationInputIsModified()
        {
            var view = new Plan()
            {
                Relations = new List<Relation>()
                {
                    new RootRelation()
                    {
                        Names = new List<string>() { "a" },
                        Input = new ReadRelation()
                        {
                            BaseSchema = new NamedStruct() { Names = new List<string>() { "a" } },
                            NamedTable = new NamedTable() { Names = new List<string>() { "basetable" } }
                        }
                    }
                }
            };
            var root = new Plan()
            {
                Relations = new List<Relation>()
                {
                    new RootRelation()
                    {
                        Names = new List<string>() { "a" },
                        Input = new CheckRelation()
                        {
                            Input = new ReadRelation()
                            {
                                BaseSchema = new NamedStruct() { Names = new List<string>() { "a" } },
                                NamedTable = new NamedTable() { Names = new List<string>() { "viewtable" } }
                            },
                            Checks = new List<CheckDefinition>()
                            {
                                new CheckDefinition()
                                {
                                    Condition = new BoolLiteral() { Value = true },
                                    Message = "failed",
                                    Tags = new List<CheckTag>(),
                                    Guards = new List<CheckGuard>()
                                }
                            }
                        }
                    }
                }
            };

            var modifiedPlan = new PlanModifier()
                .AddPlanAsView("viewtable", view)
                .AddRootPlan(root)
                .Modify();

            var check = Assert.IsType<CheckRelation>(modifiedPlan.Relations[1]);
            var reference = Assert.IsType<ReferenceRelation>(check.Input);
            Assert.Equal(0, reference.RelationId);
        }
    }
}
