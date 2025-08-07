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

using FlowtideDotNet.Core.Optimizer.WatermarkOutput;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Tests.OptimizerTests
{
    public class WatermarkOutputTests
    {
        [Fact]
        public void WatermarkOutputWithViewSingleReference()
        {
            var plan = new Plan()
            {
                Relations = new List<Substrait.Relations.Relation>()
                {
                    new ReadRelation()
                    {
                        BaseSchema = new Substrait.Type.NamedStruct()
                        {
                            Names = new List<string> { "a" },
                        },
                        NamedTable = new Substrait.Type.NamedTable()
                        {
                            Names = new List<string> { "test_source" },
                        }
                    },
                    new WriteRelation()
                    {
                        Input = new ReferenceRelation()
                        {
                            RelationId = 0
                        },
                        NamedObject = new Substrait.Type.NamedTable()
                        {
                            Names = new List<string> { "test_dest" }
                        },
                        TableSchema = new Substrait.Type.NamedStruct()
                        {
                            Names = new List<string> { "a" }
                        }
                    }
                }
            };

            plan = WatermarkOutputOptimizer.Optimize(plan);

            var expected = new Plan()
            {
                Relations = new List<Substrait.Relations.Relation>()
                {
                    new ReadRelation()
                    {
                        BaseSchema = new Substrait.Type.NamedStruct()
                        {
                            Names = new List<string> { "a" },
                        },
                        NamedTable = new Substrait.Type.NamedTable()
                        {
                            Names = new List<string> { "test_source" },
                        },
                        Hint = new Substrait.Hints.Hint()
                        {
                            Optimizations = new Substrait.Hints.HintOptimizations()
                            {
                                Properties = new Dictionary<string, string>()
                                {
                                    { "WATERMARK_OUTPUT_MODE", "ON_EACH_BATCH" }
                                }
                            }
                        }
                    },
                    new WriteRelation()
                    {
                        Input = new ReferenceRelation()
                        {
                            RelationId = 0
                        },
                        NamedObject = new Substrait.Type.NamedTable()
                        {
                            Names = new List<string> { "test_dest" }
                        },
                        TableSchema = new Substrait.Type.NamedStruct()
                        {
                            Names = new List<string> { "a" }
                        }
                    }
                }
            };

            Assert.Equal(expected, plan);
        }

        [Fact]
        public void WatermarkOutputWithViewReferenceChain()
        {
            var plan = new Plan()
            {
                Relations = new List<Substrait.Relations.Relation>()
                {
                    new ReadRelation()
                    {
                        BaseSchema = new Substrait.Type.NamedStruct()
                        {
                            Names = new List<string> { "a" },
                        },
                        NamedTable = new Substrait.Type.NamedTable()
                        {
                            Names = new List<string> { "test_source" },
                        }
                    },
                    new ReferenceRelation()
                    {
                        RelationId = 0
                    },
                    new WriteRelation()
                    {
                        Input = new ReferenceRelation()
                        {
                            RelationId = 1
                        },
                        NamedObject = new Substrait.Type.NamedTable()
                        {
                            Names = new List<string> { "test_dest" }
                        },
                        TableSchema = new Substrait.Type.NamedStruct()
                        {
                            Names = new List<string> { "a" }
                        }
                    }
                }
            };

            plan = WatermarkOutputOptimizer.Optimize(plan);

            var expected = new Plan()
            {
                Relations = new List<Substrait.Relations.Relation>()
                {
                    new ReadRelation()
                    {
                        BaseSchema = new Substrait.Type.NamedStruct()
                        {
                            Names = new List<string> { "a" },
                        },
                        NamedTable = new Substrait.Type.NamedTable()
                        {
                            Names = new List<string> { "test_source" },
                        },
                        Hint = new Substrait.Hints.Hint()
                        {
                            Optimizations = new Substrait.Hints.HintOptimizations()
                            {
                                Properties = new Dictionary<string, string>()
                                {
                                    { "WATERMARK_OUTPUT_MODE", "ON_EACH_BATCH" }
                                }
                            }
                        }
                    },
                    new ReferenceRelation()
                    {
                        RelationId = 0
                    },
                    new WriteRelation()
                    {
                        Input = new ReferenceRelation()
                        {
                            RelationId = 1
                        },
                        NamedObject = new Substrait.Type.NamedTable()
                        {
                            Names = new List<string> { "test_dest" }
                        },
                        TableSchema = new Substrait.Type.NamedStruct()
                        {
                            Names = new List<string> { "a" }
                        }
                    }
                }
            };

            Assert.Equal(expected, plan);
        }

        [Fact]
        public void WatermarkOutputWithViewReferenceChainAndUnionAll()
        {
            var plan = new Plan()
            {
                Relations = new List<Substrait.Relations.Relation>()
                {
                    new ReadRelation()
                    {
                        BaseSchema = new Substrait.Type.NamedStruct()
                        {
                            Names = new List<string> { "a" },
                        },
                        NamedTable = new Substrait.Type.NamedTable()
                        {
                            Names = new List<string> { "test_source" },
                        }
                    },
                    new ReferenceRelation()
                    {
                        RelationId = 0
                    },
                    new WriteRelation()
                    {
                        Input = new SetRelation()
                        {
                            Operation = SetOperation.UnionAll,
                            Inputs = new List<Relation>()
                            {
                                new ReferenceRelation()
                                {
                                    RelationId = 0
                                },
                                new ReferenceRelation()
                                {
                                    RelationId = 1
                                }
                            }
                        },
                        NamedObject = new Substrait.Type.NamedTable()
                        {
                            Names = new List<string> { "test_dest" }
                        },
                        TableSchema = new Substrait.Type.NamedStruct()
                        {
                            Names = new List<string> { "a" }
                        }
                    }
                }
            };

            plan = WatermarkOutputOptimizer.Optimize(plan);

            var expected = new Plan()
            {
                Relations = new List<Substrait.Relations.Relation>()
                {
                    new ReadRelation()
                    {
                        BaseSchema = new Substrait.Type.NamedStruct()
                        {
                            Names = new List<string> { "a" },
                        },
                        NamedTable = new Substrait.Type.NamedTable()
                        {
                            Names = new List<string> { "test_source" },
                        },
                        Hint = new Substrait.Hints.Hint()
                        {
                            Optimizations = new Substrait.Hints.HintOptimizations()
                            {
                                Properties = new Dictionary<string, string>()
                                {
                                    { "WATERMARK_OUTPUT_MODE", "ON_EACH_BATCH" }
                                }
                            }
                        }
                    },
                    new ReferenceRelation()
                    {
                        RelationId = 0
                    },
                    new WriteRelation()
                    {
                        Input = new SetRelation()
                        {
                            Operation = SetOperation.UnionAll,
                            Inputs = new List<Relation>()
                            {
                                new ReferenceRelation()
                                {
                                    RelationId = 0
                                },
                                new ReferenceRelation()
                                {
                                    RelationId = 1
                                }
                            }
                        },
                        NamedObject = new Substrait.Type.NamedTable()
                        {
                            Names = new List<string> { "test_dest" }
                        },
                        TableSchema = new Substrait.Type.NamedStruct()
                        {
                            Names = new List<string> { "a" }
                        }
                    }
                }
            };

            Assert.Equal(expected, plan);
        }

        /// <summary>
        /// Tests that the optimizer does not apply watermark output optimization when there is an aggregate relation in the plan.
        /// </summary>
        [Fact]
        public void WatermarkOutputWithViewAndAggregate()
        {
            var plan = new Plan()
            {
                Relations = new List<Substrait.Relations.Relation>()
                {
                    new ReadRelation()
                    {
                        BaseSchema = new Substrait.Type.NamedStruct()
                        {
                            Names = new List<string> { "a" },
                        },
                        NamedTable = new Substrait.Type.NamedTable()
                        {
                            Names = new List<string> { "test_source" },
                        }
                    },
                    new AggregateRelation()
                    {
                        Input = new ReferenceRelation()
                        {
                            RelationId = 0
                        },
                    },
                    new WriteRelation()
                    {
                        Input = new SetRelation()
                        {
                            Inputs = new List<Relation>()
                            {
                                new ReferenceRelation()
                                {
                                    RelationId = 0
                                },
                                new ReferenceRelation()
                                {
                                    RelationId = 1
                                }
                            }
                        },
                        NamedObject = new Substrait.Type.NamedTable()
                        {
                            Names = new List<string> { "test_dest" }
                        },
                        TableSchema = new Substrait.Type.NamedStruct()
                        {
                            Names = new List<string> { "a" }
                        }
                    }
                }
            };

            plan = WatermarkOutputOptimizer.Optimize(plan);

            var expected = new Plan()
            {
                Relations = new List<Substrait.Relations.Relation>()
                {
                    new ReadRelation()
                    {
                        BaseSchema = new Substrait.Type.NamedStruct()
                        {
                            Names = new List<string> { "a" },
                        },
                        NamedTable = new Substrait.Type.NamedTable()
                        {
                            Names = new List<string> { "test_source" },
                        }
                    },
                    new AggregateRelation()
                    {
                        Input = new ReferenceRelation()
                        {
                            RelationId = 0
                        },
                    },
                    new WriteRelation()
                    {
                        Input = new SetRelation()
                        {
                            Inputs = new List<Relation>()
                            {
                                new ReferenceRelation()
                                {
                                    RelationId = 0
                                },
                                new ReferenceRelation()
                                {
                                    RelationId = 1
                                }
                            }
                        },
                        NamedObject = new Substrait.Type.NamedTable()
                        {
                            Names = new List<string> { "test_dest" }
                        },
                        TableSchema = new Substrait.Type.NamedStruct()
                        {
                            Names = new List<string> { "a" }
                        }
                    }
                }
            };

            Assert.Equal(expected, plan);
        }
    }
}
