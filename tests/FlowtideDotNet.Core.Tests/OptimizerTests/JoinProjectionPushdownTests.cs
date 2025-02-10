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

using FlowtideDotNet.Core.Optimizer.JoinProjectionPushdown;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Core.Tests.OptimizerTests
{
    public class JoinProjectionPushdownTests
    {
        [Fact]
        public void TestPushdownTrimOnBothSides()
        {
            var plan = new Plan()
            {
                Relations = new List<Substrait.Relations.Relation>()
                {
                    new JoinRelation()
                    {
                        Type = JoinType.Inner,
                        Expression = new ScalarFunction()
                        {
                            ExtensionUri = FunctionsComparison.Uri,
                            ExtensionName = FunctionsComparison.Equal,
                            Arguments = new List<Expression>()
                            {
                                new ScalarFunction()
                                {
                                    ExtensionUri = FunctionsString.Uri,
                                    ExtensionName = FunctionsString.Trim,
                                    Arguments = new List<Expression>()
                                    {
                                        new DirectFieldReference()
                                        {
                                            ReferenceSegment = new StructReferenceSegment()
                                            {
                                                Field = 0
                                            }
                                        }
                                    }
                                },
                                new ScalarFunction()
                                {
                                    ExtensionUri = FunctionsString.Uri,
                                    ExtensionName = FunctionsString.Trim,
                                    Arguments = new List<Expression>()
                                    {
                                        new DirectFieldReference()
                                        {
                                            ReferenceSegment = new StructReferenceSegment()
                                            {
                                                Field = 1
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        Left = new ReadRelation()
                        {
                            BaseSchema = new Substrait.Type.NamedStruct()
                            {
                                Names = new List<string>(){ "a" },
                                Struct = new Substrait.Type.Struct() { Types = new List<SubstraitBaseType>() { new AnyType() } }
                            },
                            NamedTable = new Substrait.Type.NamedTable
                            {
                                Names = new List<string>(){ "a" }
                            }
                        },
                        Right = new ReadRelation()
                        {
                            BaseSchema = new Substrait.Type.NamedStruct()
                            {
                                Names = new List<string>(){ "a" },
                                Struct = new Substrait.Type.Struct() { Types = new List<SubstraitBaseType>() { new AnyType() } }
                            },
                            NamedTable = new Substrait.Type.NamedTable
                            {
                                Names = new List<string>(){ "b" }
                            }
                        },
                    }
                }
            };

            plan = JoinProjectionPushdown.Optimize(plan);

            var expected = new Plan()
            {
                Relations = new List<Substrait.Relations.Relation>()
                {
                    new JoinRelation()
                    {
                        Emit = new List<int>() { 0, 2 },
                        Type = JoinType.Inner,
                        Expression = new ScalarFunction()
                        {
                            ExtensionUri = FunctionsComparison.Uri,
                            ExtensionName = FunctionsComparison.Equal,
                            Arguments = new List<Expression>()
                            {
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = 1
                                    }
                                },
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = 3
                                    }
                                }
                            }
                        },
                        Left = new ProjectRelation()
                        {
                            Expressions = new List<Expression>()
                            {
                                new ScalarFunction()
                                {
                                    ExtensionUri = FunctionsString.Uri,
                                    ExtensionName = FunctionsString.Trim,
                                    Arguments = new List<Expression>()
                                    {
                                        new DirectFieldReference()
                                        {
                                            ReferenceSegment = new StructReferenceSegment()
                                            {
                                                Field = 0
                                            }
                                        }
                                    }
                                }
                            },
                            Input = new ReadRelation()
                            {
                                BaseSchema = new Substrait.Type.NamedStruct()
                                {
                                    Names = new List<string>(){ "a" },
                                    Struct = new Substrait.Type.Struct() { Types = new List<SubstraitBaseType>() { new AnyType() } }
                                },
                                NamedTable = new Substrait.Type.NamedTable
                                {
                                    Names = new List<string>(){ "a" }
                                }
                            }
                        },
                        Right = new ProjectRelation()
                        {
                            Expressions = new List<Expression>()
                            {
                                new ScalarFunction()
                                {
                                    ExtensionUri = FunctionsString.Uri,
                                    ExtensionName = FunctionsString.Trim,
                                    Arguments = new List<Expression>()
                                    {
                                        new DirectFieldReference()
                                        {
                                            ReferenceSegment = new StructReferenceSegment()
                                            {
                                                Field = 0
                                            }
                                        }
                                    }
                                }
                            },
                            Input = new ReadRelation()
                            {
                                BaseSchema = new Substrait.Type.NamedStruct()
                                {
                                    Names = new List<string>(){ "a" },
                                    Struct = new Substrait.Type.Struct() { Types = new List<SubstraitBaseType>() { new AnyType() } }
                                },
                                NamedTable = new Substrait.Type.NamedTable
                                {
                                    Names = new List<string>(){ "b" }
                                }
                            }
                        }
                    }
                }
            };

            Assert.Equal(expected, plan);
        }
    }
}
