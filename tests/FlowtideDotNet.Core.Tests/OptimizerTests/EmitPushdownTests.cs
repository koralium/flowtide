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

using FlowtideDotNet.Core.Optimizer.EmitPushdown;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Tests.OptimizerTests
{
    public class EmitPushdownTests
    {
        [Fact]
        public void ReadRelationProjection()
        {
            var plan = new Plan()
            {
                Relations = new List<Substrait.Relations.Relation>()
                {
                    new ProjectRelation()
                    {
                        Emit = new List<int>(){0, 1, 3, 5, 6},
                        Expressions = new List<Substrait.Expressions.Expression>()
                        {
                            new DirectFieldReference()
                            {
                                ReferenceSegment = new StructReferenceSegment()
                                {
                                    Field = 0
                                }
                            }
                        },
                        Input = new ReadRelation()
                        {
                            NamedTable = new Substrait.Type.NamedTable(){ Names = new List<string>() { "Table1" } },
                            BaseSchema = new Substrait.Type.NamedStruct()
                            {
                                Names = new List<string>() { "Col1", "Col2", "Col3", "Col4", "Col5", "Col6" },
                                Struct = new Substrait.Type.Struct()
                                {
                                    Types = new List<Substrait.Type.SubstraitBaseType>()
                                    {
                                        new AnyType(),
                                        new AnyType(),
                                        new AnyType(),
                                        new AnyType(),
                                        new AnyType(),
                                        new AnyType()
                                    }
                                }
                            }
                        }
                    }
                }
            };

            var optimizedPlan = EmitPushdown.Optimize(plan);

            optimizedPlan.Should()
                .BeEquivalentTo(
                    new Plan()
                    {
                        Relations = new List<Substrait.Relations.Relation>()
                    {
                        new ProjectRelation()
                        {
                            Emit = new List<int>(){0, 1, 2, 3, 4},
                            Expressions = new List<Substrait.Expressions.Expression>()
                            {
                                new DirectFieldReference()
                                {
                                    ReferenceSegment = new StructReferenceSegment()
                                    {
                                        Field = 0
                                    }
                                }
                            },
                            Input = new ReadRelation()
                            {
                                Emit = new List<int>(){ 0, 1, 2, 3},
                                NamedTable = new Substrait.Type.NamedTable(){ Names = new List<string>() { "Table1" } },
                                BaseSchema = new Substrait.Type.NamedStruct()
                                {
                                    Names = new List<string>() { "Col1", "Col2", "Col4", "Col6" },
                                    Struct = new Substrait.Type.Struct()
                                    {
                                        Types = new List<Substrait.Type.SubstraitBaseType>()
                                        {
                                            new AnyType(),
                                            new AnyType(),
                                            new AnyType(),
                                            new AnyType()
                                        }
                                    }
                                }
                            }
                        }
                    }
                    }, opt => opt.AllowingInfiniteRecursion().IncludingNestedObjects().ThrowingOnMissingMembers().RespectingRuntimeTypes()
                );
        }
    }
}
