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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Substrait.Tests.EqualityTests.RelationEquality
{
    public class AggregateRelationEquality
    {
        readonly AggregateRelation root;
        readonly AggregateRelation clone;
        readonly AggregateRelation notEqual;

        public AggregateRelationEquality()
        {
            root = new AggregateRelation()
            {
                Input = new ReadRelation()
                {
                    BaseSchema = new Type.NamedStruct()
                    {
                        Names = new List<string>() { "c1" },
                    },
                    NamedTable = new Type.NamedTable
                    {
                        Names = new List<string>() { "t1" }
                    }
                },
                Emit = new List<int>() { 1 },
                Groupings = new List<AggregateGrouping>()
                {
                    new AggregateGrouping()
                    {
                        GroupingExpressions = new List<Expressions.Expression>()
                        {
                            new StringLiteral() { Value = "g1" }
                        }
                    }
                },
                Measures = new List<AggregateMeasure>()
                {
                    new AggregateMeasure()
                    {
                        Measure = new Expressions.AggregateFunction()
                        {
                            ExtensionUri = "uri",
                            ExtensionName = "name",
                            Arguments = new List<Expressions.Expression>()
                            {
                                new StringLiteral() { Value = "test1" }
                            }
                        }
                    }
                }
            };
            clone = new AggregateRelation()
            {
                Input = new ReadRelation()
                {
                    BaseSchema = new Type.NamedStruct()
                    {
                        Names = new List<string>() { "c1" },
                    },
                    NamedTable = new Type.NamedTable
                    {
                        Names = new List<string>() { "t1" }
                    }
                },
                Emit = new List<int>() { 1 },
                Groupings = new List<AggregateGrouping>()
                {
                    new AggregateGrouping()
                    {
                        GroupingExpressions = new List<Expressions.Expression>()
                        {
                            new StringLiteral() { Value = "g1" }
                        }
                    }
                },
                Measures = new List<AggregateMeasure>()
                {
                    new AggregateMeasure()
                    {
                        Measure = new Expressions.AggregateFunction()
                        {
                            ExtensionUri = "uri",
                            ExtensionName = "name",
                            Arguments = new List<Expressions.Expression>()
                            {
                                new StringLiteral() { Value = "test1" }
                            }
                        }
                    }
                }
            };
            notEqual = new AggregateRelation()
            {
                Input = new ReadRelation()
                {
                    BaseSchema = new Type.NamedStruct()
                    {
                        Names = new List<string>() { "c2" },
                    },
                    NamedTable = new Type.NamedTable
                    {
                        Names = new List<string>() { "t2" }
                    }
                },
                Emit = new List<int>() { 2 },
                Groupings = new List<AggregateGrouping>()
                {
                    new AggregateGrouping()
                    {
                        GroupingExpressions = new List<Expressions.Expression>()
                        {
                            new StringLiteral() { Value = "g2" }
                        }
                    }
                },
                Measures = new List<AggregateMeasure>()
                {
                    new AggregateMeasure()
                    {
                        Measure = new Expressions.AggregateFunction()
                        {
                            ExtensionUri = "uri2",
                            ExtensionName = "name2",
                            Arguments = new List<Expressions.Expression>()
                            {
                                new StringLiteral() { Value = "test2" }
                            }
                        }
                    }
                }
            };
        }

        [Fact]
        public void IsEqual()
        {
            Assert.Equal(root, clone);
        }

        [Fact]
        public void HashCodeIsEqual()
        {
            Assert.Equal(root.GetHashCode(), clone.GetHashCode());
        }

        [Fact]
        public void IsNotEqual()
        {
            Assert.NotEqual(root, notEqual);
        }

        [Fact]
        public void EmitChangedNotEqual()
        {
            clone.Emit = notEqual.Emit;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void GroupingsChangedNotEqual()
        {
            clone.Groupings = notEqual.Groupings;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void MeasuresChangedNotEqual()
        {
            clone.Measures = notEqual.Measures;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void InputChangedNotEqual()
        {
            clone.Input = notEqual.Input;
            Assert.NotEqual(root, clone);
        }
    }
}
