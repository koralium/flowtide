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

namespace FlowtideDotNet.Substrait.Tests.EqualityTests.RelationEquality
{
    public class TopNRelationEquality
    {
        readonly TopNRelation root;
        readonly TopNRelation clone;
        readonly TopNRelation notEqual;

        public TopNRelationEquality()
        {
            root = new TopNRelation()
            {
                Emit = new List<int> { 1, 2, 3 },
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
                Offset = 5,
                Count = 10,
                Sorts = new List<Expressions.SortField>()
                {
                    new Expressions.SortField()
                    {
                        Expression = new BoolLiteral() { Value = true },
                        SortDirection = Expressions.SortDirection.SortDirectionDescNullsFirst
                    }
                }
            };
            clone = new TopNRelation()
            {
                Emit = new List<int> { 1, 2, 3 },
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
                Offset = 5,
                Count = 10,
                Sorts = new List<Expressions.SortField>()
                {
                    new Expressions.SortField()
                    {
                        Expression = new BoolLiteral() { Value = true },
                        SortDirection = Expressions.SortDirection.SortDirectionDescNullsFirst
                    }
                }
            };
            notEqual = new TopNRelation()
            {
                Emit = new List<int> { 1, 2, 3, 4 },
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
                Offset = 7,
                Count = 13,
                Sorts = new List<Expressions.SortField>()
                {
                    new Expressions.SortField()
                    {
                        Expression = new BoolLiteral() { Value = false },
                        SortDirection = Expressions.SortDirection.SortDirectionClustered
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
        public void IsNotEqual()
        {
            Assert.NotEqual(root, notEqual);
        }

        [Fact]
        public void HashCodeIsEqual()
        {
            Assert.Equal(root.GetHashCode(), clone.GetHashCode());
        }

        [Fact]
        public void EmitChangedNotEqual()
        {
            clone.Emit = notEqual.Emit;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void InputChangedNotEqual()
        {
            clone.Input = notEqual.Input;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void OffsetChangedNotEqual()
        {
            clone.Offset = notEqual.Offset;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void CountChangedNotEqual()
        {
            clone.Count = notEqual.Count;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void SortsChangedNotEqual()
        {
            clone.Sorts = notEqual.Sorts;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void EmitNullNotEqual()
        {
            clone.Emit = null;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void EqualsOperator()
        {
            Assert.True(root == clone);
            Assert.False(root == notEqual);
        }

        [Fact]
        public void NotEqualsOperator()
        {
            Assert.False(root != clone);
            Assert.True(root != notEqual);
        }
    }
}
