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
    public class JoinRelationEquality
    {
        readonly JoinRelation root;
        readonly JoinRelation clone;
        readonly JoinRelation notEqual;

        public JoinRelationEquality()
        {
            root = new JoinRelation()
            {
                Emit = new List<int> { 1, 2, 3 },
                Expression = new BoolLiteral() { Value = true },
                Left = new ReadRelation()
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
                Right = new ReadRelation()
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
                PostJoinFilter = new BoolLiteral() { Value = true },
                Type = JoinType.Left
            };
            clone = new JoinRelation()
            {
                Emit = new List<int> { 1, 2, 3 },
                Expression = new BoolLiteral() { Value = true },
                Left = new ReadRelation()
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
                Right = new ReadRelation()
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
                PostJoinFilter = new BoolLiteral() { Value = true },
                Type = JoinType.Left
            };
            notEqual = new JoinRelation()
            {
                Emit = new List<int> { 1, 2, 3, 4 },
                Expression = new BoolLiteral() { Value = false },
                Left = new ReadRelation()
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
                Right = new ReadRelation()
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
                PostJoinFilter = new BoolLiteral() { Value = false },
                Type = JoinType.Inner
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
        public void ExpressionChangedNotEqual()
        {
            clone.Expression = notEqual.Expression;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void LeftChangedNotEqual()
        {
            clone.Left = notEqual.Left;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void RightChangedNotEqual()
        {
            clone.Right = notEqual.Right;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void PostJoinFilterChangedNotEqual()
        {
            clone.PostJoinFilter = notEqual.PostJoinFilter;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void TypeChangedNotEqual()
        {
            clone.Type = notEqual.Type;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void EmitNullNotEqual()
        {
            clone.Emit = null;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void ExpressionNullNotEqual()
        {
            clone.Expression = null;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void PostJoinFilterNullNotEqual()
        {
            clone.PostJoinFilter = null;
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
            Assert.True(root != notEqual);
            Assert.False(root != clone);
        }
    }
}
