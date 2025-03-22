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

using FlowtideDotNet.Substrait.Expressions.IfThen;
using FlowtideDotNet.Substrait.Expressions.Literals;

namespace FlowtideDotNet.Substrait.Tests.EqualityTests.ExpressionEquality
{
    public class IfThenEquality
    {
        IfThenExpression root;
        IfThenExpression clone;
        IfThenExpression notEqual;

        public IfThenEquality()
        {
            root = new IfThenExpression()
            {
                Ifs = new List<IfClause>()
                {
                    new IfClause()
                    {
                        If = new BoolLiteral()
                        {
                            Value = true
                        },
                        Then = new BoolLiteral()
                        {
                            Value = true
                        }
                    }
                },
                Else = new BoolLiteral()
                {
                    Value = false
                }
            };

            clone = new IfThenExpression()
            {
                Ifs = new List<IfClause>()
                {
                    new IfClause()
                    {
                        If = new BoolLiteral()
                        {
                            Value = true
                        },
                        Then = new BoolLiteral()
                        {
                            Value = true
                        }
                    }
                },
                Else = new BoolLiteral()
                {
                    Value = false
                }
            };

            notEqual = new IfThenExpression()
            {
                Ifs = new List<IfClause>()
                {
                    new IfClause()
                    {
                        If = new BoolLiteral()
                        {
                            Value = true
                        },
                        Then = new BoolLiteral()
                        {
                            Value = true
                        }
                    },
                    new IfClause()
                    {
                        If = new BoolLiteral()
                        {
                            Value = false
                        },
                        Then = new BoolLiteral()
                        {
                            Value = true
                        }
                    }
                },
                Else = new BoolLiteral()
                {
                    Value = true
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
        public void IfsChangedNotEqual()
        {
            clone.Ifs = notEqual.Ifs;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void ElseChangedNotEqual()
        {
            clone.Else = notEqual.Else;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void ElseNullNotEqual()
        {
            clone.Else = null;
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
