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

using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Substrait.Tests.EqualityTests.ExpressionEquality
{
    public class CastEquality
    {
        private CastExpression root;
        private CastExpression clone;
        private CastExpression notEqual;

        public CastEquality()
        {
            root = new CastExpression()
            {
                Type = new Fp32Type(),
                Expression = new NumericLiteral()
                {
                    Value = 1
                }
            };
            clone = new CastExpression()
            {
                Type = new Fp32Type(),
                Expression = new NumericLiteral()
                {
                    Value = 1
                }
            };
            notEqual = new CastExpression()
            {
                Type = new Fp64Type(),
                Expression = new NumericLiteral()
                {
                    Value = 3
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
        public void TypeChangedIsNotEqual()
        {
            clone.Type = notEqual.Type;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void ExpressionChangedIsNotEqual()
        {
            clone.Expression = notEqual.Expression;
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
