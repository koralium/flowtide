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

namespace FlowtideDotNet.Substrait.Tests.CloneTests.ExpressionClone
{
    public class CastExpressionClone
    {
        private CastExpression root;

        public CastExpressionClone()
        {
            root = new CastExpression()
            {
                Expression = new StringLiteral { Value = "hello" },
                Type = new Fp32Type()
            };
        }

        [Fact]
        public void CloneIsEqualToOriginal()
        {
            var clone = (CastExpression)root.Clone();
            Assert.Equal(root, clone);
        }

        [Fact]
        public void CloneIsNotSameReference()
        {
            var clone = root.Clone();
            Assert.False(ReferenceEquals(root, clone));
        }

        [Fact]
        public void CloneExpressionIsIndependent()
        {
            var clone = (CastExpression)root.Clone();
            clone.Expression = new StringLiteral { Value = "other" };
            Assert.NotEqual(root, clone);
        }
    }
}
