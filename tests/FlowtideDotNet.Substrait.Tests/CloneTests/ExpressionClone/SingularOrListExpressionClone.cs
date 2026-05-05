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

namespace FlowtideDotNet.Substrait.Tests.CloneTests.ExpressionClone
{
    public class SingularOrListExpressionClone
    {
        private readonly SingularOrListExpression root;

        public SingularOrListExpressionClone()
        {
            root = new SingularOrListExpression()
            {
                Value = new DirectFieldReference()
                {
                    ReferenceSegment = new StructReferenceSegment() { Field = 0 }
                },
                Options = new List<Expression>
                {
                    new NumericLiteral { Value = 1 },
                    new NumericLiteral { Value = 2 }
                }
            };
        }

        [Fact]
        public void CloneIsEqualToOriginal()
        {
            var clone = (SingularOrListExpression)root.Clone();
            Assert.Equal(root, clone);
        }

        [Fact]
        public void CloneIsNotSameReference()
        {
            var clone = root.Clone();
            Assert.False(ReferenceEquals(root, clone));
        }

        [Fact]
        public void CloneOptionsListIsIndependent()
        {
            var clone = (SingularOrListExpression)root.Clone();
            clone.Options.Add(new NumericLiteral { Value = 99 });
            Assert.NotEqual(root.Options.Count, clone.Options.Count);
        }

        [Fact]
        public void CloneValueIsIndependent()
        {
            var clone = (SingularOrListExpression)root.Clone();
            clone.Value = new NumericLiteral { Value = 0 };
            Assert.NotEqual(root, clone);
        }
    }
}
