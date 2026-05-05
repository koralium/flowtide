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

namespace FlowtideDotNet.Substrait.Tests.CloneTests.LiteralClone
{
    public class ArrayLiteralClone
    {
        private ArrayLiteral root;

        public ArrayLiteralClone()
        {
            root = new ArrayLiteral()
            {
                Expressions = new List<Expression>
                {
                    new StringLiteral { Value = "a" },
                    new NumericLiteral { Value = 1 }
                }
            };
        }

        [Fact]
        public void CloneIsEqualToOriginal()
        {
            var clone = (ArrayLiteral)root.Clone();
            Assert.Equal(root, clone);
        }

        [Fact]
        public void CloneIsNotSameReference()
        {
            var clone = root.Clone();
            Assert.False(ReferenceEquals(root, clone));
        }

        [Fact]
        public void CloneExpressionsListIsIndependent()
        {
            var clone = (ArrayLiteral)root.Clone();
            clone.Expressions.Add(new StringLiteral { Value = "extra" });
            Assert.NotEqual(root.Expressions.Count, clone.Expressions.Count);
        }
    }
}
