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

namespace FlowtideDotNet.Substrait.Tests.CloneTests.LiteralClone
{
    public class NumericLiteralClone
    {
        private readonly NumericLiteral root;

        public NumericLiteralClone()
        {
            root = new NumericLiteral()
            {
                Value = 42
            };
        }

        [Fact]
        public void CloneIsEqualToOriginal()
        {
            var clone = (NumericLiteral)root.Clone();
            Assert.Equal(root, clone);
        }

        [Fact]
        public void CloneIsNotSameReference()
        {
            var clone = root.Clone();
            Assert.False(ReferenceEquals(root, clone));
        }

        [Fact]
        public void CloneValueIsIndependent()
        {
            var clone = (NumericLiteral)root.Clone();
            clone.Value = 99;
            Assert.NotEqual(root, clone);
        }
    }
}
