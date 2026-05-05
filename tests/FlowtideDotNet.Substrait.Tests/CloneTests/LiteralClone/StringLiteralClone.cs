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
    public class StringLiteralClone
    {
        private readonly StringLiteral root;

        public StringLiteralClone()
        {
            root = new StringLiteral()
            {
                Value = "hello"
            };
        }

        [Fact]
        public void CloneIsEqualToOriginal()
        {
            var clone = (StringLiteral)root.Clone();
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
            var clone = (StringLiteral)root.Clone();
            clone.Value = "world";
            Assert.NotEqual(root, clone);
        }
    }
}
