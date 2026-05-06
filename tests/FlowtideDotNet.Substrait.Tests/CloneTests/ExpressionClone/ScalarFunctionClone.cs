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
    public class ScalarFunctionClone
    {
        private readonly ScalarFunction root;

        public ScalarFunctionClone()
        {
            root = new ScalarFunction()
            {
                ExtensionUri = "http://example.com",
                ExtensionName = "myFunc",
                Arguments = new List<Expression>
                {
                    new StringLiteral { Value = "arg1" },
                    new NumericLiteral { Value = 42 }
                },
                Options = new SortedList<string, string> { { "key", "value" } }
            };
        }

        [Fact]
        public void CloneIsEqualToOriginal()
        {
            var clone = (ScalarFunction)root.Clone();
            Assert.Equal(root, clone);
        }

        [Fact]
        public void CloneIsNotSameReference()
        {
            var clone = root.Clone();
            Assert.False(ReferenceEquals(root, clone));
        }

        [Fact]
        public void CloneArgumentsListIsIndependent()
        {
            var clone = (ScalarFunction)root.Clone();
            clone.Arguments.Add(new StringLiteral { Value = "extra" });
            Assert.NotEqual(root.Arguments.Count, clone.Arguments.Count);
        }
    }
}
