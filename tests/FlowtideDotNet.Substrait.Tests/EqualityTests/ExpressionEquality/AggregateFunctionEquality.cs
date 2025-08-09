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

namespace FlowtideDotNet.Substrait.Tests.EqualityTests.ExpressionEquality
{
    public class AggregateFunctionEquality
    {
        private AggregateFunction root;
        private AggregateFunction clone;
        private AggregateFunction notEqual;

        public AggregateFunctionEquality()
        {
            root = new AggregateFunction()
            {
                ExtensionUri = "testuri",
                ExtensionName = "func1",
                Arguments = new List<Expression>()
                {
                    new StringLiteral() { Value = "test" }
                }
            };
            clone = new AggregateFunction()
            {
                ExtensionUri = "testuri",
                ExtensionName = "func1",
                Arguments = new List<Expression>()
                {
                    new StringLiteral() { Value = "test" }
                }
            };
            notEqual = new AggregateFunction()
            {
                ExtensionUri = "testuri2",
                ExtensionName = "func2",
                Arguments = new List<Expression>()
                {
                    new StringLiteral() { Value = "test2" }
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
        public void ExtensionUriChangedIsNotEqual()
        {
            clone.ExtensionUri = notEqual.ExtensionUri;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void ExtensionNameChangedIsNotEqual()
        {
            clone.ExtensionName = notEqual.ExtensionName;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void ArgumentsChangedIsNotEqual()
        {
            clone.Arguments = notEqual.Arguments;
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
