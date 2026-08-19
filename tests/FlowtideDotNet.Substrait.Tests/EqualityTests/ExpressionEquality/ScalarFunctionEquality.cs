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
    public class ScalarFunctionEquality
    {
        readonly ScalarFunction root;
        readonly ScalarFunction clone;
        readonly ScalarFunction notEqual;

        public ScalarFunctionEquality()
        {
            root = new ScalarFunction()
            {
                ExtensionUri = "testuri1",
                ExtensionName = "testname1",
                Arguments = new List<Expression>()
                {
                    new StringLiteral() { Value = "test1" }
                }
            };
            clone = new ScalarFunction()
            {
                ExtensionUri = "testuri1",
                ExtensionName = "testname1",
                Arguments = new List<Expression>()
                {
                    new StringLiteral() { Value = "test1" }
                }
            };
            notEqual = new ScalarFunction()
            {
                ExtensionUri = "testuri2",
                ExtensionName = "testname2",
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
        public void ExtensionUriChangedNotEqual()
        {
            clone.ExtensionUri = notEqual.ExtensionUri;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void ExtensionNameChangedNotEqual()
        {
            clone.ExtensionName = notEqual.ExtensionName;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void ArgumentsChangedNotEqual()
        {
            clone.Arguments = notEqual.Arguments;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void OptionsChangedNotEqual()
        {
            // Options change how the function behaves, substring uses negative_start
            root.Options = new SortedList<string, string>() { { "negative_start", "WRAP_FROM_END" } };
            clone.Options = new SortedList<string, string>() { { "negative_start", "LEFT_OF_BEGINNING" } };
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void OptionsAddedNotEqual()
        {
            clone.Options = new SortedList<string, string>() { { "negative_start", "WRAP_FROM_END" } };
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void SameOptionsIsEqual()
        {
            root.Options = new SortedList<string, string>() { { "negative_start", "WRAP_FROM_END" } };
            clone.Options = new SortedList<string, string>() { { "negative_start", "WRAP_FROM_END" } };
            Assert.Equal(root, clone);
            Assert.Equal(root.GetHashCode(), clone.GetHashCode());
        }

        [Fact]
        public void EmptyOptionsEqualsNullOptions()
        {
            clone.Options = new SortedList<string, string>();
            Assert.Equal(root, clone);
            Assert.Equal(root.GetHashCode(), clone.GetHashCode());
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
