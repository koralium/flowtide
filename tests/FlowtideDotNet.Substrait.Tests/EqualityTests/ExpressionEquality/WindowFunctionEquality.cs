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
    public class WindowFunctionEquality
    {
        readonly WindowFunction root;
        readonly WindowFunction clone;
        readonly WindowFunction notEqual;

        private static WindowFunction Create(string uri, string name, string argument, string optionValue, long lowerOffset)
        {
            return new WindowFunction()
            {
                ExtensionUri = uri,
                ExtensionName = name,
                Arguments = new List<Expression>()
                {
                    new StringLiteral() { Value = argument }
                },
                Options = new SortedList<string, string>()
                {
                    { "option", optionValue }
                },
                LowerBound = new PreceedingRowWindowBound() { Offset = lowerOffset },
                UpperBound = new CurrentRowWindowBound()
            };
        }

        public WindowFunctionEquality()
        {
            root = Create("uri1", "name1", "arg1", "value1", 1);
            clone = Create("uri1", "name1", "arg1", "value1", 1);
            notEqual = Create("uri2", "name2", "arg2", "value2", 2);
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
            clone.Options = notEqual.Options;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void OptionsNullNotEqual()
        {
            clone.Options = null;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void BothOptionsNullIsEqual()
        {
            root.Options = null;
            clone.Options = null;
            Assert.Equal(root, clone);
        }

        [Fact]
        public void EmptyOptionsEqualsNullOptions()
        {
            // Both mean no options, serialization does not keep them apart
            root.Options = null;
            clone.Options = new SortedList<string, string>();
            Assert.Equal(root, clone);
            Assert.Equal(root.GetHashCode(), clone.GetHashCode());
        }

        [Fact]
        public void LowerBoundChangedNotEqual()
        {
            clone.LowerBound = notEqual.LowerBound;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void LowerBoundTypeChangedNotEqual()
        {
            // A row bound with the same offset is not a following bound
            clone.LowerBound = new FollowingRowWindowBound() { Offset = 1 };
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void LowerBoundNullNotEqual()
        {
            clone.LowerBound = null;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void UpperBoundChangedNotEqual()
        {
            clone.UpperBound = new UnboundedWindowBound();
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void UpperBoundNullNotEqual()
        {
            clone.UpperBound = null;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void BothBoundsNullIsEqual()
        {
            root.LowerBound = null;
            root.UpperBound = null;
            clone.LowerBound = null;
            clone.UpperBound = null;
            Assert.Equal(root, clone);
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
