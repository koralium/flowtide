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
    public class WindowBoundEquality
    {
        [Fact]
        public void PreceedingRowIsEqual()
        {
            var root = new PreceedingRowWindowBound() { Offset = 3 };
            var clone = new PreceedingRowWindowBound() { Offset = 3 };
            Assert.Equal(root, clone);
            Assert.Equal(root.GetHashCode(), clone.GetHashCode());
            Assert.True(root == clone);
        }

        [Fact]
        public void PreceedingRowOffsetChangedNotEqual()
        {
            var root = new PreceedingRowWindowBound() { Offset = 3 };
            var notEqual = new PreceedingRowWindowBound() { Offset = 4 };
            Assert.NotEqual(root, notEqual);
            Assert.True(root != notEqual);
        }

        [Fact]
        public void FollowingRowIsEqual()
        {
            var root = new FollowingRowWindowBound() { Offset = 3 };
            var clone = new FollowingRowWindowBound() { Offset = 3 };
            Assert.Equal(root, clone);
            Assert.Equal(root.GetHashCode(), clone.GetHashCode());
            Assert.True(root == clone);
        }

        [Fact]
        public void FollowingRowOffsetChangedNotEqual()
        {
            var root = new FollowingRowWindowBound() { Offset = 3 };
            var notEqual = new FollowingRowWindowBound() { Offset = 4 };
            Assert.NotEqual(root, notEqual);
            Assert.True(root != notEqual);
        }

        [Fact]
        public void PreceedingRangeIsEqual()
        {
            var root = new PreceedingRangeWindowBound() { Expression = new StringLiteral() { Value = "v" } };
            var clone = new PreceedingRangeWindowBound() { Expression = new StringLiteral() { Value = "v" } };
            Assert.Equal(root, clone);
            Assert.Equal(root.GetHashCode(), clone.GetHashCode());
            Assert.True(root == clone);
        }

        [Fact]
        public void PreceedingRangeExpressionChangedNotEqual()
        {
            var root = new PreceedingRangeWindowBound() { Expression = new StringLiteral() { Value = "v1" } };
            var notEqual = new PreceedingRangeWindowBound() { Expression = new StringLiteral() { Value = "v2" } };
            Assert.NotEqual(root, notEqual);
            Assert.True(root != notEqual);
        }

        [Fact]
        public void FollowingRangeIsEqual()
        {
            var root = new FollowingRangeWindowBound() { Expression = new StringLiteral() { Value = "v" } };
            var clone = new FollowingRangeWindowBound() { Expression = new StringLiteral() { Value = "v" } };
            Assert.Equal(root, clone);
            Assert.Equal(root.GetHashCode(), clone.GetHashCode());
            Assert.True(root == clone);
        }

        [Fact]
        public void FollowingRangeExpressionChangedNotEqual()
        {
            var root = new FollowingRangeWindowBound() { Expression = new StringLiteral() { Value = "v1" } };
            var notEqual = new FollowingRangeWindowBound() { Expression = new StringLiteral() { Value = "v2" } };
            Assert.NotEqual(root, notEqual);
            Assert.True(root != notEqual);
        }

        [Fact]
        public void CurrentRowIsEqual()
        {
            var root = new CurrentRowWindowBound();
            var clone = new CurrentRowWindowBound();
            Assert.Equal(root, clone);
            Assert.Equal(root.GetHashCode(), clone.GetHashCode());
            Assert.True(root == clone);
        }

        [Fact]
        public void UnboundedIsEqual()
        {
            var root = new UnboundedWindowBound();
            var clone = new UnboundedWindowBound();
            Assert.Equal(root, clone);
            Assert.Equal(root.GetHashCode(), clone.GetHashCode());
            Assert.True(root == clone);
        }

        [Fact]
        public void DifferentBoundTypesAreNotEqual()
        {
            // Compared through the base type, the same way a window function compares its bounds
            var bounds = new WindowBound[]
            {
                new PreceedingRowWindowBound() { Offset = 1 },
                new FollowingRowWindowBound() { Offset = 1 },
                new PreceedingRangeWindowBound() { Expression = new StringLiteral() { Value = "v" } },
                new FollowingRangeWindowBound() { Expression = new StringLiteral() { Value = "v" } },
                new CurrentRowWindowBound(),
                new UnboundedWindowBound()
            };

            for (int i = 0; i < bounds.Length; i++)
            {
                for (int j = 0; j < bounds.Length; j++)
                {
                    if (i == j)
                    {
                        continue;
                    }
                    Assert.False(bounds[i].Equals(bounds[j]), $"{bounds[i].Type} should not equal {bounds[j].Type}");
                }
            }
        }
    }
}
