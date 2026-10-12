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
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Substrait.Tests.EqualityTests.RelationEquality
{
    public class CheckGuardEquality
    {
        readonly CheckGuard root;
        readonly CheckGuard clone;
        readonly CheckGuard notEqual;

        public CheckGuardEquality()
        {
            root = new CheckGuard() { Expression = new BoolLiteral() { Value = true }, Kind = CheckGuardKind.IsTrue };
            clone = new CheckGuard() { Expression = new BoolLiteral() { Value = true }, Kind = CheckGuardKind.IsTrue };
            notEqual = new CheckGuard() { Expression = new BoolLiteral() { Value = false }, Kind = CheckGuardKind.IsNotTrue };
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
        public void ExpressionChangedNotEqual()
        {
            clone.Expression = notEqual.Expression;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void KindChangedNotEqual()
        {
            clone.Kind = CheckGuardKind.IsNull;
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
