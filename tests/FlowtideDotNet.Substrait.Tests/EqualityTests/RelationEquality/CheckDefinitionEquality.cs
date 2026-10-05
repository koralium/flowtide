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
    public class CheckDefinitionEquality
    {
        readonly CheckDefinition root;
        readonly CheckDefinition clone;
        readonly CheckDefinition notEqual;

        public CheckDefinitionEquality()
        {
            root = Create(true, "message1", "tag1", CheckGuardKind.IsTrue);
            clone = Create(true, "message1", "tag1", CheckGuardKind.IsTrue);
            notEqual = Create(false, "message2", "tag2", CheckGuardKind.IsNull);
        }

        private static CheckDefinition Create(bool condition, string message, string tagKey, CheckGuardKind guardKind)
        {
            return new CheckDefinition()
            {
                Condition = new BoolLiteral() { Value = condition },
                Message = message,
                Tags = new List<CheckTag>()
                {
                    new CheckTag() { Key = tagKey, Value = new StringLiteral() { Value = "v1" } }
                },
                Guards = new List<CheckGuard>()
                {
                    new CheckGuard() { Expression = new BoolLiteral() { Value = true }, Kind = guardKind }
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
        public void ConditionChangedNotEqual()
        {
            clone.Condition = notEqual.Condition;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void MessageChangedNotEqual()
        {
            clone.Message = notEqual.Message;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void MessageCaseChangedNotEqual()
        {
            clone.Message = "Message1";
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void TagsChangedNotEqual()
        {
            clone.Tags = notEqual.Tags;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void TagsEmptyNotEqual()
        {
            clone.Tags = new List<CheckTag>();
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void GuardsChangedNotEqual()
        {
            clone.Guards = notEqual.Guards;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void GuardsEmptyNotEqual()
        {
            clone.Guards = new List<CheckGuard>();
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
