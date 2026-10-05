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
    public class CheckRelationEquality
    {
        readonly CheckRelation root;
        readonly CheckRelation clone;
        readonly CheckRelation notEqual;

        public CheckRelationEquality()
        {
            root = Create("t1", "message1", new List<int>() { 1, 2, 3 });
            clone = Create("t1", "message1", new List<int>() { 1, 2, 3 });
            notEqual = Create("t2", "message2", new List<int>() { 1, 2, 3, 4 });
        }

        private static CheckRelation Create(string table, string message, List<int> emit)
        {
            return new CheckRelation()
            {
                Input = new ReadRelation()
                {
                    BaseSchema = new Type.NamedStruct()
                    {
                        Names = new List<string>() { "c1" },
                    },
                    NamedTable = new Type.NamedTable
                    {
                        Names = new List<string>() { table }
                    }
                },
                Checks = new List<CheckDefinition>()
                {
                    new CheckDefinition()
                    {
                        Condition = new BoolLiteral() { Value = true },
                        Message = message,
                        Tags = new List<CheckTag>(),
                        Guards = new List<CheckGuard>()
                    }
                },
                Emit = emit
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
        public void InputChangedNotEqual()
        {
            clone.Input = notEqual.Input;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void ChecksChangedNotEqual()
        {
            clone.Checks = notEqual.Checks;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void ExtraCheckNotEqual()
        {
            clone.Checks.Add(notEqual.Checks[0]);
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void EmitChangedNotEqual()
        {
            clone.Emit = notEqual.Emit;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void EmitNullNotEqual()
        {
            clone.Emit = null;
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
