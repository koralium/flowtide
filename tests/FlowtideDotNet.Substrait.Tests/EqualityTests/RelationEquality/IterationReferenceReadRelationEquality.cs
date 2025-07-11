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

using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Substrait.Tests.EqualityTests.RelationEquality
{
    public class IterationReferenceReadRelationEquality
    {
        readonly IterationReferenceReadRelation root;
        readonly IterationReferenceReadRelation clone;
        readonly IterationReferenceReadRelation notEqual;

        public IterationReferenceReadRelationEquality()
        {
            root = new IterationReferenceReadRelation()
            {
                IterationName = "test1",
                Emit = new List<int>() { 1, 2, 3 },
                ReferenceOutputLength = 3
            };
            clone = new IterationReferenceReadRelation()
            {
                IterationName = "test1",
                Emit = new List<int>() { 1, 2, 3 },
                ReferenceOutputLength = 3
            };
            notEqual = new IterationReferenceReadRelation()
            {
                IterationName = "test2",
                Emit = new List<int>() { 1, 2, 3, 4 },
                ReferenceOutputLength = 5
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
        public void EmitChangedNotEqual()
        {
            clone.Emit = notEqual.Emit;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void ReferenceOutputLengthChangedNotEqual()
        {
            clone.ReferenceOutputLength = notEqual.ReferenceOutputLength;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void IterationNameChangedNotEqual()
        {
            clone.IterationName = notEqual.IterationName;
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
