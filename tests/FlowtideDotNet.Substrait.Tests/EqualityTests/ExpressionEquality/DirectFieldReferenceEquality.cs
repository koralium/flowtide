﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

namespace FlowtideDotNet.Substrait.Tests.EqualityTests.ExpressionEquality
{
    public class DirectFieldReferenceEquality
    {
        private DirectFieldReference root;
        private DirectFieldReference clone;
        private DirectFieldReference notEqual;

        public DirectFieldReferenceEquality()
        {
            root = new DirectFieldReference()
            {
                ReferenceSegment = new StructReferenceSegment()
                {
                    Field = 0
                }
            };
            clone = new DirectFieldReference()
            {
                ReferenceSegment = new StructReferenceSegment()
                {
                    Field = 0
                }
            };
            notEqual = new DirectFieldReference()
            {
                ReferenceSegment = new StructReferenceSegment()
                {
                    Field = 1
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
