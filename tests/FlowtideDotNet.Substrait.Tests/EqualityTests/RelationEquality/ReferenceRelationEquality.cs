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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Substrait.Tests.EqualityTests.RelationEquality
{
    public class ReferenceRelationEquality
    {
        readonly ReferenceRelation root;
        readonly ReferenceRelation clone;
        readonly ReferenceRelation notEqual;

        public ReferenceRelationEquality()
        {
            root = new ReferenceRelation()
            {
                Emit = new List<int> { 1, 2, 3 },
                ReferenceOutputLength = 3,
                RelationId = 2
            };
            clone = new ReferenceRelation()
            {
                Emit = new List<int> { 1, 2, 3 },
                ReferenceOutputLength = 3,
                RelationId = 2
            };
            notEqual = new ReferenceRelation()
            {
                Emit = new List<int> { 1, 2, 3, 4 },
                ReferenceOutputLength = 5,
                RelationId = 7
            };
        }

        [Fact]
        public void IsEqual()
        {
            Assert.Equal(root, clone);
        }

        [Fact]
        public void IsNotEqual()
        {
            Assert.NotEqual(root, notEqual);
        }

        [Fact]
        public void HashCodeIsEqual()
        {
            Assert.Equal(root.GetHashCode(), clone.GetHashCode());
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
        public void RelationIdChangedNotEqual()
        {
            clone.RelationId = notEqual.RelationId;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void EmitNullNotEqual()
        {
            clone.Emit = null;
            Assert.NotEqual(root, clone);
        }
    }
}
