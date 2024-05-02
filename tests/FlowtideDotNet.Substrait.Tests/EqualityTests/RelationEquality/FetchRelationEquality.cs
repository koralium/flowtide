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
    public class FetchRelationEquality
    {
        readonly FetchRelation root;
        readonly FetchRelation clone;
        readonly FetchRelation notEqual;

        public FetchRelationEquality()
        {
            root = new FetchRelation()
            {
                Input = new ReadRelation()
                {
                    BaseSchema = new Type.NamedStruct()
                    {
                        Names = new List<string>() { "c2" },
                    },
                    NamedTable = new Type.NamedTable
                    {
                        Names = new List<string>() { "t2" }
                    }
                },
                Count = 3,
                Offset = 5,
                Emit = new List<int>() { 1, 2, 3 }
            };
            clone = new FetchRelation()
            {
                Input = new ReadRelation()
                {
                    BaseSchema = new Type.NamedStruct()
                    {
                        Names = new List<string>() { "c2" },
                    },
                    NamedTable = new Type.NamedTable
                    {
                        Names = new List<string>() { "t2" }
                    }
                },
                Count = 3,
                Offset = 5,
                Emit = new List<int>() { 1, 2, 3 }
            };
            notEqual = new FetchRelation()
            {
                Input = new ReadRelation()
                {
                    BaseSchema = new Type.NamedStruct()
                    {
                        Names = new List<string>() { "c3" },
                    },
                    NamedTable = new Type.NamedTable
                    {
                        Names = new List<string>() { "t3" }
                    }
                },
                Count = 7,
                Offset = 9,
                Emit = new List<int>() { 1, 2, 3, 4 }
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
        public void CountChangedNotEqual()
        {
            clone.Count = notEqual.Count;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void OffsetChangedNotEqual()
        {
            clone.Offset = notEqual.Offset;
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
    }
}
