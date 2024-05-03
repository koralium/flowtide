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
    public class VirtualTableReadRelationEquality
    {
        readonly VirtualTableReadRelation root;
        readonly VirtualTableReadRelation clone;
        readonly VirtualTableReadRelation notEqual;

        public VirtualTableReadRelationEquality()
        {
            root = new VirtualTableReadRelation()
            {
                BaseSchema = new Type.NamedStruct()
                {
                    Names = new List<string>() { "c1" },
                },
                Emit = new List<int> { 1, 2, 3 },
                Values = new Type.VirtualTable()
                {
                    JsonValues = new List<string>() { "1" }
                }
            };
            clone = new VirtualTableReadRelation()
            {
                BaseSchema = new Type.NamedStruct()
                {
                    Names = new List<string>() { "c1" },
                },
                Emit = new List<int> { 1, 2, 3 },
                Values = new Type.VirtualTable()
                {
                    JsonValues = new List<string>() { "1" }
                }
            };
            notEqual = new VirtualTableReadRelation()
            {
                BaseSchema = new Type.NamedStruct()
                {
                    Names = new List<string>() { "c2" },
                },
                Emit = new List<int> { 1, 2, 3, 4 },
                Values = new Type.VirtualTable()
                {
                    JsonValues = new List<string>() { "2" }
                }
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
        public void BaseSchemaChangedNotEqual()
        {
            clone.BaseSchema = notEqual.BaseSchema;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void ValuesChangedNotEqual()
        {
            clone.Values = notEqual.Values;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void JsonValuesChangedNotEqual()
        {
            clone.Values.JsonValues = notEqual.Values.JsonValues;
            Assert.NotEqual(root, clone);
        }
    }
}
