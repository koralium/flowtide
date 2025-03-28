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

using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Substrait.Tests.EqualityTests.RelationEquality
{
    public class WriteRelationEquality
    {
        readonly WriteRelation root;
        readonly WriteRelation clone;
        readonly WriteRelation notEqual;

        public WriteRelationEquality()
        {
            root = new WriteRelation()
            {
                Emit = new List<int> { 1, 2, 3 },
                Input = new ReadRelation()
                {
                    BaseSchema = new Type.NamedStruct()
                    {
                        Names = new List<string>() { "c1" },
                    },
                    NamedTable = new Type.NamedTable
                    {
                        Names = new List<string>() { "t1" }
                    }
                },
                NamedObject = new Type.NamedTable() { Names = new List<string>() { "t1" } },
                TableSchema = new Type.NamedStruct() { Names = new List<string>() { "c1" } }
            };
            clone = new WriteRelation()
            {
                Emit = new List<int> { 1, 2, 3 },
                Input = new ReadRelation()
                {
                    BaseSchema = new Type.NamedStruct()
                    {
                        Names = new List<string>() { "c1" },
                    },
                    NamedTable = new Type.NamedTable
                    {
                        Names = new List<string>() { "t1" }
                    }
                },
                NamedObject = new Type.NamedTable() { Names = new List<string>() { "t1" } },
                TableSchema = new Type.NamedStruct() { Names = new List<string>() { "c1" } }
            };
            notEqual = new WriteRelation()
            {
                Emit = new List<int> { 1, 2, 3, 4 },
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
                NamedObject = new Type.NamedTable() { Names = new List<string>() { "t2" } },
                TableSchema = new Type.NamedStruct() { Names = new List<string>() { "c2" } }
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
        public void InputChangedNotEqual()
        {
            clone.Input = notEqual.Input;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void NamedObjectChangedNotEqual()
        {
            clone.NamedObject = notEqual.NamedObject;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void TableSchemaChangedNotEqual()
        {
            clone.TableSchema = notEqual.TableSchema;
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
            Assert.True(root != notEqual);
            Assert.False(root != clone);
        }
    }
}
