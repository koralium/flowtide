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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Substrait.Tests.EqualityTests.RelationEquality
{
    public class UnwrapRelationEquality
    {
        readonly UnwrapRelation root;
        readonly UnwrapRelation clone;
        readonly UnwrapRelation notEqual;

        public UnwrapRelationEquality()
        {
            root = new UnwrapRelation()
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
                BaseSchema = new Type.NamedStruct()
                {
                    Names = new List<string>() { "c1" },
                },
                Field = new BoolLiteral() { Value = true },
                Filter = new BoolLiteral() { Value = true }
            };
            clone = new UnwrapRelation()
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
                BaseSchema = new Type.NamedStruct()
                {
                    Names = new List<string>() { "c1" },
                },
                Field = new BoolLiteral() { Value = true },
                Filter = new BoolLiteral() { Value = true }
            };
            notEqual = new UnwrapRelation()
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
                BaseSchema = new Type.NamedStruct()
                {
                    Names = new List<string>() { "c2" },
                },
                Field = new BoolLiteral() { Value = false },
                Filter = new BoolLiteral() { Value = false }
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
        public void BaseSchemaChangedNotEqual()
        {
            clone.BaseSchema = notEqual.BaseSchema;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void FieldChangedNotEqual()
        {
            clone.Field = notEqual.Field;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void FilterChangedNotEqual()
        {
            clone.Filter = notEqual.Filter;
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
