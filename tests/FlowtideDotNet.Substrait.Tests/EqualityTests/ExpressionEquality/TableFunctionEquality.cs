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

using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Substrait.Tests.EqualityTests.ExpressionEquality
{
    public class TableFunctionEquality
    {
        readonly TableFunction root;
        readonly TableFunction clone;
        readonly TableFunction notEqual;

        public TableFunctionEquality()
        {
            root = new TableFunction()
            {
                ExtensionUri = "testuri1",
                ExtensionName = "testname1",
                Arguments = new List<Expression>()
                {
                    new StringLiteral() { Value = "test1" } 
                },
                TableSchema = new Type.NamedStruct()
                {
                    Names = new List<string>() { "c1" }
                }
            };
            clone = new TableFunction()
            {
                ExtensionUri = "testuri1",
                ExtensionName = "testname1",
                Arguments = new List<Expression>()
                {
                    new StringLiteral() { Value = "test1" }
                },
                TableSchema = new Type.NamedStruct()
                {
                    Names = new List<string>() { "c1" }
                }
            };
            notEqual = new TableFunction()
            {
                ExtensionUri = "testuri2",
                ExtensionName = "testname2",
                Arguments = new List<Expression>()
                {
                    new StringLiteral() { Value = "test2" }
                },
                TableSchema = new Type.NamedStruct()
                {
                    Names = new List<string>() { "c2" }
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
        public void ExtensionUriChangedNotEqual()
        {
            clone.ExtensionUri = notEqual.ExtensionUri;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void ExtensionNameChangedNotEqual()
        {
            clone.ExtensionName = notEqual.ExtensionName;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void ArgumentsChangedNotEqual()
        {
            clone.Arguments = notEqual.Arguments;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void TableSchemaChangedNotEqual()
        {
            clone.TableSchema = notEqual.TableSchema;
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
