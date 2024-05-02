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
    public class SingularOrListEquality
    {
        readonly SingularOrListExpression root;
        readonly SingularOrListExpression clone;
        readonly SingularOrListExpression notEqual;

        public SingularOrListEquality()
        {
            root = new SingularOrListExpression()
            {
                Value = new StringLiteral() { Value = "test" },
                Options = new List<Expression>()
                {
                    new StringLiteral() { Value = "test1" },
                    new StringLiteral() { Value = "test2" },
                }
            };
            clone = new SingularOrListExpression()
            {
                Value = new StringLiteral() { Value = "test" },
                Options = new List<Expression>()
                {
                    new StringLiteral() { Value = "test1" },
                    new StringLiteral() { Value = "test2" },
                }
            };
            notEqual = new SingularOrListExpression()
            {
                Value = new StringLiteral() { Value = "test3" },
                Options = new List<Expression>()
                {
                    new StringLiteral() { Value = "test4" },
                    new StringLiteral() { Value = "test5" },
                    new StringLiteral() { Value = "test6" },
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
        public void ValueChangedNotEqual()
        {
            clone.Value = notEqual.Value;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void OptionsChangedNotEqual()
        {
            clone.Options = notEqual.Options;
            Assert.NotEqual(root, clone);
        }
    }
}
