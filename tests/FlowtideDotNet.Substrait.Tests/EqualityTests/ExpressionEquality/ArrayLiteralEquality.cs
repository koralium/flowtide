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
    public class ArrayLiteralEquality
    {
        ArrayLiteral root;
        ArrayLiteral clone;
        ArrayLiteral notEqual;
        public ArrayLiteralEquality()
        {
            root = new ArrayLiteral()
            {
                Expressions = new List<Expression>()
                {
                    new NumericLiteral() { Value = 1 },
                    new NumericLiteral() { Value = 2 }
                }
            };
            clone = new ArrayLiteral()
            {
                Expressions = new List<Expression>()
                {
                    new NumericLiteral() { Value = 1 },
                    new NumericLiteral() { Value = 2 }
                }
            };
            notEqual = new ArrayLiteral()
            {
                Expressions = new List<Expression>()
                {
                    new NumericLiteral() { Value = 1 },
                    new NumericLiteral() { Value = 2 },
                    new NumericLiteral() { Value = 3 }
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
    }
}
