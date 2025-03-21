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

using FlowtideDotNet.Substrait.Expressions.Literals;

namespace FlowtideDotNet.Substrait.Tests.EqualityTests.ExpressionEquality
{
    public class NullLiteralEquality
    {
        [Fact]
        public void IsEqual()
        {
            Assert.Equal(new NullLiteral(), new NullLiteral());
        }

        [Fact]
        public void HashCodeIsEqual()
        {
            Assert.Equal(new NullLiteral().GetHashCode(), new NullLiteral().GetHashCode());
        }

        [Fact]
        public void EqualOperator()
        {
            Assert.True(new NullLiteral() == new NullLiteral());
        }

        [Fact]
        public void NotEqualOperator()
        {
            Assert.False(new NullLiteral() != new NullLiteral());
        }
    }
}
