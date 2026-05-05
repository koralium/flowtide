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

namespace FlowtideDotNet.Substrait.Tests.CloneTests.ExpressionClone
{
    public class MapNestedExpressionClone
    {
        private readonly MapNestedExpression root;

        public MapNestedExpressionClone()
        {
            root = new MapNestedExpression()
            {
                KeyValues = new List<KeyValuePair<Expression, Expression>>
                {
                    new KeyValuePair<Expression, Expression>(
                        new StringLiteral { Value = "key1" },
                        new NumericLiteral { Value = 10 }
                    )
                }
            };
        }

        [Fact]
        public void CloneIsEqualToOriginal()
        {
            var clone = (MapNestedExpression)root.Clone();
            Assert.Equal(root, clone);
        }

        [Fact]
        public void CloneIsNotSameReference()
        {
            var clone = root.Clone();
            Assert.False(ReferenceEquals(root, clone));
        }

        [Fact]
        public void CloneKeyValuesListIsIndependent()
        {
            var clone = (MapNestedExpression)root.Clone();
            clone.KeyValues.Add(new KeyValuePair<Expression, Expression>(
                new StringLiteral { Value = "key2" },
                new NumericLiteral { Value = 20 }
            ));
            Assert.NotEqual(root.KeyValues.Count, clone.KeyValues.Count);
        }
    }
}
