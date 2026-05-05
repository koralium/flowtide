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
    public class MultiOrListExpressionClone
    {
        private MultiOrListExpression root;

        public MultiOrListExpressionClone()
        {
            root = new MultiOrListExpression()
            {
                Value = new List<Expression>
                {
                    new DirectFieldReference()
                    {
                        ReferenceSegment = new StructReferenceSegment() { Field = 0 }
                    }
                },
                Options = new List<OrListRecord>
                {
                    new OrListRecord()
                    {
                        Fields = new List<Expression>
                        {
                            new NumericLiteral { Value = 1 }
                        }
                    }
                }
            };
        }

        [Fact]
        public void CloneIsEqualToOriginal()
        {
            var clone = (MultiOrListExpression)root.Clone();
            Assert.Equal(root, clone);
        }

        [Fact]
        public void CloneIsNotSameReference()
        {
            var clone = root.Clone();
            Assert.False(ReferenceEquals(root, clone));
        }

        [Fact]
        public void CloneValueListIsIndependent()
        {
            var clone = (MultiOrListExpression)root.Clone();
            clone.Value.Add(new NumericLiteral { Value = 99 });
            Assert.NotEqual(root.Value.Count, clone.Value.Count);
        }

        [Fact]
        public void CloneOptionsListIsIndependent()
        {
            var clone = (MultiOrListExpression)root.Clone();
            clone.Options.Add(new OrListRecord()
            {
                Fields = new List<Expression> { new NumericLiteral { Value = 2 } }
            });
            Assert.NotEqual(root.Options.Count, clone.Options.Count);
        }
    }
}
