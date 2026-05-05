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
using FlowtideDotNet.Substrait.Expressions.IfThen;
using FlowtideDotNet.Substrait.Expressions.Literals;

namespace FlowtideDotNet.Substrait.Tests.CloneTests.ExpressionClone
{
    public class IfThenExpressionClone
    {
        private readonly IfThenExpression root;

        public IfThenExpressionClone()
        {
            root = new IfThenExpression()
            {
                Ifs = new List<IfClause>
                {
                    new IfClause()
                    {
                        If = new BoolLiteral { Value = true },
                        Then = new StringLiteral { Value = "yes" }
                    }
                },
                Else = new StringLiteral { Value = "no" }
            };
        }

        [Fact]
        public void CloneIsEqualToOriginal()
        {
            var clone = (IfThenExpression)root.Clone();
            Assert.Equal(root, clone);
        }

        [Fact]
        public void CloneIsNotSameReference()
        {
            var clone = root.Clone();
            Assert.False(ReferenceEquals(root, clone));
        }

        [Fact]
        public void CloneIfsListIsIndependent()
        {
            var clone = (IfThenExpression)root.Clone();
            clone.Ifs.Add(new IfClause()
            {
                If = new BoolLiteral { Value = false },
                Then = new StringLiteral { Value = "extra" }
            });
            Assert.NotEqual(root.Ifs.Count, clone.Ifs.Count);
        }

        [Fact]
        public void CloneElseIsIndependent()
        {
            var clone = (IfThenExpression)root.Clone();
            clone.Else = new StringLiteral { Value = "different" };
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void CloneWithNullElseIsEqualToOriginal()
        {
            root.Else = null;
            var clone = (IfThenExpression)root.Clone();
            Assert.Equal(root, clone);
        }
    }
}
