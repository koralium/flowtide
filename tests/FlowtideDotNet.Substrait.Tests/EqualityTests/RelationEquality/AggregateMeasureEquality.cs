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

namespace FlowtideDotNet.Substrait.Tests.EqualityTests.RelationEquality
{
    public class AggregateMeasureEquality
    {
        readonly AggregateMeasure root;
        readonly AggregateMeasure clone;
        readonly AggregateMeasure notEqual;

        public AggregateMeasureEquality()
        {
            root = new AggregateMeasure()
            {
                Measure = new Expressions.AggregateFunction()
                {
                    ExtensionUri = "uri",
                    ExtensionName = "name",
                    Arguments = new List<Expressions.Expression>()
                    {
                        new StringLiteral() { Value = "test1" }
                    }
                },
                Filter = new BoolLiteral() { Value = true }
            };
            clone = new AggregateMeasure()
            {
                Measure = new Expressions.AggregateFunction()
                {
                    ExtensionUri = "uri",
                    ExtensionName = "name",
                    Arguments = new List<Expressions.Expression>()
                    {
                        new StringLiteral() { Value = "test1" }
                    }
                },
                Filter = new BoolLiteral() { Value = true }
            };
            notEqual = new AggregateMeasure()
            {
                Measure = new Expressions.AggregateFunction()
                {
                    ExtensionUri = "uri2",
                    ExtensionName = "name2",
                    Arguments = new List<Expressions.Expression>()
                    {
                        new StringLiteral() { Value = "test2" }
                    }
                },
                Filter = new BoolLiteral() { Value = false }
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
        public void MeasureChangedNotEqual()
        {
            clone.Measure = notEqual.Measure;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void FilterChangedNotEqual()
        {
            clone.Filter = notEqual.Filter;
            Assert.NotEqual(root, clone);
        }

        [Fact]
        public void FilterNullNotEqual()
        {
            clone.Filter = null;
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
