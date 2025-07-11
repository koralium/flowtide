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

namespace FlowtideDotNet.Base.Tests
{
    public class LongWatermarkValueTests
    {
        [Theory]
        [InlineData(1, 2, -1)]
        [InlineData(2, 1, 1)]
        [InlineData(1, 1, 0)]
        public void CompareTo(int p1, int p2, int expected)
        {
            LongWatermarkValue v1 = new LongWatermarkValue(p1);
            LongWatermarkValue v2 = new LongWatermarkValue(p2);

            Assert.Equal(expected, v1.CompareTo(v2));
        }

        [Fact]
        public void CompareToNull()
        {
            LongWatermarkValue v1 = new LongWatermarkValue(1);
            LongWatermarkValue? v2 = null;

            Assert.Equal(1, v1.CompareTo(v2));
        }

        [Theory]
        [InlineData(1, 2, false)]
        [InlineData(2, 1, false)]
        [InlineData(1, 1, true)]
        public void EqualsNotNull(int p1, int p2, bool expected)
        {
            LongWatermarkValue v1 = new LongWatermarkValue(p1);
            LongWatermarkValue v2 = new LongWatermarkValue(p2);

            Assert.Equal(expected, v1.Equals(v2));
        }

        [Fact]
        public void EqualsNull()
        {
            LongWatermarkValue v1 = new LongWatermarkValue(1);
            LongWatermarkValue? v2 = null;

            Assert.False(v1.Equals(v2));
        }

        [Fact]
        public void EqualityOverrideEqual()
        {
            LongWatermarkValue v1 = new LongWatermarkValue(1);
            LongWatermarkValue v2 = new LongWatermarkValue(1);

            Assert.True(v1 == v2);
            Assert.False(v1 != v2);
        }

        [Fact]
        public void EqualityOverrideNotEqual()
        {
            LongWatermarkValue v1 = new LongWatermarkValue(1);
            LongWatermarkValue v2 = new LongWatermarkValue(2);

            Assert.False(v1 == v2);
            Assert.True(v1 != v2);
        }

        [Fact]
        public void EqualityOverrideLeftNull()
        {
            LongWatermarkValue? v1 = null;
            LongWatermarkValue v2 = new LongWatermarkValue(1);

            Assert.False(v1 == v2);
            Assert.True(v1 != v2);
        }

        [Fact]
        public void EqualityOverrideRightNull()
        {
            LongWatermarkValue v1 = new LongWatermarkValue(1);
            LongWatermarkValue? v2 = null;

            Assert.False(v1 == v2);
            Assert.True(v1 != v2);
        }

        [Fact]
        public void EqualityOverrideBothNull()
        {
            LongWatermarkValue? v1 = null;
            LongWatermarkValue? v2 = null;

            Assert.True(v1 == v2);
            Assert.False(v1 != v2);
        }

        [Fact]
        public void LessThanGreaterThanOverrideLess()
        {
            LongWatermarkValue v1 = new LongWatermarkValue(1);
            LongWatermarkValue v2 = new LongWatermarkValue(2);

            Assert.True(v1 < v2);
            Assert.False(v1 > v2);
        }

        [Fact]
        public void LessThanGreaterThanOverrideGreater()
        {
            LongWatermarkValue v1 = new LongWatermarkValue(2);
            LongWatermarkValue v2 = new LongWatermarkValue(1);

            Assert.False(v1 < v2);
            Assert.True(v1 > v2);
        }

        [Fact]
        public void LessThanGreaterThanOverrideEqual()
        {
            LongWatermarkValue v1 = new LongWatermarkValue(1);
            LongWatermarkValue v2 = new LongWatermarkValue(1);

            Assert.False(v1 < v2);
            Assert.False(v1 > v2);
        }

        [Fact]
        public void LessThanGreaterThanOverrideLeftNull()
        {
            LongWatermarkValue? v1 = null;
            LongWatermarkValue v2 = new LongWatermarkValue(1);

            // Null is less than the value
            Assert.True(v1 < v2);
            Assert.False(v1 > v2);
        }

        [Fact]
        public void LessThanGreaterThanOverrideRightNull()
        {
            LongWatermarkValue v1 = new LongWatermarkValue(1);
            LongWatermarkValue? v2 = null;

            Assert.False(v1 < v2);
            Assert.True(v1 > v2);
        }

        [Fact]
        public void LessThanGreaterThanOverrideBothNull()
        {
            LongWatermarkValue? v1 = null;
            LongWatermarkValue? v2 = null;

            Assert.False(v1 < v2);
            Assert.False(v1 > v2);
        }

        [Fact]
        public void LessThanGreaterThanOrEqualOverrideLess()
        {
            LongWatermarkValue v1 = new LongWatermarkValue(1);
            LongWatermarkValue v2 = new LongWatermarkValue(2);

            Assert.True(v1 <= v2);
            Assert.False(v1 >= v2);
        }

        [Fact]
        public void LessThanGreaterThanOrEqualOverrideGreater()
        {
            LongWatermarkValue v1 = new LongWatermarkValue(2);
            LongWatermarkValue v2 = new LongWatermarkValue(1);

            Assert.False(v1 <= v2);
            Assert.True(v1 >= v2);
        }

        [Fact]
        public void LessThanGreaterThanOrEqualOverrideEqual()
        {
            LongWatermarkValue v1 = new LongWatermarkValue(1);
            LongWatermarkValue v2 = new LongWatermarkValue(1);

            Assert.True(v1 <= v2);
            Assert.True(v1 >= v2);
        }

        [Fact]
        public void LessThanGreaterThanOrEqualOverrideLeftNull()
        {
            LongWatermarkValue? v1 = null;
            LongWatermarkValue v2 = new LongWatermarkValue(1);

            // Null is less than the value
            Assert.True(v1 <= v2);
            Assert.False(v1 >= v2);
        }

        [Fact]
        public void LessThanGreaterThanOrEqualOverrideRightNull()
        {
            LongWatermarkValue v1 = new LongWatermarkValue(1);
            LongWatermarkValue? v2 = null;

            Assert.False(v1 <= v2);
            Assert.True(v1 >= v2);
        }

        [Fact]
        public void LessThanGreaterThanOrEqualOverrideBothNull()
        {
            LongWatermarkValue? v1 = null;
            LongWatermarkValue? v2 = null;

            Assert.True(v1 <= v2);
            Assert.True(v1 >= v2);
        }

        [Fact]
        public void GetHashCodeTest()
        {
            LongWatermarkValue v1 = new LongWatermarkValue(1);
            LongWatermarkValue v2 = new LongWatermarkValue(1);

            Assert.Equal(v1.GetHashCode(), v2.GetHashCode());
        }

        [Fact]
        public void GetHashCodeDifferentValuesTest()
        {
            LongWatermarkValue v1 = new LongWatermarkValue(1);
            LongWatermarkValue v2 = new LongWatermarkValue(2);

            Assert.NotEqual(v1.GetHashCode(), v2.GetHashCode());
        }
    }
}
