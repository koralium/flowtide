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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class SchemaEqualityTests
    {
        [Fact]
        public void TestBinaryTypeEquality()
        {
            var x = new BinaryType();
            var y = new BinaryType();

            Assert.Equal(x, y);
            Assert.Equal(x.GetHashCode(), y.GetHashCode());
        }

        [Fact]
        public void TestBooleanTypeEquality()
        {
            var x = new BooleanType();
            var y = new BooleanType();

            Assert.Equal(x, y);
            Assert.Equal(x.GetHashCode(), y.GetHashCode());
        }

        [Fact]
        public void TestByteTypeEquality()
        {
            var x = new ByteType();
            var y = new ByteType();

            Assert.Equal(x, y);
            Assert.Equal(x.GetHashCode(), y.GetHashCode());
        }

        [Fact]
        public void TestDateTypeEquality()
        {
            var x = new DateType();
            var y = new DateType();

            Assert.Equal(x, y);
            Assert.Equal(x.GetHashCode(), y.GetHashCode());
        }

        [Fact]
        public void TestDecimalTypeEquality()
        {
            var x = new DecimalType(19, 3);
            var y = new DecimalType(19, 3);

            Assert.Equal(x, y);
            Assert.Equal(x.GetHashCode(), y.GetHashCode());
        }

        [Fact]
        public void TestDoubleTypeEquality()
        {
            var x = new DoubleType();
            var y = new DoubleType();

            Assert.Equal(x, y);
            Assert.Equal(x.GetHashCode(), y.GetHashCode());
        }

        [Fact]
        public void TestFloatTypeEquality()
        {
            var x = new FloatType();
            var y = new FloatType();

            Assert.Equal(x, y);
            Assert.Equal(x.GetHashCode(), y.GetHashCode());
        }

        [Fact]
        public void TestIntegerTypeEquality()
        {
            var x = new IntegerType();
            var y = new IntegerType();

            Assert.Equal(x, y);
            Assert.Equal(x.GetHashCode(), y.GetHashCode());
        }

        [Fact]
        public void TestLongTypeEquality()
        {
            var x = new LongType();
            var y = new LongType();

            Assert.Equal(x, y);
            Assert.Equal(x.GetHashCode(), y.GetHashCode());
        }

        [Fact]
        public void TestMapTypeEquality()
        {
            var x = new MapType(new LongType(), new BooleanType(), false);
            var y = new MapType(new LongType(), new BooleanType(), false);

            Assert.Equal(x, y);
            Assert.Equal(x.GetHashCode(), y.GetHashCode());
        }

        [Fact]
        public void TestShortTypeEquality()
        {
            var x = new ShortType();
            var y = new ShortType();

            Assert.Equal(x, y);
            Assert.Equal(x.GetHashCode(), y.GetHashCode());
        }

        [Fact]
        public void TestStringTypeEquality()
        {
            var x = new StringType();
            var y = new StringType();

            Assert.Equal(x, y);
            Assert.Equal(x.GetHashCode(), y.GetHashCode());
        }

        [Fact]
        public void TestStructFieldEquality()
        {
            var x = new StructField("name", new StringType(), true, new Dictionary<string, object>());
            var y = new StructField("name", new StringType(), true, new Dictionary<string, object>());

            Assert.Equal(x, y);
            Assert.Equal(x.GetHashCode(), y.GetHashCode());
        }

        [Fact]
        public void TestStructTypeEquality()
        {
            var x = new StructType(new List<StructField> { new StructField("name", new StringType(), true, new Dictionary<string, object>()) });
            var y = new StructType(new List<StructField> { new StructField("name", new StringType(), true, new Dictionary<string, object>()) });

            Assert.Equal(x, y);
            Assert.Equal(x.GetHashCode(), y.GetHashCode());
        }

        [Fact]
        public void TestTimestampTypeEquality()
        {
            var x = new TimestampType();
            var y = new TimestampType();

            Assert.Equal(x, y);
            Assert.Equal(x.GetHashCode(), y.GetHashCode());
        }
    }
}
