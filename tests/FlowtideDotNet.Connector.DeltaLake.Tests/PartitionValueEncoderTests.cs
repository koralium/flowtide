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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.ArrowPartitionEncoders;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter;
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class PartitionValueEncoderTests
    {
        [Fact]
        public void TestDate()
        {
            var date = "2021-01-01";

            var encoder = new DatePartitionEncoder("date");

            encoder.NewFile(new Dictionary<string, string> { { "date", date } });

            AddToColumnFunc func = new AddToColumnFunc();
            encoder.AddValue(0, ref func);

            Assert.Equal(new TimestampTzValue(new DateTime(2021, 1, 1)), func.BoxedValue);
        }

        [Fact]
        public void TestInteger()
        {
            var integer = "123";

            var encoder = new IntegerPartitionEncoder("integer");

            encoder.NewFile(new Dictionary<string, string> { { "integer", integer } });

            AddToColumnFunc func = new AddToColumnFunc();
            encoder.AddValue(0, ref func);
            Assert.Equal(new Int64Value(123), func.BoxedValue);
        }

        [Fact]
        public void TestDouble()
        {
            var doubleValue = "123.45";

            var encoder = new FloatingPointPartitionEncoder("double");

            encoder.NewFile(new Dictionary<string, string> { { "double", doubleValue } });

            AddToColumnFunc func = new AddToColumnFunc();
            encoder.AddValue(0, ref func);
            Assert.Equal(new DoubleValue(123.45), func.BoxedValue);
        }

        [Fact]
        public void TestString()
        {
            var stringValue = "test";

            var encoder = new StringPartitionEncoder("string");

            encoder.NewFile(new Dictionary<string, string> { { "string", stringValue } });

            AddToColumnFunc func = new AddToColumnFunc();
            encoder.AddValue(0, ref func);
            Assert.Equal("test", func.BoxedValue!.AsString.ToString());
        }

        [Fact]
        public void TestTimestampISO8601()
        {
            var timestamp = "2021-01-01T00:00:00Z";

            var encoder = new TimestampPartitionEncoder("timestamp");

            encoder.NewFile(new Dictionary<string, string> { { "timestamp", timestamp } });

            AddToColumnFunc func = new AddToColumnFunc();

            encoder.AddValue(0, ref func);

            var dt = func.BoxedValue!.AsTimestamp.ToDateTimeOffset();
            Assert.Equal(new DateTimeOffset(2021, 1, 1, 0, 0, 0, TimeSpan.Zero), dt);
        }

        [Fact]
        public void TestTimestampSpaceBetweenDateAndTime()
        {
            var timestamp = "2021-01-01 00:00:00";

            var encoder = new TimestampPartitionEncoder("timestamp");

            encoder.NewFile(new Dictionary<string, string> { { "timestamp", timestamp } });

            AddToColumnFunc func = new AddToColumnFunc();
            encoder.AddValue(0, ref func);

            var dt = func.BoxedValue!.AsTimestamp.ToDateTimeOffset();
            Assert.Equal(DateTimeOffset.Parse(timestamp), dt);
        }

        [Fact]
        public void TestTimestampWithSpaceAndMicrosecondsAtEnd()
        {
            var timestamp = "2021-01-01 00:00:00.123456";

            var encoder = new TimestampPartitionEncoder("timestamp");

            encoder.NewFile(new Dictionary<string, string> { { "timestamp", timestamp } });

            AddToColumnFunc func = new AddToColumnFunc();
            encoder.AddValue(0, ref func);

            var dt = func.BoxedValue!.AsTimestamp.ToDateTimeOffset();
            Assert.Equal(DateTimeOffset.Parse(timestamp), dt);
        }

        [Fact]
        public void TestBoolTrue()
        {
            var boolValue = "true";

            var encoder = new BoolPartitionEncoder("bool");

            encoder.NewFile(new Dictionary<string, string> { { "bool", boolValue } });

            AddToColumnFunc func = new AddToColumnFunc();
            encoder.AddValue(0, ref func);
            Assert.Equal(new BoolValue(true), func.BoxedValue);
        }

        [Fact]
        public void TestBoolFalse()
        {
            var boolValue = "false";

            var encoder = new BoolPartitionEncoder("bool");

            encoder.NewFile(new Dictionary<string, string> { { "bool", boolValue } });

            AddToColumnFunc func = new AddToColumnFunc();
            encoder.AddValue(0, ref func);
            Assert.Equal(new BoolValue(false), func.BoxedValue);
        }

        [Fact]
        public void TestBinaryValue()
        {
            var binaryValue = "\u0001\u0002\u0003";

            var encoder = new BinaryPartitionEncoder("binary");

            encoder.NewFile(new Dictionary<string, string> { { "binary", binaryValue } });

            AddToColumnFunc func = new AddToColumnFunc();
            encoder.AddValue(0, ref func);
            Assert.Equal(Encoding.Unicode.GetBytes(binaryValue), func.BoxedValue!.AsBinary.ToArray());
        }

        [Fact]
        public void TestDecimalValue()
        {
            var decimalValue = "123.45";

            var encoder = new DecimalPartitionEncoder("decimal");

            encoder.NewFile(new Dictionary<string, string> { { "decimal", decimalValue } });

            AddToColumnFunc func = new AddToColumnFunc();
            encoder.AddValue(0, ref func);
            Assert.Equal(new DecimalValue(123.45m), func.BoxedValue);
        }
    }
}
