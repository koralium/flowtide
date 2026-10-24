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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter.Converters;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Core.Compute.Columnar.Functions.Datetime;
using FlowtideDotNet.Storage.Memory;
using System.IO.Hashing;

namespace FlowtideDotNet.Core.Tests.ColumnStore
{
    public class TimestampTzValueTests
    {
        [Fact]
        public void TestTicksAreUtc()
        {
            var expected = new DateTimeOffset(2024, 1, 1, 12, 0, 0, TimeSpan.FromHours(2));
            var value = new TimestampTzValue(expected);

            Assert.Equal(new DateTime(2024, 1, 1, 10, 0, 0).Ticks, value.ticks);
            Assert.Equal(120, value.offset);
            Assert.Equal(expected, value.ToDateTimeOffset());
            Assert.Equal(expected.Offset, value.ToDateTimeOffset().Offset);
        }

        [Fact]
        public void TestCompareByInstant()
        {
            var utc = new TimestampTzValue(new DateTimeOffset(2024, 1, 1, 11, 0, 0, TimeSpan.Zero));
            var plusTwo = new TimestampTzValue(new DateTimeOffset(2024, 1, 1, 12, 0, 0, TimeSpan.FromHours(2)));
            var sameInstant = new TimestampTzValue(new DateTimeOffset(2024, 1, 1, 13, 0, 0, TimeSpan.FromHours(2)));

            Assert.True(utc.CompareTo(plusTwo) > 0);
            Assert.Equal(0, utc.CompareTo(sameInstant));
        }

        [Fact]
        public void TestSameInstantHashesEqual()
        {
            Column column = new Column(GlobalMemoryManager.Instance)
            {
                new TimestampTzValue(new DateTimeOffset(2024, 1, 1, 11, 0, 0, TimeSpan.Zero)),
                new TimestampTzValue(new DateTimeOffset(2024, 1, 1, 13, 0, 0, TimeSpan.FromHours(2)))
            };

            var hash = new XxHash32();
            column.AddToHash(0, default, hash);
            var utcHash = hash.GetHashAndReset();
            column.AddToHash(1, default, hash);
            var plusTwoHash = hash.GetHashAndReset();

            Assert.Equal(utcHash, plusTwoHash);
        }

        [Fact]
        public void TestCastStringWithOffset()
        {
            var result = new DataValueContainer();
            ColumnCastImplementations.CastToTimestamp(new StringValue("2024-01-01T12:00:00+02:00"), result);

            Assert.Equal(new DateTime(2024, 1, 1, 10, 0, 0).Ticks, result.AsTimestamp.ticks);
            Assert.Equal(120, result.AsTimestamp.offset);
        }

        [Fact]
        public void TestCastStringWithoutOffsetIsUtc()
        {
            var result = new DataValueContainer();
            ColumnCastImplementations.CastToTimestamp(new StringValue("2024-01-01T12:00:00"), result);

            Assert.Equal(new DateTime(2024, 1, 1, 12, 0, 0).Ticks, result.AsTimestamp.ticks);
            Assert.Equal(0, result.AsTimestamp.offset);
        }

        [Fact]
        public void TestCastToStringIsUtc()
        {
            var result = new DataValueContainer();
            var value = new TimestampTzValue(new DateTimeOffset(2024, 1, 1, 12, 0, 0, TimeSpan.FromHours(2)));
            ColumnCastImplementations.CastToString(value, result, new ColumnCastImplementations.CastToStringContainer());

            Assert.Equal("2024-01-01T10:00:00.000Z", result.AsString.ToString());
        }

        [Fact]
        public void TestLocalDateTimeIsUtc()
        {
            var local = new DateTime(2024, 6, 1, 12, 0, 0, DateTimeKind.Local);
            var value = new TimestampTzValue(local);

            Assert.Equal(local.ToUniversalTime().Ticks, value.ticks);
            Assert.Equal(new DateTimeOffset(local), value.ToDateTimeOffset());
        }

        [Fact]
        public void TestDateTimeConverterRoundTrip()
        {
            var converter = new DateTimeConverter();
            var value = new TimestampTzValue(new DateTimeOffset(2024, 1, 1, 12, 0, 0, TimeSpan.FromHours(2)));
            var dateTime = (DateTime)converter.Deserialize(value);

            Assert.Equal(new DateTime(2024, 1, 1, 10, 0, 0), dateTime);
            Assert.Equal(value.ticks, new TimestampTzValue(dateTime).ticks);
        }

        [Fact]
        public void TestFloorTimestampDayKeepsOffset()
        {
            var result = new DataValueContainer();
            var value = new TimestampTzValue(new DateTimeOffset(2024, 1, 2, 0, 30, 0, TimeSpan.FromHours(2)));
            BuiltInDatetimeFunctions.FloorTimestampDayImplementation(value, result);

            Assert.Equal(new DateTimeOffset(2024, 1, 2, 0, 0, 0, TimeSpan.FromHours(2)), result.AsTimestamp.ToDateTimeOffset());
            Assert.Equal(120, result.AsTimestamp.offset);
        }

        [Fact]
        public void TestRoundCalendarKeepsOffset()
        {
            var result = new DataValueContainer();
            var value = new TimestampTzValue(new DateTimeOffset(2024, 1, 1, 12, 34, 56, TimeSpan.FromHours(2)));
            BuiltInDatetimeFunctions.TimestampRoundCalendar(value, new StringValue("FLOOR"), new StringValue("MINUTE"), new StringValue("MINUTE"), new Int64Value(1), result);

            Assert.Equal(new DateTimeOffset(2024, 1, 1, 12, 34, 0, TimeSpan.FromHours(2)), result.AsTimestamp.ToDateTimeOffset());
            Assert.Equal(120, result.AsTimestamp.offset);
        }

        [Fact]
        public void TestDatediffSameInstant()
        {
            var result = new DataValueContainer();
            BuiltInDatetimeFunctions.TimestampDatediffHours(
                new TimestampTzValue(new DateTimeOffset(2024, 1, 1, 10, 0, 0, TimeSpan.Zero)),
                new TimestampTzValue(new DateTimeOffset(2024, 1, 1, 12, 0, 0, TimeSpan.FromHours(2))),
                result);
            Assert.Equal(0, result.AsLong);

            BuiltInDatetimeFunctions.TimestampDatediffDays(
                new TimestampTzValue(new DateTimeOffset(2024, 1, 1, 23, 0, 0, TimeSpan.Zero)),
                new TimestampTzValue(new DateTimeOffset(2024, 1, 2, 1, 0, 0, TimeSpan.FromHours(2))),
                result);
            Assert.Equal(0, result.AsLong);
        }

        [Fact]
        public void TestDatediffNearMaxDate()
        {
            var result = new DataValueContainer();
            var start = new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.FromHours(2));
            var end = new DateTimeOffset(9999, 12, 31, 23, 0, 0, TimeSpan.Zero);
            BuiltInDatetimeFunctions.TimestampDatediffHours(new TimestampTzValue(start), new TimestampTzValue(end), result);

            Assert.Equal(end.UtcTicks / TimeSpan.TicksPerHour - start.UtcTicks / TimeSpan.TicksPerHour, result.AsLong);
        }
    }
}
