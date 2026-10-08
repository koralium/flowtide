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

using FlowtideDotNet.Core.ColumnStore.DataValues;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats
{
    /// <summary>
    /// Values as the parquet writers store them.
    /// </summary>
    internal static class StoredValue
    {
        private static readonly long s_unixEpochTicks = DateTimeOffset.UnixEpoch.UtcTicks;

        public static DateTimeOffset Timestamp(DateTimeOffset value)
        {
            return DateTimeOffset.UnixEpoch.AddTicks(TimestampMilliseconds(value) * TimeSpan.TicksPerMillisecond);
        }

        // Matches the Arrow millisecond builder, truncates toward zero
        public static long TimestampMilliseconds(DateTimeOffset value)
        {
            return (value.UtcTicks - s_unixEpochTicks) / TimeSpan.TicksPerMillisecond;
        }

        // Same instant as ToDateTimeOffset, without building one
        public static long TimestampMilliseconds(in TimestampTzValue value)
        {
            return (value.ticks - value.offset * TimeSpan.TicksPerMinute - s_unixEpochTicks) / TimeSpan.TicksPerMillisecond;
        }

        public static DateTime Date(DateTime value)
        {
            return value.Date;
        }

        public static double Float32(double value)
        {
            return (float)value;
        }
    }
}
