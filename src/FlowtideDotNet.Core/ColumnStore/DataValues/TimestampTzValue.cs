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

using FlowtideDotNet.Core.Flexbuffer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.DataValues
{
    [StructLayout(LayoutKind.Sequential)]
    public struct TimestampTzValue : IDataValue, IComparable<TimestampTzValue>
    {
        const int DaysPerYear = 365;
        // Number of days in 4 years
        const int DaysPer4Years = DaysPerYear * 4 + 1;       // 1461
                                                             // Number of days in 100 years
        const int DaysPer100Years = DaysPer4Years * 25 - 1;  // 36524
                                                             // Number of days in 400 years
        const int DaysPer400Years = DaysPer100Years * 4 + 1;
        const int DaysTo1970 = DaysPer400Years * 4 + DaysPer100Years * 3 + DaysPer4Years * 17 + DaysPerYear;

        const long TicksPerMicrosecond = 10;
        const int HoursPerDay = 24;
        const int MicrosecondsPerMillisecond = 1000;
        const long TicksPerMillisecond = TicksPerMicrosecond * MicrosecondsPerMillisecond;
        const long TicksPerSecond = TicksPerMillisecond * 1000;
        const long TicksPerMinute = TicksPerSecond * 60;
        const long TicksPerHour = TicksPerMinute * 60;
        const long TicksPerDay = TicksPerHour * HoursPerDay;
        const long UnixEpochTicks = DaysTo1970 * TicksPerDay;

        public long ticks;
        public long offset;

        public TimestampTzValue(long ticks, long offset)
        {
            this.ticks = ticks;
            this.offset = offset;
        }

        public TimestampTzValue(DateTime dateTime)
        {
            this.ticks = dateTime.Ticks;
        }

        public TimestampTzValue(DateTimeOffset dateTimeOffset)
        {
            this.ticks = dateTimeOffset.Ticks;
            this.offset = (short)dateTimeOffset.Offset.TotalMinutes;
        }

        public ArrowTypeId Type => ArrowTypeId.Timestamp;

        public long AsLong => throw new NotImplementedException();

        public FlxString AsString => throw new NotImplementedException();

        public bool AsBool => throw new NotImplementedException();

        public double AsDouble => throw new NotImplementedException();

        public IListValue AsList => throw new NotImplementedException();

        public Span<byte> AsBinary => throw new NotImplementedException();

        public IMapValue AsMap => throw new NotImplementedException();

        public decimal AsDecimal => throw new NotImplementedException();

        public bool IsNull => false;

        public TimestampTzValue AsTimestamp => this;

        public int CompareTo(TimestampTzValue other)
        {
            return ticks.CompareTo(other.ticks);
        }

        public void CopyToContainer(DataValueContainer container)
        {
            container._timestampValue = this;
        }

        public DateTimeOffset ToDateTimeOffset()
        {
            return new DateTimeOffset(ticks, TimeSpan.FromMinutes(offset));
        }

        public static TimestampTzValue FromUnixMicroseconds(long microseconds)
        {
            return new TimestampTzValue(UnixEpochTicks + (microseconds * TicksPerMicrosecond), 0);
        }

        public long UnixTimestampMicroseconds => (ticks - UnixEpochTicks) / TicksPerMicrosecond;
    }
}
