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
using FlowtideDotNet.Core.Compute.Internal.StrftimeImpl;
using FlowtideDotNet.Core.Flexbuffer;
using FlowtideDotNet.Substrait.FunctionExtensions;
using Microsoft.VisualBasic;
using System.Globalization;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions
{
    internal static class BuiltInDatetimeFunctions
    {
        public static void AddBuiltInDatetimeFunctions(FunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterScalarMethod(FunctionsDatetime.Uri, FunctionsDatetime.Strftime, typeof(BuiltInDatetimeFunctions), nameof(StrfTimeImplementation));
            functionsRegister.RegisterScalarMethod(FunctionsDatetime.Uri, FunctionsDatetime.FloorTimestampDay, typeof(BuiltInDatetimeFunctions), nameof(FloorTimestampDayImplementation));
            functionsRegister.RegisterScalarMethod(FunctionsDatetime.Uri, FunctionsDatetime.ParseTimestamp, typeof(BuiltInDatetimeFunctions), nameof(TimestampParseImplementation));

            functionsRegister.RegisterScalarMethod(FunctionsDatetime.Uri, FunctionsDatetime.Extract, typeof(BuiltInDatetimeFunctions), nameof(ExtractImplementation));
        }

        internal static IDataValue StrfTimeImplementation<T1, T2>(T1 value, T2 format, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (value.Type == ArrowTypeId.Timestamp)
            {
                var dt = value.AsTimestamp.ToDateTimeOffset();
                result._type = ArrowTypeId.String;
                result._stringValue = new StringValue(Strftime.ToStrFTime(dt, format.AsString.ToString(), CultureInfo.InvariantCulture));
                return result;
            }
            long timestamp = 0;
            if (value.Type == ArrowTypeId.Int64)
            {
                timestamp = value.AsLong;
            }
            else
            {
                result._type = ArrowTypeId.Null;
                return result;
            }
            if (format.Type != ArrowTypeId.String)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }
            var dateTime = DateTimeOffset.UnixEpoch.AddTicks(timestamp).DateTime;

            result._type = ArrowTypeId.String;
            result._stringValue = new StringValue(Strftime.ToStrFTime(dateTime, format.AsString.ToString(), CultureInfo.InvariantCulture));
            return result;
        }

        internal static IDataValue FloorTimestampDayImplementation<T1>(T1 value, DataValueContainer result)
            where T1 : IDataValue
        {
            if (value.Type == ArrowTypeId.Timestamp)
            {
                var dt = value.AsTimestamp.ToDateTimeOffset();
                // Remove hours, seconds, etc so its only year month day left
                var newDate = new DateTimeOffset(dt.Year, dt.Month, dt.Day, 0, 0, 0, TimeSpan.Zero);

                result._type = ArrowTypeId.Timestamp;
                result._timestampValue = new TimestampTzValue(newDate);
                return result;
            }

            result._type = ArrowTypeId.Null;
            return result;
        }

        internal static IDataValue TimestampParseImplementation<T1, T2>(T1 value, T2 format, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (value.Type != ArrowTypeId.String)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }
            if (format.Type != ArrowTypeId.String)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            var valueStr = value.AsString.ToString();
            var formatStr = format.AsString.ToString();

            if (DateTimeOffset.TryParseExact(valueStr, formatStr, CultureInfo.InvariantCulture, DateTimeStyles.None, out var dt))
            {
                result._type = ArrowTypeId.Timestamp;
                result._timestampValue = new TimestampTzValue(dt);
                return result;
            }

            result._type = ArrowTypeId.Null;
            return result;
        }

        private enum DateComponent
        {
            YEAR,
            ISO_YEAR,
            US_YEAR,
            QUARTER,
            MONTH,
            DAY,
            DAY_OF_YEAR,
            MONDAY_DAY_OF_WEEK,
            SUNDAY_DAY_OF_WEEK,
            MONDAY_WEEK,
            SUNDAY_WEEK,
            ISO_WEEK,
            US_WEEK,
            HOUR,
            MINUTE,
            SECOND,
            MILLISECOND,
            MICROSECOND,
            NANOSECOND,
            PICOSECOND,
            SUBSECOND,
            UNIX_TIME,
            TIMEZONE_OFFSET
        }


        internal static IDataValue ExtractImplementation<T1, T2>(T1 component, T2 value, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (component.Type != ArrowTypeId.String)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            var componentSpan = component.AsString.Span;

            if (componentSpan.CompareToOrdinalIgnoreCaseUtf8("YEAR"u8) == 0)
            {
                return ExtractYearImplementation(value, result);
            }
            else if (componentSpan.CompareToOrdinalIgnoreCaseUtf8("ISO_YEAR"u8) == 0)
            {
                return ExtractIsoYearImplementation(value, result);
            }
            else if (componentSpan.CompareToOrdinalIgnoreCaseUtf8("US_YEAR"u8) == 0)
            {
                return ExtractUsYearImplementation(value, result);
            }
            else if (componentSpan.CompareToOrdinalIgnoreCaseUtf8("QUARTER"u8) == 0)
            {
                return ExtractQuarterImplementation(value, result);
            }
            else if (componentSpan.CompareToOrdinalIgnoreCaseUtf8("MONTH"u8) == 0)
            {
                return ExtractMonthImplementation(value, result);
            }
            else if (componentSpan.CompareToOrdinalIgnoreCaseUtf8("DAY"u8) == 0)
            {
                return ExtractDaysImplementation(value, result);
            }
            else if (componentSpan.CompareToOrdinalIgnoreCaseUtf8("DAY_OF_YEAR"u8) == 0)
            {
                return ExtractDayOfYearImplementation(value, result);
            }
            else if (componentSpan.CompareToOrdinalIgnoreCaseUtf8("MONDAY_DAY_OF_WEEK"u8) == 0)
            {
                return ExtractMondayDayOfWeekImplementation(value, result);
            }
            else if (componentSpan.CompareToOrdinalIgnoreCaseUtf8("SUNDAY_DAY_OF_WEEK"u8) == 0)
            {
                return ExtractSundayDayOfWeekImplementation(value, result);
            }
            else if (componentSpan.CompareToOrdinalIgnoreCaseUtf8("MONDAY_WEEK"u8) == 0)
            {
                return ExtractMondayWeekImplementation(value, result);
            }
            else if (componentSpan.CompareToOrdinalIgnoreCaseUtf8("SUNDAY_WEEK"u8) == 0)
            {
                return ExtractSundayWeekImplementation(value, result);
            }
            else if (componentSpan.CompareToOrdinalIgnoreCaseUtf8("ISO_WEEK"u8) == 0)
            {
                return ExtractWeekImplementation(value, result);
            }
            else if (componentSpan.CompareToOrdinalIgnoreCaseUtf8("US_WEEK"u8) == 0)
            {
                return ExtractUsWeekImplementation(value, result);
            }
            else if (componentSpan.CompareToOrdinalIgnoreCaseUtf8("SECOND"u8) == 0)
            {
                return ExtractSecondImplementation(value, result);
            }
            
            
            else if (componentSpan.CompareToOrdinalIgnoreCaseUtf8("MINUTE"u8) == 0)
            {
                return ExtractMinuteImplementation(value, result);
            }
            else if (componentSpan.CompareToOrdinalIgnoreCaseUtf8("MILLISECONDS"u8) == 0)
            {
                return ExtractMillisecondsImplementation(value, result);
            }

            result._type = ArrowTypeId.Null;
            return result;
        }

        internal static IDataValue ExtractYearImplementation<T1>(T1 value, DataValueContainer result)
            where T1 : IDataValue
        {
            if (value.Type != ArrowTypeId.Timestamp)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            var dt = value.AsTimestamp.ToDateTimeOffset();

            result._type = ArrowTypeId.Int64;
            result._int64Value = new Int64Value(dt.Year);
            return result;
        }

        internal static IDataValue ExtractIsoYearImplementation<T1>(T1 value, DataValueContainer result)
            where T1 : IDataValue
        {
            if (value.Type != ArrowTypeId.Timestamp)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            var dt = value.AsTimestamp.ToDateTimeOffset();
            result._type = ArrowTypeId.Int64;
            result._int64Value = new Int64Value(ISOWeek.GetYear(dt.DateTime));
            return result;
        }

        internal static IDataValue ExtractUsYearImplementation<T1>(T1 value, DataValueContainer result)
            where T1 : IDataValue
        {
            if (value.Type != ArrowTypeId.Timestamp)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }
            result._type = ArrowTypeId.Int64;

            var dt = value.AsTimestamp.ToDateTimeOffset();

            DateTimeOffset firstDayOfWeek = GetFirstDayOfWeekSunday(dt);
            DateTimeOffset lastWednesdayOfDecember = GetLastWednesdayOfDecember(firstDayOfWeek.Year);

            if (firstDayOfWeek <= lastWednesdayOfDecember)
            {
                result._int64Value = new Int64Value(firstDayOfWeek.Year);
                return result;
            }
            else
            {
                result._int64Value = new Int64Value(firstDayOfWeek.Year + 1);
                return result;
            }
        }

        private static DateTimeOffset GetFirstDayOfWeekSunday(DateTimeOffset date)
        {
            // Get the Day of Week for the given date.
            DayOfWeek dayOfWeek = date.DayOfWeek;
            int difference = dayOfWeek - DayOfWeek.Sunday;
            if (difference < 0) difference += 7; // Adjust for negative values to get the previous Sunday
            return date.AddDays(-difference);
        }

        private static DateTimeOffset GetLastWednesdayOfDecember(int year)
        {
            // Find the last Wednesday in December of the given year.
            DateTime dec31 = new DateTime(year, 12, 31);
            int daysToSubtract = (dec31.DayOfWeek - DayOfWeek.Wednesday + 7) % 7;
            return dec31.AddDays(-daysToSubtract);
        }


        internal static IDataValue ExtractWeekImplementation<T1>(T1 value, DataValueContainer result)
            where T1 : IDataValue
        {
            if (value.Type != ArrowTypeId.Timestamp)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            var dt = value.AsTimestamp.ToDateTimeOffset();
            result._type = ArrowTypeId.Int64;
            result._int64Value = new Int64Value(ISOWeek.GetWeekOfYear(dt.DateTime));
            return result;
        }

        internal static IDataValue ExtractSecondImplementation<T1>(T1 value, DataValueContainer result)
            where T1 : IDataValue
        {
            if (value.Type != ArrowTypeId.Timestamp)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            var dt = value.AsTimestamp.ToDateTimeOffset();

            result._type = ArrowTypeId.Int64;
            result._int64Value = new Int64Value(dt.Second);
            return result;
        }

        internal static IDataValue ExtractQuarterImplementation<T1>(T1 value, DataValueContainer result)
            where T1 : IDataValue
        {
            if (value.Type != ArrowTypeId.Timestamp)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            var dt = value.AsTimestamp.ToDateTimeOffset();

            result._type = ArrowTypeId.Int64;
            result._int64Value = new Int64Value((dt.Month - 1) / 3 + 1);
            return result;
        }

        internal static IDataValue ExtractMonthImplementation<T1>(T1 value, DataValueContainer result)
            where T1 : IDataValue
        {
            if (value.Type != ArrowTypeId.Timestamp)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            var dt = value.AsTimestamp.ToDateTimeOffset();

            result._type = ArrowTypeId.Int64;
            result._int64Value = new Int64Value(dt.Month);
            return result;
        }

        internal static IDataValue ExtractMinuteImplementation<T1>(T1 value, DataValueContainer result)
            where T1 : IDataValue
        {
            if (value.Type != ArrowTypeId.Timestamp)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            var dt = value.AsTimestamp.ToDateTimeOffset();

            result._type = ArrowTypeId.Int64;
            result._int64Value = new Int64Value(dt.Minute);
            return result;
        }

        internal static IDataValue ExtractMillisecondsImplementation<T1>(T1 value, DataValueContainer result)
            where T1 : IDataValue
        {
            if (value.Type != ArrowTypeId.Timestamp)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            var dt = value.AsTimestamp.ToDateTimeOffset();

            result._type = ArrowTypeId.Int64;
            result._int64Value = new Int64Value(dt.Millisecond);
            return result;
        }

        internal static IDataValue ExtractDaysImplementation<T1>(T1 value, DataValueContainer result)
            where T1 : IDataValue
        {
            if (value.Type != ArrowTypeId.Timestamp)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            var dt = value.AsTimestamp.ToDateTimeOffset();

            result._type = ArrowTypeId.Int64;
            result._int64Value = new Int64Value(dt.Day);
            return result;
        }

        internal static IDataValue ExtractDayOfYearImplementation<T1>(T1 value, DataValueContainer result)
            where T1 : IDataValue
        {
            if (value.Type != ArrowTypeId.Timestamp)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            var dt = value.AsTimestamp.ToDateTimeOffset();

            result._type = ArrowTypeId.Int64;
            result._int64Value = new Int64Value(dt.DayOfYear);
            return result;
        }

        internal static IDataValue ExtractMondayDayOfWeekImplementation<T1>(T1 value, DataValueContainer result)
            where T1 : IDataValue
        {
            if (value.Type != ArrowTypeId.Timestamp)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            var dt = value.AsTimestamp.ToDateTimeOffset();

            var dayOfWeek = (int)dt.DayOfWeek;
            dayOfWeek = dayOfWeek == 0 ? 7 : dayOfWeek;
            result._type = ArrowTypeId.Int64;
            result._int64Value = new Int64Value(dayOfWeek);
            return result;
        }

        internal static IDataValue ExtractSundayDayOfWeekImplementation<T1>(T1 value, DataValueContainer result)
            where T1 : IDataValue
        {
            if (value.Type != ArrowTypeId.Timestamp)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }
            var dt = value.AsTimestamp.ToDateTimeOffset();
            result._type = ArrowTypeId.Int64;
            result._int64Value = new Int64Value((int)dt.DayOfWeek + 1);
            return result;
        }

        internal static IDataValue ExtractMondayWeekImplementation<T1>(T1 value, DataValueContainer result)
            where T1 : IDataValue
        {
            if (value.Type != ArrowTypeId.Timestamp)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            // Return the number of the week within the year. First week starts on first Monday of January.

            var dt = value.AsTimestamp.ToDateTimeOffset();

            int lastYear = dt.Year;
            int year = dt.Year;
            int weekNumber = 0;
            do
            {
                lastYear = year;
                var firstDayOfYear = new DateTimeOffset(dt.Year, 1, 1, 0, 0, 0, dt.Offset);
                var firstMonday = firstDayOfYear.AddDays(((int)DayOfWeek.Monday - (int)firstDayOfYear.DayOfWeek + 7) % 7);

                var totalDays = (int)(dt - firstMonday).TotalDays;

                if (totalDays < 0)
                {
                    dt = dt.AddDays(totalDays);
                    year = dt.Year;
                }
                weekNumber = (int)((dt - firstMonday).TotalDays / 7) + 1;

            } while (year != lastYear);

            result._type = ArrowTypeId.Int64;
            result._int64Value = new Int64Value(weekNumber);
            return result;
        }

        internal static IDataValue ExtractSundayWeekImplementation<T1>(T1 value, DataValueContainer result)
            where T1 : IDataValue
        {
            if (value.Type != ArrowTypeId.Timestamp)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            // Return the number of the week within the year. First week starts on first Monday of January.

            var dt = value.AsTimestamp.ToDateTimeOffset();

            int lastYear = dt.Year;
            int year = dt.Year;
            int weekNumber = 0;
            do
            {
                lastYear = year;
                var firstDayOfYear = new DateTimeOffset(dt.Year, 1, 1, 0, 0, 0, dt.Offset);
                var firstSunday = firstDayOfYear.AddDays(((int)DayOfWeek.Sunday - (int)firstDayOfYear.DayOfWeek + 7) % 7);

                var totalDays = (int)(dt - firstSunday).TotalDays;

                if (totalDays < 0)
                {
                    dt = dt.AddDays(totalDays);
                    year = dt.Year;
                }
                weekNumber = (int)((dt - firstSunday).TotalDays / 7) + 1;

            } while (year != lastYear);
            
            

            result._type = ArrowTypeId.Int64;
            result._int64Value = new Int64Value(weekNumber);
            return result;
        }

        internal static IDataValue ExtractUsWeekImplementation<T1>(T1 value, DataValueContainer result)
            where T1 : IDataValue
        {
            if (value.Type != ArrowTypeId.Timestamp)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            // Return the number of the week within the year. First week starts on first Monday of January.

            var dt = value.AsTimestamp.ToDateTimeOffset();

            var firstDay = GetFirstDayOfWeekSunday(dt);
            DateTimeOffset lastWednesdayOfDecember = GetLastWednesdayOfDecember(firstDay.Year);

            return result;
        }
    }
}
