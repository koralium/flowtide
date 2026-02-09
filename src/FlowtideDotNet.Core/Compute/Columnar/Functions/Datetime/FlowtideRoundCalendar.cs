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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.Datetime
{
    internal static class FlowtideRoundCalendar
    {
        public enum RoundingMode
        {
            Floor,
            Ceil,
            RoundTieDown,
            RoundTieUp
        }

        public enum CalendarUnit
        {
            Year,
            Month,
            Week,
            Day,
            Hour,
            Minute,
            Second,
            Millisecond
        }

        public enum CalendarOrigin
        {
            Year,
            Month,
            MondayWeek,
            SundayWeek,
            IsoWeek,
            UsWeek,
            Day,
            Hour,
            Minute,
            Second,
            Millisecond
        }

        public static DateTime RoundCalendarScalar(
        DateTime input,
        RoundingMode rounding,
        CalendarUnit unit,
        CalendarOrigin origin,
        int multiple,
        TimeZoneInfo? tz)
        {
            if (multiple <= 0)
                throw new ArgumentException("multiple must be > 0");

            // timezone → local calendar
            DateTime local = tz == null
                ? input
                : TimeZoneInfo.ConvertTime(input, tz);

            // snap to origin boundary
            DateTime anchor = SnapToOrigin(local, origin);

            // floor bucket using math (not loops)
            DateTime prev = FloorToGrid(local, anchor, unit, multiple);

            // next bucket
            DateTime next = AdvanceCalendar(prev, unit, multiple);

            // rounding
            DateTime chosen = ApplyRounding(local, prev, next, rounding);

            // convert back
            return tz == null
                ? chosen
                : TimeZoneInfo.ConvertTime(chosen, tz, input.Kind == DateTimeKind.Utc
                    ? TimeZoneInfo.Utc
                    : tz);
        }

        private static DateTime SnapToOrigin(DateTime dt, CalendarOrigin origin)
        {
            switch (origin)
            {
                case CalendarOrigin.Year:
                    return new DateTime(dt.Year, 1, 1, 0, 0, 0, dt.Kind);

                case CalendarOrigin.Month:
                    return new DateTime(dt.Year, dt.Month, 1, 0, 0, 0, dt.Kind);

                case CalendarOrigin.MondayWeek:
                case CalendarOrigin.IsoWeek:
                    return StartOfWeek(dt, DayOfWeek.Monday);

                case CalendarOrigin.SundayWeek:
                case CalendarOrigin.UsWeek:
                    return StartOfWeek(dt, DayOfWeek.Sunday);

                case CalendarOrigin.Day:
                    return dt.Date;

                case CalendarOrigin.Hour:
                    return new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, 0, 0, dt.Kind);

                case CalendarOrigin.Minute:
                    return new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, 0, dt.Kind);

                case CalendarOrigin.Second:
                    return new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second, dt.Kind);

                case CalendarOrigin.Millisecond:
                    return new DateTime(dt.Year, dt.Month, dt.Day,
                                        dt.Hour, dt.Minute, dt.Second, dt.Millisecond, dt.Kind);

                default:
                    throw new ArgumentOutOfRangeException(nameof(origin));
            }
        }

        private static DateTime StartOfWeek(DateTime dt, DayOfWeek start)
        {
            int diff = (7 + (dt.DayOfWeek - start)) % 7;
            return dt.Date.AddDays(-diff);
        }

        private static DateTime FloorToGrid(DateTime input, DateTime anchor, CalendarUnit unit, int multiple)
        {
            if (input < anchor)
            {
                DateTime cur = anchor;
                do
                {
                    cur = AdvanceCalendar(cur, unit, -multiple);
                } while (cur > input);

                return cur;
            }
            else
            {
                DateTime cur = anchor;
                DateTime next = AdvanceCalendar(cur, unit, multiple);

                while (next <= input)
                {
                    cur = next;
                    next = AdvanceCalendar(cur, unit, multiple);
                }

                return cur;
            }
        }

        private static DateTime AdvanceCalendar(DateTime dt, CalendarUnit unit, int amount)
        {
            switch (unit)
            {
                case CalendarUnit.Year:
                    return dt.AddYears(amount);

                case CalendarUnit.Month:
                    return dt.AddMonths(amount);

                case CalendarUnit.Week:
                    return dt.AddDays(amount * 7);

                case CalendarUnit.Day:
                    return dt.AddDays(amount);

                case CalendarUnit.Hour:
                    return dt.AddHours(amount);

                case CalendarUnit.Minute:
                    return dt.AddMinutes(amount);

                case CalendarUnit.Second:
                    return dt.AddSeconds(amount);

                case CalendarUnit.Millisecond:
                    return dt.AddMilliseconds(amount);

                default:
                    throw new ArgumentOutOfRangeException(nameof(unit));
            }
        }

        private static DateTime ApplyRounding(
        DateTime input,
        DateTime prev,
        DateTime next,
        RoundingMode mode)
        {
            switch (mode)
            {
                case RoundingMode.Floor:
                    return prev;

                case RoundingMode.Ceil:
                    return next;

                case RoundingMode.RoundTieDown:
                case RoundingMode.RoundTieUp:
                    var distPrev = input - prev;
                    var distNext = next - input;

                    if (distPrev < distNext) return prev;
                    if (distNext < distPrev) return next;

                    return mode == RoundingMode.RoundTieDown ? prev : next;

                default:
                    throw new ArgumentOutOfRangeException(nameof(mode));
            }
        }


    }
}
