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

using System.Globalization;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils
{
    /// <summary>
    /// Parses Spark interval strings such as "interval 7 days", null for anything else.
    /// </summary>
    internal static class DeltaInterval
    {
        public static TimeSpan? Parse(string? value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return null;
            }

            var parts = value.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries);
            int index = parts[0].Equals("interval", StringComparison.OrdinalIgnoreCase) ? 1 : 0;
            if (index == parts.Length || (parts.Length - index) % 2 != 0)
            {
                return null;
            }

            long ticks = 0;
            for (; index < parts.Length; index += 2)
            {
                if (!long.TryParse(parts[index], NumberStyles.None, CultureInfo.InvariantCulture, out var amount))
                {
                    return null;
                }
                var unitTicks = UnitTicks(parts[index + 1]);
                if (unitTicks == 0 || amount > (long.MaxValue - ticks) / unitTicks)
                {
                    return null;
                }
                ticks += amount * unitTicks;
            }
            return TimeSpan.FromTicks(ticks);
        }

        private static long UnitTicks(string unit)
        {
            switch (unit.ToLowerInvariant())
            {
                case "week":
                case "weeks":
                    return TimeSpan.TicksPerDay * 7;
                case "day":
                case "days":
                    return TimeSpan.TicksPerDay;
                case "hour":
                case "hours":
                    return TimeSpan.TicksPerHour;
                case "minute":
                case "minutes":
                    return TimeSpan.TicksPerMinute;
                case "second":
                case "seconds":
                    return TimeSpan.TicksPerSecond;
                case "millisecond":
                case "milliseconds":
                    return TimeSpan.TicksPerMillisecond;
                case "microsecond":
                case "microseconds":
                    return TimeSpan.TicksPerMicrosecond;
                default:
                    return 0;
            }
        }
    }
}
