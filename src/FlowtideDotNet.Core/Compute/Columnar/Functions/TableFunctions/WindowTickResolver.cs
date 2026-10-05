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

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.TableFunctions
{
    /// <summary>
    /// Resolves literal amount and unit arguments to ticks for window table functions.
    /// </summary>
    internal static class WindowTickResolver
    {
        public static long ResolveTicks(Substrait.Expressions.Expression amountArg, Substrait.Expressions.Expression unitArg, string functionName, string which)
        {
            if (amountArg is not NumericLiteral amount)
            {
                throw new ArgumentException($"{functionName} {which} amount must be a numeric literal");
            }
            if (unitArg is not StringLiteral unit)
            {
                throw new ArgumentException($"{functionName} {which} unit must be a string literal");
            }

            long ticksPerUnit = unit.Value.ToUpperInvariant() switch
            {
                "WEEK" => TimeSpan.TicksPerDay * 7,
                "DAY" => TimeSpan.TicksPerDay,
                "HOUR" => TimeSpan.TicksPerHour,
                "MINUTE" => TimeSpan.TicksPerMinute,
                "SECOND" => TimeSpan.TicksPerSecond,
                "MILLISECOND" => TimeSpan.TicksPerMillisecond,
                "MICROSECOND" => TimeSpan.TicksPerMicrosecond,
                _ => throw new ArgumentException($"{functionName} {which} unit '{unit.Value}' is not supported. Use WEEK, DAY, HOUR, MINUTE, SECOND, MILLISECOND or MICROSECOND.")
            };

            // A fraction can always be written with a smaller unit, so reject it instead of truncating
            if (amount.Value != decimal.Truncate(amount.Value))
            {
                throw new ArgumentException($"{functionName} {which} amount must be a whole number, use a smaller unit instead");
            }
            if (amount.Value <= 0)
            {
                throw new ArgumentException($"{functionName} {which} must be a positive duration");
            }
            // Check the range before multiplying, the multiplication would otherwise overflow silently
            if (amount.Value > long.MaxValue / ticksPerUnit)
            {
                throw new ArgumentException($"{functionName} {which} is too large");
            }
            return (long)amount.Value * ticksPerUnit;
        }
    }
}
