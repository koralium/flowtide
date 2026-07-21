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
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System.Linq.Expressions;
using System.Reflection;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.TableFunctions
{
    /// <summary>
    /// Emits one window_start/window_end row for each hopping window the timestamp falls into.
    /// Hop and size are converted to ticks at compile time, so they must be literals
    /// with a fixed duration unit. Calendar units such as MONTH vary in length and are not allowed.
    /// </summary>
    internal static class HoppingWindowFunction
    {
        // Guard against a tiny hop with a large size, it would emit a huge amount of rows
        private const long MaxWindowsPerRow = 100_000;

        private static readonly MethodInfo _hoppingMethod = typeof(HoppingWindowFunction).GetMethod(nameof(DoHopping), BindingFlags.Static | BindingFlags.Public)
            ?? throw new InvalidOperationException("Could not find DoHopping method");

        public static void AddBuiltInHoppingWindowFunction(FunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterColumnTableFunction(FunctionsDatetime.Uri, FunctionsDatetime.HoppingWindow,
                (tableFunc, parameterInfo, visitor, memoryAllocator, outputParam) =>
                {
                    if (tableFunc.Arguments.Count != 5)
                    {
                        throw new ArgumentException("hopping_window function requires five arguments: (timestamp, hop_amount, hop_unit, size_amount, size_unit)");
                    }
                    if (tableFunc.TableSchema.Names.Count != 2)
                    {
                        throw new ArgumentException("hopping_window function requires two output columns");
                    }

                    var hopTicks = ResolveTicks(tableFunc.Arguments[1], tableFunc.Arguments[2], "hop");
                    var sizeTicks = ResolveTicks(tableFunc.Arguments[3], tableFunc.Arguments[4], "size");

                    // Floor division, the real count is at most one higher, but this avoids overflow
                    var maxWindowsPerRow = sizeTicks / hopTicks;
                    if (maxWindowsPerRow > MaxWindowsPerRow)
                    {
                        throw new ArgumentException($"hopping_window would produce up to {maxWindowsPerRow + 1} windows per row (size / hop), exceeding the limit of {MaxWindowsPerRow}. Increase the hop or decrease the size.");
                    }

                    var timestampExpr = visitor.Visit(tableFunc.Arguments[0], parameterInfo);
                    if (timestampExpr == null)
                    {
                        throw new InvalidOperationException("hopping_window could not compile the timestamp argument");
                    }

                    var call = Expression.Call(
                        _hoppingMethod,
                        Expression.Convert(timestampExpr, typeof(IDataValue)),
                        Expression.Constant(hopTicks),
                        Expression.Constant(sizeTicks),
                        outputParam);

                    return new TableFunctionResult(call);
                });
        }

        private static long ResolveTicks(Substrait.Expressions.Expression amountArg, Substrait.Expressions.Expression unitArg, string which)
        {
            if (amountArg is not NumericLiteral amount)
            {
                throw new ArgumentException($"hopping_window {which} amount must be a numeric literal");
            }
            if (unitArg is not StringLiteral unit)
            {
                throw new ArgumentException($"hopping_window {which} unit must be a string literal");
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
                _ => throw new ArgumentException($"hopping_window {which} unit '{unit.Value}' is not supported. Use WEEK, DAY, HOUR, MINUTE, SECOND, MILLISECOND or MICROSECOND.")
            };

            var ticks = (long)amount.Value * ticksPerUnit;
            if (ticks <= 0)
            {
                throw new ArgumentException($"hopping_window {which} must be a positive duration");
            }
            return ticks;
        }

        public static void DoHopping(IDataValue timestamp, long hopTicks, long sizeTicks, ITableFunctionOutput output)
        {
            if (timestamp.Type != ArrowTypeId.Timestamp)
            {
                // Null or non timestamp values belong to no window
                return;
            }

            var ts = timestamp.AsTimestamp;
            long t = ts.ticks;
            long offset = ts.offset;

            var startColumn = output.Columns[0];
            var endColumn = output.Columns[1];

            // Find the last window start before or on the timestamp
            long mod = t % hopTicks;
            if (mod < 0)
            {
                mod += hopTicks;
            }
            long lastStart = t - mod;

            // Go backwards as long as the window still contains the timestamp
            int count = 0;
            for (long start = lastStart; start > t - sizeTicks; start -= hopTicks)
            {
                startColumn.Add(new TimestampTzValue(start, offset));
                endColumn.Add(new TimestampTzValue(start + sizeTicks, offset));
                count++;
            }

            if (count > 0)
            {
                output.CommitRows(count, 1, 0);
            }
        }
    }
}
