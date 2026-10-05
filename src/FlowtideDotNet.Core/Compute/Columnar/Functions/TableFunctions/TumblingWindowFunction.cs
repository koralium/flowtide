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
using FlowtideDotNet.Substrait.FunctionExtensions;
using System.Linq.Expressions;
using System.Reflection;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.TableFunctions
{
    /// <summary>
    /// Emits window_start and window_end for the single tumbling window the timestamp falls into.
    /// The size is converted to ticks at compile time, so it must be a literal
    /// with a fixed duration unit. Calendar units such as MONTH vary in length and are not allowed.
    /// </summary>
    internal static class TumblingWindowFunction
    {
        private static readonly MethodInfo _tumblingMethod = typeof(TumblingWindowFunction).GetMethod(nameof(DoTumbling), BindingFlags.Static | BindingFlags.Public)
            ?? throw new InvalidOperationException("Could not find DoTumbling method");

        public static void AddBuiltInTumblingWindowFunction(FunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterColumnTableFunction(FunctionsDatetime.Uri, FunctionsDatetime.TumblingWindow,
                (tableFunc, parameterInfo, visitor, memoryAllocator, outputParam) =>
                {
                    if (tableFunc.Arguments.Count != 3)
                    {
                        throw new ArgumentException("tumbling_window function requires three arguments: (timestamp, size_amount, size_unit)");
                    }
                    if (tableFunc.TableSchema.Names.Count != 2)
                    {
                        throw new ArgumentException("tumbling_window function requires two output columns");
                    }

                    var sizeTicks = WindowTickResolver.ResolveTicks(tableFunc.Arguments[1], tableFunc.Arguments[2], "tumbling_window", "size");

                    var timestampExpr = visitor.Visit(tableFunc.Arguments[0], parameterInfo);
                    if (timestampExpr == null)
                    {
                        throw new InvalidOperationException("tumbling_window could not compile the timestamp argument");
                    }

                    var call = Expression.Call(
                        _tumblingMethod,
                        Expression.Convert(timestampExpr, typeof(IDataValue)),
                        Expression.Constant(sizeTicks),
                        outputParam);

                    return new TableFunctionResult(call);
                });
        }

        public static void DoTumbling(IDataValue timestamp, long sizeTicks, ITableFunctionOutput output)
        {
            if (timestamp.Type != ArrowTypeId.Timestamp)
            {
                // Null or non timestamp values belong to no window
                return;
            }

            var ts = timestamp.AsTimestamp;

            // Align the window start on or before the timestamp
            long mod = ts.ticks % sizeTicks;
            if (mod < 0)
            {
                mod += sizeTicks;
            }
            long start = ts.ticks - mod;

            output.Columns[0].Add(new TimestampTzValue(start, ts.offset));
            output.Columns[1].Add(new TimestampTzValue(start + sizeTicks, ts.offset));
            output.CommitRows(1, 1, 0);
        }
    }
}
