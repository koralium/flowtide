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
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System.Globalization;
using System.Reflection;
using static SqlParser.Ast.AlterRoleOperation;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions
{
    internal static class BuiltInDatetimeFunctions
    {
        public static void AddBuiltInDatetimeFunctions(FunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterScalarMethod(FunctionsDatetime.Uri, FunctionsDatetime.Strftime, typeof(BuiltInDatetimeFunctions), nameof(StrfTimeImplementation));
            functionsRegister.RegisterScalarMethod(FunctionsDatetime.Uri, FunctionsDatetime.FloorTimestampDay, typeof(BuiltInDatetimeFunctions), nameof(FloorTimestampDayImplementation));
            functionsRegister.RegisterScalarMethod(FunctionsDatetime.Uri, FunctionsDatetime.ParseTimestamp, typeof(BuiltInDatetimeFunctions), nameof(TimestampParseImplementation));

            functionsRegister.RegisterColumnScalarFunction(FunctionsDatetime.Uri, FunctionsDatetime.Extract,
                (function, parameterInfo, visitor, functionServices) =>
                {
                    if (function.Arguments.Count != 2)
                    {
                        throw new InvalidOperationException("Extract function must have two arguments");
                    }

                    var componentArg = function.Arguments[0];
                    var valueArg = function.Arguments[1];

                    var valueExpr = visitor.Visit(valueArg, parameterInfo);

                    if (valueExpr == null)
                    {
                        throw new InvalidOperationException("Value argument could not be compiled for extract");
                    }

                    if (componentArg is StringLiteral stringLiteral)
                    {
                        var component = stringLiteral.Value.ToUpper();

                        switch (component)
                        {
                            case "YEAR":
                                return CallExtractFunction(nameof(ExtractYearImplementation), valueExpr);
                            case "ISO_YEAR":
                                return CallExtractFunction(nameof(ExtractIsoYearImplementation), valueExpr);
                            case "US_YEAR":
                                return CallExtractFunction(nameof(ExtractUsYearImplementation), valueExpr);
                            case "QUARTER":
                                return CallExtractFunction(nameof(ExtractQuarterImplementation), valueExpr);
                            case "MONTH":
                                return CallExtractFunction(nameof(ExtractMonthImplementation), valueExpr);
                            case "DAY":
                                return CallExtractFunction(nameof(ExtractDaysImplementation), valueExpr);
                            case "DAY_OF_YEAR":
                                return CallExtractFunction(nameof(ExtractDayOfYearImplementation), valueExpr);
                            case "MONDAY_DAY_OF_WEEK":
                                return CallExtractFunction(nameof(ExtractMondayDayOfWeekImplementation), valueExpr);
                            case "SUNDAY_DAY_OF_WEEK":
                                return CallExtractFunction(nameof(ExtractSundayDayOfWeekImplementation), valueExpr);
                            case "MONDAY_WEEK":
                                return CallExtractFunction(nameof(ExtractMondayWeekImplementation), valueExpr);
                            case "SUNDAY_WEEK":
                                return CallExtractFunction(nameof(ExtractSundayWeekImplementation), valueExpr);
                            case "ISO_WEEK":
                                return CallExtractFunction(nameof(ExtractWeekImplementation), valueExpr);
                            case "US_WEEK":
                                return CallExtractFunction(nameof(ExtractUsWeekImplementation), valueExpr);
                            case "HOUR":
                                return CallExtractFunction(nameof(ExtractHoursImplementation), valueExpr);
                            case "SECOND":
                                return CallExtractFunction(nameof(ExtractSecondImplementation), valueExpr);
                            case "MINUTE":
                                return CallExtractFunction(nameof(ExtractMinuteImplementation), valueExpr);
                            case "MILLISECOND":
                                return CallExtractFunction(nameof(ExtractMillisecondsImplementation), valueExpr);
                            case "MICROSECOND":
                                return CallExtractFunction(nameof(ExtractMicrosecondsImplementation), valueExpr);
                            default:
                                throw new InvalidOperationException($"Unknown component {component} for extract function");
                        }
                    }

                    var method = typeof(BuiltInDatetimeFunctions).GetMethod(nameof(ExtractImplementation), BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic);

                    var componentExpr = visitor.Visit(componentArg, parameterInfo);


                    var genericMethod = method!.MakeGenericMethod(componentExpr!.Type, valueExpr!.Type);

                    System.Linq.Expressions.Expression[] parameters = new System.Linq.Expressions.Expression[3];
                    parameters[0] = componentExpr;
                    parameters[1] = valueExpr;
                    parameters[2] = System.Linq.Expressions.Expression.Constant(new DataValueContainer());

                    var call = System.Linq.Expressions.Expression.Call(genericMethod, parameters);
                    return call;
                });

            functionsRegister.RegisterColumnScalarFunction(FunctionsDatetime.Uri, FunctionsDatetime.Format,
                (function, parametersInfo, visitor, functionServices) =>
                {
                    if (function.Arguments.Count != 2)
                    {
                        throw new InvalidOperationException("Format function must have two arguments");
                    }

                    var valueArg = function.Arguments[0];
                    var formatArg = function.Arguments[1];

                    var valueExpr = visitor.Visit(valueArg, parametersInfo);

                    if (valueExpr == null)
                    {
                        throw new InvalidOperationException("Value argument could not be compiled for format");
                    }

                    var memory = new Memory<byte>(new byte[256]);

                    var memoryConstant = System.Linq.Expressions.Expression.Constant(memory);

                    var resultConstant = System.Linq.Expressions.Expression.Constant(new DataValueContainer());

                    if (formatArg is StringLiteral stringLiteral)
                    {
                        var format = stringLiteral.Value.ToString();

                        var staticFormatMethod = typeof(BuiltInDatetimeFunctions).GetMethod(nameof(FormatImplementationStaticFormat), BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)
                            ?.MakeGenericMethod(valueExpr.Type);

                        if (staticFormatMethod == null)
                        {
                            throw new InvalidOperationException($"Method {nameof(FormatImplementationStaticFormat)} not found");
                        }

                        return System.Linq.Expressions.Expression.Call(staticFormatMethod, [valueExpr, System.Linq.Expressions.Expression.Constant(format), memoryConstant, resultConstant]);
                    }

                    var formatExpr = visitor.Visit(formatArg, parametersInfo);

                    if (formatExpr == null)
                    {
                        throw new InvalidOperationException("Format argument could not be compiled for format");
                    }

                    var method = typeof(BuiltInDatetimeFunctions).GetMethod(nameof(FormatImplementation), BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)
                        ?.MakeGenericMethod(valueExpr.Type, formatExpr.Type);

                    if (method == null)
                    {
                        throw new InvalidOperationException($"Method {nameof(FormatImplementation)} not found");
                    }

                    return System.Linq.Expressions.Expression.Call(method, [valueExpr, formatExpr, memoryConstant, resultConstant]);
                });

            functionsRegister.RegisterColumnScalarFunction(FunctionsDatetime.Uri, FunctionsDatetime.TimestampAdd,
                (function, parameterInfo, visitor, functionServices) =>
                {
                    if (function.Arguments.Count != 3)
                    {
                        throw new InvalidOperationException("Timestamp_add function must have three arguments");
                    }

                    var componentArg = function.Arguments[0];
                    var amountArg = function.Arguments[1];
                    var valueArg = function.Arguments[2];

                    var amountExpr = visitor.Visit(amountArg, parameterInfo);

                    if (amountExpr == null)
                    {
                        throw new InvalidOperationException("Amount argument could not be compiled for timestamp_add");
                    }

                    var valueExpr = visitor.Visit(valueArg, parameterInfo);

                    if (valueExpr == null)
                    {
                        throw new InvalidOperationException("Value argument could not be compiled for timestamp_add");
                    }

                    if (componentArg is StringLiteral stringLiteral)
                    {
                        // If the component is hard-coded we can directly find the correct function
                        var component = stringLiteral.Value.ToUpper();

                        switch (component)
                        {
                            case "YEAR":
                                return CallTimestampAddFunction(nameof(TimestampAddYear), amountExpr, valueExpr);
                            case "QUARTER":
                                return CallTimestampAddFunction(nameof(TimestampAddQuarter), amountExpr, valueExpr);
                            case "MONTH":
                                return CallTimestampAddFunction(nameof(TimestampAddMonth), amountExpr, valueExpr);
                            case "WEEK":
                                return CallTimestampAddFunction(nameof(TimestampAddWeek), amountExpr, valueExpr);
                            case "DAY":
                                return CallTimestampAddFunction(nameof(TimestampAddDays), amountExpr, valueExpr);    
                            case "HOUR":
                                return CallTimestampAddFunction(nameof(TimestampAddHours), amountExpr, valueExpr);
                            case "MINUTE":
                                return CallTimestampAddFunction(nameof(TimestampAddMinutes), amountExpr, valueExpr);
                            case "SECOND":
                                return CallTimestampAddFunction(nameof(TimestampAddSeconds), amountExpr, valueExpr);
                            case "MILLISECOND":
                                return CallTimestampAddFunction(nameof(TimestampAddMilliseconds), amountExpr, valueExpr);
                            case "MICROSECOND":
                                return CallTimestampAddFunction(nameof(TimestampAddMicroseconds), amountExpr, valueExpr);
                            default:
                                throw new InvalidOperationException($"Unknown component {component} for timestamp_add function");
                        }
                    }

                    var method = typeof(BuiltInDatetimeFunctions).GetMethod(nameof(TimestampAdd), BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic);

                    var componentExpr = visitor.Visit(componentArg, parameterInfo);

                    var genericMethod = method!.MakeGenericMethod(componentExpr!.Type, amountExpr.Type, valueExpr.Type);

                    System.Linq.Expressions.Expression[] parameters =
                    [
                        componentExpr,
                        amountExpr,
                        valueExpr,
                        System.Linq.Expressions.Expression.Constant(new DataValueContainer()),
                    ];
                    var call = System.Linq.Expressions.Expression.Call(genericMethod, parameters);
                    return call;
                });
        }

        private static System.Linq.Expressions.Expression CallExtractFunction(string methodName, System.Linq.Expressions.Expression valueExpr)
        {
            var method = typeof(BuiltInDatetimeFunctions).GetMethod(methodName, BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic);

            if (method == null)
            {
                throw new InvalidOperationException($"Method {methodName} not found");
            }

            var genericMethod = method!.MakeGenericMethod(valueExpr.Type);

            System.Linq.Expressions.Expression[] parameters = [valueExpr, System.Linq.Expressions.Expression.Constant(new DataValueContainer())];

            var call = System.Linq.Expressions.Expression.Call(genericMethod, parameters);
            return call;
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
            else if (componentSpan.CompareToOrdinalIgnoreCaseUtf8("HOUR"u8) == 0)
            {
                return ExtractHoursImplementation(value, result);
            }
            else if (componentSpan.CompareToOrdinalIgnoreCaseUtf8("SECOND"u8) == 0)
            {
                return ExtractSecondImplementation(value, result);
            }
            else if (componentSpan.CompareToOrdinalIgnoreCaseUtf8("MINUTE"u8) == 0)
            {
                return ExtractMinuteImplementation(value, result);
            }
            else if (componentSpan.CompareToOrdinalIgnoreCaseUtf8("MILLISECOND"u8) == 0)
            {
                return ExtractMillisecondsImplementation(value, result);
            }
            else if (componentSpan.CompareToOrdinalIgnoreCaseUtf8("MICROSECOND"u8) == 0)
            {
                return ExtractMicrosecondsImplementation(value, result);
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

        internal static IDataValue ExtractMicrosecondsImplementation<T1>(T1 value, DataValueContainer result)
            where T1 : IDataValue
        {
            if (value.Type != ArrowTypeId.Timestamp)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            var dt = value.AsTimestamp.ToDateTimeOffset();

            result._type = ArrowTypeId.Int64;
            result._int64Value = new Int64Value(dt.Microsecond);
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

        internal static IDataValue ExtractHoursImplementation<T1>(T1 value, DataValueContainer result)
            where T1 : IDataValue
        {
            if (value.Type != ArrowTypeId.Timestamp)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            var dt = value.AsTimestamp.ToDateTimeOffset();

            result._type = ArrowTypeId.Int64;
            result._int64Value = new Int64Value(dt.Hour);
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

            var dt = value.AsTimestamp.ToDateTimeOffset();

            DateTime weekStart = dt.Date.AddDays(-(int)dt.DayOfWeek);

            DateTime weekThursday = weekStart.AddDays(4);
            var epiYear = weekThursday.Year;

            DateTime jan1 = new DateTime(epiYear, 1, 1);
            DateTime firstSunday = jan1.AddDays(-(int)jan1.DayOfWeek);

            int janDays = 0;
            for (int i = 0; i < 7; i++)
            {
                if (firstSunday.AddDays(i).Month == 1)
                    janDays++;
            }

            if (janDays < 4)
            {
                firstSunday = firstSunday.AddDays(7);
            }

            int weekNumber = ((weekStart - firstSunday).Days / 7) + 1;

            result._type = ArrowTypeId.Int64;
            result._int64Value = new Int64Value(weekNumber);
            return result;
        }

        internal static IDataValue FormatImplementation<T1, T2>(T1 value, T2 format, Memory<byte> memory, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (value.Type != ArrowTypeId.Timestamp)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }
            if (format.Type != ArrowTypeId.String)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            var formatString = format.AsString.ToString();
            return FormatImplementationStaticFormat(value, formatString, memory, result);

        }

        internal static IDataValue FormatImplementationStaticFormat<T1>(T1 value, string format, Memory<byte> memory, DataValueContainer result)
            where T1 : IDataValue
        {
            if (value.Type != ArrowTypeId.Timestamp)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            var dt = value.AsTimestamp.ToDateTimeOffset();

            if (dt.TryFormat(memory.Span, out var bytesWritten, format))
            {
                result._type = ArrowTypeId.String;
                result._stringValue = new StringValue(memory.Slice(0, bytesWritten));
                return result;
            }
            else
            {
                result._type = ArrowTypeId.Null;
                return result;
            }
        }

        internal static IDataValue TimestampAdd<T1, T2, T3>(T1 component, T2 amount, T3 value, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
            where T3 : IDataValue
        {
            if (component.Type != ArrowTypeId.String)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }
            if (value.Type != ArrowTypeId.Timestamp)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            var componentStr = component.AsString;

            if (componentStr.Span.CompareToOrdinalIgnoreCaseUtf8("YEAR"u8) == 0)
            {
                return TimestampAddYear(amount, value, result);
            }
            else if (componentStr.Span.CompareToOrdinalIgnoreCaseUtf8("QUARTER"u8) == 0)
            {
                return TimestampAddQuarter(amount, value, result);
            }
            else if (componentStr.Span.CompareToOrdinalIgnoreCaseUtf8("MONTH"u8) == 0)
            {
                return TimestampAddMonth(amount, value, result);
            }
            else if (componentStr.Span.CompareToOrdinalIgnoreCaseUtf8("WEEK"u8) == 0)
            {
                return TimestampAddWeek(amount, value, result);
            }
            else if (componentStr.Span.CompareToOrdinalIgnoreCaseUtf8("DAY"u8) == 0)
            {
                return TimestampAddDays(amount, value, result);
            }
            else if (componentStr.Span.CompareToOrdinalIgnoreCaseUtf8("HOUR"u8) == 0)
            {
                return TimestampAddHours(amount, value, result);
            }
            else if (componentStr.Span.CompareToOrdinalIgnoreCaseUtf8("MINUTE"u8) == 0)
            {
                return TimestampAddMinutes(amount, value, result);
            }
            else if (componentStr.Span.CompareToOrdinalIgnoreCaseUtf8("SECOND"u8) == 0)
            {
                return TimestampAddSeconds(amount, value, result);
            }
            else if (componentStr.Span.CompareToOrdinalIgnoreCaseUtf8("MILLISECOND"u8) == 0)
            {
                return TimestampAddMilliseconds(amount, value, result);
            }
            else if (componentStr.Span.CompareToOrdinalIgnoreCaseUtf8("MICROSECOND"u8) == 0)
            {
                return TimestampAddMicroseconds(amount, value, result);
            }

            result._type = ArrowTypeId.Null;
            return result;
        }

        private static System.Linq.Expressions.Expression CallTimestampAddFunction(string methodName, System.Linq.Expressions.Expression amountExpr, System.Linq.Expressions.Expression valueExpr)
        {
            var method = typeof(BuiltInDatetimeFunctions).GetMethod(methodName, BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic);

            if (method == null)
            {
                throw new InvalidOperationException($"Method {methodName} not found");
            }

            var genericMethod = method!.MakeGenericMethod(amountExpr.Type, valueExpr.Type);

            System.Linq.Expressions.Expression[] parameters = [amountExpr, valueExpr, System.Linq.Expressions.Expression.Constant(new DataValueContainer())];

            var call = System.Linq.Expressions.Expression.Call(genericMethod, parameters);
            return call;
        }

        private static bool TryGetAmountAndValue<T1, T2>(
            T1 amount,
            T2 value,
            out long amountValue,
            out DateTimeOffset datetime)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (amount.Type != ArrowTypeId.Int64)
            {
                amountValue = 0;
                datetime = default;
                return false;
            }

            if (value.Type != ArrowTypeId.Timestamp)
            {
                amountValue = 0;
                datetime = default;
                return false;
            }

            amountValue = amount.AsLong;
            datetime = value.AsTimestamp.ToDateTimeOffset();
            return true;
        }

        internal static IDataValue TimestampAddYear<T1, T2>(T1 amount, T2 value, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (!TryGetAmountAndValue(amount, value, out var amountValue, out var dt))
            {
                result._type = ArrowTypeId.Null;
                return result;
            }
            var newDate = dt.AddYears((int)amountValue);
            result._type = ArrowTypeId.Timestamp;
            result._timestampValue = new TimestampTzValue(newDate);
            return result;
        }

        internal static IDataValue TimestampAddQuarter<T1, T2>(T1 amount, T2 value, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (!TryGetAmountAndValue(amount, value, out var amountValue, out var dt))
            {
                result._type = ArrowTypeId.Null;
                return result;
            }
            var newDate = dt.AddMonths((int)amountValue * 3);
            result._type = ArrowTypeId.Timestamp;
            result._timestampValue = new TimestampTzValue(newDate);
            return result;
        }

        internal static IDataValue TimestampAddMonth<T1, T2>(T1 amount, T2 value, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (!TryGetAmountAndValue(amount, value, out var amountValue, out var dt))
            {
                result._type = ArrowTypeId.Null;
                return result;
            }
            var newDate = dt.AddMonths((int)amountValue);
            result._type = ArrowTypeId.Timestamp;
            result._timestampValue = new TimestampTzValue(newDate);
            return result;
        }

        internal static IDataValue TimestampAddWeek<T1, T2>(T1 amount, T2 value, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (!TryGetAmountAndValue(amount, value, out var amountValue, out var dt))
            {
                result._type = ArrowTypeId.Null;
                return result;
            }
            var newDate = dt.AddDays(amountValue * 7);
            result._type = ArrowTypeId.Timestamp;
            result._timestampValue = new TimestampTzValue(newDate);
            return result;
        }

        internal static IDataValue TimestampAddDays<T1, T2>(T1 amount, T2 value, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (!TryGetAmountAndValue(amount, value, out var amountValue, out var dt))
            {
                result._type = ArrowTypeId.Null;
                return result;
            }
            var newDate = dt.AddDays(amountValue);
            result._type = ArrowTypeId.Timestamp;
            result._timestampValue = new TimestampTzValue(newDate);
            return result;
        }

        internal static IDataValue TimestampAddHours<T1, T2>(T1 amount, T2 value, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (!TryGetAmountAndValue(amount, value, out var amountValue, out var dt))
            {
                result._type = ArrowTypeId.Null;
                return result;
            }
            var newDate = dt.AddHours(amountValue);
            result._type = ArrowTypeId.Timestamp;
            result._timestampValue = new TimestampTzValue(newDate);
            return result;
        }

        internal static IDataValue TimestampAddMinutes<T1, T2>(T1 amount, T2 value, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (!TryGetAmountAndValue(amount, value, out var amountValue, out var dt))
            {
                result._type = ArrowTypeId.Null;
                return result;
            }
            var newDate = dt.AddMinutes(amountValue);
            result._type = ArrowTypeId.Timestamp;
            result._timestampValue = new TimestampTzValue(newDate);
            return result;
        }

        internal static IDataValue TimestampAddSeconds<T1, T2>(T1 amount, T2 value, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (!TryGetAmountAndValue(amount, value, out var amountValue, out var dt))
            {
                result._type = ArrowTypeId.Null;
                return result;
            }
            var newDate = dt.AddSeconds(amountValue);
            result._type = ArrowTypeId.Timestamp;
            result._timestampValue = new TimestampTzValue(newDate);
            return result;
        }

        internal static IDataValue TimestampAddMilliseconds<T1, T2>(T1 amount, T2 value, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (!TryGetAmountAndValue(amount, value, out var amountValue, out var dt))
            {
                result._type = ArrowTypeId.Null;
                return result;
            }
            var newDate = dt.AddMilliseconds(amountValue);
            result._type = ArrowTypeId.Timestamp;
            result._timestampValue = new TimestampTzValue(newDate);
            return result;
        }

        internal static IDataValue TimestampAddMicroseconds<T1, T2>(T1 amount, T2 value, DataValueContainer result)
            where T1 : IDataValue
            where T2 : IDataValue
        {
            if (!TryGetAmountAndValue(amount, value, out var amountValue, out var dt))
            {
                result._type = ArrowTypeId.Null;
                return result;
            }
            var newDate = dt.AddMicroseconds(amountValue);
            result._type = ArrowTypeId.Timestamp;
            result._timestampValue = new TimestampTzValue(newDate);
            return result;
        }
    }
}
