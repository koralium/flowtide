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
using FlowtideDotNet.Substrait.FunctionExtensions;
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
    }
}
