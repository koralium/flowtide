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
using System.Diagnostics;
using System.Linq.Expressions;
using System.Text;

namespace FlowtideDotNet.Core.Compute.Columnar
{
    internal static class ColumnCastImplementations
    {
        private static Int64Value Int1Value = new Int64Value(1);
        private static Int64Value Int0Value = new Int64Value(0);
        private static DecimalValue Decimal1Value = new DecimalValue(1);
        private static DecimalValue Decimal0Value = new DecimalValue(0);
        private static DoubleValue OneDoubleValue = new DoubleValue(1.0);
        private static DoubleValue ZeroDoubleValue = new DoubleValue(0.0);

        internal class CastToStringContainer
        {
            public byte[] utf8bytes = new byte[128];
        }

        internal static Expression CallCastToString(Expression value, Expression result)
        {
            var genericMethod = typeof(ColumnCastImplementations).GetMethod(nameof(CastToString), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
            Debug.Assert(genericMethod != null);
            var method = genericMethod.MakeGenericMethod(value.Type);
            var container = Expression.Constant(new CastToStringContainer());
            return Expression.Call(method, value, result, container);
        }

        internal static Expression CallCastToInt(Expression value, Expression result)
        {
            var genericMethod = typeof(ColumnCastImplementations).GetMethod(nameof(CastToInt), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
            Debug.Assert(genericMethod != null);
            var method = genericMethod.MakeGenericMethod(value.Type);
            return Expression.Call(method, value, result);
        }

        internal static Expression CallCastToDecimal(Expression value, Expression result)
        {
            var genericMethod = typeof(ColumnCastImplementations).GetMethod(nameof(CastToDecimal), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
            Debug.Assert(genericMethod != null);
            var method = genericMethod.MakeGenericMethod(value.Type);
            return Expression.Call(method, value, result);
        }

        internal static Expression CallCastToBool(Expression value, Expression result)
        {
            var genericMethod = typeof(ColumnCastImplementations).GetMethod(nameof(CastToBool), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
            Debug.Assert(genericMethod != null);
            var method = genericMethod.MakeGenericMethod(value.Type);
            return Expression.Call(method, value, result);
        }

        internal static Expression CallCastToDouble(Expression value, Expression result)
        {
            var genericMethod = typeof(ColumnCastImplementations).GetMethod(nameof(CastToDouble), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
            Debug.Assert(genericMethod != null);
            var method = genericMethod.MakeGenericMethod(value.Type);
            return Expression.Call(method, value, result);
        }

        internal static Expression CallCastToTimestamp(Expression value, Expression result)
        {
            var genericMethod = typeof(ColumnCastImplementations).GetMethod(nameof(CastToTimestamp), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
            Debug.Assert(genericMethod != null);
            var method = genericMethod.MakeGenericMethod(value.Type);
            return Expression.Call(method, value, result);
        }

        internal static IDataValue CastToTimestamp<T>(T value, DataValueContainer result)
            where T : IDataValue
        {
            if (value.IsNull)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            if (value.Type == ArrowTypeId.Timestamp)
            {
                result._type = ArrowTypeId.Timestamp;
                result._timestampValue = value.AsTimestamp;
                return result;
            }
            if (value.Type == ArrowTypeId.String)
            {
                if (DateTimeOffset.TryParse(value.AsString.ToString(), out var parsedTime))
                {
                    result._type = ArrowTypeId.Timestamp;
                    result._timestampValue = new TimestampTzValue(parsedTime.Ticks, (short)parsedTime.Offset.TotalMinutes);
                    return result;
                }
            }
            if (value.Type == ArrowTypeId.Int64)
            {
                result._type = ArrowTypeId.Timestamp;
                // Time is in ticks from unix time
                result._timestampValue = TimestampTzValue.FromUnixMicroseconds(value.AsLong / 10);
                return result;
            }

            result._type = ArrowTypeId.Null;
            return result;
        }

        internal static IDataValue CastToString<T>(T value, DataValueContainer result, CastToStringContainer container)
            where T : IDataValue
        {
            if (value.IsNull)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }

            if (value.Type == ArrowTypeId.String)
            {
                result._type = ArrowTypeId.String;
                result._stringValue = new StringValue(value.AsString.ToString());
                return result;
            }
            if (value.Type == ArrowTypeId.Boolean)
            {
                result._type = ArrowTypeId.String;
                result._stringValue = new StringValue(value.AsBool.ToString());
                return result;
            }
            if (value.Type == ArrowTypeId.Binary)
            {
                result._type = ArrowTypeId.String;
                result._stringValue = new StringValue(Encoding.UTF8.GetString(value.AsBinary));
                return result;
            }
            if (value.Type == ArrowTypeId.Double)
            {
                result._type = ArrowTypeId.String;
                result._stringValue = new StringValue(value.AsDouble.ToString());
                return result;
            }
            if (value.Type == ArrowTypeId.Int64)
            {
                result._type = ArrowTypeId.String;
                result._stringValue = new StringValue(value.AsLong.ToString());
                return result;
            }
            if (value.Type == ArrowTypeId.Decimal128)
            {
                result._type = ArrowTypeId.String;
                result._stringValue = new StringValue(value.AsDecimal.ToString());
                return result;
            }
            if (value.Type == ArrowTypeId.Timestamp)
            {
                result._type = ArrowTypeId.String;
                var dto = value.AsTimestamp.ToDateTimeOffset();
                if (dto.TryFormat(container.utf8bytes, out var bytesWritten, "yyyy-MM-ddTHH:mm:ss.fffZ"))
                {
                    result._stringValue = new StringValue(container.utf8bytes.AsMemory().Slice(0, bytesWritten));
                }
                else
                {
                    result._stringValue = new StringValue(value.AsTimestamp.ToDateTimeOffset().ToString("yyyy-MM-ddTHH:mm:ss.fffZ"));
                }

                return result;
            }

            result._type = ArrowTypeId.Null;
            return result;
        }

        internal static IDataValue CastToInt<T>(T value, DataValueContainer result)
            where T : IDataValue
        {
            if (value.IsNull)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }
            switch (value.Type)
            {
                case ArrowTypeId.Int64:
                    result._type = ArrowTypeId.Int64;
                    result._int64Value = new Int64Value(value.AsLong);
                    return result;
                case ArrowTypeId.Boolean:
                    if (value.AsBool)
                    {
                        result._type = ArrowTypeId.Int64;
                        result._int64Value = Int1Value;
                        return result;
                    }
                    else
                    {
                        result._type = ArrowTypeId.Int64;
                        result._int64Value = Int0Value;
                        return result;
                    }
                case ArrowTypeId.String:
                    if (long.TryParse(value.AsString.ToString(), out long intValue))
                    {
                        result._type = ArrowTypeId.Int64;
                        result._int64Value = new Int64Value(intValue);
                        return result;
                    }
                    else
                    {
                        result._type = ArrowTypeId.Null;
                        return result;
                    }
                case ArrowTypeId.Double:
                    result._type = ArrowTypeId.Int64;
                    result._int64Value = new Int64Value((long)value.AsDouble);
                    return result;
                case ArrowTypeId.Decimal128:
                    result._type = ArrowTypeId.Int64;
                    result._int64Value = new Int64Value((long)value.AsDecimal);
                    return result;
                case ArrowTypeId.Timestamp:
                    result._type = ArrowTypeId.Int64;
                    result._int64Value = new Int64Value(value.AsTimestamp.UnixTimestampMicroseconds);
                    return result;
                default:
                    result._type = ArrowTypeId.Null;
                    return result;
            }
        }

        internal static IDataValue CastToDecimal<T>(T value, DataValueContainer result)
            where T : IDataValue
        {
            if (value.IsNull)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }
            switch (value.Type)
            {
                case ArrowTypeId.Boolean:
                    if (value.AsBool)
                    {
                        result._type = ArrowTypeId.Decimal128;
                        result._decimalValue = Decimal1Value;
                        return result;
                    }
                    else
                    {
                        result._type = ArrowTypeId.Decimal128;
                        result._decimalValue = Decimal0Value;
                        return result;
                    }
                case ArrowTypeId.String:
                    if (decimal.TryParse(value.AsString.ToString(), out var decimalValue))
                    {
                        result._type = ArrowTypeId.Decimal128;
                        result._decimalValue = new DecimalValue(decimalValue);
                        return result;
                    }
                    else
                    {
                        result._type = ArrowTypeId.Null;
                        return result;
                    }
                case ArrowTypeId.Double:
                    result._type = ArrowTypeId.Decimal128;
                    result._decimalValue = new DecimalValue((decimal)value.AsDouble);
                    return result;
                case ArrowTypeId.Decimal128:
                    result._type = ArrowTypeId.Decimal128;
                    result._decimalValue = new DecimalValue(value.AsDecimal);
                    return result;
                case ArrowTypeId.Int64:
                    result._type = ArrowTypeId.Decimal128;
                    result._decimalValue = new DecimalValue((decimal)value.AsLong);
                    return result;
                default:
                    result._type = ArrowTypeId.Null;
                    return result;
            }
        }

        internal static IDataValue CastToBool<T>(T value, DataValueContainer result)
            where T : IDataValue
        {
            if (value.IsNull)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }
            switch (value.Type)
            {
                case ArrowTypeId.Boolean:
                    result._type = ArrowTypeId.Boolean;
                    result._boolValue = new BoolValue(value.AsBool);
                    return result;
                case ArrowTypeId.String:
                    if (bool.TryParse(value.AsString.ToString(), out var boolValue))
                    {
                        result._type = ArrowTypeId.Boolean;
                        result._boolValue = new BoolValue(boolValue);
                        return result;
                    }
                    else
                    {
                        result._type = ArrowTypeId.Null;
                        return result;
                    }
                case ArrowTypeId.Double:
                    result._type = ArrowTypeId.Boolean;
                    result._boolValue = new BoolValue(value.AsDouble != 0.0);
                    return result;
                case ArrowTypeId.Decimal128:
                    result._type = ArrowTypeId.Boolean;
                    result._boolValue = new BoolValue(value.AsDecimal != (decimal)0.0);
                    return result;
                case ArrowTypeId.Int64:
                    result._type = ArrowTypeId.Boolean;
                    result._boolValue = new BoolValue(value.AsLong != 0);
                    return result;
                default:
                    result._type = ArrowTypeId.Null;
                    return result;
            }
        }

        internal static IDataValue CastToDouble<T>(T value, DataValueContainer result)
            where T : IDataValue
        {
            if (value.IsNull)
            {
                result._type = ArrowTypeId.Null;
                return result;
            }
            switch (value.Type)
            {
                case ArrowTypeId.Boolean:
                    if (value.AsBool)
                    {
                        result._type = ArrowTypeId.Double;
                        result._doubleValue = OneDoubleValue;
                        return result;
                    }
                    else
                    {
                        result._type = ArrowTypeId.Double;
                        result._doubleValue = ZeroDoubleValue;
                        return result;
                    }
                case ArrowTypeId.String:
                    if (double.TryParse(value.AsString.ToString(), out var doubleValue))
                    {
                        result._type = ArrowTypeId.Double;
                        result._doubleValue = new DoubleValue(doubleValue);
                        return result;
                    }
                    else
                    {
                        result._type = ArrowTypeId.Null;
                        return result;
                    }
                case ArrowTypeId.Double:
                    result._type = ArrowTypeId.Double;
                    result._doubleValue = new DoubleValue(value.AsDouble);
                    return result;
                case ArrowTypeId.Decimal128:
                    result._type = ArrowTypeId.Double;
                    result._doubleValue = new DoubleValue((double)value.AsDecimal);
                    return result;
                case ArrowTypeId.Int64:
                    result._type = ArrowTypeId.Double;
                    result._doubleValue = new DoubleValue((double)value.AsLong);
                    return result;
                default:
                    result._type = ArrowTypeId.Null;
                    return result;
            }
        }
    }
}
