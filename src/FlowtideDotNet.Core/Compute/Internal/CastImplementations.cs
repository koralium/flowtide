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

using FlexBuffers;
using Google.Protobuf.WellKnownTypes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Internal
{
    internal static class CastImplementations
    {
        private static FlxValue NullValue = FlxValue.FromBytes(FlexBuffer.Null());
        private static FlxValue OneIntValue = FlxValue.FromBytes(FlexBuffer.SingleValue(1));
        private static FlxValue ZeroIntValue = FlxValue.FromBytes(FlexBuffer.SingleValue(0));
        private static FlxValue OneDecimalValue = FlxValue.FromBytes(FlexBuffer.SingleValue((decimal)1));
        private static FlxValue ZeroDecimalValue = FlxValue.FromBytes(FlexBuffer.SingleValue((decimal)0));
        private static FlxValue OneDoubleValue = FlxValue.FromBytes(FlexBuffer.SingleValue((double)1));
        private static FlxValue ZeroDoubleValue = FlxValue.FromBytes(FlexBuffer.SingleValue((double)0));

        internal static FlxValue CastToString(FlxValue value)
        {
            if (value.IsNull)
            {
                return NullValue;
            }
            return FlxValue.FromBytes(FlexBuffer.SingleValue(FlxValueStringFunctions.ToString(value)));
        }

        internal static Expression CallCastToString(Expression value)
        {
            return Expression.Call(typeof(CastImplementations), nameof(CastToString), null, value);
        }

        internal static Expression CallCastToInt(Expression value)
        {
            return Expression.Call(typeof(CastImplementations), nameof(CastToInt), null, value);
        }

        internal static Expression CallCastToDecimal(Expression value)
        {
            return Expression.Call(typeof(CastImplementations), nameof(CastToDecimal), null, value);
        }

        internal static Expression CallCastToBool(Expression value)
        {
            return Expression.Call(typeof(CastImplementations), nameof(CastToBool), null, value);
        }

        internal static Expression CallCastToDouble(Expression value)
        {
            return Expression.Call(typeof(CastImplementations), nameof(CastToDouble), null, value);
        }

        internal static FlxValue CastToInt(FlxValue value)
        {
            if (value.IsNull)
            {
                return NullValue;
            }
            switch (value.ValueType)
            {
                case FlexBuffers.Type.Int:
                    return value;
                case FlexBuffers.Type.Bool:
                    if (value.AsBool)
                    {
                        return OneIntValue;
                    }
                    else
                    {
                        return ZeroIntValue;
                    }
                case FlexBuffers.Type.String:
                    if (long.TryParse(value.AsString, out long intValue))
                    {
                        return FlxValue.FromBytes(FlexBuffer.SingleValue(intValue));
                    }
                    else
                    {
                        return NullValue;
                    }
                case FlexBuffers.Type.Float:
                    return FlxValue.FromBytes(FlexBuffer.SingleValue((long)value.AsDouble));
                case FlexBuffers.Type.Decimal:
                    return FlxValue.FromBytes(FlexBuffer.SingleValue((long)value.AsDecimal));
                default:
                    return NullValue;
            }
        }

        internal static FlxValue CastToDecimal(FlxValue value)
        {
            if (value.IsNull)
            {
                return NullValue;
            }
            switch (value.ValueType)
            {
                case FlexBuffers.Type.Bool:
                    if (value.AsBool)
                    {
                        return OneDecimalValue;
                    }
                    else
                    {
                        return ZeroDecimalValue;
                    }
                case FlexBuffers.Type.String:
                    if (decimal.TryParse(value.AsString, out var decimalValue))
                    {
                        return FlxValue.FromBytes(FlexBuffer.SingleValue(decimalValue));
                    }
                    else
                    {
                        return NullValue;
                    }
                case FlexBuffers.Type.Float:
                    return FlxValue.FromBytes(FlexBuffer.SingleValue((decimal)value.AsDouble));
                case FlexBuffers.Type.Decimal:
                    return value;
                case FlexBuffers.Type.Int:
                    return FlxValue.FromBytes(FlexBuffer.SingleValue((decimal)value.AsLong));
                default:
                    return NullValue;
            }
        }

        internal static FlxValue CastToBool(FlxValue value)
        {
            if (value.IsNull)
            {
                return NullValue;
            }
            switch (value.ValueType)
            {
                case FlexBuffers.Type.Bool:
                    return value;
                case FlexBuffers.Type.String:
                    if (bool.TryParse(value.AsString, out var boolValue))
                    {
                        return FlxValue.FromBytes(FlexBuffer.SingleValue(boolValue));
                    }
                    else
                    {
                        return NullValue;
                    }
                case FlexBuffers.Type.Float:
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(value.AsDouble != 0.0));
                case FlexBuffers.Type.Decimal:
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(value.AsDecimal != (decimal)0.0));
                case FlexBuffers.Type.Int:
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(value.AsLong != 0));
                default:
                    return NullValue;
            }
        }

        internal static FlxValue CastToDouble(FlxValue value)
        {
            if (value.IsNull)
            {
                return NullValue;
            }
            switch (value.ValueType)
            {
                case FlexBuffers.Type.Bool:
                    if (value.AsBool)
                    {
                        return OneDoubleValue;
                    }
                    else
                    {
                        return ZeroDoubleValue;
                    }
                case FlexBuffers.Type.String:
                    if (double.TryParse(value.AsString, out var doubleValue))
                    {
                        return FlxValue.FromBytes(FlexBuffer.SingleValue(doubleValue));
                    }
                    else
                    {
                        return NullValue;
                    }
                case FlexBuffers.Type.Float:
                    return value;
                case FlexBuffers.Type.Decimal:
                    return FlxValue.FromBytes(FlexBuffer.SingleValue((double)value.AsDecimal));
                case FlexBuffers.Type.Int:
                    return FlxValue.FromBytes(FlexBuffer.SingleValue((double)value.AsLong));
                default:
                    return NullValue;
            }
        }
    }
}
