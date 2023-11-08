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
using FlowtideDotNet.Core.Compute.Internal.StrftimeImpl;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Compute.Internal
{
    internal static class BuiltInDatetimeFunctions
    {
        private static FlxValue NullValue = FlxValue.FromBytes(FlexBuffer.Null());

        public static void AddBuiltInDatetimeFunctions(FunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterScalarFunctionWithExpression(FunctionsDatetime.Uri, FunctionsDatetime.Strftime, (x, y) => StrfTimeImplementation(x, y));
        }

        internal static FlxValue StrfTimeImplementation(FlxValue value, FlxValue format)
        {
            long timestamp = 0;
            if (value.ValueType == FlexBuffers.Type.Int)
            {
                timestamp = value.AsLong;
            }
            else if (value.ValueType == FlexBuffers.Type.Uint)
            {
                timestamp = (long)value.AsULong;
            }
            else
            {
                return NullValue;
            }
            if (format.ValueType != FlexBuffers.Type.String)
            {
                return NullValue;
            }
            var datetimeOffset = DateTimeOffset.FromUnixTimeMilliseconds(timestamp / 1000);

            return FlxValue.FromMemory(FlexBuffer.SingleValue(Strftime.ToStrFTime(datetimeOffset.DateTime, format.AsString, CultureInfo.InvariantCulture)));
        }
    }
}
