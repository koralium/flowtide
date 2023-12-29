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
using FlowtideDotNet.Substrait.FunctionExtensions;

namespace FlowtideDotNet.Core.Compute.Internal
{
    internal static class BuiltInGuidFunctions
    {
        private static FlxValue NullValue = FlxValue.FromBytes(FlexBuffer.Null());

        public static void AddBuiltInGuidFunctions(FunctionsRegister functionsRegister)
        {
            functionsRegister.RegisterScalarFunctionWithExpression(
                FunctionsGuid.Uri,
                FunctionsGuid.ParseGuid,
                (val) => ParseGuidImplementation(val));
        }

        private static FlxValue ParseGuidImplementation(FlxValue value)
        {
            if (value.IsNull)
            {
                return value;
            }
            if (value.ValueType == FlexBuffers.Type.String)
            {
                if (Guid.TryParse(value.AsString, out var result))
                {
                    return FlxValue.FromMemory(FlexBuffer.SingleValue(result.ToByteArray()));
                }
            }
            return NullValue;
        }
    }
}
