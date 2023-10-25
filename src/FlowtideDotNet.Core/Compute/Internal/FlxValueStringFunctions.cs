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
using System.Text;

namespace FlowtideDotNet.Core.Compute.Internal
{
    internal static class FlxValueStringFunctions
    {
        /// <summary>
        /// Converts values to string and also concatinates the values together to one single string.
        /// This function is used to concatinate permission ids together
        /// </summary>
        /// <param name="values"></param>
        /// <returns></returns>
        public static FlxValue Concat(params FlxValue[] values)
        {
            if (values.Length == 1 && values[0].ValueType == FlexBuffers.Type.String)
            {
                return values[0];
            }
            StringBuilder stringBuilder = new StringBuilder();
            foreach (var v in values)
            {
                if (v.IsNull)
                {
                    return FlxValue.FromMemory(FlexBuffer.Null());
                }
                stringBuilder.Append(ToString(v));
            }
            var str = stringBuilder.ToString();
            return FlxValue.FromMemory(FlexBuffer.SingleValue(str));
        }

        public static string ToString(FlxValue value)
        {
            if (value.ValueType == FlexBuffers.Type.String)
            {
                return value.AsString;
            }
            if (value.ValueType == FlexBuffers.Type.Int)
            {
                return value.AsLong.ToString();
            }
            if (value.ValueType == FlexBuffers.Type.Null)
            {
                return "null";
            }
            if (value.ValueType == FlexBuffers.Type.Float)
            {
                return value.AsDouble.ToString();
            }
            if (value.ValueType == FlexBuffers.Type.Bool)
            {
                return value.AsBool.ToString();
            }
            throw new NotImplementedException();
        }
    }
}
