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

namespace FlowtideDotNet.Core.Compute.Unwrap
{
    public static class UnwrapCompiler
    {
        public static Func<FlexBuffers.FlxValue, IReadOnlyList<IReadOnlyList<FlexBuffers.FlxValue>>> CompileUnwrap(IReadOnlyList<string> FieldNames)
        {
            return (val) =>
            {
                List<IReadOnlyList<FlexBuffers.FlxValue>> output = new List<IReadOnlyList<FlexBuffers.FlxValue>>();
                if (val.ValueType == FlexBuffers.Type.Vector)
                {
                    var arr = val.AsVector;

                    for (int i = 0; i < arr.Length; i++)
                    {
                        var element = arr.Get(i);

                        List<FlexBuffers.FlxValue> columns = new List<FlxValue>();
                        foreach (var fieldName in FieldNames)
                        {
                            if (fieldName.Equals("__id__"))
                            {
                                columns.Add(FlxValue.FromMemory(FlexBuffer.SingleValue(i)));
                            }
                            else if (fieldName.Equals("__value__"))
                            {
                                columns.Add(element);
                            }
                            else
                            {
                                if (element.ValueType == FlexBuffers.Type.Map)
                                {
                                    var map = element.AsMap;
                                    var fieldIndex = map.KeyIndex(fieldName);

                                    if (fieldIndex < 0)
                                    {
                                        columns.Add(FlxValue.FromMemory(FlexBuffer.Null()));
                                    }
                                    else
                                    {
                                        columns.Add(map.ValueByIndex(fieldIndex));
                                    }
                                }
                            }
                        }
                        output.Add(columns);
                    }
                }

                return output;
            };
        }
    }
}
