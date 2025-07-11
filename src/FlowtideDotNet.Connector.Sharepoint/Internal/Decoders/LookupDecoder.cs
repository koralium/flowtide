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
using Microsoft.Kiota.Abstractions.Serialization;
using System.Buffers;

namespace FlowtideDotNet.Connector.Sharepoint.Internal.Decoders
{
    internal class LookupDecoder : BaseDecoder
    {
        private readonly FlexBuffer flexBuffer = new FlexBuffer(ArrayPool<byte>.Shared);

        public override string ColumnType => "Lookup";

        private void HandleUntypedNode(UntypedNode val)
        {
            if (val is Microsoft.Kiota.Abstractions.Serialization.UntypedObject untypedObject)
            {
                var mapStart = flexBuffer.StartVector();
                IDictionary<string, UntypedNode> nodes = untypedObject.GetValue();
                if (nodes.TryGetValue("LookupId", out var idNode) && idNode is UntypedInteger untypedInteger)
                {
                    flexBuffer.AddKey("LookupId");
                    flexBuffer.Add(untypedInteger.GetValue());
                }
                if (nodes.TryGetValue("LookupValue", out var valueNode) && valueNode is UntypedString untypedString)
                {
                    flexBuffer.AddKey("LookupValue");
                    var untypedStringValue = untypedString.GetValue();

                    if (untypedStringValue == null)
                    {
                        flexBuffer.AddNull();
                    }
                    else
                    {
                        flexBuffer.Add(untypedStringValue);
                    }
                }
                flexBuffer.SortAndEndMap(mapStart);
            }
        }

        protected override ValueTask<FlxValue> DecodeValue(object? item)
        {
            if (item is string str)
            {
                return ValueTask.FromResult(FlxValue.FromBytes(FlexBuffer.SingleValue(str)));
            }
            if (item is Microsoft.Kiota.Abstractions.Serialization.UntypedArray untypedArray)
            {
                flexBuffer.NewObject();
                var startArr = flexBuffer.StartVector();
                var values = untypedArray.GetValue();
                foreach (var val in values)
                {
                    HandleUntypedNode(val);
                }
                flexBuffer.EndVector(startArr, false, false);
                var bytes = flexBuffer.Finish();
                return ValueTask.FromResult(FlxValue.FromBytes(bytes));
            }
            return ValueTask.FromResult(FlxValue.FromBytes(FlexBuffer.Null()));
        }
    }
}
