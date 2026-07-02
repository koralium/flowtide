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
using Microsoft.Kiota.Abstractions.Serialization;

namespace FlowtideDotNet.Connector.Sharepoint.Internal.Decoders
{
    internal class LookupDecoder : BaseDecoder
    {
        private readonly StructHeader _structHeader = StructHeader.Create("LookupId", "LookupValue");
        public override string ColumnType => "Lookup";

        private void HandleUntypedNode(UntypedNode val, List<IDataValue> arrayValues)
        {
            if (val is UntypedObject untypedObject)
            {
                IDataValue[] values = new IDataValue[2];
                values[0] = NullValue.Instance;
                values[1] = NullValue.Instance;
                IDictionary<string, UntypedNode> nodes = untypedObject.GetValue();
                if (nodes.TryGetValue("LookupId", out var idNode) && idNode is UntypedInteger untypedInteger)
                {
                    values[0] = new Int64Value(untypedInteger.GetValue());
                }
                if (nodes.TryGetValue("LookupValue", out var valueNode) && valueNode is UntypedString untypedString)
                {
                    var untypedStringValue = untypedString.GetValue();

                    if (untypedStringValue == null)
                    {
                        values[1] = NullValue.Instance;
                    }
                    else
                    {
                        values[1] = new StringValue(untypedStringValue);
                    }
                }
                arrayValues.Add(new StructValue(_structHeader, values));
            }
        }

        protected override ValueTask<IDataValue> DecodeDataValue(object? item)
        {
            if (item is string str)
            {
                return ValueTask.FromResult<IDataValue>(new StringValue(str));
            }
            if (item is UntypedArray untypedArray)
            {
                List<IDataValue> arrayValues = new List<IDataValue>();
                var values = untypedArray.GetValue();

                foreach (var val in values)
                {
                    HandleUntypedNode(val, arrayValues);
                }
                return ValueTask.FromResult<IDataValue>(new ListValue(arrayValues));
            }
            return ValueTask.FromResult<IDataValue>(NullValue.Instance);
        }

        protected override ValueTask DecodeValue(object? item, Column column)
        {
            if (item is string str)
            {
                column.Add(new StringValue(str));
                return ValueTask.CompletedTask;
            }
            if (item is UntypedArray untypedArray)
            {
                List<IDataValue> arrayValues = new List<IDataValue>();
                var values = untypedArray.GetValue();

                foreach (var val in values)
                {
                    HandleUntypedNode(val, arrayValues);
                }
                column.Add(new ListValue(arrayValues));
                return ValueTask.CompletedTask;
            }
            column.Add(NullValue.Instance);
            return ValueTask.CompletedTask;
        }
    }
}
