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

using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Flexbuffer;
using System.Globalization;
using System.Text.Json;

namespace FlowtideDotNet.Connector.CosmosDB.Internal
{
    internal class StreamEventToJsonCosmos : StreamEventToJson
    {
        private readonly int primaryKeyIndex;

        public StreamEventToJsonCosmos(List<string> names, int primaryKeyIndex) : base(names)
        {
            this.primaryKeyIndex = primaryKeyIndex;
        }

        protected override void WriteColumnValue(in Utf8JsonWriter writer, in FlxValueRef val, in int index)
        {
            if (primaryKeyIndex == index)
            {
                var idColumn = val;
                if (idColumn.ValueType == FlexBuffers.Type.Null)
                {
                    writer.WriteNullValue();
                }
                else if (idColumn.ValueType == FlexBuffers.Type.Int)
                {
                    writer.WriteStringValue(idColumn.AsLong.ToString());
                }
                else if (idColumn.ValueType == FlexBuffers.Type.Uint)
                {
                    writer.WriteStringValue(idColumn.AsULong.ToString());
                }
                else if (idColumn.ValueType == FlexBuffers.Type.Bool)
                {
                    writer.WriteStringValue(idColumn.AsBool.ToString());
                }
                else if (idColumn.ValueType == FlexBuffers.Type.Float)
                {
                    writer.WriteStringValue(idColumn.AsDouble.ToString());
                }
                else if (idColumn.ValueType == FlexBuffers.Type.String)
                {
                    writer.WriteStringValue(idColumn.AsString);
                }
                else if (idColumn.ValueType == FlexBuffers.Type.Decimal)
                {
                    writer.WriteStringValue(idColumn.AsDecimal.ToString(CultureInfo.InvariantCulture));
                }
                else
                {
                    throw new InvalidOperationException("Could not parse id column");
                }
                return;
            }
            base.WriteColumnValue(writer, val, index);
        }
    }
}
