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
using System.Text.Json;

namespace FlowtideDotNet.Connector.StarRocks.Internal
{
    internal class StarRocksJsonWriter

    {
        private readonly List<JsonEncodedText> m_names;
        private static readonly JsonEncodedText OperationPropertyName = JsonEncodedText.Encode("__op");

        public StarRocksJsonWriter(List<string> names)
        {
            m_names = new List<JsonEncodedText>();
            for (int i = 0; i < names.Count; i++)
            {
                m_names.Add(JsonEncodedText.Encode(names[i]));
            }
        }

        public void WriteObject(ref readonly Utf8JsonWriter jsonWriter, EventBatchData data, int index, bool isDelete)
        {
            jsonWriter.WriteStartObject();

            if (isDelete)
            {
                jsonWriter.WriteNumber(OperationPropertyName, 1);
            }
            else
            {
                jsonWriter.WriteNumber(OperationPropertyName, 0);
            }


            for (int i = 0; i < m_names.Count; i++)
            {
                jsonWriter.WritePropertyName(m_names[i]);
                data.Columns[i].WriteToJson(in jsonWriter, index);
            }
            jsonWriter.WriteEndObject();
            jsonWriter.Flush();
        }
    }
}
