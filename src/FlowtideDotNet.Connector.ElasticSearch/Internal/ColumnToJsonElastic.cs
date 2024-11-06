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
using FlowtideDotNet.Core.ColumnStore;
using Nest;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.ElasticSearch.Internal
{
    internal class ColumnToJsonElastic
    {
        private static readonly JsonEncodedText indexOperationPropertyName = JsonEncodedText.Encode("index");
        private static readonly JsonEncodedText deleteOperationPropertyName = JsonEncodedText.Encode("delete");
        private static readonly JsonEncodedText underscoreIndexPropertyName = JsonEncodedText.Encode("_index");
        private static readonly JsonEncodedText idPropertyName = JsonEncodedText.Encode("_id");

        private readonly int m_idIndex;
        private readonly JsonEncodedText m_indexName;
        private readonly List<JsonEncodedText> m_names;

        public ColumnToJsonElastic(List<string> names, int idIndex, string indexName)
        {
            m_idIndex = idIndex;
            m_indexName = JsonEncodedText.Encode(indexName);
            m_names = new List<JsonEncodedText>();
            for (int i = 0; i < names.Count; i++)
            {
                m_names.Add(JsonEncodedText.Encode(names[i]));
            }
        }

        public void WriteIndexUpsertMetadata(ref readonly Utf8JsonWriter writer, EventBatchData data, int index)
        {
            writer.WriteStartObject();
            writer.WritePropertyName(indexOperationPropertyName);
            writer.WriteStartObject();
            writer.WritePropertyName(underscoreIndexPropertyName);
            writer.WriteStringValue(m_indexName);
            writer.WritePropertyName(idPropertyName);
            WriteIdField(in writer, data, index);
            writer.WriteEndObject();
            writer.WriteEndObject();
            writer.Flush();
        }

        public void WriteIndexDeleteMetadata(ref readonly Utf8JsonWriter writer, EventBatchData data, int index)
        {
            writer.WriteStartObject();
            writer.WritePropertyName(deleteOperationPropertyName);
            writer.WriteStartObject();
            writer.WritePropertyName(underscoreIndexPropertyName);
            writer.WriteStringValue(m_indexName);
            writer.WritePropertyName(idPropertyName);
            WriteIdField(writer, data, index);
            writer.WriteEndObject();
            writer.WriteEndObject();
            writer.Flush();
        }

        public void WriteObject(ref readonly Utf8JsonWriter jsonWriter, EventBatchData data, int index)
        {
            jsonWriter.WriteStartObject();
            for (int i = 0; i < m_names.Count; i++)
            {
                if (i == m_idIndex)
                {
                    continue;
                }
                jsonWriter.WritePropertyName(m_names[i]);
                data.Columns[i].WriteToJson(in jsonWriter, index);
            }
            jsonWriter.WriteEndObject();
            jsonWriter.Flush();
        }

        private void WriteIdField(ref readonly Utf8JsonWriter writer, EventBatchData data, int index)
        {
            data.Columns[m_idIndex].WriteToJson(in writer, index);
        }
    }
}
