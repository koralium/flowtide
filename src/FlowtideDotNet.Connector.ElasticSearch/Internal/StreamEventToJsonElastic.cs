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

using FlowtideDotNet.Base;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Flexbuffer;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.ElasticSearch.Internal
{
    internal class StreamEventToJsonElastic : StreamEventToJson
    {
        private static readonly JsonEncodedText indexOperationPropertyName = JsonEncodedText.Encode("index");
        private static readonly JsonEncodedText deleteOperationPropertyName = JsonEncodedText.Encode("delete");
        private static readonly JsonEncodedText underscoreIndexPropertyName = JsonEncodedText.Encode("_index");
        private static readonly JsonEncodedText idPropertyName = JsonEncodedText.Encode("_id");

        private readonly int m_idIndex;
        private readonly JsonEncodedText m_indexName;

        public StreamEventToJsonElastic(int idIndex, string indexName, List<string> names) : base(names)
        {
            m_idIndex = idIndex;
            m_indexName = JsonEncodedText.Encode(indexName);
        }

        /// <summary>
        /// Writes elasticsearch bulk metadata such as:
        /// { "index": { "_index": "movies", "_id": "tt1979320" } }
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="ev"></param>
        public void WriteIndexUpsertMetadata(in Utf8JsonWriter writer, in RowEvent ev)
        {
            writer.WriteStartObject();
            writer.WritePropertyName(indexOperationPropertyName);
            writer.WriteStartObject();
            writer.WritePropertyName(underscoreIndexPropertyName);
            writer.WriteStringValue(m_indexName);
            writer.WritePropertyName(idPropertyName);
            WriteIdField(writer, ev);
            writer.WriteEndObject();
            writer.WriteEndObject();
            writer.Flush();
        }

        /// <summary>
        /// Writes elasticsearch bulk metadata such as:
        /// { "delete": { "_index": "movies", "_id": "tt1979320" } }
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="ev"></param>
        public void WriteIndexDeleteMetadata(in Utf8JsonWriter writer, in RowEvent ev)
        {
            writer.WriteStartObject();
            writer.WritePropertyName(deleteOperationPropertyName);
            writer.WriteStartObject();
            writer.WritePropertyName(underscoreIndexPropertyName);
            writer.WriteStringValue(m_indexName);
            writer.WritePropertyName(idPropertyName);
            WriteIdField(writer, ev);
            writer.WriteEndObject();
            writer.WriteEndObject();
            writer.Flush();
        }

        public void WriteObject(in Utf8JsonWriter writer, in RowEvent ev)
        {
            writer.WriteStartObject();
            for (int i = 0; i < ev.Length; i++)
            {
                // Skip the _id field
                if (i == m_idIndex)
                {
                    continue;
                }
                WritePropertyName(writer, i);
                WriteColumnValue(writer, ev.GetColumnRef(i), i);
            }
            writer.WriteEndObject();
            writer.Flush();
        }

        private void WriteIdField(in Utf8JsonWriter writer, in RowEvent ev)
        {
            var idColumn = ev.GetColumnRef(m_idIndex);
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
                writer.WriteStringValue(idColumn.AsFlxString.Span);
            }
            else if (idColumn.ValueType == FlexBuffers.Type.Decimal)
            {
                writer.WriteStringValue(idColumn.AsDecimal.ToString(CultureInfo.InvariantCulture));
            }
            else
            {
                throw new InvalidOperationException("Could not parse id column");
            }
        }
    }
}
