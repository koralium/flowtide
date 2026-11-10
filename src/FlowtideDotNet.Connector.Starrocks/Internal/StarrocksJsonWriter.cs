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
using System.Text;
using System.Text.Json;

namespace FlowtideDotNet.Connector.StarRocks.Internal
{
    internal class StarRocksJsonWriter

    {
        private readonly List<JsonEncodedText> m_names;
        // Delete rows only carry keys, NOT NULL columns need a value
        private readonly byte[]?[] m_deletePlaceholders;
        private static readonly JsonEncodedText OperationPropertyName = JsonEncodedText.Encode("__op");

        public StarRocksJsonWriter(List<string> names, IReadOnlyDictionary<string, string>? notNullColumnTypes = null)
        {
            m_names = new List<JsonEncodedText>();
            m_deletePlaceholders = new byte[]?[names.Count];
            for (int i = 0; i < names.Count; i++)
            {
                m_names.Add(JsonEncodedText.Encode(names[i]));
                if (notNullColumnTypes != null && notNullColumnTypes.TryGetValue(names[i], out var dataType))
                {
                    m_deletePlaceholders[i] = GetDeletePlaceholder(dataType);
                }
            }
        }

        // Any valid value, delete only matches on key
        private static byte[]? GetDeletePlaceholder(string dataType)
        {
            string? placeholder = dataType.ToLowerInvariant() switch
            {
                "boolean" or "tinyint" or "smallint" or "int" or "bigint" or "bigint unsigned" or "largeint" or "float" or "double" => "0",
                var t when t.StartsWith("decimal") => "0",
                "char" or "varchar" or "string" or "binary" or "varbinary" => "\"\"",
                "date" => "\"1970-01-01\"",
                "datetime" => "\"1970-01-01 00:00:00\"",
                "json" or "map" or "struct" => "{}",
                "array" => "[]",
                _ => null
            };
            return placeholder == null ? null : Encoding.UTF8.GetBytes(placeholder);
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
                var placeholder = m_deletePlaceholders[i];
                if (isDelete && placeholder != null && data.Columns[i].GetTypeAt(index, default) == ArrowTypeId.Null)
                {
                    jsonWriter.WriteRawValue(placeholder, skipInputValidation: true);
                }
                else
                {
                    data.Columns[i].WriteToJson(in jsonWriter, index);
                }
            }
            jsonWriter.WriteEndObject();
            jsonWriter.Flush();
        }
    }
}
