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

using FlowtideDotNet.Connector.Kafka.Internal;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Text.Json;

namespace FlowtideDotNet.Connector.Kafka
{
    public class FlowtideKafkaUpsertJsonSerializer : IFlowtideKafkaValueSerializer
    {
        private StreamEventToJsonKafka? _streamEventToJsonKafka;
        private List<string>? _names;
        private int _keyIndex;
        public Task Initialize(WriteRelation writeRelation)
        {
            _names = writeRelation.TableSchema.Names;
            int keyIndex = -1;
            for (int i = 0; i < writeRelation.TableSchema.Names.Count; i++)
            {
                if (writeRelation.TableSchema.Names[i].ToLower() == "_key")
                {
                    keyIndex = i;
                    break;
                }
            }
            _keyIndex = keyIndex;
            _streamEventToJsonKafka = new StreamEventToJsonKafka(keyIndex, writeRelation.TableSchema.Names);
            return Task.CompletedTask;
        }

        public byte[]? Serialize(RowEvent streamEvent, bool isDeleted)
        {
            if (isDeleted)
            {
                return null;
            }
            Debug.Assert(_streamEventToJsonKafka != null);

            using MemoryStream memoryStream = new MemoryStream();
            _streamEventToJsonKafka.Write(memoryStream, streamEvent);
            return memoryStream.ToArray();
        }

        public byte[]? Serialize(ColumnRowReference row, bool isDeleted)
        {
            Debug.Assert(_names != null);
            if (isDeleted)
            {
                return null;
            }
            using MemoryStream memoryStream = new MemoryStream();
            Utf8JsonWriter jsonWriter = new Utf8JsonWriter(memoryStream);
            jsonWriter.WriteStartObject();
            for (int i = 0; i < row.referenceBatch.Columns.Count; i++)
            {
                if (i == _keyIndex)
                {
                    continue;
                }
                jsonWriter.WritePropertyName(_names[i]);
                row.referenceBatch.Columns[i].WriteToJson(in jsonWriter, row.RowIndex);
            }
            jsonWriter.WriteEndObject();
            jsonWriter.Flush();
            return memoryStream.ToArray();
        }
    }
}
