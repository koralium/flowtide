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
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Json;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter;
using FlowtideDotNet.Core.Flexbuffer;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Text.Json;

namespace FlowtideDotNet.Connector.Kafka
{
    public class FlowtideKafkaUpsertJsonDeserializer : IFlowtideKafkaDeserializer
    {
        private List<string>? _names;
        private Dictionary<string, int>? _nameToIndex;

        public virtual RowEvent Deserialize(IFlowtideKafkaKeyDeserializer keyDeserializer, byte[]? valueBytes, byte[]? keyBytes)
        {
            Debug.Assert(_names != null);

            if (valueBytes == null)
            {
                Debug.Assert(keyBytes != null);
                return RowEvent.Create(-1, 0, b =>
                {
                    for (int i = 0; i < _names.Count; i++)
                    {
                        if (_names[i] == "_key")
                        {
                            b.Add(keyDeserializer.Deserialize(keyBytes));
                        }
                        else
                        {
                            b.AddNull();
                        }
                    }
                });
            }
            else
            {
                var jsonDocument = JsonSerializer.Deserialize<JsonElement>(valueBytes);
                return RowEvent.Create(1, 0, b =>
                {
                    for (int i = 0; i < _names.Count; i++)
                    {
                        if (_names[i] == "_key")
                        {
                            Debug.Assert(keyBytes != null);
                            b.Add(keyDeserializer.Deserialize(keyBytes));
                        }
                        else if (jsonDocument.TryGetProperty(_names[i], out var property))
                        {
                            b.Add(JsonSerializerUtils.JsonElementToValue(property));
                        }
                        else
                        {
                            b.AddNull();
                        }
                    }
                });
            }
        }

        public virtual void Deserialize(IFlowtideKafkaKeyDeserializer keyDeserializer, byte[]? valueBytes, byte[]? keyBytes, IColumn[] columns, PrimitiveList<int> weights)
        {
            Debug.Assert(_names != null);
            Debug.Assert(_nameToIndex != null);

            if (valueBytes == null)
            {
                Debug.Assert(keyBytes != null);
                weights.Add(-1);
                for (int i = 0; i < _names.Count; i++)
                {
                    if (_names[i] == "_key")
                    {
                        keyDeserializer.Deserialize(keyBytes, columns[i]);
                    }
                    else
                    {
                        columns[i].Add(NullValue.Instance);
                    }
                }
            }
            else
            {
                Utf8JsonReader utf8JsonReader = new Utf8JsonReader(valueBytes);

                if (!utf8JsonReader.Read())
                {
                    throw new InvalidOperationException("Invalid JSON");
                }
                if (utf8JsonReader.TokenType != JsonTokenType.StartObject)
                {
                    throw new InvalidOperationException("Json deserializer expects an object");
                }

                weights.Add(1);

                while (utf8JsonReader.Read())
                {
                    if (utf8JsonReader.TokenType == JsonTokenType.EndObject)
                    {
                        break;
                    }
                    if (utf8JsonReader.TokenType != JsonTokenType.PropertyName)
                    {
                        throw new InvalidOperationException("Invalid JSON");
                    }
                    var propertyName = utf8JsonReader.GetString()!;
                    if (_nameToIndex!.TryGetValue(propertyName, out var index))
                    {
                        utf8JsonReader.Read();
                        if (utf8JsonReader.TokenType == JsonTokenType.Null)
                        {
                            columns[index].Add(NullValue.Instance);
                        }
                        else
                        {
                            columns[index].Add(DataValueJsonReader.Read(ref utf8JsonReader));
                        }
                    }
                    else
                    {
                        // Skip
                        utf8JsonReader.Skip();
                    }

                }

                if (_nameToIndex.TryGetValue("_key", out var keyIndex))
                {
                    Debug.Assert(keyBytes != null);
                    keyDeserializer.Deserialize(keyBytes, columns[keyIndex]);
                }
            }
        }

        public Task Initialize(ReadRelation readRelation)
        {
            _names = readRelation.BaseSchema.Names;
            _nameToIndex = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < _names.Count; i++)
            {
                _nameToIndex[_names[i]] = i;
            }
            return Task.CompletedTask;
        }
    }
}
