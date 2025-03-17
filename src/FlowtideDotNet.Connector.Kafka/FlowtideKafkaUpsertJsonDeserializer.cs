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
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Text.Json;

namespace FlowtideDotNet.Connector.Kafka
{
    public class FlowtideKafkaUpsertJsonDeserializer : IFlowtideKafkaDeserializer
    {
        private List<string>? _names;

        public RowEvent Deserialize(IFlowtideKafkaKeyDeserializer keyDeserializer, byte[]? valueBytes, byte[]? keyBytes)
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

        public Task Initialize(ReadRelation readRelation)
        {
            _names = readRelation.BaseSchema.Names;
            return Task.CompletedTask;
        }
    }
}
