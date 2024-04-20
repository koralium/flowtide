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
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.Kafka
{
    public class FlowtideDebeziumValueDeserializer : IFlowtideKafkaDeserializer
    {
        private List<string>? _names;

        private RowEvent GetRowEvent(IFlowtideKafkaKeyDeserializer keyDeserializer, byte[]? keyBytes, JsonElement data, int weight)
        {
            Debug.Assert(_names != null);

            return RowEvent.Create(weight, 0, b =>
            {
                for (int i = 0; i < _names.Count; i++)
                {
                    if (_names[i] == "_key")
                    {
                        if (keyBytes != null)
                        {
                            b.Add(keyDeserializer.Deserialize(keyBytes));
                        }
                        else
                        {
                            b.AddNull();
                        }
                    }
                    else if (data.TryGetProperty(_names[i], out var property))
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

        public RowEvent Deserialize(IFlowtideKafkaKeyDeserializer keyDeserializer, byte[]? valueBytes, byte[]? keyBytes)
        {
            Debug.Assert(_names != null);
            
            if (keyBytes == null)
            {
                throw new InvalidOperationException("Key bytes cannot be null");
            }

            if (valueBytes == null)
            {
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

            var jsonDocument = JsonSerializer.Deserialize<JsonElement>(valueBytes);
            var payload = jsonDocument.GetProperty("payload");
            var op = payload.GetProperty("op").GetString();

            if (op == "d")
            {
                var before = payload.GetProperty("before");
                return GetRowEvent(keyDeserializer, keyBytes, before, -1);
            }
            else if (op == "c" ||
                op == "u" ||
                op == "r")
            {
                var after = payload.GetProperty("after");
                return GetRowEvent(keyDeserializer, keyBytes, after, 1);
            }
            else
            {
                return new RowEvent();
            }
        }

        public Task Initialize(ReadRelation readRelation)
        {
            _names = readRelation.BaseSchema.Names;
            return Task.CompletedTask;
        }
    }
}
