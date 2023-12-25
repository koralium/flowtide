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

using FlexBuffers;
using FlowtideDotNet.Core;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.Kafka
{
    public class FlowtideKafkaUpsertJsonDeserializer : IFlowtideKafkaDeserializer
    {
        private List<string>? _names;

        public StreamEvent Deserialize(IFlowtideKafkaKeyDeserializer keyDeserializer, byte[]? valueBytes, byte[]? keyBytes)
        {
            if (valueBytes == null)
            {
                return StreamEvent.Create(-1, 0, b =>
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
                return StreamEvent.Create(1, 0, b =>
                {
                    for (int i = 0; i < _names.Count; i++)
                    {
                        if (_names[i] == "_key")
                        {
                            b.Add(keyDeserializer.Deserialize(keyBytes));
                        }
                        else if (jsonDocument.TryGetProperty(_names[i], out var property))
                        {
                            b.Add(JsonElementToValue(property));
                        }
                        else
                        {
                            b.AddNull();
                        }
                    }
                });
            }
        }

        internal static FlxValue JsonElementToValue(JsonElement jsonElement)
        {
            if (jsonElement.ValueKind == JsonValueKind.Null)
            {
                return FlxValue.FromBytes(FlexBuffer.Null());
            }
            if (jsonElement.ValueKind == JsonValueKind.True)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(true));
            }
            if (jsonElement.ValueKind == JsonValueKind.False)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(false));
            }
            if (jsonElement.ValueKind == JsonValueKind.Number)
            {
                if (jsonElement.TryGetInt64(out var value))
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(value));
                }
                else if (jsonElement.TryGetDouble(out var doubleValue))
                {
                    return FlxValue.FromBytes(FlexBuffer.SingleValue(doubleValue));
                }
                else
                {
                    throw new NotImplementedException();
                }
            }
            if (jsonElement.ValueKind == JsonValueKind.String)
            {
                return FlxValue.FromBytes(FlexBuffer.SingleValue(jsonElement.GetString()));
            }
            if (jsonElement.ValueKind == JsonValueKind.Array)
            {
                var bytes = FlexBufferBuilder.Vector(v =>
                {
                    foreach (var item in jsonElement.EnumerateArray())
                    {
                        v.Add(JsonElementToValue(item));
                    }
                });
                return FlxValue.FromBytes(bytes);
            }
            if (jsonElement.ValueKind == JsonValueKind.Object)
            {
                var bytes = FlexBufferBuilder.Map(m =>
                {
                    foreach (var item in jsonElement.EnumerateObject())
                    {
                        m.Add(item.Name, JsonElementToValue(item.Value));
                    }
                });
                return FlxValue.FromBytes(bytes);
            }
            throw new NotImplementedException();
        }

        public Task Initialize(ReadRelation readRelation)
        {
            _names = readRelation.BaseSchema.Names;
            return Task.CompletedTask;
        }
    }
}
