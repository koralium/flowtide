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
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Sources.Generic.Internal
{
    internal class ObjectToRowEvent
    {
        private readonly List<string> _names;
        private RowEvent _deleteEvent;

        public ObjectToRowEvent(ReadRelation readRelation)
        {
            _names = readRelation.BaseSchema.Names;
            _deleteEvent = RowEvent.Create(-1, 0, b =>
            {
                for (int i = 0; i < _names.Count; i++)
                {
                    b.AddNull();
                }
            });
        }
        public RowEvent Convert<T>(T obj, bool isDelete)
        {
            if (isDelete)
            {
                return _deleteEvent;
            }
            var document = JsonSerializer.SerializeToDocument(obj);
            var root = document.RootElement;
            return RowEvent.Create(1, 0, b =>
            {
                for (int i = 0; i < _names.Count; i++)
                {
                    if (root.TryGetProperty(_names[i], out var property))
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

        private static FlxValue JsonElementToValue(JsonElement jsonElement)
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
                var str = jsonElement.GetString();
                if (str == null)
                {
                    return FlxValue.FromBytes(FlexBuffer.Null());
                }
                return FlxValue.FromBytes(FlexBuffer.SingleValue(str));
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
    }
}
