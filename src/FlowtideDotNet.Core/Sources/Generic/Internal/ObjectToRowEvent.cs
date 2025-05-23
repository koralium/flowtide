﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

using FlowtideDotNet.Core.Flexbuffer;
using FlowtideDotNet.Substrait.Relations;
using System.Text.Json;

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
}
