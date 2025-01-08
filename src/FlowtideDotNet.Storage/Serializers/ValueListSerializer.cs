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

using FlowtideDotNet.Storage.Tree;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Serializers
{
    public class ValueListSerializer<V> : IBplusTreeValueSerializer<V, ListValueContainer<V>>
    {
        private readonly IBplusTreeSerializer<V> serializer;

        public ValueListSerializer(IBplusTreeSerializer<V> serializer)
        {
            this.serializer = serializer;
        }

        public Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context)
        {
            return Task.CompletedTask;
        }

        public ListValueContainer<V> CreateEmpty()
        {
            return new ListValueContainer<V>();
        }

        public ListValueContainer<V> Deserialize(in BinaryReader reader)
        {
            var container = new ListValueContainer<V>();
            serializer.Deserialize(reader, container._values);
            return container;
        }

        public Task InitializeAsync(IBPlusTreeSerializerInitializeContext context)
        {
            return Task.CompletedTask;
        }

        public void Serialize(in BinaryWriter writer, in ListValueContainer<V> values)
        {
            serializer.Serialize(writer, values._values);
        }
    }
}
