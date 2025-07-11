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

using FlowtideDotNet.Storage.StateManager.Internal;
using System.Buffers;

namespace FlowtideDotNet.Storage
{
    public struct SerializableObject
    {
        private ICacheObject? obj;

        private IStateSerializer? serializer;

        // If writing from file cache directly to persistence, the data is passed directly
        private ReadOnlyMemory<byte>? serialized;

        public bool HasPreSerializedData => serialized.HasValue;

        public ReadOnlyMemory<byte>? PreSerializedData => serialized;

        public SerializableObject(ICacheObject obj, IStateSerializer serializer)
        {
            this.obj = obj;
            this.serializer = serializer;
            serialized = null;
        }

        public SerializableObject(ReadOnlyMemory<byte> serialized)
        {
            this.obj = null;
            this.serializer = null;
            this.serialized = serialized;
        }

        public void Serialize(IBufferWriter<byte> writer)
        {
            if (serialized.HasValue)
            {
                writer.Write(serialized.Value.Span);
            }
            else
            {
                if (obj == null || serializer == null)
                {
                    throw new InvalidOperationException("Object not initialized");
                }
                serializer.Serialize(writer, obj);
            }
        }
    }
}
