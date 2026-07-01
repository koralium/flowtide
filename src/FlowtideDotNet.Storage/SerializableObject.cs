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
    /// <summary>
    /// A value handed to a file cache or persistent storage session to be written.
    /// It carries the data in one of two forms so callers do not have to serialize ahead of time:
    /// either a live <see cref="ICacheObject"/> together with the <see cref="IStateSerializer"/> that
    /// knows how to serialize it, or a block of bytes that has already been serialized.
    /// The bytes form is used when data is copied straight from the file cache to persistent storage,
    /// or for already serialized blobs such as metadata, where re-serialization is not needed.
    /// </summary>
    public struct SerializableObject
    {
        private ICacheObject? obj;

        private IStateSerializer? serializer;

        // If writing from file cache directly to persistence, the data is passed directly
        private ReadOnlyMemory<byte>? serialized;

        /// <summary>
        /// True when the value was created from already serialized bytes, meaning <see cref="PreSerializedData"/>
        /// can be written directly without invoking a serializer.
        /// </summary>
        public bool HasPreSerializedData => serialized.HasValue;

        /// <summary>
        /// The already serialized bytes, or null when the value still has to be serialized through its
        /// <see cref="IStateSerializer"/>. Lets a session that can write raw bytes (for example pinning them
        /// for a native call) avoid going through <see cref="Serialize(IBufferWriter{byte})"/>.
        /// </summary>
        public ReadOnlyMemory<byte>? PreSerializedData => serialized;

        /// <summary>
        /// Creates a value that is serialized on demand, used when flushing a live object from the in-memory cache.
        /// </summary>
        /// <param name="obj">The cache object to serialize when the value is written.</param>
        /// <param name="serializer">The serializer that knows how to serialize <paramref name="obj"/>.</param>
        public SerializableObject(ICacheObject obj, IStateSerializer serializer)
        {
            this.obj = obj;
            this.serializer = serializer;
            serialized = null;
        }

        /// <summary>
        /// Creates a value from bytes that are already serialized, used when copying data straight from the
        /// file cache to persistent storage or when writing pre-serialized blobs such as metadata.
        /// </summary>
        /// <param name="serialized">The serialized bytes to write.</param>
        public SerializableObject(ReadOnlyMemory<byte> serialized)
        {
            this.obj = null;
            this.serializer = null;
            this.serialized = serialized;
        }

        /// <summary>
        /// Writes the value to <paramref name="writer"/>, either by copying the pre-serialized bytes or by
        /// running the serializer over the cache object.
        /// </summary>
        /// <param name="writer">The buffer writer that receives the serialized bytes.</param>
        /// <exception cref="InvalidOperationException">Thrown when the value was not constructed with either form of data.</exception>
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
