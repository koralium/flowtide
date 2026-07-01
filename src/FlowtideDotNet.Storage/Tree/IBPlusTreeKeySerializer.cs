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

using System.Buffers;

namespace FlowtideDotNet.Storage.Tree
{
    /// <summary>
    /// Serializes and deserializes the keys of a B+ tree node, converting a node's key container to and from the bytes
    /// stored for that page. It also has checkpoint and initialize hooks for any serializer level metadata, such as
    /// schema information or global dictionaries.
    /// </summary>
    /// <typeparam name="K">The key type.</typeparam>
    /// <typeparam name="TKeyContainer">The container used to store the keys of a node.</typeparam>
    public interface IBPlusTreeKeySerializer<K, TKeyContainer>
        where TKeyContainer : IKeyContainer<K>
    {
        /// <summary>
        /// Creates a new empty key container, used when a new node is created.
        /// </summary>
        TKeyContainer CreateEmpty();

        /// <summary>
        /// Reads a key container from its serialized bytes.
        /// </summary>
        /// <param name="reader">The reader positioned at the serialized keys.</param>
        /// <returns>The deserialized key container.</returns>
        TKeyContainer Deserialize(ref SequenceReader<byte> reader);

        /// <summary>
        /// Writes a key container to bytes.
        /// </summary>
        /// <param name="writer">The buffer writer that receives the serialized keys.</param>
        /// <param name="values">The key container to serialize.</param>
        void Serialize(in IBufferWriter<byte> writer, in TKeyContainer values);

        /// <summary>
        /// Called on each checkpoint, its primary function is to allow a serializer to write any potential metadata
        /// such as schema information, global dictionaries and similar.
        /// </summary>
        /// <param name="context"></param>
        /// <returns></returns>
        Task CheckpointAsync(IBPlusTreeSerializerCheckpointContext context);

        /// <summary>
        /// Called once during the initialization of the serializer.
        /// This allows the serializer to fetch metadata pages such as schema information, global dictionaries and similar.
        /// During initialize any in memory data should be cleared and refetched since it can be called after a restore from
        /// a previous checkpoint.
        /// </summary>
        /// <param name="context">Context that allows reading pages and also allocating new page ids.</param>
        /// <returns></returns>
        Task InitializeAsync(IBPlusTreeSerializerInitializeContext context);
    }
}
