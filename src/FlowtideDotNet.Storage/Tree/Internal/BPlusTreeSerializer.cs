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

using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.StateManager.Internal;
using System.Buffers;
using System.Collections.Immutable;

namespace FlowtideDotNet.Storage.Tree.Internal
{
    internal class BPlusTreeSerializer<K, V, TKeyContainer, TValueContainer> : IStateSerializer<IBPlusTreeNode>
        where TKeyContainer: IKeyContainer<K>
        where TValueContainer: IValueContainer<V>
    {
        private readonly IBPlusTreeKeySerializer<K, TKeyContainer> _keySerializer;
        private readonly IBplusTreeValueSerializer<V, TValueContainer> _valueSerializer;
        private readonly IMemoryAllocator _memoryAllocator;
        private readonly BPlusTreeSerializerCheckpointContext _serializeContext;

        public BPlusTreeSerializer(
            IBPlusTreeKeySerializer<K, TKeyContainer> keySerializer,
            IBplusTreeValueSerializer<V, TValueContainer> valueSerializer,
            IMemoryAllocator memoryAllocator
            )
        {
            _keySerializer = keySerializer;
            _valueSerializer = valueSerializer;
            _memoryAllocator = memoryAllocator;
            _serializeContext = new BPlusTreeSerializerCheckpointContext();
        }

        public async Task CheckpointAsync<TMetadata>(IStateSerializerCheckpointWriter checkpointWriter, StateClientMetadata<TMetadata> metadata)
            where TMetadata : IStorageMetadata
        {
            if (metadata is StateClientMetadata<BPlusTreeMetadata> treeMetadata &&
                treeMetadata.Metadata != null)
            {
                // Initialize context that will update the list of written page ids.
                _serializeContext.Initialize(treeMetadata.Metadata.KeyMetadataPages, checkpointWriter);
                await _keySerializer.CheckpointAsync(_serializeContext);

                if (_serializeContext.ListUpdated)
                {
                    treeMetadata.Metadata.Updated = true;
                }

                _serializeContext.Initialize(treeMetadata.Metadata.ValueMetadataPages, checkpointWriter);
                await _valueSerializer.CheckpointAsync(_serializeContext);

                if (_serializeContext.ListUpdated)
                {
                    treeMetadata.Metadata.Updated = true;
                }

                return;
            }
            throw new NotSupportedException();
        }

        public async Task InitializeAsync<TMetadata>(IStateSerializerInitializeReader reader, StateClientMetadata<TMetadata> metadata) where TMetadata : IStorageMetadata
        {
            if (metadata is StateClientMetadata<BPlusTreeMetadata> treeMetadata)
            {
                IReadOnlyList<long> keyMetadataPages;
                IReadOnlyList<long> valueMetadataPages;

                if (treeMetadata.Metadata != null)
                {
                    keyMetadataPages = treeMetadata.Metadata.KeyMetadataPages;
                    valueMetadataPages = treeMetadata.Metadata.ValueMetadataPages;
                }
                else 
                { 
                    keyMetadataPages = ImmutableList<long>.Empty;
                    valueMetadataPages = ImmutableList<long>.Empty;
                }

                // Initialize the key serializer
                var keyContext = new BPlusTreeSerializerInitializeContext(reader, keyMetadataPages);
                await _keySerializer.InitializeAsync(keyContext);

                // Initialize the value serializer
                var valueContext = new BPlusTreeSerializerInitializeContext(reader, valueMetadataPages);
                await _valueSerializer.InitializeAsync(valueContext);

                return;
            }
            throw new NotSupportedException();
        }

        public IBPlusTreeNode Deserialize(IMemoryOwner<byte> bytes, int length, StateSerializeOptions stateSerializeOptions)
        {
            var arr = bytes.Memory.ToArray();
            using var memoryStream = new MemoryStream(arr);
            Stream readStream = memoryStream;
            if (stateSerializeOptions.DecompressFunc != null)
            {
                readStream = stateSerializeOptions.DecompressFunc(memoryStream);
            }
            bytes.Dispose();
            using var reader = new BinaryReader(readStream);

            var typeId = reader.ReadByte();

            if (typeId == 2)
            {
                var id = reader.ReadInt64();
                
                
                var leafNext = reader.ReadInt64();

                var keyContainer = _keySerializer.Deserialize(reader);
                var valueContainer = _valueSerializer.Deserialize(reader);
                var leaf = new LeafNode<K, V, TKeyContainer, TValueContainer>(id, keyContainer, valueContainer);
                leaf.next = leafNext;
                return leaf;
            }
            if (typeId == 3)
            {
                var id = reader.ReadInt64();

                var keyContainer = _keySerializer.Deserialize(reader);

                var childrenByteLength = reader.ReadInt32();
                var childrenBytes = reader.ReadBytes(childrenByteLength);
                var childrenMemory = _memoryAllocator.Allocate(childrenByteLength, 64);
                childrenBytes.CopyTo(childrenMemory.Memory.Span);
                var childrenList = new PrimitiveList<long>(childrenMemory, childrenByteLength / sizeof(long), _memoryAllocator);

                var parent = new InternalNode<K, V, TKeyContainer>(id, keyContainer, childrenList);

                return parent;
            }
            throw new NotImplementedException();
        }

        public ICacheObject DeserializeCacheObject(IMemoryOwner<byte> bytes, int length, StateSerializeOptions stateSerializeOptions)
        {
            return Deserialize(bytes, length, stateSerializeOptions);
        }

        public byte[] Serialize(in IBPlusTreeNode value, in StateSerializeOptions stateSerializeOptions)
        {
            using var memoryStream = new MemoryStream();
            Stream writeMemStream = memoryStream;
            if (stateSerializeOptions.CompressFunc != null)
            {
                writeMemStream = stateSerializeOptions.CompressFunc(memoryStream);
            }
            using var writer = new BinaryWriter(writeMemStream);
            if (value is LeafNode<K, V, TKeyContainer, TValueContainer> leaf)
            {
                // Write type id
                writer.Write((byte)2);

                writer.Write(leaf.Id);
                writer.Write(leaf.next);

                _keySerializer.Serialize(writer, leaf.keys);
                _valueSerializer.Serialize(writer, leaf.values);

                writer.Flush();
                writeMemStream.Close();
                return memoryStream.ToArray();
            }
            if (value is InternalNode<K, V, TKeyContainer> parent)
            {
                writer.Write((byte)3);
                writer.Write(parent.Id);

                _keySerializer.Serialize(writer, parent.keys);

                var childrenSpan = parent.children.SlicedMemory.Span;
                writer.Write(childrenSpan.Length);
                writer.Write(childrenSpan);
                writer.Flush();
                writeMemStream.Close();
                return memoryStream.ToArray();
            }
            throw new NotImplementedException();
        }

        public byte[] Serialize(in ICacheObject value, in StateSerializeOptions stateSerializeOptions)
        {
            if (value is IBPlusTreeNode node)
            {
                return Serialize(node, stateSerializeOptions);
            }
            throw new NotImplementedException();
        }
    }
}
