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
using System.Buffers.Binary;
using System.Collections.Immutable;
using ZstdSharp;

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
        }

        public IBPlusTreeNode Deserialize(ReadOnlyMemory<byte> bytes, int length)
        {
            var sequenceReader = new SequenceReader<byte>(new ReadOnlySequence<byte>(bytes));
            if (!sequenceReader.TryRead(out byte typeId))
            {
                throw new Exception("Could not read typeId");
            }

            if (typeId == 2)
            {
                if (!sequenceReader.TryReadLittleEndian(out long id))
                {
                    throw new Exception("Could not read id");
                }

                if (!sequenceReader.TryReadLittleEndian(out long leafNext))
                {
                    throw new Exception("Could not read leafNext");
                }

                var keyContainer = _keySerializer.Deserialize(ref sequenceReader);
                var valueContainer = _valueSerializer.Deserialize(ref sequenceReader);

                if (sequenceReader.UnreadSpan.Length > 0)
                {
                    throw new Exception("Did not read all bytes");
                }

                var leaf = new LeafNode<K, V, TKeyContainer, TValueContainer>(id, keyContainer, valueContainer);
                leaf.next = leafNext;
                return leaf;
            }
            if (typeId == 3)
            {
                if (!sequenceReader.TryReadLittleEndian(out long id))
                {
                    throw new Exception("Could not read id");
                }

                var keyContainer = _keySerializer.Deserialize(ref sequenceReader);

                if (!sequenceReader.TryReadLittleEndian(out int childrenByteLength))
                {
                    throw new Exception("Could not read childrenByteLength");
                }

                var childrenMemory = _memoryAllocator.Allocate(childrenByteLength, 64);
                if (!sequenceReader.TryCopyTo(childrenMemory.Memory.Span.Slice(0, childrenByteLength)))
                {
                    throw new Exception("Could not read children data");
                }
                sequenceReader.Advance(childrenByteLength);

                if (sequenceReader.UnreadSpan.Length > 0)
                {
                    throw new Exception("Did not read all bytes");
                }

                var childrenList = new PrimitiveList<long>(childrenMemory, childrenByteLength / sizeof(long), _memoryAllocator);

                var parent = new InternalNode<K, V, TKeyContainer>(id, keyContainer, childrenList);

                return parent;
            }
            throw new NotImplementedException();
        }

        public ICacheObject DeserializeCacheObject(ReadOnlyMemory<byte> bytes, int length)
        {
            return Deserialize(bytes, length);
        }

        public void Serialize(in IBufferWriter<byte> bufferWriter,in IBPlusTreeNode value)
        {
            if (value is LeafNode<K, V, TKeyContainer, TValueContainer> leaf)
            {
                var headerSpan = bufferWriter.GetSpan(17);
                headerSpan[0] = 2;
                BinaryPrimitives.WriteInt64LittleEndian(headerSpan.Slice(1), leaf.Id);
                BinaryPrimitives.WriteInt64LittleEndian(headerSpan.Slice(9), leaf.next);
                bufferWriter.Advance(17);

                _keySerializer.Serialize(bufferWriter, leaf.keys);
                _valueSerializer.Serialize(bufferWriter, leaf.values);
                return;
            }
            if (value is InternalNode<K, V, TKeyContainer> parent)
            {
                var headerSpan = bufferWriter.GetSpan(9);
                headerSpan[0] = 3;
                BinaryPrimitives.WriteInt64LittleEndian(headerSpan.Slice(1), parent.Id);
                bufferWriter.Advance(9);

                _keySerializer.Serialize(bufferWriter, parent.keys);

                var childrenLengthSpan = bufferWriter.GetSpan(4);
                var childrenSpan = parent.children.SlicedMemory.Span;
                BinaryPrimitives.WriteInt32LittleEndian(childrenLengthSpan, childrenSpan.Length);
                bufferWriter.Advance(4);
                bufferWriter.Write(childrenSpan);
                return;
            }
            throw new NotImplementedException();
        }

        public void Serialize(in IBufferWriter<byte> bufferWriter, in ICacheObject value)
        {
            if (value is IBPlusTreeNode node)
            {
                Serialize(bufferWriter, node);
                return;
            }
            throw new NotImplementedException();
        }

        public void Dispose()
        {
        }
    }
}
