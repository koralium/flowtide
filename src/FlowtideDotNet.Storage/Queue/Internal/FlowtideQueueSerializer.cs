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

using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.StateManager.Internal;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Storage.Tree.Internal;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Queue.Internal
{
    internal class FlowtideQueueSerializer<V, TValueContainer> : IStateSerializer<IBPlusTreeNode>
        where TValueContainer : IValueContainer<V>
    {
        private readonly IBplusTreeValueSerializer<V, TValueContainer> _valueSerializer;

        public FlowtideQueueSerializer(IBplusTreeValueSerializer<V, TValueContainer> valueSerializer)
        {
            this._valueSerializer = valueSerializer;
        }

        public Task CheckpointAsync<TMetadata>(IStateSerializerCheckpointWriter checkpointWriter, StateClientMetadata<TMetadata> metadata) where TMetadata : IStorageMetadata
        {
            return Task.CompletedTask;
        }

        public void ClearTemporaryAllocations()
        {
        }

        public IBPlusTreeNode Deserialize(ReadOnlyMemory<byte> bytes, int length)
        {
            var sequenceReader = new SequenceReader<byte>(new ReadOnlySequence<byte>(bytes));

            if (!sequenceReader.TryReadLittleEndian(out long id))
            {
                throw new Exception("Could not read id");
            }

            if (!sequenceReader.TryReadLittleEndian(out long leafNext))
            {
                throw new Exception("Could not read leafNext");
            }

            if (!sequenceReader.TryReadLittleEndian(out long leafPrevious))
            {
                throw new Exception("Could not read leafPrevious");
            }

            var valueContainer = _valueSerializer.Deserialize(ref sequenceReader);

            if (sequenceReader.UnreadSpan.Length > 0)
            {
                throw new Exception("Did not read all bytes");
            }
            var node = new QueueNode<V, TValueContainer>(id, valueContainer);
            node.next = leafNext;
            node.previous = leafPrevious;
            return node;
        }

        public ICacheObject DeserializeCacheObject(ReadOnlyMemory<byte> bytes, int length)
        {
            return Deserialize(bytes, length);
        }

        public void Dispose()
        {
        }

        public Task InitializeAsync<TMetadata>(IStateSerializerInitializeReader reader, StateClientMetadata<TMetadata> metadata) where TMetadata : IStorageMetadata
        {
            return Task.CompletedTask;
        }

        public void Serialize(in IBufferWriter<byte> bufferWriter, in IBPlusTreeNode value)
        {
            if (value is QueueNode<V, TValueContainer> queueNode)
            {
                var headerSpan = bufferWriter.GetSpan(24);
                BinaryPrimitives.WriteInt64LittleEndian(headerSpan.Slice(0), queueNode.Id);
                BinaryPrimitives.WriteInt64LittleEndian(headerSpan.Slice(8), queueNode.next);
                BinaryPrimitives.WriteInt64LittleEndian(headerSpan.Slice(16), queueNode.previous);
                bufferWriter.Advance(24);

                _valueSerializer.Serialize(bufferWriter, queueNode.values);
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
    }
}
