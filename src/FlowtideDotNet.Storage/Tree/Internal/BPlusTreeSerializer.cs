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

namespace FlowtideDotNet.Storage.Tree.Internal
{
    internal class BPlusTreeSerializer<K, V> : IStateSerializer<IBPlusTreeNode>
    {
        private readonly IBplusTreeSerializer<K> _keySerializer;
        private readonly IBplusTreeSerializer<V> _valueSerializer;

        public BPlusTreeSerializer(
            IBplusTreeSerializer<K> keySerializer,
            IBplusTreeSerializer<V> valueSerializer
            )
        {
            this._keySerializer = keySerializer;
            this._valueSerializer = valueSerializer;
        }

        public IBPlusTreeNode Deserialize(IMemoryOwner<byte> bytes, int length)
        {
            var arr = bytes.Memory.ToArray();
            using var memoryStream = new MemoryStream(arr);
            bytes.Dispose();
            using var reader = new BinaryReader(memoryStream);

            var typeId = reader.ReadByte();

            if (typeId == 2)
            {
                var id = reader.ReadInt64();
                var leaf = new LeafNode<K, V>(id);
                leaf.next = reader.ReadInt64();

                var keyLength = reader.ReadInt32();

                for (int i = 0; i < keyLength; i++)
                {
                    var key = _keySerializer.Deserialize(reader);
                    leaf.keys.Add(key);
                }

                var valueLength = reader.ReadInt32();

                for (int i = 0; i < valueLength; i++)
                {
                    var value = _valueSerializer.Deserialize(reader);
                    leaf.values.Add(value);
                }
                return leaf;
            }
            if (typeId == 3)
            {
                var id = reader.ReadInt64();

                var parent = new InternalNode<K, V>(id);

                var keyLength = reader.ReadInt32();

                for (int i = 0; i < keyLength; i++)
                {
                    var key = _keySerializer.Deserialize(reader);
                    parent.keys.Add(key);
                }

                var childrenLength = reader.ReadInt32();
                for (int i = 0; i < childrenLength; i++)
                {
                    var childId = reader.ReadInt64();
                    parent.children.Add(childId);
                }

                return parent;
            }
            throw new NotImplementedException();
        }

        public ICacheObject DeserializeCacheObject(IMemoryOwner<byte> bytes, int length)
        {
            return Deserialize(bytes, length);
        }

        public byte[] Serialize(in IBPlusTreeNode value)
        {
            using var memoryStream = new MemoryStream();
            using var writer = new BinaryWriter(memoryStream);
            if (value is LeafNode<K, V> leaf)
            {
                // Write type id
                writer.Write((byte)2);

                writer.Write(leaf.Id);
                writer.Write(leaf.next);

                writer.Write(leaf.keys.Count);

                for (int i = 0; i < leaf.keys.Count; i++)
                {
                    _keySerializer.Serialize(writer, leaf.keys[i]);
                }
                writer.Write(leaf.values.Count);

                for (int i = 0; i < leaf.values.Count; i++)
                {
                    _valueSerializer.Serialize(writer, leaf.values[i]);
                }
                return memoryStream.ToArray();
            }
            if (value is InternalNode<K, V> parent)
            {
                writer.Write((byte)3);
                writer.Write(parent.Id);

                writer.Write(parent.keys.Count);
                for (int i = 0; i < parent.keys.Count; i++)
                {
                    _keySerializer.Serialize(writer, parent.keys[i]);
                }

                writer.Write(parent.children.Count);
                for (int i = 0; i < parent.children.Count; i++)
                {
                    writer.Write(parent.children[i]);
                }
                return memoryStream.ToArray();
            }
            throw new NotImplementedException();
        }

        public byte[] Serialize(in ICacheObject value)
        {
            if (value is IBPlusTreeNode node)
            {
                return Serialize(node);
            }
            throw new NotImplementedException();
        }
    }
}
