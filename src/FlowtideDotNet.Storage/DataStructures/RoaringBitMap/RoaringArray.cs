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
using System.IO;

namespace FlowtideDotNet.Storage.DataStructures
{
    internal class RoaringArray
    {
        private const int SerialCookieNoRuncontainer = 12346;
        private const int SerialCookie = 12347;
        private const int NoOffsetThreshold = 4;

        private int size = 0;
        private ushort[] keys;
        private Container?[] values;

        public RoaringArray()
            : this(4)
        {
            
        }

        public int Size => size;

        public RoaringArray(int initialCapacity)
            : this(new ushort[initialCapacity], new Container[initialCapacity], 0)
        {

        }

        public RoaringArray(ushort[] keys, Container[] values, int size)
        {
            this.keys = keys;
            this.values = values;
            this.size = size;
        }

        public int GetIndex(ushort x)
        {
            if ((size == 0) || (keys[size - 1] == x))
            {
                return size - 1;
            }
            // no luck we have to go through the list
            return this.BinarySearch(0, size, x);
        }

        private int BinarySearch(int begin, int end, ushort key)
        {
            return Utils.UnsignedBinarySearch(keys, begin, end, key);
        }

        internal void SetContainerAtIndex(int i, Container container)
        {
            values[i] = container;
        }

        internal Container GetContainerAtIndex(int i)
        {
            return this.values[i]!;
        }

        public int GetContainerIndex(ushort x)
        {
            int i = this.BinarySearch(0, size, x);
            return i;
        }

        public void InsertNewKeyValueAt(int i, ushort key, Container value)
        {
            ExtendArray(1);
            Array.Copy(keys, i, keys, i + 1, size - i);
            keys[i] = key;
            Array.Copy(values, i, values, i + 1, size - i);
            values[i] = value;
            size++;
        }

        void ExtendArray(int k)
        {
            // size + 1 could overflow
            if (this.size + k > this.keys.Length)
            {
                int newCapacity;
                if (this.keys.Length < 1024)
                {
                    newCapacity = 2 * (this.size + k);
                }
                else
                {
                    newCapacity = 5 * (this.size + k) / 4;
                }
                var newKeys = new ushort[newCapacity];
                var newValues = new Container[newCapacity];
                Array.Copy(this.keys, 0, newKeys, 0, this.size);
                Array.Copy(this.values, 0, newValues, 0, this.size);
                this.keys = newKeys;
                this.values = newValues;
            }
        }

        public void RemoveAtIndex(int i)
        {
            Array.Copy(keys, i + 1, keys, i, size - i - 1);
            keys[size - 1] = 0;

            Array.Copy(values, i + 1, values, i, size - i - 1);
            values[size - 1] = null;
            size--;
        }

        internal ushort GetKeyAtIndex(int i)
        {
            return this.keys[i];
        }

        private static bool HasRunContainer(RoaringArray roaringArray)
        {
            for (var i = 0; i < roaringArray.size; i++)
            {
                var container = roaringArray.values[i];
                if (container != null && (container.Equals(ArrayContainer.One) || container.Equals(BitmapContainer.One)))
                {
                    return true;
                }
            }
            return false;
        }

        public void Serialize(IBufferWriter<byte> bufferWriter)
        {
            var hasRun = HasRunContainer(this);

            var startOffset = 0;
            if (hasRun)
            {
                Utils.WriteInt(in bufferWriter, SerialCookie | size - 1 << 16);

                var bitmapOfRunContainers = bufferWriter.GetSpan((size + 7) / 8);
                for (var i = 0; i < size; ++i)
                {
                    var val = values[i];
                    if (val != null && (val.Equals(ArrayContainer.One) || val.Equals(BitmapContainer.One)))
                    {
                        bitmapOfRunContainers[i / 8] |= (byte)(1 << i % 8);
                    }
                }
                bufferWriter.Advance((size + 7) / 8);
            }
            else
            {
                Utils.WriteInt(in bufferWriter, SerialCookieNoRuncontainer);
                Utils.WriteInt(in bufferWriter, in size);
                startOffset = 4 + 4 + 4 * size + 4 * size;
            }

            for (var k = 0; k < size; ++k)
            {
                var val = values[k];
                if (val != null)
                {
                    Utils.WriteUshort(in bufferWriter, in keys[k]);
                    Utils.WriteUshort(in bufferWriter, (ushort)(val.GetCardinality() - 1));
                }
            }

            if (!hasRun || size >= NoOffsetThreshold)
            {
                for (var k = 0; k < size; k++)
                {
                    var val = values[k];

                    if (val != null)
                    {
                        Utils.WriteInt(in bufferWriter, in startOffset);
                        startOffset += val.ArraySizeInBytes;
                    }
                }
            }

            for (var k = 0; k < size; ++k)
            {
                var container = values[k];
                ArrayContainer ac;
                BitmapContainer bc;
                if ((ac = (container as ArrayContainer)!) != null)
                {
                    if (ac.Equals(ArrayContainer.One))
                    {
                        Utils.WriteUshort(in bufferWriter, 1);
                        Utils.WriteUshort(in bufferWriter, 0);
                        Utils.WriteUshort(in bufferWriter, ArrayContainer.DEFAULT_MAX_SIZE - 1);
                    }
                    else
                    {
                        ArrayContainer.Serialize(ac, binaryWriter);
                    }
                }
                else if ((bc = (container as BitmapContainer)!) != null)
                {
                    if (bc.Equals(BitmapContainer.One))
                    {
                        binaryWriter.Write((ushort)1);
                        binaryWriter.Write((ushort)0);
                        binaryWriter.Write((ushort)(Container.MaxCapacity - 1));
                    }
                    else
                    {
                        BitmapContainer.Serialize(bc, binaryWriter);
                    }
                }
            }
        }
    }
}
