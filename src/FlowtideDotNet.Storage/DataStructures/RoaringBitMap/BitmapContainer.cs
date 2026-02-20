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

using FlowtideDotNet.Storage.DataStructures.RoaringBitMap;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.DataStructures
{
    internal class BitmapContainer : Container, IEquatable<BitmapContainer>
    {
        public static readonly BitmapContainer One;

        static BitmapContainer()
        {
            var data = new ulong[MAX_CAPACITY_LONG];
            for (var i = 0; i < MAX_CAPACITY_LONG; i++)
            {
                data[i] = ulong.MaxValue;
            }
            One = new BitmapContainer(data, MAX_CAPACITY);
        }

        public const int MAX_CAPACITY = 1 << 16;
        private const int MAX_CAPACITY_BYTE = MAX_CAPACITY / 1;
        private const int MAX_CAPACITY_LONG = MAX_CAPACITY / 8;
        private const int BitmapLength = 1024;

        internal ulong[] bitmap;
        internal int cardinality;

        public override bool IsEmpty => throw new NotImplementedException();

        public override int ArraySizeInBytes => MAX_CAPACITY / 8;

        public BitmapContainer()
        {
            this.cardinality = 0;
            this.bitmap = new ulong[MAX_CAPACITY_LONG];
        }

        private BitmapContainer(int cardinality)
        {
            bitmap = new ulong[BitmapLength];
            this.cardinality = cardinality;
        }

        public BitmapContainer(ulong[] bitmap, int cardinality)
        {
            this.bitmap = bitmap;
            this.cardinality = cardinality;
        }

        public BitmapContainer(int cardinality, ushort[] values, bool negated) : this(negated ? MaxCapacity - cardinality : cardinality)
        {
            if (negated)
            {
                for (var i = 0; i < BitmapLength; i++)
                {
                    bitmap[i] = ulong.MaxValue;
                }
                for (var i = 0; i < cardinality; i++)
                {
                    var v = values[i];
                    bitmap[v >> 6] &= ~(1UL << v);
                }
            }
            else
            {
                for (var i = 0; i < cardinality; i++)
                {
                    var v = values[i];
                    bitmap[v >> 6] |= 1UL << v;
                }
            }
        }

        internal void LoadData(ArrayContainer arrayContainer)
        {
            this.cardinality = arrayContainer.cardinality;
            for (int k = 0; k < arrayContainer.cardinality; ++k)
            {
                ushort x = arrayContainer.content[k];
                bitmap[(x) / 64] |= (1UL << x);
            }
        }

        public override Container Add(ushort x)
        {
            ulong previous = bitmap[x >>> 6];
            ulong newval = previous | (1UL << x);
            bitmap[x >>> 6] = newval;
            if (previous != newval)
            {
                ++cardinality;
            }
            return this;
        }

        public override bool Contains(ushort x)
        {
            return (bitmap[x >>> 6] & (1UL << x)) != 0;
        }

        public override Container Remove(ushort x)
        {
            int index = x >>> 6;
            ulong bef = bitmap[index];
            ulong mask = 1UL << x;
            if (cardinality == ArrayContainer.DEFAULT_MAX_SIZE + 1)
            {
                if ((bef & mask) != 0)
                {
                    --cardinality;
                    bitmap[x >>> 6] = bef & ~mask;
                    return this.ToArrayContainer();
                }
            }
            ulong aft = bef & ~mask;
            cardinality -= (int)(aft - bef) >>> 63;
            bitmap[index] = aft;
            return this;
        }

        ArrayContainer ToArrayContainer()
        {
            ArrayContainer ac = new ArrayContainer(cardinality);
            if (cardinality != 0)
            {
                ac.LoadData(this);
            }
            if (ac.GetCardinality() != cardinality)
            {
                throw new Exception("Internal error.");
            }
            return ac;
        }

        public override int GetCardinality()
        {
            return cardinality;
        }

        public override IEnumerator<ushort> GetEnumerator()
        {
            return new BitmapContainerEnumerator(bitmap);
        }

        public override IContainerEnumerator GetContainerEnumerator()
        {
            return new BitmapContainerEnumerator(bitmap);
        }

        public bool Equals(BitmapContainer? other)
        {
            if (ReferenceEquals(this, other))
            {
                return true;
            }
            if (ReferenceEquals(null, other))
            {
                return false;
            }
            if (cardinality != other.cardinality)
            {
                return false;
            }
            for (var i = 0; i < MAX_CAPACITY_LONG; i++)
            {
                if (bitmap[i] != other.bitmap[i])
                {
                    return false;
                }
            }
            return true;
        }

        public static void Serialize(in BitmapContainer bc, in IBufferWriter<byte> bufferWriter)
        {
            for (var i = 0; i < BitmapLength; i++)
            {
                Utils.WriteULong(in bufferWriter, in bc.bitmap[i]);
            }
        }

        public static BitmapContainer Deserialize(ref SequenceReader<byte> reader, int cardinality)
        {
            var data = new ulong[BitmapLength];
            for (var i = 0; i < BitmapLength; i++)
            {
                data[i] = Utils.ReadUint64(ref reader);
            }
            return new BitmapContainer(data, cardinality);
        }
    }
}
