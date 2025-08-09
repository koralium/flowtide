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

using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors.RoaringBitmap
{
    /// <summary>
    /// Code from https://github.com/Tornhoof/RoaringBitmap/tree/master which is archived
    /// </summary>
    internal class BitmapContainer : Container, IEquatable<BitmapContainer>
    {
        private const int BitmapLength = 1024;
        public static readonly BitmapContainer One;
        private readonly ulong[] m_Bitmap;
        private readonly int m_Cardinality;

        static BitmapContainer()
        {
            var data = new ulong[BitmapLength];
            for (var i = 0; i < BitmapLength; i++)
            {
                data[i] = ulong.MaxValue;
            }
            One = new BitmapContainer(1 << 16, data);
        }

        private BitmapContainer(int cardinality)
        {
            m_Bitmap = new ulong[BitmapLength];
            m_Cardinality = cardinality;
        }

        private BitmapContainer(int cardinality, ulong[] data)
        {
            m_Bitmap = data;
            m_Cardinality = cardinality;
        }

        private BitmapContainer(int cardinality, ushort[] values, bool negated) : this(negated ? MaxCapacity - cardinality : cardinality)
        {
            if (negated)
            {
                for (var i = 0; i < BitmapLength; i++)
                {
                    m_Bitmap[i] = ulong.MaxValue;
                }
                for (var i = 0; i < cardinality; i++)
                {
                    var v = values[i];
                    m_Bitmap[v >> 6] &= ~(1UL << v);
                }
            }
            else
            {
                for (var i = 0; i < cardinality; i++)
                {
                    var v = values[i];
                    m_Bitmap[v >> 6] |= 1UL << v;
                }
            }
        }

        protected internal override int Cardinality => m_Cardinality;

        public override int ArraySizeInBytes => MaxCapacity / 8;

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
            if (m_Cardinality != other.m_Cardinality)
            {
                return false;
            }
            for (var i = 0; i < BitmapLength; i++)
            {
                if (m_Bitmap[i] != other.m_Bitmap[i])
                {
                    return false;
                }
            }
            return true;
        }


        internal static BitmapContainer Create(ushort[] values)
        {
            return new BitmapContainer(values.Length, values, false);
        }

        internal static BitmapContainer Create(int cardinality, ushort[] values)
        {
            return new BitmapContainer(cardinality, values, false);
        }

        internal static BitmapContainer Create(int cardinality, ushort[] values, bool negated)
        {
            return new BitmapContainer(cardinality, values, negated);
        }


        internal static BitmapContainer CreateXor(ushort[] first, int firstCardinality, ushort[] second, int secondCardinality)
        {
            var data = new ulong[BitmapLength];
            for (var i = 0; i < firstCardinality; i++)
            {
                var v = first[i];
                data[v >> 6] ^= 1UL << v;
            }

            for (var i = 0; i < secondCardinality; i++)
            {
                var v = second[i];
                data[v >> 6] ^= 1UL << v;
            }
            var cardinality = Util.BitCount(data);
            return new BitmapContainer(cardinality, data);
        }

        /// <summary>
        ///     Java version has an optimized version of this, but it's using bitcount internally which should make it slower in
        ///     .NET
        /// </summary>
        public static Container operator &(BitmapContainer x, BitmapContainer y)
        {
            var data = Clone(x.m_Bitmap);
            var bc = new BitmapContainer(AndInternal(data, y.m_Bitmap), data);
            return bc.m_Cardinality <= MaxSize ? ArrayContainer.Create(bc) : bc;
        }

        private static ulong[] Clone(ulong[] data)
        {
            var result = new ulong[BitmapLength];
            Buffer.BlockCopy(data, 0, result, 0, BitmapLength * sizeof(ulong));
            return result;
        }

        public static ArrayContainer operator &(BitmapContainer x, ArrayContainer y)
        {
            return y & x;
        }

        public static BitmapContainer operator |(BitmapContainer x, BitmapContainer y)
        {
            var data = Clone(x.m_Bitmap);
            return new BitmapContainer(OrInternal(data, y.m_Bitmap), data);
        }

        public static BitmapContainer operator |(BitmapContainer x, ArrayContainer y)
        {
            var data = Clone(x.m_Bitmap);
            return new BitmapContainer(x.m_Cardinality + y.OrArray(data), data);
        }

        public static Container operator ~(BitmapContainer x)
        {
            var data = Clone(x.m_Bitmap);
            var bc = new BitmapContainer(NotInternal(data), data);
            return bc.m_Cardinality <= MaxSize ? ArrayContainer.Create(bc) : bc;
        }

        /// <summary>
        ///     Java version has an optimized version of this, but it's using bitcount internally which should make it slower in
        ///     .NET
        /// </summary>
        public static Container operator ^(BitmapContainer x, BitmapContainer y)
        {
            var data = Clone(x.m_Bitmap);
            var bc = new BitmapContainer(XorInternal(data, y.m_Bitmap), data);
            return bc.m_Cardinality <= MaxSize ? ArrayContainer.Create(bc) : bc;
        }


        public static Container operator ^(BitmapContainer x, ArrayContainer y)
        {
            var data = Clone(x.m_Bitmap);
            var bc = new BitmapContainer(x.m_Cardinality + y.XorArray(data), data);
            return bc.m_Cardinality <= MaxSize ? ArrayContainer.Create(bc) : bc;
        }

        public static Container AndNot(BitmapContainer x, BitmapContainer y)
        {
            var data = Clone(x.m_Bitmap);
            var bc = new BitmapContainer(AndNotInternal(data, y.m_Bitmap), data);
            return bc.m_Cardinality <= MaxSize ? ArrayContainer.Create(bc) : bc;
        }

        public static Container AndNot(BitmapContainer x, ArrayContainer y)
        {
            var data = Clone(x.m_Bitmap);
            var bc = new BitmapContainer(x.m_Cardinality + y.AndNotArray(data), data);
            return bc.m_Cardinality <= MaxSize ? ArrayContainer.Create(bc) : bc;
        }

        private static int XorInternal(ulong[] first, ulong[] second)
        {
            for (var k = 0; k < BitmapLength; k++)
            {
                first[k] = first[k] ^ second[k];
            }
            var c = Util.BitCount(first);
            return c;
        }

        private static int AndNotInternal(ulong[] first, ulong[] second)
        {
            for (var k = 0; k < first.Length; k++)
            {
                first[k] = first[k] & ~second[k];
            }
            var c = Util.BitCount(first);
            return c;
        }

        private static int NotInternal(ulong[] data)
        {
            for (var k = 0; k < BitmapLength; k++)
            {
                data[k] = ~data[k];
            }
            var c = Util.BitCount(data);
            return c;
        }

        private static int OrInternal(ulong[] first, ulong[] second)
        {
            for (var k = 0; k < BitmapLength; k++)
            {
                first[k] = first[k] | second[k];
            }
            var c = Util.BitCount(first);
            return c;
        }

        private static int AndInternal(ulong[] first, ulong[] second)
        {
            for (var k = 0; k < BitmapLength; k++)
            {
                first[k] = first[k] & second[k];
            }
            var c = Util.BitCount(first);
            return c;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Contains(ushort x)
        {
            return Contains(m_Bitmap, x);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool Contains(ulong[] bitmap, ushort x)
        {
            return (bitmap[x >> 6] & 1UL << x) != 0;
        }

        protected override bool EqualsInternal(Container other)
        {
            var bc = other as BitmapContainer;
            return bc != null && Equals(bc);
        }

        public override IEnumerator<ushort> GetEnumerator()
        {
            for (var k = 0; k < BitmapLength; k++)
            {
                var bitset = m_Bitmap[k];
                var shiftedK = k << 6;
                while (bitset != 0)
                {
                    var t = bitset & ~bitset + 1;
                    var result = (ushort)(shiftedK + Util.BitCount(t - 1));
                    yield return result;
                    bitset ^= t;
                }
            }
        }

        internal int FillArray(ushort[] data)
        {
            var pos = 0;
            for (var k = 0; k < BitmapLength; k++)
            {
                var bitset = m_Bitmap[k];
                var shiftedK = k << 6;
                while (bitset != 0)
                {
                    var t = bitset & ~bitset + 1;
                    data[pos++] = (ushort)(shiftedK + Util.BitCount(t - 1));
                    bitset ^= t;
                }
            }
            return m_Cardinality;
        }

        public override bool Equals(object? obj)
        {
            var bc = obj as BitmapContainer;
            return bc != null && Equals(bc);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var code = 17;
                code = code * 23 + m_Cardinality;
                for (var i = 0; i < BitmapLength; i++)
                {
                    code = code * 23 + m_Bitmap[i].GetHashCode();
                }
                return code;
            }
        }

        public static void Serialize(BitmapContainer bc, BinaryWriter binaryWriter)
        {
            for (var i = 0; i < BitmapLength; i++)
            {
                binaryWriter.Write(bc.m_Bitmap[i]);
            }
        }

        public static BitmapContainer Deserialize(BinaryReader binaryReader, int cardinality)
        {
            var data = new ulong[BitmapLength];
            for (var i = 0; i < BitmapLength; i++)
            {
                data[i] = binaryReader.ReadUInt64();
            }
            return new BitmapContainer(cardinality, data);
        }
    }
}