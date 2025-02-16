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

using System;
using System.Collections.Generic;
using System.IO;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors.RoaringBitmap
{
    /// <summary>
    /// Code from https://github.com/Tornhoof/RoaringBitmap/tree/master which is archived
    /// </summary>
    internal class ArrayContainer : Container, IEquatable<ArrayContainer>
    {
        public static readonly ArrayContainer One;
        private readonly int m_Cardinality;
        private readonly ushort[] m_Content;

        static ArrayContainer()
        {
            var data = new ushort[MaxSize];
            for (ushort i = 0; i < MaxSize; i++)
            {
                data[i] = i;
            }
            One = new ArrayContainer(MaxSize, data);
        }

        private ArrayContainer(int cardinality, ushort[] data)
        {
            m_Content = data;
            m_Cardinality = cardinality;
        }

        protected internal override int Cardinality => m_Cardinality;

        public override int ArraySizeInBytes => m_Cardinality * sizeof(ushort);


        public bool Equals(ArrayContainer? other)
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
            for (var i = 0; i < m_Cardinality; i++)
            {
                if (m_Content[i] != other.m_Content[i])
                {
                    return false;
                }
            }
            return true;
        }

        internal static ArrayContainer Create(ushort[] values)
        {
            return new ArrayContainer(values.Length, values);
        }

        internal static ArrayContainer Create(BitmapContainer bc)
        {
            var data = new ushort[bc.Cardinality];
            var cardinality = bc.FillArray(data);
            var result = new ArrayContainer(cardinality, data);
            return result;
        }

        protected override bool EqualsInternal(Container other)
        {
            var ac = other as ArrayContainer;
            return ac != null && Equals(ac);
        }

        public override IEnumerator<ushort> GetEnumerator()
        {
            for (var i = 0; i < m_Cardinality; i++)
            {
                yield return m_Content[i];
            }
        }


        public static Container operator &(ArrayContainer x, ArrayContainer y)
        {
            var desiredCapacity = Math.Min(x.m_Cardinality, y.m_Cardinality);
            var data = new ushort[desiredCapacity];
            var calculatedCardinality = Util.IntersectArrays(x.m_Content, x.m_Cardinality, y.m_Content, y.m_Cardinality, data);
            return new ArrayContainer(calculatedCardinality, data);
        }

        public static ArrayContainer operator &(ArrayContainer x, BitmapContainer y)
        {
            var data = new ushort[x.m_Content.Length];
            var c = x.m_Cardinality;
            var pos = 0;
            for (var i = 0; i < c; i++)
            {
                var v = x.m_Content[i];
                if (y.Contains(v))
                {
                    data[pos++] = v;
                }
            }
            return new ArrayContainer(pos, data);
        }

        public static Container operator |(ArrayContainer x, ArrayContainer y)
        {
            var totalCardinality = x.m_Cardinality + y.m_Cardinality;
            if (totalCardinality > MaxSize)
            {
                var output = new ushort[totalCardinality];
                var calcCardinality = Util.UnionArrays(x.m_Content, x.m_Cardinality, y.m_Content, y.m_Cardinality, output);
                if (calcCardinality > MaxSize)
                {
                    return BitmapContainer.Create(calcCardinality, output);
                }
                return new ArrayContainer(calcCardinality, output);
            }
            var desiredCapacity = totalCardinality;
            var data = new ushort[desiredCapacity];
            var calculatedCardinality = Util.UnionArrays(x.m_Content, x.m_Cardinality, y.m_Content, y.m_Cardinality, data);
            return new ArrayContainer(calculatedCardinality, data);
        }

        public static Container operator |(ArrayContainer x, BitmapContainer y)
        {
            return y | x;
        }

        public static Container operator ~(ArrayContainer x)
        {
            return BitmapContainer.Create(x.m_Cardinality, x.m_Content, true); // an arraycontainer only contains up to 4096 values, so the negation is a bitmap container
        }

        public static Container operator ^(ArrayContainer x, ArrayContainer y)
        {
            var totalCardinality = x.m_Cardinality + y.m_Cardinality;
            if (totalCardinality > MaxSize)
            {
                var bc = BitmapContainer.CreateXor(x.m_Content, x.Cardinality, y.m_Content, y.Cardinality);
                if (bc.Cardinality <= MaxSize)
                {
                    Create(bc);
                }
            }
            var desiredCapacity = totalCardinality;
            var data = new ushort[desiredCapacity];
            var calculatedCardinality = Util.XorArrays(x.m_Content, x.m_Cardinality, y.m_Content, y.m_Cardinality, data);
            return new ArrayContainer(calculatedCardinality, data);
        }

        public static Container operator ^(ArrayContainer x, BitmapContainer y)
        {
            return y ^ x;
        }

        public static Container AndNot(ArrayContainer x, ArrayContainer y)
        {
            var desiredCapacity = x.m_Cardinality;
            var data = new ushort[desiredCapacity];
            var calculatedCardinality = Util.DifferenceArrays(x.m_Content, x.m_Cardinality, y.m_Content, y.m_Cardinality, data);
            return new ArrayContainer(calculatedCardinality, data);
        }

        public static Container AndNot(ArrayContainer x, BitmapContainer y)
        {
            var data = new ushort[x.m_Content.Length];
            var c = x.m_Cardinality;
            var pos = 0;
            for (var i = 0; i < c; i++)
            {
                var v = x.m_Content[i];
                if (!y.Contains(v))
                {
                    data[pos++] = v;
                }
            }
            return new ArrayContainer(pos, data);
        }

        public int OrArray(ulong[] bitmap)
        {
            var extraCardinality = 0;
            var yC = m_Cardinality;
            for (var i = 0; i < yC; i++)
            {
                var yValue = m_Content[i];
                var index = yValue >> 6;
                var previous = bitmap[index];
                var after = previous | 1UL << yValue;
                bitmap[index] = after;
                extraCardinality += (int)(previous - after >> 63);
            }
            return extraCardinality;
        }

        public int XorArray(ulong[] bitmap)
        {
            var extraCardinality = 0;
            var yC = m_Cardinality;
            for (var i = 0; i < yC; i++)
            {
                var yValue = m_Content[i];
                var index = yValue >> 6;
                var previous = bitmap[index];
                var mask = 1UL << yValue;
                bitmap[index] = previous ^ mask;
                extraCardinality += (int)(1 - 2 * ((previous & mask) >> yValue));
            }
            return extraCardinality;
        }


        public int AndNotArray(ulong[] bitmap)
        {
            var extraCardinality = 0;
            var yC = m_Cardinality;
            for (var i = 0; i < yC; i++)
            {
                var yValue = m_Content[i];
                var index = yValue >> 6;
                var previous = bitmap[index];
                var after = previous & ~(1UL << yValue);
                bitmap[index] = after;
                extraCardinality -= (int)((previous ^ after) >> yValue);
            }
            return extraCardinality;
        }

        public override bool Equals(object? obj)
        {
            var ac = obj as ArrayContainer;
            return ac != null && Equals(ac);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var code = 17;
                code = code * 23 + m_Cardinality;
                for (var i = 0; i < m_Cardinality; i++)
                {
                    code = code * 23 + m_Content[i];
                }
                return code;
            }
        }

        public static void Serialize(ArrayContainer ac, BinaryWriter binaryWriter)
        {
            for (var i = 0; i < ac.m_Cardinality; i++)
            {
                binaryWriter.Write(ac.m_Content[i]);
            }
        }

        public static ArrayContainer Deserialize(BinaryReader binaryReader, int cardinality)
        {
            var data = new ushort[cardinality];
            for (var i = 0; i < cardinality; i++)
            {
                data[i] = binaryReader.ReadUInt16();
            }
            return new ArrayContainer(cardinality, data);
        }
    }
}