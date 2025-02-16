using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors.RoaringBitmap
{
    internal class RoaringArray : IEnumerable<int>, IEquatable<RoaringArray>
    {
        private const int SerialCookieNoRuncontainer = 12346;
        private const int SerialCookie = 12347;
        private const int NoOffsetThreshold = 4;
        private readonly ushort[] m_Keys;
        private readonly int m_Size;
        private readonly Container[] m_Values;


        // ReSharper disable once SuggestBaseTypeForParameter
        /// <summary>
        ///     Use List directly, because the enumerator is a struct
        /// </summary>
        internal RoaringArray(int size, List<ushort> keys, List<Container> containers)
        {
            m_Size = size;
            m_Keys = new ushort[m_Size];
            m_Values = new Container[m_Size];
            for (var i = 0; i < m_Size; i++)
            {
                m_Keys[i] = keys[i];
                m_Values[i] = containers[i];
                Cardinality += m_Values[i].Cardinality;
            }
        }

        private RoaringArray(int size, ushort[] keys, Container[] containers)
        {
            m_Size = size;
            m_Keys = keys;
            m_Values = containers;
            for (var i = 0; i < containers.Length; i++)
            {
                Cardinality += containers[i].Cardinality;
            }
        }

        public long Cardinality { get; }

        public IEnumerator<int> GetEnumerator()
        {
            for (var i = 0; i < m_Size; i++)
            {
                var key = m_Keys[i];
                var shiftedKey = key << 16;
                var container = m_Values[i];
                foreach (var @ushort in container)
                {
                    yield return shiftedKey | @ushort;
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public bool Equals(RoaringArray other)
        {
            if (ReferenceEquals(this, other))
            {
                return true;
            }
            if (ReferenceEquals(null, other))
            {
                return false;
            }
            if (m_Size != other.m_Size)
            {
                return false;
            }
            for (var i = 0; i < m_Size; i++)
            {
                if (m_Keys[i] != other.m_Keys[i] || !m_Values[i].Equals(other.m_Values[i]))
                {
                    return false;
                }
            }
            return true;
        }

        private int AdvanceUntil(ushort key, int index)
        {
            return Util.AdvanceUntil(m_Keys, index, m_Keys.Length, key);
        }

        public static RoaringArray operator |(RoaringArray x, RoaringArray y)
        {
            var xLength = x.m_Size;
            var yLength = y.m_Size;
            var keys = new List<ushort>(xLength + yLength);
            var containers = new List<Container>(xLength + yLength);
            var size = 0;
            var xPos = 0;
            var yPos = 0;
            if (xPos < xLength && yPos < yLength)
            {
                var xKey = x.m_Keys[xPos];
                var yKey = y.m_Keys[yPos];
                while (true)
                {
                    if (xKey == yKey)
                    {
                        keys.Add(xKey);
                        containers.Add(x.m_Values[xPos] | y.m_Values[yPos]);
                        size++;
                        xPos++;
                        yPos++;
                        if (xPos == xLength || yPos == yLength)
                        {
                            break;
                        }
                        xKey = x.m_Keys[xPos];
                        yKey = y.m_Keys[yPos];
                    }
                    else if (xKey < yKey)
                    {
                        keys.Add(xKey);
                        containers.Add(x.m_Values[xPos]);
                        size++;
                        xPos++;
                        if (xPos == xLength)
                        {
                            break;
                        }
                        xKey = x.m_Keys[xPos];
                    }
                    else
                    {
                        keys.Add(yKey);
                        containers.Add(y.m_Values[yPos]);
                        size++;
                        yPos++;
                        if (yPos == yLength)
                        {
                            break;
                        }
                        yKey = y.m_Keys[yPos];
                    }
                }
            }
            if (xPos == xLength)
            {
                for (var i = yPos; i < yLength; i++)
                {
                    keys.Add(y.m_Keys[i]);
                    containers.Add(y.m_Values[i]);
                    size++;
                }
            }
            else if (yPos == yLength)
            {
                for (var i = xPos; i < xLength; i++)
                {
                    keys.Add(x.m_Keys[i]);
                    containers.Add(x.m_Values[i]);
                    size++;
                }
            }
            return new RoaringArray(size, keys, containers);
        }

        public static RoaringArray operator &(RoaringArray x, RoaringArray y)
        {
            var xLength = x.m_Size;
            var yLength = y.m_Size;
            List<ushort> keys = null;
            List<Container> containers = null;
            var size = 0;
            var xPos = 0;
            var yPos = 0;
            while (xPos < xLength && yPos < yLength)
            {
                var xKey = x.m_Keys[xPos];
                var yKey = y.m_Keys[yPos];
                if (xKey == yKey)
                {
                    var c = x.m_Values[xPos] & y.m_Values[yPos];
                    if (c.Cardinality > 0)
                    {
                        if (keys == null)
                        {
                            var length = Math.Min(xLength, yLength);
                            keys = new List<ushort>(length);
                            containers = new List<Container>(length);
                        }
                        keys.Add(xKey);
                        containers.Add(c);
                        size++;
                    }
                    xPos++;
                    yPos++;
                }
                else if (xKey < yKey)
                {
                    xPos = x.AdvanceUntil(yKey, xPos);
                }
                else
                {
                    yPos = y.AdvanceUntil(xKey, yPos);
                }
            }
            return new RoaringArray(size, keys, containers);
        }

        public static RoaringArray operator ^(RoaringArray x, RoaringArray y)
        {
            var xLength = x.m_Size;
            var yLength = y.m_Size;
            var keys = new List<ushort>(xLength + yLength);
            var containers = new List<Container>(xLength + yLength);
            var size = 0;
            var xPos = 0;
            var yPos = 0;
            if (xPos < xLength && yPos < yLength)
            {
                var xKey = x.m_Keys[xPos];
                var yKey = y.m_Keys[yPos];
                while (true)
                {
                    if (xKey == yKey)
                    {
                        keys.Add(xKey);
                        containers.Add(x.m_Values[xPos] ^ y.m_Values[yPos]);
                        size++;
                        xPos++;
                        yPos++;
                        if (xPos == xLength || yPos == yLength)
                        {
                            break;
                        }
                        xKey = x.m_Keys[xPos];
                        yKey = y.m_Keys[yPos];
                    }
                    else if (xKey < yKey)
                    {
                        keys.Add(xKey);
                        containers.Add(x.m_Values[xPos]);
                        size++;
                        xPos++;
                        if (xPos == xLength)
                        {
                            break;
                        }
                        xKey = x.m_Keys[xPos];
                    }
                    else
                    {
                        keys.Add(yKey);
                        containers.Add(y.m_Values[yPos]);
                        size++;
                        yPos++;
                        if (yPos == yLength)
                        {
                            break;
                        }
                        yKey = y.m_Keys[yPos];
                    }
                }
            }
            if (xPos == xLength)
            {
                for (var i = yPos; i < yLength; i++)
                {
                    keys.Add(y.m_Keys[i]);
                    containers.Add(y.m_Values[i]);
                    size++;
                }
            }
            else if (yPos == yLength)
            {
                for (var i = xPos; i < xLength; i++)
                {
                    keys.Add(x.m_Keys[i]);
                    containers.Add(x.m_Values[i]);
                    size++;
                }
            }
            return new RoaringArray(size, keys, containers);
        }

        public static RoaringArray operator ~(RoaringArray x)
        {
            var keys = new List<ushort>(Container.MaxCapacity);
            var size = 0;
            var containers = new List<Container>(Container.MaxCapacity);
            var oldIndex = 0;
            for (var i = 0; i < Container.MaxCapacity; i++)
            {
                var ushortI = (ushort)i;
                var index = Array.BinarySearch(x.m_Keys, oldIndex, x.m_Size - oldIndex, ushortI);
                if (index < 0)
                {
                    keys.Add(ushortI);
                    containers.Add(BitmapContainer.One);
                    size++;
                }
                else
                {
                    var c = x.m_Values[index];
                    if (!c.Equals(BitmapContainer.One)) // the bitwise negation of the one container is the zero container
                    {
                        var nc = ~c;
                        if (nc.Cardinality > 0)
                        {
                            keys.Add(ushortI);
                            containers.Add(nc);
                            size++;
                        }
                    }
                    oldIndex = index;
                }
            }
            return new RoaringArray(size, keys, containers);
        }

        public static RoaringArray AndNot(RoaringArray x, RoaringArray y)
        {
            var xLength = x.m_Size;
            var yLength = y.m_Size;
            var keys = new List<ushort>(xLength);
            var containers = new List<Container>(xLength);
            var size = 0;
            var xPos = 0;
            var yPos = 0;
            while (xPos < xLength && yPos < yLength)
            {
                var xKey = x.m_Keys[xPos];
                var yKey = y.m_Keys[yPos];
                if (xKey == yKey)
                {
                    var c = Container.AndNot(x.m_Values[xPos], y.m_Values[yPos]);
                    if (c.Cardinality > 0)
                    {
                        keys.Add(xKey);
                        containers.Add(c);
                        size++;
                    }
                    xPos++;
                    yPos++;
                }
                else if (xKey < yKey)
                {
                    var next = x.AdvanceUntil(yKey, xPos);
                    for (var i = xPos; i < next; i++)
                    {
                        keys.Add(x.m_Keys[i]);
                        containers.Add(x.m_Values[i]);
                        size++;
                    }
                    xPos = next;
                }
                else
                {
                    yPos = y.AdvanceUntil(xKey, yPos);
                }
            }
            if (yPos == yLength)
            {
                for (var i = xPos; i < xLength; i++)
                {
                    keys.Add(x.m_Keys[i]);
                    containers.Add(x.m_Values[i]);
                    size++;
                }
            }
            return new RoaringArray(size, keys, containers);
        }

        public override bool Equals(object obj)
        {
            var ra = obj as RoaringArray;
            return ra != null && Equals(ra);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var code = 17;
                code = code * 23 + m_Size;
                for (var i = 0; i < m_Size; i++)
                {
                    code = code * 23 + m_Keys[i].GetHashCode();
                    code = code * 23 + m_Values[i].GetHashCode();
                }
                return code;
            }
        }

        public static void Serialize(RoaringArray roaringArray, Stream stream)
        {
            var hasRun = HasRunContainer(roaringArray);
            using (var binaryWriter = new BinaryWriter(stream, Encoding.UTF8, true))
            {
                var size = roaringArray.m_Size;
                var keys = roaringArray.m_Keys;
                var values = roaringArray.m_Values;
                var startOffset = 0;
                if (hasRun)
                {
                    binaryWriter.Write(SerialCookie | size - 1 << 16);
                    var bitmapOfRunContainers = new byte[(size + 7) / 8];
                    for (var i = 0; i < size; ++i)
                    {
                        if (values[i].Equals(ArrayContainer.One) || values[i].Equals(BitmapContainer.One))
                        {
                            bitmapOfRunContainers[i / 8] |= (byte)(1 << i % 8);
                        }
                    }
                    binaryWriter.Write(bitmapOfRunContainers);
                }
                else // no run containers
                {
                    binaryWriter.Write(SerialCookieNoRuncontainer);
                    binaryWriter.Write(size);
                    startOffset = 4 + 4 + 4 * size + 4 * size;
                }
                for (var k = 0; k < size; ++k)
                {
                    binaryWriter.Write(keys[k]);
                    binaryWriter.Write((ushort)(values[k].Cardinality - 1));
                }
                if (!hasRun || size >= NoOffsetThreshold)
                {
                    for (var k = 0; k < size; k++)
                    {
                        binaryWriter.Write(startOffset);
                        startOffset += values[k].ArraySizeInBytes;
                    }
                }
                for (var k = 0; k < size; ++k)
                {
                    var container = values[k];
                    ArrayContainer ac;
                    BitmapContainer bc;
                    if ((ac = container as ArrayContainer) != null)
                    {
                        if (ac.Equals(ArrayContainer.One))
                        {
                            binaryWriter.Write((ushort)1);
                            binaryWriter.Write((ushort)0);
                            binaryWriter.Write((ushort)(Container.MaxSize - 1));
                        }
                        else
                        {
                            ArrayContainer.Serialize(ac, binaryWriter);
                        }
                    }
                    else if ((bc = container as BitmapContainer) != null)
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
                binaryWriter.Flush();
            }
        }

        private static bool HasRunContainer(RoaringArray roaringArray)
        {
            for (var i = 0; i < roaringArray.m_Size; i++)
            {
                if (roaringArray.m_Values[i].Equals(ArrayContainer.One) || roaringArray.m_Values[i].Equals(BitmapContainer.One))
                {
                    return true;
                }
            }
            return false;
        }

        public static RoaringArray Deserialize(Stream stream)
        {
            using (var binaryReader = new BinaryReader(stream, Encoding.UTF8, true))
            {
                var cookie = binaryReader.ReadUInt32();
                var lbcookie = cookie & 0xFFFF;
                if (lbcookie != SerialCookie && cookie != SerialCookieNoRuncontainer)
                {
                    throw new InvalidDataException("No RoaringBitmap file.");
                }
                var hasRun = lbcookie == SerialCookie;
                var size = (int)(hasRun ? (cookie >> 16) + 1 : binaryReader.ReadUInt32());
                var keys = new ushort[size];
                var containers = new Container[size];
                var cardinalities = new int[size];
                var isBitmap = new bool[size];


                byte[] bitmapOfRunContainers = null;
                if (hasRun)
                {
                    bitmapOfRunContainers = binaryReader.ReadBytes((size + 7) / 8);
                }
                for (var k = 0; k < size; ++k)
                {
                    keys[k] = binaryReader.ReadUInt16();
                    cardinalities[k] = 1 + (0xFFFF & binaryReader.ReadUInt16());
                    isBitmap[k] = cardinalities[k] > Container.MaxSize;
                    if (bitmapOfRunContainers != null && (bitmapOfRunContainers[k / 8] & 1 << k % 8) != 0)
                    {
                        isBitmap[k] = false;
                    }
                }
                if (!hasRun || size >= NoOffsetThreshold)
                {
                    // skipping the offsets
                    binaryReader.ReadBytes(size * 4);
                }
                for (var k = 0; k < size; ++k)
                {
                    if (isBitmap[k])
                    {
                        containers[k] = BitmapContainer.Deserialize(binaryReader, cardinalities[k]);
                    }
                    else if (bitmapOfRunContainers != null && (bitmapOfRunContainers[k / 8] & 1 << k % 8) != 0)
                    {
                        var nbrruns = binaryReader.ReadUInt16();
                        var values = new List<ushort>(nbrruns * 2); // probably more
                        var count = 0;
                        var specialCase = false;
                        for (var j = 0; j < nbrruns; ++j)
                        {
                            var value = binaryReader.ReadUInt16();
                            var length = binaryReader.ReadUInt16();

                            if (nbrruns == 1 && value == 0 && length == Container.MaxCapacity - 1) // special one scenario
                            {
                                containers[k] = BitmapContainer.One;
                                specialCase = true;
                                break;
                            }
                            if (nbrruns == 1 && value == 0 && length == Container.MaxSize - 1) // special one scenario
                            {
                                containers[k] = ArrayContainer.One;
                                specialCase = true;
                                break;
                            }
                            for (int i = value; i < value + length + 1; i++)
                            {
                                values.Add((ushort)i);
                            }
                            count += length;
                        }
                        if (!specialCase)
                        {
                            if (count > Container.MaxSize)
                            {
                                containers[k] = BitmapContainer.Create(values.ToArray());
                            }
                            else
                            {
                                containers[k] = ArrayContainer.Create(values.ToArray());
                            }
                        }
                    }
                    else
                    {
                        containers[k] = ArrayContainer.Deserialize(binaryReader, cardinalities[k]);
                    }
                }
                for (var i = 0; i < size; i++)
                {
                    if (containers[i].Equals(ArrayContainer.One))
                    {
                        containers[i] = ArrayContainer.One;
                    }
                    else if (containers[i].Equals(BitmapContainer.One))
                    {
                        containers[i] = BitmapContainer.One;
                    }
                }
                return new RoaringArray(size, keys, containers);
            }
        }

        public static RoaringArray Optimize(RoaringArray roaringArray)
        {
            var keys = new ushort[roaringArray.m_Size];
            Array.Copy(roaringArray.m_Keys, keys, roaringArray.m_Size);
            var containers = new Container[roaringArray.m_Size];
            for (var i = 0; i < roaringArray.m_Size; i++)
            {
                var currentContainer = roaringArray.m_Values[i];
                if (currentContainer.Equals(ArrayContainer.One))
                {
                    containers[i] = ArrayContainer.One;
                }
                else if (currentContainer.Equals(BitmapContainer.One))
                {
                    containers[i] = BitmapContainer.One;
                }
                else
                {
                    containers[i] = currentContainer;
                }
            }
            return new RoaringArray(roaringArray.m_Size, keys, containers);
        }
    }
}