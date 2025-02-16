using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors.RoaringBitmap
{
    internal class RoaringBitmapArray : IEnumerable<long>, IDeleteVector
    {
        private RoaringBitmap[] _bitmaps;

        public RoaringBitmapArray(RoaringBitmap[] bitmaps)
        {
            _bitmaps = bitmaps;
        }

        static int HighBytes(long value)
        {
            return (int)(value >> 32);
        }

        static int LowBytes(long value)
        {
            return (int)value;
        }

        public bool Contains(long value)
        {
            var high = HighBytes(value);

            if (high > _bitmaps.Length)
            {
                return false;
            }
            RoaringBitmap highBitmap = _bitmaps[high];
            var low = LowBytes(value);
            return highBitmap.Contains(low);
        }

        public static RoaringBitmapArray Deserialize(BinaryReader binaryReader)
        {
            var arr = DeserializeArray(binaryReader);

            return new RoaringBitmapArray(arr);
        }

        static RoaringBitmap[] DeserializeArray(BinaryReader binaryReader)
        {
            var numberOfBitmaps = binaryReader.ReadInt64();

            List<RoaringBitmap> bitmaps = new List<RoaringBitmap>();
            int lastIndex = 0;
            for (int i = 0; i < numberOfBitmaps; i++)
            {
                var key = binaryReader.ReadInt32();
                if (key < 0)
                {
                    throw new InvalidDataException("Invalid key in RoaringBitmapArray");
                }

                while (lastIndex < key)
                {
                    bitmaps.Add(RoaringBitmap.Create());
                    lastIndex++;
                }
                var bitmap = RoaringBitmap.Deserialize(binaryReader.BaseStream);
                bitmaps.Add(bitmap);
                lastIndex++;
            }
            return bitmaps.ToArray();
        }

        private IEnumerable<long> Enumerate()
        {
            for (int i = 0; i < _bitmaps.Length; i++)
            {
                var high = (long)i << 32;
                foreach (var low in _bitmaps[i])
                {
                    yield return ComposeFromHighLowBytes(i, low);
                }
            }
        }

        static long ComposeFromHighLowBytes(int high, int low)
        {
            // Must bitmask to avoid sign extension.
            return (long)high << 32 | low & 0xFFFFFFFFL;
        }

        public IEnumerator<long> GetEnumerator()
        {
            return Enumerate().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return Enumerate().GetEnumerator();
        }
    }
}
