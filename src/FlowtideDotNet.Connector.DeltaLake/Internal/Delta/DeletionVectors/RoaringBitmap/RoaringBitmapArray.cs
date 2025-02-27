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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors.RoaringBitmap
{
    /// <summary>
    /// Code from https://github.com/Tornhoof/RoaringBitmap/tree/master which is archived
    /// </summary>
    internal class RoaringBitmapArray : IEnumerable<long>, IDeleteVector
    {
        private RoaringBitmap[] _bitmaps;

        public RoaringBitmapArray(RoaringBitmap[] bitmaps)
        {
            _bitmaps = bitmaps;
        }

        public long Cardinality => _bitmaps.Sum(x => x.Cardinality);

        public static RoaringBitmapArray Create(IEnumerable<long> values)
        {
            var groupbyHb = values.Distinct().OrderBy(t => t).GroupBy(HighBytes).OrderBy(t => t.Key).ToList();

            List<RoaringBitmap> result = new List<RoaringBitmap>();
            foreach(var group in groupbyHb)
            {
                if (group.Key > result.Count)
                {
                    for (int i = result.Count; i < group.Key; i++)
                    {
                        result.Add(RoaringBitmap.Create());
                    }
                }
                var lowValues = group.Select(LowBytes).ToArray();
                var bitmap = RoaringBitmap.Create(lowValues);
                result.Add(bitmap);
            }

            return new RoaringBitmapArray(result.ToArray());
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

        public void Serialize(System.IO.BinaryWriter binaryWriter)
        {
            binaryWriter.Write((long)_bitmaps.Length);
            for (int i = 0; i < _bitmaps.Length; i++)
            {
                binaryWriter.Write(i);
                RoaringBitmap.Serialize(_bitmaps[i], binaryWriter.BaseStream);
            }
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
