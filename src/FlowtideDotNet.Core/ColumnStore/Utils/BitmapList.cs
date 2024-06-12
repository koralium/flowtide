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
using System.Linq;
using System.Runtime.Intrinsics;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Utils
{
    internal class BitmapList
    {
        private const int firstBitMask = 1 << 31;
        private const int lastBitMask = 1;

        private static readonly int[] BitPatternArray = new int[32]
        {
            (1 << 1) - 1,  // 0th element with 1 bit set
            (1 << 2) - 1,  // 1st element with 2 bits set
            (1 << 3) - 1,  // 2nd element with 3 bits set
            (1 << 4) - 1,  // 3rd element with 4 bits set
            (1 << 5) - 1,  // 4th element with 5 bits set
            (1 << 6) - 1,  // 5th element with 6 bits set
            (1 << 7) - 1,  // 6th element with 7 bits set
            (1 << 8) - 1,  // 7th element with 8 bits set
            (1 << 9) - 1,  // 8th element with 9 bits set
            (1 << 10) - 1, // 9th element with 10 bits set
            (1 << 11) - 1, // 10th element with 11 bits set
            (1 << 12) - 1, // 11th element with 12 bits set
            (1 << 13) - 1, // 12th element with 13 bits set
            (1 << 14) - 1, // 13th element with 14 bits set
            (1 << 15) - 1, // 14th element with 15 bits set
            (1 << 16) - 1, // 15th element with 16 bits set
            (1 << 17) - 1, // 16th element with 17 bits set
            (1 << 18) - 1, // 17th element with 18 bits set
            (1 << 19) - 1, // 18th element with 19 bits set
            (1 << 20) - 1, // 19th element with 20 bits set
            (1 << 21) - 1, // 20th element with 21 bits set
            (1 << 22) - 1, // 21st element with 22 bits set
            (1 << 23) - 1, // 22nd element with 23 bits set
            (1 << 24) - 1, // 23rd element with 24 bits set
            (1 << 25) - 1, // 24th element with 25 bits set
            (1 << 26) - 1, // 25th element with 26 bits set
            (1 << 27) - 1, // 26th element with 27 bits set
            (1 << 28) - 1, // 27th element with 28 bits set
            (1 << 29) - 1, // 28th element with 29 bits set
            (1 << 30) - 1, // 29th element with 30 bits set
            int.MaxValue, // 30th element with 31 bits set
            (int.MaxValue << 1) | 1  // 31st element with 32 bits set (all bits set)
        };

        private static readonly int[] topBitsSetMask = new int[]
        {
            ~((1 << 1) - 1),  // 0th element with all bits except the lowest 1 bit set
            ~((1 << 2) - 1),  // 1st element with all bits except the lowest 2 bits set
            ~((1 << 3) - 1),  // 2nd element with all bits except the lowest 3 bits set
            ~((1 << 4) - 1),  // 3rd element with all bits except the lowest 4 bits set
            ~((1 << 5) - 1),  // 4th element with all bits except the lowest 5 bits set
            ~((1 << 6) - 1),  // 5th element with all bits except the lowest 6 bits set
            ~((1 << 7) - 1),  // 6th element with all bits except the lowest 7 bits set
            ~((1 << 8) - 1),  // 7th element with all bits except the lowest 8 bits set
            ~((1 << 9) - 1),  // 8th element with all bits except the lowest 9 bits set
            ~((1 << 10) - 1), // 9th element with all bits except the lowest 10 bits set
            ~((1 << 11) - 1), // 10th element with all bits except the lowest 11 bits set
            ~((1 << 12) - 1), // 11th element with all bits except the lowest 12 bits set
            ~((1 << 13) - 1), // 12th element with all bits except the lowest 13 bits set
            ~((1 << 14) - 1), // 13th element with all bits except the lowest 14 bits set
            ~((1 << 15) - 1), // 14th element with all bits except the lowest 15 bits set
            ~((1 << 16) - 1), // 15th element with all bits except the lowest 16 bits set
            ~((1 << 17) - 1), // 16th element with all bits except the lowest 17 bits set
            ~((1 << 18) - 1), // 17th element with all bits except the lowest 18 bits set
            ~((1 << 19) - 1), // 18th element with all bits except the lowest 19 bits set
            ~((1 << 20) - 1), // 19th element with all bits except the lowest 20 bits set
            ~((1 << 21) - 1), // 20th element with all bits except the lowest 21 bits set
            ~((1 << 22) - 1), // 21st element with all bits except the lowest 22 bits set
            ~((1 << 23) - 1), // 22nd element with all bits except the lowest 23 bits set
            ~((1 << 24) - 1), // 23rd element with all bits except the lowest 24 bits set
            ~((1 << 25) - 1), // 24th element with all bits except the lowest 25 bits set
            ~((1 << 26) - 1), // 25th element with all bits except the lowest 26 bits set
            ~((1 << 27) - 1), // 26th element with all bits except the lowest 27 bits set
            ~((1 << 28) - 1), // 27th element with all bits except the lowest 28 bits set
            ~((1 << 29) - 1), // 28th element with all bits except the lowest 29 bits set
            ~((1 << 30) - 1), // 29th element with all bits except the lowest 30 bits set
            -2147483648, // 30th element with all bits except the lowest 31 bits set
            ~((1 << 32) - 1), // 31st element with all bits except the lowest 32 bits set (all bits are set to 1 in a signed 32-bit int, this overflows to 0)
        };


        private int[] _data;
        private int _length;


        public BitmapList()
        {
            _data = new int[0];
        }

        public void Set(int index)
        {
            var wordIndex = index >> 5;
            int bitIndex = 1 << index;
            if (wordIndex >= _data.Length)
            {
                var newData = new int[wordIndex + 1];
                Array.Copy(_data, newData, _data.Length);
                _data = newData;
            }
            _data[wordIndex] |= bitIndex;

            if (_length < index)
            {
                _length = index;
            }
        }

        public bool Get(int index)
        {
            var wordIndex = index >> 5;
            int bitIndex = 1 << index;
            if (wordIndex >= _data.Length)
            {
                return false;
            }
            return (_data[wordIndex] & bitIndex) != 0;
        }

        public void Unset(int index)
        {
            var wordIndex = index >> 5;
            int bitIndex = 1 << index;
            if (wordIndex >= _data.Length)
            {
                return;
            }
            _data[wordIndex] &= ~(1 << bitIndex);
        }

        public void InsertAt(int index, bool value)
        {
            var toIndex = index >> 5;
            var mod = index % 32;
            if ((_data[_data.Length - 1] & lastBitMask) != 0)
            {
                // add an extra element so it does not overflow.
                var newData = new int[_data.Length + 1];
                Array.Copy(_data, newData, _data.Length);
                _data = newData;
            }
            if (mod > 0)
            {
                var topBitsMask = topBitsSetMask[mod];
                var bottomBitsMask = BitPatternArray[mod];
                var val = _data[toIndex] & topBitsMask;
                ShiftLeft(toIndex);
                var newVal = _data[toIndex] & bottomBitsMask;
                _data[toIndex] = (val | newVal);
            }

            ShiftLeft(toIndex);
        }

        private void ShiftLeft(int toIndex)
        {
            var fromindex = _data.Length - 1;
            //int fromindex = lastIndex - lengthToClear;
            unchecked
            {
                int lastIndex = fromindex;
                while (fromindex > toIndex)
                {
                    int left = _data[fromindex] << 1;
                    uint right = (uint)_data[--fromindex] >> (32 - 1);
                    _data[lastIndex] = left | (int)right;
                    lastIndex--;
                }
                _data[lastIndex] = _data[fromindex] << 1;
            }
        }

        /// <summary>
        /// Removes the bit at this index and shifts all bits above it down.
        /// </summary>
        /// <param name="index"></param>
        public void RemoveAt(int index)
        {
            if (index < 0 || index >= _data.Length * 32)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }

            var fromIndex = index >> 5;
            var mod = index % 32;

            if (index > 0)
            {
                var beforeMask = BitPatternArray[mod - 1];
                var clearMask = topBitsSetMask[mod - 1];
                var val = _data[fromIndex] & beforeMask;
                ShiftRight(fromIndex, mod);
                var newVal = _data[fromIndex] & clearMask;
                _data[fromIndex] = (val | newVal);
            }
            else
            {
                ShiftRight(fromIndex, mod);
            }
        }

        private void ShiftRight(int fromIndex, int mod)
        {
            // Loop from BitArray.
            int toIndex = 0;
            int lastIndex = _data.Length - 1;
            unchecked
            {
                while (fromIndex < lastIndex)
                {
                    uint right = (uint)_data[fromIndex] >> 1;
                    int left = _data[++fromIndex] << (32 - 1);
                    _data[toIndex++] = left | (int)right;
                }
                uint mask = uint.MaxValue >> (32 - mod);
                mask &= (uint)_data[fromIndex];
                _data[toIndex++] = (int)(mask >> 1);
            }
        }

    }
}
