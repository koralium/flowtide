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

using FlowtideDotNet.Core.ColumnStore.Memory;
using FlowtideDotNet.Storage.Memory;
using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Utils
{
    public unsafe class BitmapList : IReadOnlyList<bool>, IEnumerable<bool>, IDisposable
    {
        private const int firstBitMask = 1 << 31;
        private const int lastBitMask = int.MinValue;

        private static readonly int[] BitPatternArray =
        [
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
        ];

        private static readonly int[] topBitsSetMask =
        [
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
            0
        ];
        private IMemoryAllocator? memoryAllocator;
        private int _length;
        private void* _data;
        private int _dataLength;
        private IMemoryOwner<byte>? _memoryOwner;
        private bool disposedValue;

        public Memory<byte> Memory => _memoryOwner?.Memory ?? new Memory<byte>();

        public int Count => _length;

        public BitmapList()
        {
            
        }

        public void Assign(IMemoryAllocator memoryAllocator)
        {
            _data = null;
            _dataLength = 0;
            _length = 0;
            _memoryOwner = null;
            this.memoryAllocator = memoryAllocator;
            disposedValue = false;
        }

        public void Assign(IMemoryOwner<byte> memory, int length, IMemoryAllocator memoryAllocator)
        {
            _memoryOwner = memory;
            _data = _memoryOwner.Memory.Pin().Pointer;
            _dataLength = memory.Memory.Length / sizeof(int);
            _length = length;
            this.memoryAllocator = memoryAllocator;
            disposedValue = false;
        }

        public BitmapList(IMemoryAllocator memoryAllocator)
        {
            _data = null;
            this.memoryAllocator = memoryAllocator;
        }

        public BitmapList(IMemoryOwner<byte> memoryOwner, int length, IMemoryAllocator memoryAllocator)
        {
            _memoryOwner = memoryOwner;
            _data = _memoryOwner.Memory.Pin().Pointer;
            _dataLength = memoryOwner.Memory.Length / sizeof(int);
            _length = length;
            this.memoryAllocator = memoryAllocator;
        }

        private Span<int> AccessSpan => new Span<int>(_data, _dataLength);

        public bool this[int index] => Get(index);

        private void EnsureSize(int length)
        {
            Debug.Assert(memoryAllocator != null);
            if (length > _dataLength)
            {
                int allocationSize = length * sizeof(int);
                if (_memoryOwner == null)
                {
                    _memoryOwner = memoryAllocator.Allocate(allocationSize, 64);
                    _data = _memoryOwner.Memory.Pin().Pointer;
                    NativeMemory.Fill(_data, (nuint)allocationSize, 0);
                }
                else
                {
                    int oldSize = _dataLength * sizeof(int);
                    var newMemory = memoryAllocator.Allocate(allocationSize, 64);
                    var newPtr = newMemory.Memory.Pin().Pointer;
                    NativeMemory.Copy(_data, newPtr, (nuint)oldSize);
                    _memoryOwner.Dispose();
                    _memoryOwner = newMemory;
                    _data = newPtr;
                    NativeMemory.Fill((byte*)(_data) + oldSize, (nuint)(allocationSize - oldSize), 0);
                }
                _dataLength = length;
            }
        }

        public void Add(bool value)
        {
            var index = _length;
            var wordIndex = index >> 5;
            int bitIndex = 1 << index;
            EnsureSize(wordIndex + 1);

            if (value)
            {
                AccessSpan[wordIndex] |= bitIndex;
            }
            else
            {
                AccessSpan[wordIndex] &= ~bitIndex;
            }
            _length++;
        }

        public void Set(int index)
        {
            var wordIndex = index >> 5;
            int bitIndex = 1 << index;
            EnsureSize(wordIndex + 1);

            AccessSpan[wordIndex] |= bitIndex;

            if (_length <= index)
            {
                _length = index + 1;
            }
        }

        public bool Get(int index)
        {
            var wordIndex = index >> 5;
            int bitIndex = 1 << index;
            if (wordIndex >= _dataLength)
            {
                return false;
            }
            return (AccessSpan[wordIndex] & bitIndex) != 0;
        }

        public void Unset(int index)
        {
            var wordIndex = index >> 5;
            int bitIndex = 1 << index;
            EnsureSize(wordIndex + 1);

            AccessSpan[wordIndex] &= ~bitIndex;

            if (_length <= index)
            {
                _length = index + 1;
            }
        }

        public void InsertAt(int index, bool value)
        {
            var toIndex = index >> 5;

            EnsureSize(toIndex + 1);

            var mod = index % 32;
            int bitIndex = 1 << index;
            var span = AccessSpan;
            if ((_length / 32) >= _dataLength)
            {
                EnsureSize(_dataLength + 1);
            }
            span = AccessSpan;
            if (mod > 0)
            {
                var topBitsMask = topBitsSetMask[mod];
                var bottomBitsMask = BitPatternArray[mod];
                var val = span[toIndex] & bottomBitsMask;
                ShiftLeft(toIndex);
                var newVal = span[toIndex] & topBitsMask;
                span[toIndex] = (val | newVal);
            }
            else
            {
                ShiftLeft(toIndex);
            }
            if (value)
            {
                span[toIndex] |= bitIndex;
            }
            else
            {
                span[toIndex] &= ~bitIndex;
            }
            if (index >= _length)
            {
                _length = index + 1;
            }
            else
            {
                _length++;
            }
        }

        private void ShiftLeft(int toIndex)
        {
            var span = AccessSpan;
            var fromindex = _dataLength - 1;
            unchecked
            {
                int lastIndex = fromindex;
                while (fromindex > toIndex)
                {
                    int left = span[fromindex] << 1;
                    uint right = (uint)span[--fromindex] >> (32 - 1);
                    span[lastIndex] = left | (int)right;
                    lastIndex--;
                }
                span[lastIndex] = span[fromindex] << 1;
            }
        }

        /// <summary>
        /// Removes the bit at this index and shifts all bits above it down.
        /// </summary>
        /// <param name="index"></param>
        public void RemoveAt(int index)
        {
            if (index < 0 || index >= _dataLength * 32)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }
            var span = AccessSpan;
            var fromIndex = index >> 5;
            var mod = index % 32;
            _length--;
            if (mod > 0)
            {
                var beforeMask = BitPatternArray[mod - 1];
                var clearMask = topBitsSetMask[mod - 1];
                var val = span[fromIndex] & beforeMask;
                ShiftRight(fromIndex);
                var newVal = span[fromIndex] & clearMask;
                span[fromIndex] = (val | newVal);
            }
            else
            {
                ShiftRight(fromIndex);
            }
        }

        private void ShiftRight(int fromIndex)
        {
            var span = AccessSpan;
            // Loop from BitArray.
            int toIndex = fromIndex;
            int lastIndex = _dataLength - 1;
            unchecked
            {
                while (fromIndex < lastIndex)
                {
                    uint right = (uint)span[fromIndex] >> 1;
                    int left = span[++fromIndex] << (32 - 1);
                    span[toIndex++] = left | (int)right;
                }

                span[toIndex++] = (int)(span[fromIndex] >> 1);
            }
        }

        private IEnumerable<bool> GetEnumerable()
        {
            for (int i = 0; i < _length; i++)
            {
                yield return Get(i);
            }
        }

        public IEnumerator<bool> GetEnumerator()
        {
            return GetEnumerable().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerable().GetEnumerator();
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!Volatile.Read(ref disposedValue))
            {
                Volatile.Write(ref disposedValue, true);
                if (_memoryOwner != null)
                {
                    _memoryOwner.Dispose();
                    _memoryOwner = null;
                    _data = null;
                }

                if (disposing)
                {
                    BitmapListFactory.Return(this);
                }
            }
        }

        ~BitmapList()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: false);
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        public void Clear()
        {
            _length = 0;
        }
    }
}
