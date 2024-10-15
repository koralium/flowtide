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

using FlowtideDotNet.Storage.Memory;
using System.Buffers;
using System.Collections;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using static SqlParser.Ast.DataType;

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

        public Memory<byte> MemorySlice => GetMemorySlice();

        private Memory<byte> GetMemorySlice()
        {
            if (_memoryOwner == null)
            {
                return new Memory<byte>();
            }
            return _memoryOwner.Memory.Slice(0, ((_length + 31) / 32) * 4);
        }

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
                    _memoryOwner = memoryAllocator.Realloc(_memoryOwner, allocationSize, 64);
                    _data = _memoryOwner.Memory.Pin().Pointer;
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
                ShiftLeft(toIndex, _dataLength - 1, 1);
                var newVal = span[toIndex] & topBitsMask;
                span[toIndex] = (val | newVal);
            }
            else
            {
                ShiftLeft(toIndex, _dataLength - 1, 1);
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

        public void InsertRangeFrom(int index, BitmapList other, int start, int count)
        {
            var toIndex = index >> 5;

            var numberOfNewInts = ((count + 32) / 32);
            var countDivided32 = count >> 5;
            var countRemainder = count & 31;
            var startMod32 = start & 31;
            EnsureSize(toIndex + numberOfNewInts);

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
                var bottomBitsMask = BitPatternArray[mod - 1];
                var topBitsMask = topBitsSetMask[mod - 1];

                // Debug code
                ref var valRef = ref MemoryMarshal.Cast<int, uint>(span)[toIndex];

                // Fetch the value of the most left integer and and mask it to get the bits that should remain unchanged
                var val = span[toIndex] & bottomBitsMask;
                // Shift the all bits to the left
                ShiftLeft(toIndex, _dataLength - 1, count);

                
                // Fetch the value of the most right integer and mask it to get the bits that should remain unchanged
                var endIntPreviousValue = span[toIndex + countDivided32] & topBitsSetMask[countRemainder]; // was -1 before
                // Copy in the data, this could change the value of the most right integer and most left integer
                other.AccessSpan.Slice(start >> 5, numberOfNewInts).CopyTo(span.Slice(toIndex));

                var modDifference = mod - startMod32;
                if (modDifference > 0)
                {
                    // The difference is positive, so the copied values must be shifted left to make them get to the correct indices.
                    ShiftLeft(toIndex, toIndex + countDivided32, modDifference);
                }
                else if (modDifference < 0)
                {
                    // The difference is negative, so the copied values must be shifted right to make them get to the correct indices.
                    ShiftRight(toIndex, toIndex + countDivided32, -modDifference);
                }

                // Merge together values in the most left integer
                var newVal = span[toIndex] & topBitsMask;
                span[toIndex] = (val | newVal);

                // Merge together values in the most right integer
                newVal = span[toIndex + countDivided32] & BitPatternArray[countRemainder];
                span[toIndex + countDivided32] = (endIntPreviousValue | newVal);
            }
            else
            {
                // Shift left to open up space for new values
                ShiftLeft(toIndex, _dataLength - 1, count);
                //Fetch the value of the most right integer and mask it to get the bits that should remain unchanged
                var endIntPreviousValue = span[toIndex + countDivided32] & topBitsSetMask[countRemainder - 1];
                // Copy in the data, this could change the value of the most right integer
                other.AccessSpan.Slice(start >> 5, numberOfNewInts).CopyTo(span.Slice(toIndex));
                // Merge together values in the most right integer
                var newVal = span[toIndex + countDivided32] & BitPatternArray[countRemainder];
                span[toIndex + countDivided32] = (endIntPreviousValue | newVal);
            }
            if (index >= _length)
            {
                _length = index + count;
            }
            else
            {
                _length += count;
            }
        }

        private void ShiftLeft(int toIndex, int fromIndex, int count)
        {
            var span = AccessSpan;
            var fromindex = fromIndex;
            int intsToCopy = count / 32;
            var remainder = (byte)(count & 31);
            unchecked
            {
                int lastIndex = fromindex;
                fromindex = fromindex - intsToCopy;
                if (remainder == 0)
                {
                    span.Slice(fromindex, intsToCopy).CopyTo(span.Slice(lastIndex + 1 - intsToCopy));
                }
                else
                {
                    while (fromindex > toIndex)
                    {
                        int left = span[fromindex] << remainder;
                        uint right = (uint)span[--fromindex] >> (32 - remainder);
                        span[lastIndex] = left | (int)right;
                        lastIndex--;
                    }
                    span[lastIndex] = span[fromindex] << remainder;
                }
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
                ShiftRight(fromIndex, _dataLength - 1, 1);
                var newVal = span[fromIndex] & clearMask;
                span[fromIndex] = (val | newVal);
            }
            else
            {
                ShiftRight(fromIndex, _dataLength - 1, 1);
            }
        }

        public void RemoveRange(in int index, in int count)
        {
            var span = AccessSpan;
            var fromIndex = index >> 5;
            var mod = index % 32;
            _length -= count;
            if (mod > 0)
            {
                var beforeMask = BitPatternArray[mod - 1];
                var clearMask = topBitsSetMask[mod - 1];
                var val = span[fromIndex] & beforeMask;
                ShiftRight(fromIndex, _dataLength - 1, count);
                var newVal = span[fromIndex] & clearMask;
                span[fromIndex] = (val | newVal);
                var vv = span[fromIndex];
            }
            else
            {
                ShiftRight(fromIndex, _dataLength - 1, count);
            }
        }

        private void ShiftRight(int fromIndex, int lastIndex, int count)
        {
            var span = AccessSpan;
            // Loop from BitArray.
            int toIndex = fromIndex;
            //int lastIndex = ;
            var numberOfInts = count / 32;
            var remainder = (byte)(count & 31);
            fromIndex = fromIndex + numberOfInts;
            unchecked
            {
                if (remainder == 0)
                {
                    span.Slice(fromIndex, lastIndex - fromIndex + 1).CopyTo(span.Slice(toIndex));
                }
                else
                {
                    // If there are more than 16 integers to copy, use AVX2
                    if (fromIndex + 16 <= lastIndex && Avx2.IsSupported)
                    {
                        ShiftRightAvxSelector(ref span, ref fromIndex, ref toIndex, ref lastIndex, remainder);
                    }
                    while (fromIndex < lastIndex)
                    {
                        uint right = (uint)span[fromIndex] >> remainder;
                        int left = span[++fromIndex] << (32 - remainder);
                        span[toIndex++] = left | (int)right;
                    }
                    span[toIndex++] = (int)(span[fromIndex] >> remainder);
                }
            }
        }

        private void ShiftRightAvx(ref Span<int> span, ref int fromIndex, ref int toIndex, ref int lastIndex, [ConstantExpected] byte remainder, [ConstantExpected] byte bitsMinusRemainder)
        {
            fixed (int* spanPtr = span)
            {
                while (fromIndex + 8 <= lastIndex)
                {
                    // Load 8 ints from span into an AVX register
                    Vector256<int> current = Avx2.LoadVector256(spanPtr + fromIndex);

                    // Shift all elements right by `remainder` bits
                    Vector256<int> rightShifted = Avx2.ShiftRightLogical(current, remainder);

                    // Load the next 8 ints and shift them left to handle cross-boundary bits
                    Vector256<int> next = Avx2.LoadVector256(spanPtr + fromIndex + 1);
                    Vector256<int> leftShifted = Avx2.ShiftLeftLogical(next, bitsMinusRemainder);

                    // Combine the results using bitwise OR
                    Vector256<int> result = Avx2.Or(rightShifted, leftShifted);

                    // Store the result back into the span
                    Avx2.Store(spanPtr + toIndex, result);

                    // Move to the next set of integers
                    fromIndex += 8;
                    toIndex += 8;
                }
            }
        }

        /// <summary>
        /// ShiftRightLogical and ShiftLeftLogical wants a constant value for the shift amount.
        /// </summary>
        /// <param name="span"></param>
        /// <param name="fromIndex"></param>
        /// <param name="toIndex"></param>
        /// <param name="lastIndex"></param>
        /// <param name="remainder"></param>
        private void ShiftRightAvxSelector(ref Span<int> span, ref int fromIndex, ref int toIndex, ref int lastIndex, byte remainder)
        {
            // Perform binary search on remainder to set actual shift values
            if (remainder < 16)  // remainder < 16
            {
                if (remainder < 8)  // remainder < 8
                {
                    if (remainder < 4)  // remainder < 4
                    {
                        // remainder is 0, 1, 2, or 3
                        switch (remainder)
                        {
                            case 0: ShiftRightAvx(ref span, ref fromIndex, ref toIndex, ref lastIndex, 0, 32); break;
                            case 1: ShiftRightAvx(ref span, ref fromIndex, ref toIndex, ref lastIndex, 1, 31); break;
                            case 2: ShiftRightAvx(ref span, ref fromIndex, ref toIndex, ref lastIndex, 2, 30); break;
                            case 3: ShiftRightAvx(ref span, ref fromIndex, ref toIndex, ref lastIndex, 3, 29); break;
                        }
                    }
                    else  // 4 <= remainder < 8
                    {
                        switch (remainder)
                        {
                            case 4: ShiftRightAvx(ref span, ref fromIndex, ref toIndex, ref lastIndex, 4, 28); break;
                            case 5: ShiftRightAvx(ref span, ref fromIndex, ref toIndex, ref lastIndex, 5, 27); break;
                            case 6: ShiftRightAvx(ref span, ref fromIndex, ref toIndex, ref lastIndex, 6, 26); break;
                            case 7: ShiftRightAvx(ref span, ref fromIndex, ref toIndex, ref lastIndex, 7, 25); break;
                        }
                    }
                }
                else  // 8 <= remainder < 16
                {
                    if (remainder < 12)  // 8 <= remainder < 12
                    {
                        switch (remainder)
                        {
                            case 8: ShiftRightAvx(ref span, ref fromIndex, ref toIndex, ref lastIndex, 8, 24); break;
                            case 9: ShiftRightAvx(ref span, ref fromIndex, ref toIndex, ref lastIndex, 9, 23); break;
                            case 10: ShiftRightAvx(ref span, ref fromIndex, ref toIndex, ref lastIndex, 10, 22); break;
                            case 11: ShiftRightAvx(ref span, ref fromIndex, ref toIndex, ref lastIndex, 11, 21); break;
                        }
                    }
                    else  // 12 <= remainder < 16
                    {
                        switch (remainder)
                        {
                            case 12: ShiftRightAvx(ref span, ref fromIndex, ref toIndex, ref lastIndex, 12, 20); break;
                            case 13: ShiftRightAvx(ref span, ref fromIndex, ref toIndex, ref lastIndex, 13, 19); break;
                            case 14: ShiftRightAvx(ref span, ref fromIndex, ref toIndex, ref lastIndex, 14, 18); break;
                            case 15: ShiftRightAvx(ref span, ref fromIndex, ref toIndex, ref lastIndex, 15, 17); break;
                        }
                    }
                }
            }
            else  // remainder >= 16
            {
                if (remainder < 24)  // 16 <= remainder < 24
                {
                    if (remainder < 20)  // 16 <= remainder < 20
                    {
                        switch (remainder)
                        {
                            case 16: ShiftRightAvx(ref span, ref fromIndex, ref toIndex, ref lastIndex, 16, 16); break;
                            case 17: ShiftRightAvx(ref span, ref fromIndex, ref toIndex, ref lastIndex, 17, 15); break;
                            case 18: ShiftRightAvx(ref span, ref fromIndex, ref toIndex, ref lastIndex, 18, 14); break;
                            case 19: ShiftRightAvx(ref span, ref fromIndex, ref toIndex, ref lastIndex, 19, 13); break;
                        }
                    }
                    else  // 20 <= remainder < 24
                    {
                        switch (remainder)
                        {
                            case 20: ShiftRightAvx(ref span, ref fromIndex, ref toIndex, ref lastIndex, 20, 12); break;
                            case 21: ShiftRightAvx(ref span, ref fromIndex, ref toIndex, ref lastIndex, 21, 11); break;
                            case 22: ShiftRightAvx(ref span, ref fromIndex, ref toIndex, ref lastIndex, 22, 10); break;
                            case 23: ShiftRightAvx(ref span, ref fromIndex, ref toIndex, ref lastIndex, 23, 9); break;
                        }
                    }
                }
                else  // 24 <= remainder < 32
                {
                    if (remainder < 28)  // 24 <= remainder < 28
                    {
                        switch (remainder)
                        {
                            case 24: ShiftRightAvx(ref span, ref fromIndex, ref toIndex, ref lastIndex, 24, 8); break;
                            case 25: ShiftRightAvx(ref span, ref fromIndex, ref toIndex, ref lastIndex, 25, 7); break;
                            case 26: ShiftRightAvx(ref span, ref fromIndex, ref toIndex, ref lastIndex, 26, 6); break;
                            case 27: ShiftRightAvx(ref span, ref fromIndex, ref toIndex, ref lastIndex, 27, 5); break;
                        }
                    }
                    else  // 28 <= remainder < 32
                    {
                        switch (remainder)
                        {
                            case 28: ShiftRightAvx(ref span, ref fromIndex, ref toIndex, ref lastIndex, 28, 4); break;
                            case 29: ShiftRightAvx(ref span, ref fromIndex, ref toIndex, ref lastIndex, 29, 3); break;
                            case 30: ShiftRightAvx(ref span, ref fromIndex, ref toIndex, ref lastIndex, 30, 2); break;
                            case 31: ShiftRightAvx(ref span, ref fromIndex, ref toIndex, ref lastIndex, 31, 1); break;
                        }
                    }
                }
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

        public int GetByteSize(int start, int end)
        {
            return (((end - start) + 31) / 32) * 4;
        }
    }
}
