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

using Apache.Arrow.Memory;
using FASTER.core;
using FlowtideDotNet.Storage.Memory;
using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using System.Text;
using System.Threading.Tasks;
using static SqlParser.Ast.TableConstraint;

namespace FlowtideDotNet.Core.ColumnStore.Utils
{
    /// <summary>
    /// Special list data structure that stores integers only
    /// This data structure is useful when storing offsets for instance since it can change offset locations during removal.
    /// </summary>
    internal unsafe class IntList : IDisposable
    {
        private int* _data;
        IMemoryOwner<byte>? _memoryOwner;
        private int _dataLength;
        private int _length;
        private bool disposedValue;
        private readonly IMemoryAllocator memoryAllocator;

        private Span<int> AccessSpan => new Span<int>(_data, _dataLength);

        public Memory<byte> Memory => _memoryOwner?.Memory.Slice(0, _length * sizeof(int)) ?? new Memory<byte>();

        public IntList(IMemoryAllocator memoryAllocator)
        {
            _data = null;
            this.memoryAllocator = memoryAllocator;
        }

        public IntList(IMemoryOwner<byte> memory, int length, IMemoryAllocator memoryAllocator)
        {
            _memoryOwner = memory;
            _data = (int*)memory.Memory.Pin().Pointer;
            _dataLength = memory.Memory.Length / 4;
            _length = length;
            this.memoryAllocator = memoryAllocator;
        }

        public ReadOnlySpan<int> Span => new ReadOnlySpan<int>(_data, _length);

        public int Count => _length;

        private void EnsureCapacity(int length)
        {
            if (_dataLength < length)
            {
                var newLength = length * 2;
                if (newLength < 64)
                {
                    newLength = 64;
                }
                
                var allocLength = newLength * 2 * sizeof(int);
                if (_memoryOwner == null)
                {
                    _memoryOwner = memoryAllocator.Allocate(allocLength, 64);
                    var newMemoryHandle = _memoryOwner.Memory.Pin();
                    _data = (int*)newMemoryHandle.Pointer;
                }
                else
                {
                    _memoryOwner = memoryAllocator.Realloc(_memoryOwner, allocLength, 64);
                    _data = (int*)_memoryOwner.Memory.Pin().Pointer;
                }
                _dataLength = newLength;
            }
        }

        public void Add(int item)
        {
            EnsureCapacity(_length + 1);
            _data[_length++] = item;
        }

        public void RemoveAt(int index)
        {
            AccessSpan.Slice(index + 1, _length - index - 1).CopyTo(AccessSpan.Slice(index));
            _length--;
        }

        /// <summary>
        /// Special remove at, where it runs an addition on all elements that are larger than the removed index.
        /// This is useful if this is used to store offsets where all offsets can be moved during the copy.
        /// </summary>
        /// <param name="index"></param>
        /// <param name="additionOnMoved"></param>
        public void RemoveAt(int index, int additionOnMoved)
        {
            AvxUtils.InPlaceMemCopyWithAddition(AccessSpan, index + 1, index, _length - index - 1, additionOnMoved);
            _length--;
        }

        public void RemoveRange(int index, int count, int additionOnMoved)
        {
            AvxUtils.InPlaceMemCopyWithAddition(AccessSpan, index + count, index, _length - index - count, additionOnMoved);
            _length -= count;
        }

        public void RemoveRange(int index, int count)
        {
            AccessSpan.Slice(index + count, _length - index - count).CopyTo(AccessSpan.Slice(index));
            _length -= count;
        }

        public void RemoveAtConditionalAddition(int index, Span<sbyte> conditionalValues, sbyte conditionalValue, int additionOnMoved)
        {
            AvxUtils.InPlaceMemCopyConditionalAddition(AccessSpan, conditionalValues, index + 1, index, _length - index - 1, additionOnMoved, conditionalValue);
            _length--;
        }

        public void RemoveRangeTypeBasedAddition(int index, int count, Span<sbyte> typeIds, Span<int> toAdd, int typeCount)
        {
            AvxUtils.InPlaceMemCopyAdditionByType(AccessSpan, typeIds, index + count, index, _length - index - count, toAdd, typeCount);
            _length -= count;
        }

        public void InsertAt(int index, int item)
        {
            EnsureCapacity(_length + 1);
            var span = AccessSpan;
            span.Slice(index, _length - index).CopyTo(span.Slice(index + 1));
            span[index] = item;
            _length++;
        }

        public void InsertAt(int index, int item, int additionOnMoved)
        {
            EnsureCapacity(_length + 1);
            var span = AccessSpan;
            var source = span.Slice(index, _length - index);
            var dest = span.Slice(index + 1);
            AvxUtils.InPlaceMemCopyWithAddition(span, index, index + 1, _length - index, additionOnMoved);
            //AvxUtils.MemCpyWithAdd(source, dest, additionOnMoved);
            span[index] = item;
            _length++;
        }

        public void InsertRangeFrom(int index, IntList other, int start, int count, int additionOnMovedExisting, int additionOnCopied)
        {
            EnsureCapacity(_length + count);
            var span = AccessSpan;
            var sourceSpan = other.AccessSpan;
            AvxUtils.InPlaceMemCopyWithAddition(span, index, index + count, _length - index, additionOnMovedExisting);
            AvxUtils.MemCpyWithAdd(sourceSpan.Slice(start, count), span.Slice(index), additionOnCopied);
            _length += count;
        }

        public void InsertRangeStaticValue(int index, int count, int staticValue)
        {
            EnsureCapacity(_length + count);
            var span = AccessSpan;

            // Move data
            span.Slice(index, _length - index).CopyTo(span.Slice(index + count));

            for (int i = 0; i < count; i++)
            {
                span[index + i] = staticValue;
            }
            _length += count;

        }


        public void InsertRangeFromTypeBasedAddition(
            int index, 
            IntList other, 
            int start, 
            int count, 
            Span<sbyte> thisTypeIds, 
            Span<int> thisToAdd,
            Span<sbyte> otherTypeIds,
            Span<int> otherToAdd,
            int typeCount)
        {
            EnsureCapacity(_length + count);
            var span = AccessSpan;
            var sourceSpan = other.AccessSpan;
            var source = other.AccessSpan.Slice(start, count);
            var dest = span.Slice(index);
            AvxUtils.InPlaceMemCopyAdditionByType(span, thisTypeIds, index, index + count, _length - index, thisToAdd, typeCount);
            AvxUtils.MemCopyAdditionByType(sourceSpan, span, otherTypeIds, start, index, count, otherToAdd, typeCount);
            _length += count;
        }

        public void InsertIncrementalRangeConditionalAdditionOnExisting(
            int index,  
            int startValue, 
            int count, 
            Span<sbyte> conditionalValues, 
            sbyte conditionalValue, 
            int additionOnMovedExisting)
        {
            EnsureCapacity(_length + count);
            var span = AccessSpan;
            AvxUtils.InPlaceMemCopyConditionalAddition(span, conditionalValues, index, index + count, _length - index, additionOnMovedExisting, conditionalValue);

            int i = 0;
            if (count > 8 && Avx2.IsSupported)
            {
                var baseValue = Vector256.Create(startValue);
                var vecIndex = Vector256.Create(0, 1, 2, 3, 4, 5, 6, 7);
                var vecStride = Vector256.Create(8);

                fixed(int* spanPtr = span)
                {
                    var end = count - 8;
                    for (; i < end; i += 8)
                    {
                        var vecValues = Avx2.Add(baseValue, vecIndex);
                        Avx2.Store(spanPtr + i, vecValues);
                        baseValue = Avx2.Add(baseValue, vecStride);
                    }
                }
            }
            for (; i < count; i++)
            {
                span[index + i] = startValue + i;
            }
            _length += count;
        }

        /// <summary>
        /// Special function that allows sending in a sbyte array which contains values and only the elements that matches the sent in
        /// conditional value should be added with the additionOnMoved value.
        /// </summary>
        /// <param name="index"></param>
        /// <param name="item"></param>
        /// <param name="conditionalValues"></param>
        /// <param name="conditionalValue"></param>
        /// <param name="additionOnMoved"></param>
        public void InsertAtConditionalAddition(int index, int item, Span<sbyte> conditionalValues, sbyte conditionalValue, int additionOnMoved)
        {
            EnsureCapacity(_length + 1);
            var span = AccessSpan;
            AvxUtils.InPlaceMemCopyConditionalAddition(span, conditionalValues, index, index + 1, _length - index, additionOnMoved, conditionalValue);
            span[index] = item;
            _length++;
        }

        public void Update(int index, int item)
        {
            _data[index] = item;
        }

        /// <summary>
        /// Special update operation, it allows doing an addition on all elements above this one.
        /// This is useful if this is used to store offsets where all offsets can be moved during the copy.
        /// </summary>
        /// <param name="index"></param>
        /// <param name="item"></param>
        /// <param name="additionOnAbove"></param>
        public void Update(int index, int item, int additionOnAbove)
        {
            _data[index] = item;
            AvxUtils.AddValueToElements(AccessSpan.Slice(index + 1, _length - index - 1), additionOnAbove);
        }

        public int Get(in int index)
        {
            return _data[index];
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (_memoryOwner != null)
                {
                    _memoryOwner.Dispose();
                    _memoryOwner = null;
                    _data = null;
                }
                disposedValue = true;
            }
        }

        ~IntList()
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

        public IntList Copy(IMemoryAllocator memoryAllocator)
        {
            var mem = Memory;
            var newMemory = memoryAllocator.Allocate(mem.Length, 64);
            mem.Span.CopyTo(newMemory.Memory.Span);

            return new IntList(newMemory, _length, memoryAllocator);
        }
    }
}
