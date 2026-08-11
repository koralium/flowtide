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
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace FlowtideDotNet.Core.ColumnStore.Utils
{
    /// <summary>
    /// Special list data structure that stores integers only
    /// This data structure is useful when storing offsets for instance since it can change offset locations during removal.
    /// </summary>
    internal unsafe class IntList : IDisposable
    {
        private FlowtideMemory _memory;
        private int _length;
        private bool disposedValue;
        private readonly IMemoryAllocator memoryAllocator;

        // Capacity in elements, derived so the struct is the single source of truth.
        private int DataLength => _memory.Length / sizeof(int);

        internal Span<int> AccessSpan => new Span<int>(_memory.Pointer, DataLength);

        public Memory<byte> Memory => GetViewMemory().Slice(0, _length * sizeof(int));

        public Span<byte> SlicedSpan => new Span<byte>(_memory.Pointer, _length * sizeof(int));

        // Allocates a fresh non-owning view per call; only cold paths (Arrow interop, checkpoint
        // writers) need Memory<byte>, and they re-fetch after list mutations.
        private Memory<byte> GetViewMemory()
        {
            if (_memory.IsNull)
            {
                return new Memory<byte>();
            }
            return new NativeMemoryView(_memory.Pointer, _memory.Length).Memory;
        }

        public IntList(IMemoryAllocator memoryAllocator)
        {
            this.memoryAllocator = memoryAllocator;
        }

        public IntList(IMemoryAllocator memoryAllocator, int initialCapacity)
        {
            this.memoryAllocator = memoryAllocator;
            _memory = memoryAllocator.AllocateMemory(initialCapacity * sizeof(int));
        }

        public IntList(FlowtideMemory memory, int length, IMemoryAllocator memoryAllocator)
        {
            _memory = memory;
            _length = length;
            this.memoryAllocator = memoryAllocator;
        }

        public ReadOnlySpan<int> Span => new ReadOnlySpan<int>(_memory.Pointer, _length);

        public int Count => _length;

        public int* GetPointer_Unsafe()
        {
            return (int*)_memory.Pointer;
        }

        internal void EnsureCapacity(int length)
        {
            if (DataLength < length)
            {
                var newLength = length * 2;
                if (newLength < 64)
                {
                    newLength = 64;
                }

                memoryAllocator.Realloc(ref _memory, newLength * sizeof(int));
            }
        }

        private void CheckSizeReduction()
        {
            var multipleid = (_length << 1) + (_length >> 1);
            var dataLength = DataLength;
            if (multipleid < dataLength && dataLength > 256)
            {
                Debug.Assert(!_memory.IsNull);
                memoryAllocator.Realloc(ref _memory, _length * sizeof(int));
            }
        }

        public void Add(int item)
        {
            EnsureCapacity(_length + 1);
            ((int*)_memory.Pointer)[_length++] = item;
        }

        public void RemoveAt(int index)
        {
            AccessSpan.Slice(index + 1, _length - index - 1).CopyTo(AccessSpan.Slice(index));
            _length--;
            CheckSizeReduction();
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
            CheckSizeReduction();
        }

        public void RemoveRange(int index, int count, int additionOnMoved)
        {
            AvxUtils.InPlaceMemCopyWithAddition(AccessSpan, index + count, index, _length - index - count, additionOnMoved);
            _length -= count;
            CheckSizeReduction();
        }

        public void RemoveRange(int index, int count)
        {
            AccessSpan.Slice(index + count, _length - index - count).CopyTo(AccessSpan.Slice(index));
            _length -= count;
            CheckSizeReduction();
        }

        public void RemoveAtConditionalAddition(int index, Span<sbyte> conditionalValues, sbyte conditionalValue, int additionOnMoved)
        {
            AvxUtils.InPlaceMemCopyConditionalAddition(AccessSpan, conditionalValues, index + 1, index, _length - index - 1, additionOnMoved, conditionalValue);
            _length--;
            CheckSizeReduction();
        }

        public void RemoveRangeTypeBasedAddition(int index, int count, Span<sbyte> typeIds, Span<int> toAdd, int typeCount)
        {
            AvxUtils.InPlaceMemCopyAdditionByType(AccessSpan, typeIds, index + count, index, _length - index - count, toAdd, typeCount);
            _length -= count;
            CheckSizeReduction();
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
            AvxUtils.InPlaceMemCopyWithAddition(span, index, index + 1, _length - index, additionOnMoved);
            span[index] = item;
            _length++;
        }

        internal void IncreaseLength(int count)
        {
            _length += count;
        }

        internal static void MoveIndex(Span<int> span, int index, int moveIndiceCount, int count, int additionOnMoved)
        {
            AvxUtils.InPlaceMemCopyWithAddition(span, index, index + moveIndiceCount, count, additionOnMoved);
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

                fixed (int* spanPtr = span.Slice(index))
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
            ((int*)_memory.Pointer)[index] = item;
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
            ((int*)_memory.Pointer)[index] = item;
            AvxUtils.AddValueToElements(AccessSpan.Slice(index + 1, _length - index - 1), additionOnAbove);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Get(in int index)
        {
            return ((int*)_memory.Pointer)[index];
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (!_memory.IsNull)
                {
                    memoryAllocator.Free(ref _memory);
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
            var slicedSpan = SlicedSpan;
            var newMemory = memoryAllocator.AllocateMemory(slicedSpan.Length);
            slicedSpan.CopyTo(newMemory.Span);

            return new IntList(newMemory, _length, memoryAllocator);
        }
    }
}
