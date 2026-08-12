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
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace FlowtideDotNet.Core.ColumnStore.Utils
{
    /// <summary>
    /// Special list data structure that stores integers only
    /// This data structure is useful when storing offsets for instance since it can change offset locations during removal.
    /// Mutable struct: hold it in exactly one field, mutate only through that field or a ref local,
    /// and never copy it — a realloc in a copy frees the memory the original still points at.
    /// The struct does not store an allocator; every call that can allocate or free takes the owner's
    /// allocator, and all calls for a given list must use the same one.
    /// </summary>
    [NonCopyable]
    internal unsafe struct IntList
    {
        private FlowtideMemory _memory;
        private int _length;

        // Capacity in elements, derived so the struct is the single source of truth.
        private readonly int DataLength => _memory.Length / sizeof(int);

        internal readonly Span<int> AccessSpan => new Span<int>(_memory.Pointer, DataLength);

        public readonly Memory<byte> Memory => GetViewMemory().Slice(0, _length * sizeof(int));

        public readonly Span<byte> SlicedSpan => new Span<byte>(_memory.Pointer, _length * sizeof(int));

        // Allocates a fresh non-owning view per call; only cold paths (Arrow interop, checkpoint
        // writers) need Memory<byte>, and they re-fetch after list mutations.
        private readonly Memory<byte> GetViewMemory()
        {
            if (_memory.IsNull)
            {
                return new Memory<byte>();
            }
            return new NativeMemoryView(_memory.Pointer, _memory.Length).Memory;
        }

        public IntList(int initialCapacity, IMemoryAllocator memoryAllocator)
        {
            _memory = memoryAllocator.AllocateMemory(initialCapacity * sizeof(int));
        }

        public IntList(FlowtideMemory memory, int length)
        {
            _memory = memory;
            _length = length;
        }

        public readonly ReadOnlySpan<int> Span => new ReadOnlySpan<int>(_memory.Pointer, _length);

        public readonly int Count => _length;

        public readonly int* GetPointer_Unsafe()
        {
            return (int*)_memory.Pointer;
        }

        internal void EnsureCapacity(int length, IMemoryAllocator memoryAllocator)
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

        private void CheckSizeReduction(IMemoryAllocator memoryAllocator)
        {
            var multipleid = (_length << 1) + (_length >> 1);
            var dataLength = DataLength;
            if (multipleid < dataLength && dataLength > 256)
            {
                Debug.Assert(!_memory.IsNull);
                memoryAllocator.Realloc(ref _memory, _length * sizeof(int));
            }
        }

        public void Add(int item, IMemoryAllocator memoryAllocator)
        {
            EnsureCapacity(_length + 1, memoryAllocator);
            ((int*)_memory.Pointer)[_length++] = item;
        }

        public void RemoveAt(int index, IMemoryAllocator memoryAllocator)
        {
            AccessSpan.Slice(index + 1, _length - index - 1).CopyTo(AccessSpan.Slice(index));
            _length--;
            CheckSizeReduction(memoryAllocator);
        }

        /// <summary>
        /// Special remove at, where it runs an addition on all elements that are larger than the removed index.
        /// This is useful if this is used to store offsets where all offsets can be moved during the copy.
        /// </summary>
        /// <param name="index"></param>
        /// <param name="additionOnMoved"></param>
        /// <param name="memoryAllocator"></param>
        public void RemoveAt(int index, int additionOnMoved, IMemoryAllocator memoryAllocator)
        {
            AvxUtils.InPlaceMemCopyWithAddition(AccessSpan, index + 1, index, _length - index - 1, additionOnMoved);
            _length--;
            CheckSizeReduction(memoryAllocator);
        }

        public void RemoveRange(int index, int count, int additionOnMoved, IMemoryAllocator memoryAllocator)
        {
            AvxUtils.InPlaceMemCopyWithAddition(AccessSpan, index + count, index, _length - index - count, additionOnMoved);
            _length -= count;
            CheckSizeReduction(memoryAllocator);
        }

        public void RemoveRange(int index, int count, IMemoryAllocator memoryAllocator)
        {
            AccessSpan.Slice(index + count, _length - index - count).CopyTo(AccessSpan.Slice(index));
            _length -= count;
            CheckSizeReduction(memoryAllocator);
        }

        public void RemoveAtConditionalAddition(int index, Span<sbyte> conditionalValues, sbyte conditionalValue, int additionOnMoved, IMemoryAllocator memoryAllocator)
        {
            AvxUtils.InPlaceMemCopyConditionalAddition(AccessSpan, conditionalValues, index + 1, index, _length - index - 1, additionOnMoved, conditionalValue);
            _length--;
            CheckSizeReduction(memoryAllocator);
        }

        public void RemoveRangeTypeBasedAddition(int index, int count, Span<sbyte> typeIds, Span<int> toAdd, int typeCount, IMemoryAllocator memoryAllocator)
        {
            AvxUtils.InPlaceMemCopyAdditionByType(AccessSpan, typeIds, index + count, index, _length - index - count, toAdd, typeCount);
            _length -= count;
            CheckSizeReduction(memoryAllocator);
        }

        public void InsertAt(int index, int item, IMemoryAllocator memoryAllocator)
        {
            EnsureCapacity(_length + 1, memoryAllocator);
            var span = AccessSpan;
            span.Slice(index, _length - index).CopyTo(span.Slice(index + 1));
            span[index] = item;
            _length++;
        }

        public void InsertAt(int index, int item, int additionOnMoved, IMemoryAllocator memoryAllocator)
        {
            EnsureCapacity(_length + 1, memoryAllocator);
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

        public void InsertRangeFrom(int index, in IntList other, int start, int count, int additionOnMovedExisting, int additionOnCopied, IMemoryAllocator memoryAllocator)
        {
            EnsureCapacity(_length + count, memoryAllocator);
            var span = AccessSpan;
            var sourceSpan = other.AccessSpan;
            AvxUtils.InPlaceMemCopyWithAddition(span, index, index + count, _length - index, additionOnMovedExisting);
            AvxUtils.MemCpyWithAdd(sourceSpan.Slice(start, count), span.Slice(index), additionOnCopied);
            _length += count;
        }

        public void InsertRangeStaticValue(int index, int count, int staticValue, IMemoryAllocator memoryAllocator)
        {
            EnsureCapacity(_length + count, memoryAllocator);
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
            in IntList other,
            int start,
            int count,
            Span<sbyte> thisTypeIds,
            Span<int> thisToAdd,
            Span<sbyte> otherTypeIds,
            Span<int> otherToAdd,
            int typeCount,
            IMemoryAllocator memoryAllocator)
        {
            EnsureCapacity(_length + count, memoryAllocator);
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
            int additionOnMovedExisting,
            IMemoryAllocator memoryAllocator)
        {
            EnsureCapacity(_length + count, memoryAllocator);
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
        /// <param name="memoryAllocator"></param>
        public void InsertAtConditionalAddition(int index, int item, Span<sbyte> conditionalValues, sbyte conditionalValue, int additionOnMoved, IMemoryAllocator memoryAllocator)
        {
            EnsureCapacity(_length + 1, memoryAllocator);
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
        public readonly int Get(in int index)
        {
            return ((int*)_memory.Pointer)[index];
        }

        // No IDisposable: a using-declared struct variable is read-only and mutating calls on it
        // would run on defensive copies. Free zeroes _memory, so double dispose is a no-op.
        public void Dispose(IMemoryAllocator memoryAllocator)
        {
            if (!_memory.IsNull)
            {
                memoryAllocator.Free(ref _memory);
            }
        }

        public void Clear()
        {
            _length = 0;
        }

        public readonly IntList Copy(IMemoryAllocator memoryAllocator)
        {
            var slicedSpan = SlicedSpan;
            var newMemory = memoryAllocator.AllocateMemory(slicedSpan.Length);
            slicedSpan.CopyTo(newMemory.Span);

            return new IntList(newMemory, _length);
        }
    }
}
