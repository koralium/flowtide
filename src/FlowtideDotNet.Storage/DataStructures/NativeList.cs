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
using System.Runtime.InteropServices;

namespace FlowtideDotNet.Storage.DataStructures
{
    /// <summary>
    /// A growable list of unmanaged values stored in native memory, without a heap object.
    /// Mutable struct, keep it in one field and never copy it.
    /// The struct does not store an allocator, calls that can allocate or free take the owner's allocator.
    /// </summary>
    /// <typeparam name="T">The unmanaged element type.</typeparam>
    [NonCopyable]
    public unsafe struct NativeList<T>
        where T : unmanaged
    {
        private FlowtideMemory _memory;
        private int _length;

        // Capacity in elements, derived from the block.
        private readonly int DataLength => _memory.Length / sizeof(T);

        /// <summary>
        /// Creates a list with backing memory pre-allocated for at least <paramref name="initialCapacity"/> elements.
        /// </summary>
        /// <param name="initialCapacity">The number of elements to reserve capacity for up front.</param>
        /// <param name="memoryAllocator">The allocator used for backing memory.</param>
        public NativeList(int initialCapacity, IMemoryAllocator memoryAllocator)
        {
            _memory = memoryAllocator.AllocateMemory(initialCapacity * sizeof(T));
            _length = 0;
        }

        /// <summary>
        /// Creates a list that wraps already populated memory, taking ownership of it.
        /// </summary>
        /// <param name="memory">The memory holding the elements.</param>
        /// <param name="length">The number of valid elements in <paramref name="memory"/>.</param>
        public NativeList(FlowtideMemory memory, int length)
        {
            _memory = memory;
            _length = length;
        }

        /// <summary>
        /// A span over the current elements of the list.
        /// </summary>
        public readonly Span<T> Span => new Span<T>(_memory.Pointer, _length);

        /// <summary>
        /// The full backing memory buffer in bytes, or empty when nothing is allocated.
        /// </summary>
        public readonly Memory<byte> Memory => GetViewMemory();

        /// <summary>
        /// The current elements for consumers that need Memory.
        /// </summary>
        public readonly Memory<byte> SlicedMemory => GetViewMemory().Slice(0, _length * sizeof(T));

        /// <summary>
        /// The bytes of the current elements.
        /// </summary>
        public readonly Span<byte> SlicedSpan => new Span<byte>(_memory.Pointer, _length * sizeof(T));

        // We create a new view per call, only cold paths need it.
        private readonly Memory<byte> GetViewMemory()
        {
            if (_memory.IsNull)
            {
                return new Memory<byte>();
            }
            return new NativeMemoryView(_memory.Pointer, _memory.Length).Memory;
        }

        /// <summary>
        /// UNSAFE: Gets the raw pointer to do operations without boundary checks
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly T* GetPointer_Unsafe()
        {
            return (T*)_memory.Pointer;
        }

        /// <summary>
        /// Grows the backing memory so it can hold at least <paramref name="length"/> elements.
        /// </summary>
        /// <param name="length">The number of elements to reserve capacity for.</param>
        /// <param name="memoryAllocator">The owner's allocator.</param>
        public void EnsureCapacity(int length, IMemoryAllocator memoryAllocator)
        {
            if (DataLength < length)
            {
                var newLength = length * 2;
                if (newLength < 64)
                {
                    newLength = 64;
                }
                memoryAllocator.Realloc(ref _memory, newLength * sizeof(T));
            }
        }

        private void CheckSizeReduction(IMemoryAllocator memoryAllocator)
        {
            var multipleid = (_length << 1) + (_length >> 1);
            var dataLength = DataLength;
            if (multipleid < dataLength && dataLength > 256)
            {
                Debug.Assert(!_memory.IsNull);
                memoryAllocator.Realloc(ref _memory, _length * sizeof(T));
            }
        }

        private readonly Span<T> AccessSpan => new Span<T>(_memory.Pointer, DataLength);

        /// <summary>
        /// Appends a value to the end of the list, growing it by one.
        /// </summary>
        /// <param name="value">The value to append.</param>
        /// <param name="memoryAllocator">The owner's allocator.</param>
        public void Add(T value, IMemoryAllocator memoryAllocator)
        {
            EnsureCapacity(_length + 1, memoryAllocator);
            AccessSpan[_length++] = value;
        }

        /// <summary>
        /// Appends a run of elements copied from another list to the end of this one.
        /// </summary>
        /// <param name="list">The list to copy elements from.</param>
        /// <param name="index">The zero based index in <paramref name="list"/> to start copying from.</param>
        /// <param name="count">The number of elements to copy.</param>
        /// <param name="memoryAllocator">The owner's allocator.</param>
        public void AddRangeFrom(in NativeList<T> list, int index, int count, IMemoryAllocator memoryAllocator)
        {
            EnsureCapacity(_length + count, memoryAllocator);
            var span = AccessSpan;
            var sourceSpan = list.AccessSpan;
            sourceSpan.Slice(index, count).CopyTo(span.Slice(_length, count));
            _length += count;
        }

        /// <summary>
        /// Inserts a value at the given index, shifting every element at or after that index up by one.
        /// </summary>
        /// <param name="index">The zero based index to insert at.</param>
        /// <param name="value">The value to insert.</param>
        /// <param name="memoryAllocator">The owner's allocator.</param>
        public void InsertAt(int index, T value, IMemoryAllocator memoryAllocator)
        {
            if (index == _length)
            {
                Add(value, memoryAllocator);
                return;
            }

            EnsureCapacity(_length + 1, memoryAllocator);
            var span = AccessSpan;
            span.Slice(index, _length - index).CopyTo(span.Slice(index + 1, _length - index));
            span[index] = value;
            _length++;
        }

        /// <summary>
        /// Inserts a run of elements copied from another list, shifting existing elements up to make room.
        /// </summary>
        /// <param name="index">The zero based index in this list to insert at.</param>
        /// <param name="other">The list to copy elements from.</param>
        /// <param name="start">The zero based index in <paramref name="other"/> to start copying from.</param>
        /// <param name="count">The number of elements to copy.</param>
        /// <param name="memoryAllocator">The owner's allocator.</param>
        public void InsertRangeFrom(int index, in NativeList<T> other, int start, int count, IMemoryAllocator memoryAllocator)
        {
            EnsureCapacity(_length + count, memoryAllocator);
            var span = AccessSpan;
            var sourceSpan = other.AccessSpan;
            span.Slice(index, _length - index).CopyTo(span.Slice(index + count, _length - index));
            sourceSpan.Slice(start, count).CopyTo(span.Slice(index, count));
            _length += count;
        }

        /// <summary>
        /// Inserts the same value <paramref name="count"/> times at the given index, shifting existing elements up to make room.
        /// </summary>
        /// <param name="index">The zero based index to insert at.</param>
        /// <param name="value">The value to insert repeatedly.</param>
        /// <param name="count">The number of copies to insert.</param>
        /// <param name="memoryAllocator">The owner's allocator.</param>
        public void InsertStaticRange(int index, T value, int count, IMemoryAllocator memoryAllocator)
        {
            EnsureCapacity(_length + count, memoryAllocator);
            var span = AccessSpan;
            span.Slice(index, _length - index).CopyTo(span.Slice(index + count, _length - index));
            for (var i = 0; i < count; i++)
            {
                span[index + i] = value;
            }
            _length += count;
        }

        /// <summary>
        /// Special case insert that allows inserting a subset of elements from another list at specific positions.
        /// This is used when merging two lists together more memory efficiently than inserting each element one by one.
        /// The sortedLookup and insertPositions spans must have the same length, but do not need to cover every element in the other list.
        /// The positions in <paramref name="insertPositions"/> are interpreted relative to the original contents of the current list before any elements are inserted.
        /// Conceptually, this behaves like inserting the selected elements in order using <c>InsertAt(insertPositions[i] + i, ...)</c>.
        /// </summary>
        /// <param name="other">The other list to insert data from.</param>
        /// <param name="sortedLookup">A span containing the indices of the elements to insert from the other list.</param>
        /// <param name="insertPositions">A span containing the positions at which to insert the elements in the current list. Must be in non-decreasing order.</param>
        /// <param name="lookupNullIndex">A sentinel value in <paramref name="sortedLookup"/> that inserts a default element instead of reading from <paramref name="other"/>.</param>
        /// <param name="memoryAllocator">The owner's allocator.</param>
        public void InsertFrom(ref readonly NativeList<T> other, ref readonly ReadOnlySpan<int> sortedLookup, ref readonly ReadOnlySpan<int> insertPositions, in int lookupNullIndex, IMemoryAllocator memoryAllocator)
        {
            Debug.Assert(sortedLookup.Length == insertPositions.Length);
            int otherCount = sortedLookup.Length;
            if (otherCount == 0) return;

            int oldCount = _length;

            // Ensure we have enough capacity for all elements
            EnsureCapacity(oldCount + otherCount, memoryAllocator);

            var selfData = AccessSpan;
            var otherData = other.AccessSpan;

            int currentReadIdx = oldCount;
            int currentWriteIdx = oldCount + otherCount;

            for (int i = otherCount - 1; i >= 0; i--)
            {
                int targetInsertIdx = insertPositions[i];
                int elementsToMove = currentReadIdx - targetInsertIdx;

                if (elementsToMove > 0)
                {
                    currentWriteIdx -= elementsToMove;
                    currentReadIdx -= elementsToMove;

                    selfData.Slice(currentReadIdx, elementsToMove)
                            .CopyTo(selfData.Slice(currentWriteIdx, elementsToMove));
                }

                // Place the new element from the other list
                int oIdx = sortedLookup[i];
                currentWriteIdx--;
                selfData[currentWriteIdx] = oIdx == lookupNullIndex ? default : otherData[oIdx];

                // Move the read tracker to the left of the block we just processed
                currentReadIdx = targetInsertIdx;
            }

            _length += otherCount;
        }

        /// <summary>
        /// Batch delete elements at the specified sorted indices.
        /// This is more efficient than calling RemoveAt repeatedly because it processes
        /// contiguous blocks of retained data in a single left-to-right sweep.
        /// </summary>
        /// <param name="targets">A span of sorted indices (ascending) of elements to delete.</param>
        /// <param name="memoryAllocator">The owner's allocator.</param>
        public void DeleteBatch(ReadOnlySpan<int> targets, IMemoryAllocator memoryAllocator)
        {
            int deleteCount = targets.Length;
            if (deleteCount == 0) return;

            Debug.Assert(deleteCount <= _length);

            int oldCount = _length;
            var selfData = AccessSpan;

            int writeIdx = 0;
            int currentSourceIdx = 0;

            for (int i = 0; i < deleteCount; i++)
            {
                int targetIdx = targets[i];

                // Copy the retained block before this deletion target
                int elementsToMove = targetIdx - currentSourceIdx;
                if (elementsToMove > 0)
                {
                    if (writeIdx != currentSourceIdx)
                    {
                        selfData.Slice(currentSourceIdx, elementsToMove)
                                .CopyTo(selfData.Slice(writeIdx, elementsToMove));
                    }
                    writeIdx += elementsToMove;
                }

                // Skip the deleted element
                currentSourceIdx = targetIdx + 1;
            }

            // Copy the remaining block after the last deletion target
            int remaining = oldCount - currentSourceIdx;
            if (remaining > 0 && writeIdx != currentSourceIdx)
            {
                selfData.Slice(currentSourceIdx, remaining)
                        .CopyTo(selfData.Slice(writeIdx, remaining));
            }

            _length = oldCount - deleteCount;
            CheckSizeReduction(memoryAllocator);
        }

        /// <summary>
        /// Special case insert that allows inserting a subset of elements from an array at specific positions.
        /// This is used when merging two lists together more memory efficiently than inserting each element one by one.
        /// The sortedLookup and insertPositions spans must have the same length, but do not need to cover every element in the array.
        /// The positions in <paramref name="insertPositions"/> are interpreted relative to the original contents of the current list before any elements are inserted.
        /// Conceptually, this behaves like inserting the selected elements in order using <c>InsertAt(insertPositions[i] + i, ...)</c>.
        /// </summary>
        /// <param name="keys">The array to insert data from.</param>
        /// <param name="sortedLookup">A span containing the indices of the elements to insert from the array.</param>
        /// <param name="insertPositions">A span containing the positions at which to insert the elements in the current list. Must be in non-decreasing order.</param>
        /// <param name="memoryAllocator">The owner's allocator.</param>
        public void InsertFrom(T[] keys, ReadOnlySpan<int> sortedLookup, ReadOnlySpan<int> insertPositions, IMemoryAllocator memoryAllocator)
        {
            Debug.Assert(sortedLookup.Length == insertPositions.Length);
            int otherCount = sortedLookup.Length;
            if (otherCount == 0) return;

            int oldCount = _length;

            EnsureCapacity(oldCount + otherCount, memoryAllocator);

            var selfData = AccessSpan;
            var otherData = keys.AsSpan();

            int currentReadIdx = oldCount;
            int currentWriteIdx = oldCount + otherCount;

            for (int i = otherCount - 1; i >= 0; i--)
            {
                int targetInsertIdx = insertPositions[i];
                int elementsToMove = currentReadIdx - targetInsertIdx;

                if (elementsToMove > 0)
                {
                    currentWriteIdx -= elementsToMove;
                    currentReadIdx -= elementsToMove;

                    selfData.Slice(currentReadIdx, elementsToMove)
                            .CopyTo(selfData.Slice(currentWriteIdx, elementsToMove));
                }

                int oIdx = sortedLookup[i];
                currentWriteIdx--;
                selfData[currentWriteIdx] = otherData[oIdx];

                currentReadIdx = targetInsertIdx;
            }

            _length += otherCount;
        }

        /// <summary>
        /// Opens a gap of <paramref name="count"/> elements at the given index by shifting the elements at or after it up,
        /// without writing any values into the gap. The list grows by <paramref name="count"/>.
        /// </summary>
        /// <param name="index">The zero based index at which to open the gap.</param>
        /// <param name="count">The number of element slots to open.</param>
        /// <param name="memoryAllocator">The owner's allocator.</param>
        public void MoveAtIndex(int index, int count, IMemoryAllocator memoryAllocator)
        {
            EnsureCapacity(_length + count, memoryAllocator);
            var span = AccessSpan;
            span.Slice(index, _length - index).CopyTo(span.Slice(index + count, _length - index));
            _length += count;
        }

        /// <summary>
        /// Removes the element at the given index and shifts all elements above it down.
        /// </summary>
        /// <param name="index">The zero based index of the element to remove.</param>
        /// <param name="memoryAllocator">The owner's allocator.</param>
        public void RemoveAt(int index, IMemoryAllocator memoryAllocator)
        {
            var span = AccessSpan;
            span.Slice(index + 1, _length - index - 1).CopyTo(span.Slice(index, _length - index - 1));
            _length--;
            CheckSizeReduction(memoryAllocator);
        }

        /// <summary>
        /// Removes a run of elements starting at the given index and shifts all elements above the range down.
        /// </summary>
        /// <param name="index">The zero based index of the first element to remove.</param>
        /// <param name="count">The number of elements to remove.</param>
        /// <param name="memoryAllocator">The owner's allocator.</param>
        public void RemoveRange(int index, int count, IMemoryAllocator memoryAllocator)
        {
            var span = AccessSpan;
            var length = _length - index - count;
            span.Slice(index + count, length).CopyTo(span.Slice(index));
            _length -= count;
            CheckSizeReduction(memoryAllocator);
        }

        /// <summary>
        /// Gets the element at the given index.
        /// </summary>
        /// <param name="index">The zero based index.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public readonly T Get(int index)
        {
            Debug.Assert(index >= 0 && index < _length);
            return ((T*)_memory.Pointer)[index];
        }

        /// <summary>
        /// Gets a reference to the element at the given index, allowing it to be read or modified in place.
        /// </summary>
        /// <param name="index">The zero based index.</param>
        public readonly ref T GetRef(scoped in int index)
        {
            var span = AccessSpan;
            return ref span[index];
        }

        /// <summary>
        /// Overwrites the element at the given index.
        /// </summary>
        /// <param name="index">The zero based index.</param>
        /// <param name="value">The new value.</param>
        public readonly void Update(in int index, in T value)
        {
            AccessSpan[index] = value;
        }

        /// <summary>
        /// Gets or sets the element at the given index.
        /// </summary>
        /// <param name="index">The zero based index.</param>
        public readonly T this[int index]
        {
            get
            {
                return Get(index);
            }
            set
            {
                Update(index, value);
            }
        }

        /// <summary>
        /// The number of elements in the list.
        /// </summary>
        public readonly int Count => _length;

        /// <summary>
        /// Frees the backing memory and resets the list to the default value, dispose twice is safe.
        /// </summary>
        /// <param name="memoryAllocator">The owner's allocator.</param>
        public void Dispose(IMemoryAllocator memoryAllocator)
        {
            if (!_memory.IsNull)
            {
                memoryAllocator.Free(ref _memory);
            }
            _length = 0;
        }

        /// <summary>
        /// Resets the count to zero, keeping the backing memory for reuse.
        /// </summary>
        public void Clear()
        {
            _length = 0;
        }

        /// <summary>
        /// Creates a deep copy of the list, allocating new backing memory from the given allocator.
        /// </summary>
        /// <param name="memoryAllocator">The allocator used for the copy's backing memory.</param>
        /// <returns>A new list with the same elements.</returns>
        public readonly NativeList<T> Copy(IMemoryAllocator memoryAllocator)
        {
            var newMemory = memoryAllocator.AllocateMemory(_length * sizeof(T));
            SlicedSpan.CopyTo(newMemory.Span);

            return new NativeList<T>(newMemory, _length);
        }

        /// <summary>
        /// Performs a binary search over the elements. The list must be sorted with respect to <paramref name="value"/>.
        /// </summary>
        /// <typeparam name="TComp">A comparable that defines the ordering against the elements.</typeparam>
        /// <param name="value">The comparable to search for.</param>
        /// <returns>
        /// The index of the matching element, or the bitwise complement of the index of the next larger element when no match is found.
        /// </returns>
        public readonly int BinarySearch<TComp>(TComp value)
            where TComp : IComparable<T>
        {
            return AccessSpan.Slice(0, _length).BinarySearch(value);
        }

        /// <summary>
        /// Sets the element count directly, without allocating or clearing memory. The caller must ensure the backing
        /// memory already holds that many valid elements.
        /// </summary>
        /// <param name="newLength">The new element count.</param>
        public void SetLength(int newLength)
        {
            _length = newLength;
        }

        /// <summary>
        /// Adds this list's per-index byte size contribution to the running totals in <paramref name="sizes"/>, where each
        /// element contributes <c>sizeof(T)</c> bytes. Used together with the other containers to build prefix-sum sizes
        /// when computing serialized batch sizes.
        /// </summary>
        /// <param name="indices">The indices the sizes are being accumulated for.</param>
        /// <param name="sizes">The running per-index byte size totals to add to.</param>
        public readonly void GetPrefixSumByteSizes(ReadOnlySpan<int> indices, Span<int> sizes)
        {
            int length = indices.Length;
            int elementSize = sizeof(T);

            ref int sizesHead = ref MemoryMarshal.GetReference(sizes);

            int cumulativeMass = elementSize;

            for (int i = 0; i < length; i++)
            {
                Unsafe.Add(ref sizesHead, i) += cumulativeMass;
                cumulativeMass += elementSize;
            }
        }
    }
}
