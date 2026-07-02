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
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace FlowtideDotNet.Storage.DataStructures
{
    /// <summary>
    /// A growable list of unmanaged values stored in native memory allocated from an <see cref="IMemoryAllocator"/>.
    /// It behaves like a list (add, insert, remove, index) but keeps its elements in a contiguous unmanaged buffer,
    /// which can be exposed as a <see cref="Span"/> or <see cref="Memory"/> and serialized without copying element by element.
    /// Ownership is reference counted through <see cref="Rent(int)"/> and <see cref="Return"/>, and the buffer is
    /// released when the list is disposed or the last reference is returned.
    /// </summary>
    /// <typeparam name="T">The unmanaged element type.</typeparam>
    public unsafe class PrimitiveList<T> : IDisposable, IReadOnlyList<T>
        where T : unmanaged
    {
        private void* _data;
        private int _dataLength;
        private int _length;
        private bool _disposedValue;
        private readonly IMemoryAllocator _memoryAllocator;
        private IMemoryOwner<byte>? _memoryOwner;
        private int _rentCounter;

        /// <summary>
        /// Creates an empty list that allocates backing memory from the given allocator on demand.
        /// </summary>
        /// <param name="memoryAllocator">The allocator used for backing memory.</param>
        public PrimitiveList(IMemoryAllocator memoryAllocator)
        {
            _data = null;
            _memoryAllocator = memoryAllocator;
        }

        /// <summary>
        /// Creates an empty list with backing memory pre-allocated for at least <paramref name="initialCapacity"/> elements.
        /// </summary>
        /// <param name="memoryAllocator">The allocator used for backing memory.</param>
        /// <param name="initialCapacity">The number of elements to reserve capacity for up front.</param>
        public PrimitiveList(IMemoryAllocator memoryAllocator, int initialCapacity)
        {
            _memoryAllocator = memoryAllocator;
            EnsureCapacity(initialCapacity);
        }

        /// <summary>
        /// Creates a list that wraps already populated memory, taking ownership of it.
        /// </summary>
        /// <param name="memory">The memory holding the elements.</param>
        /// <param name="length">The number of valid elements in <paramref name="memory"/>.</param>
        /// <param name="memoryAllocator">The allocator used for any later growth.</param>
        public PrimitiveList(IMemoryOwner<byte> memory, int length, IMemoryAllocator memoryAllocator)
        {
            _memoryOwner = memory;
            _data = _memoryOwner.Memory.Pin().Pointer;
            _dataLength = memory.Memory.Length / sizeof(T);
            _length = length;
            _memoryAllocator = memoryAllocator;
        }

        /// <summary>
        /// A span over the current elements of the list.
        /// </summary>
        public Span<T> Span => new Span<T>(_data, _length);

        /// <summary>
        /// The full backing memory buffer in bytes, including any capacity beyond the current elements, or empty when nothing is allocated.
        /// </summary>
        public Memory<byte> Memory => _memoryOwner?.Memory ?? new Memory<byte>();

        /// <summary>
        /// The portion of the backing memory in bytes that actually holds the current elements.
        /// Use this when serializing or copying the list.
        /// </summary>
        public Memory<byte> SlicedMemory => _memoryOwner?.Memory.Slice(0, _length * sizeof(T)) ?? new Memory<byte>();

        /// <summary>
        /// Creates a list over an existing raw memory pointer. The caller is responsible for keeping the memory alive.
        /// </summary>
        /// <param name="data">Pointer to the element data.</param>
        /// <param name="dataLength">The capacity, in elements, of the buffer at <paramref name="data"/>.</param>
        /// <param name="length">The number of valid elements.</param>
        /// <param name="memoryAllocator">The allocator used for any later growth.</param>
        public PrimitiveList(void* data, int dataLength, int length, IMemoryAllocator memoryAllocator)
        {
            _data = data;
            _dataLength = dataLength;
            _length = length;
            _memoryAllocator = memoryAllocator;
        }

        /// <summary>
        /// UNSAFE: Gets the raw pointer to do operations without boundary checks
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal T* GetPointer_Unsafe()
        {
            return (T*)_data;
        }

        internal void EnsureCapacity(int length)
        {
            if (_dataLength < length)
            {
                var newLength = length * 2;
                if (newLength < 64)
                {
                    newLength = 64;
                }
                var allocSize = newLength * sizeof(T);

                if (_memoryOwner == null)
                {
                    _memoryOwner = _memoryAllocator.Allocate(allocSize, 64);
                    _data = _memoryOwner.Memory.Pin().Pointer;
                }
                else
                {
                    _memoryOwner = _memoryAllocator.Realloc(_memoryOwner, allocSize, 64);
                    _data = _memoryOwner.Memory.Pin().Pointer;
                }
                _dataLength = _memoryOwner.Memory.Length / sizeof(T);
            }
        }

        private void CheckSizeReduction()
        {
            var multipleid = (_length << 1) + (_length >> 1);
            if (multipleid < _dataLength && _dataLength > 256)
            {
                Debug.Assert(_memoryAllocator != null);
                Debug.Assert(_memoryOwner != null);
                _memoryOwner = _memoryAllocator.Realloc(_memoryOwner, _length * sizeof(T), 64);
                _data = _memoryOwner.Memory.Pin().Pointer;
                _dataLength = _memoryOwner.Memory.Length / sizeof(T);
            }
        }

        private Span<T> AccessSpan => new Span<T>(_data, _dataLength);

        /// <summary>
        /// Appends a value to the end of the list, growing it by one.
        /// </summary>
        /// <param name="value">The value to append.</param>
        public void Add(T value)
        {
            EnsureCapacity(_length + 1);
            AccessSpan[_length++] = value;
        }

        /// <summary>
        /// Appends a run of elements copied from another list to the end of this one.
        /// </summary>
        /// <param name="list">The list to copy elements from.</param>
        /// <param name="index">The zero based index in <paramref name="list"/> to start copying from.</param>
        /// <param name="count">The number of elements to copy.</param>
        public void AddRangeFrom(PrimitiveList<T> list, int index, int count)
        {
            EnsureCapacity(_length + count);
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
        public void InsertAt(int index, T value)
        {
            if (index == _length)
            {
                Add(value);
                return;
            }

            EnsureCapacity(_length + 1);
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
        public void InsertRangeFrom(int index, PrimitiveList<T> other, int start, int count)
        {
            EnsureCapacity(_length + count);
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
        public void InsertStaticRange(int index, T value, int count)
        {
            EnsureCapacity(_length + count);
            var span = AccessSpan;
            span.Slice(index, _length - index).CopyTo(span.Slice(index + count, _length - index));
            for (var i = 0; i < count; i++)
            {
                span[index + i] = value;
            }
            _length += count;
        }

        /// <summary>
        /// Special case insert that allows inserting a subset of elements from another primitive list at specific positions.
        /// This is used when merging two lists together more memory efficiently than inserting each element one by one.
        /// The sortedLookup and insertPositions spans must have the same length, but do not need to cover every element in the other list.
        /// The positions in <paramref name="insertPositions"/> are interpreted relative to the original contents of the current list before any elements are inserted.
        /// Conceptually, this behaves like inserting the selected elements in order using <c>InsertAt(insertPositions[i] + i, ...)</c>.
        /// </summary>
        /// <param name="other">The other primitive list to insert data from.</param>
        /// <param name="sortedLookup">A span containing the indices of the elements to insert from the other list.</param>
        /// <param name="insertPositions">A span containing the positions at which to insert the elements in the current list. Must be in non-decreasing order.</param>
        /// <param name="lookupNullIndex">A sentinel value in <paramref name="sortedLookup"/> that inserts a default element instead of reading from <paramref name="other"/>.</param>
        public void InsertFrom(ref readonly PrimitiveList<T> other, ref readonly ReadOnlySpan<int> sortedLookup, ref readonly ReadOnlySpan<int> insertPositions, in int lookupNullIndex)
        {
            Debug.Assert(sortedLookup.Length == insertPositions.Length);
            int otherCount = sortedLookup.Length;
            if (otherCount == 0) return;

            int oldCount = _length;

            // Ensure we have enough capacity for all elements
            EnsureCapacity(oldCount + otherCount);

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
        public void DeleteBatch(ReadOnlySpan<int> targets)
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
            CheckSizeReduction();
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
        public void InsertFrom(T[] keys, ReadOnlySpan<int> sortedLookup, ReadOnlySpan<int> insertPositions)
        {
            Debug.Assert(sortedLookup.Length == insertPositions.Length);
            int otherCount = sortedLookup.Length;
            if (otherCount == 0) return;

            int oldCount = _length;

            EnsureCapacity(oldCount + otherCount);

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
        public void MoveAtIndex(int index, int count)
        {
            EnsureCapacity(_length + count);
            var span = AccessSpan;
            span.Slice(index, _length - index).CopyTo(span.Slice(index + count, _length - index));
            _length += count;
        }

        /// <summary>
        /// Removes the element at the given index and shifts all elements above it down.
        /// </summary>
        /// <param name="index">The zero based index of the element to remove.</param>
        public void RemoveAt(int index)
        {
            var span = AccessSpan;
            span.Slice(index + 1, _length - index - 1).CopyTo(span.Slice(index, _length - index - 1));
            _length--;
            CheckSizeReduction();
        }

        /// <summary>
        /// Removes a run of elements starting at the given index and shifts all elements above the range down.
        /// </summary>
        /// <param name="index">The zero based index of the first element to remove.</param>
        /// <param name="count">The number of elements to remove.</param>
        public void RemoveRange(int index, int count)
        {
            var span = AccessSpan;
            var length = _length - index - count;
            span.Slice(index + count, length).CopyTo(span.Slice(index));
            _length -= count;
            CheckSizeReduction();
        }

        /// <summary>
        /// Gets the element at the given index.
        /// </summary>
        /// <param name="index">The zero based index.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T Get(int index)
        {
            Debug.Assert(index >= 0 && index < _length);
            return ((T*)_data)[index];
        }

        /// <summary>
        /// Gets a reference to the element at the given index, allowing it to be read or modified in place.
        /// </summary>
        /// <param name="index">The zero based index.</param>
        public ref T GetRef(scoped in int index)
        {
            var span = AccessSpan;
            return ref span[index];
        }

        /// <summary>
        /// Overwrites the element at the given index.
        /// </summary>
        /// <param name="index">The zero based index.</param>
        /// <param name="value">The new value.</param>
        public void Update(in int index, in T value)
        {
            AccessSpan[index] = value;
        }

        /// <summary>
        /// Gets or sets the element at the given index.
        /// </summary>
        /// <param name="index">The zero based index.</param>
        public T this[int index]
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
        public int Count => _length;

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (_memoryOwner != null)
                {
                    _memoryOwner.Dispose();
                    _memoryOwner = null;
                    _data = null;
                }

                _disposedValue = true;
            }
        }

        ~PrimitiveList()
        {
            Dispose(disposing: false);
        }

        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        private IEnumerable<T> GetEnumerable()
        {
            for (var i = 0; i < _length; i++)
            {
                yield return Get(i);
            }
        }

        /// <summary>
        /// Enumerates the elements in order from index 0 to <see cref="Count"/>.
        /// </summary>
        public IEnumerator<T> GetEnumerator()
        {
            return GetEnumerable().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerable().GetEnumerator();
        }

        /// <summary>
        /// Increases the reference count by <paramref name="count"/> to share ownership of the list.
        /// Each rent must be matched by a <see cref="Return"/>.
        /// </summary>
        /// <param name="count">The number of references to add.</param>
        public void Rent(int count)
        {
            Interlocked.Add(ref _rentCounter, count);
        }

        /// <summary>
        /// Releases one reference. When the reference count reaches zero the list is disposed and its memory freed.
        /// </summary>
        public void Return()
        {
            var result = Interlocked.Decrement(ref _rentCounter);
            if (result <= 0)
            {
                Dispose();
            }
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
        public PrimitiveList<T> Copy(IMemoryAllocator memoryAllocator)
        {
            var slicedMem = SlicedMemory;
            var newMemory = memoryAllocator.Allocate(slicedMem.Length, 64);
            slicedMem.Span.CopyTo(newMemory.Memory.Span);

            return new PrimitiveList<T>(newMemory, _length, memoryAllocator);
        }

        /// <summary>
        /// Performs a binary search over the elements. The list must be sorted with respect to <paramref name="value"/>.
        /// </summary>
        /// <typeparam name="TComp">A comparable that defines the ordering against the elements.</typeparam>
        /// <param name="value">The comparable to search for.</param>
        /// <returns>
        /// The index of the matching element, or the bitwise complement of the index of the next larger element when no match is found.
        /// </returns>
        public int BinarySearch<TComp>(TComp value)
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
        public void GetPrefixSumByteSizes(ReadOnlySpan<int> indices, Span<int> sizes)
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
