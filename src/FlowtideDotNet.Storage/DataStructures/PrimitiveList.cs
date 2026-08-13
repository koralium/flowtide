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
using System.Collections;
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Storage.DataStructures
{
    /// <summary>
    /// A growable list of unmanaged values stored in native memory allocated from an <see cref="IMemoryAllocator"/>.
    /// It behaves like a list (add, insert, remove, index) but keeps its elements in a contiguous unmanaged buffer,
    /// which can be exposed as a <see cref="Span"/> or <see cref="Memory"/> and serialized without copying element by element.
    /// Ownership is reference counted through <see cref="Rent(int)"/> and <see cref="Return"/>, and the buffer is
    /// released when the list is disposed or the last reference is returned.
    /// The list logic lives in <see cref="NativeList{T}"/>, this class adds shared ownership on top of it.
    /// </summary>
    /// <typeparam name="T">The unmanaged element type.</typeparam>
    public unsafe class PrimitiveList<T> : IDisposable, IReadOnlyList<T>
        where T : unmanaged
    {
        // Not readonly, mutations would run on a copy.
        private NativeList<T> _list;
        private bool _disposedValue;
        private readonly IMemoryAllocator _memoryAllocator;
        private int _rentCounter;

        /// <summary>
        /// Creates an empty list that allocates backing memory from the given allocator on demand.
        /// </summary>
        /// <param name="memoryAllocator">The allocator used for backing memory.</param>
        public PrimitiveList(IMemoryAllocator memoryAllocator)
        {
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
            _list.EnsureCapacity(initialCapacity, memoryAllocator);
        }

        /// <summary>
        /// Creates a list that wraps already populated memory, taking ownership of it.
        /// </summary>
        /// <param name="memory">The memory holding the elements.</param>
        /// <param name="length">The number of valid elements in <paramref name="memory"/>.</param>
        /// <param name="memoryAllocator">The allocator that produced the memory.</param>
        public PrimitiveList(FlowtideMemory memory, int length, IMemoryAllocator memoryAllocator)
        {
            _list = new NativeList<T>(memory, length);
            _memoryAllocator = memoryAllocator;
        }

        /// <summary>
        /// Creates a list that takes over an existing native list.
        /// </summary>
        /// <param name="list">The list to take ownership of, it must not be used again by the caller.</param>
        /// <param name="memoryAllocator">The allocator that produced the list.</param>
#pragma warning disable RS0042 // The list moves into this instance and is not used again.
        public PrimitiveList(NativeList<T> list, IMemoryAllocator memoryAllocator)
        {
            _list = list;
            _memoryAllocator = memoryAllocator;
        }
#pragma warning restore RS0042

        /// <summary>
        /// A span over the current elements of the list.
        /// </summary>
        public Span<T> Span => _list.Span;

        /// <summary>
        /// The full backing memory buffer in bytes, including any capacity beyond the current elements, or empty when nothing is allocated.
        /// </summary>
        public Memory<byte> Memory => _list.Memory;

        /// <summary>
        /// The current elements for consumers that need Memory.
        /// </summary>
        public Memory<byte> SlicedMemory => _list.SlicedMemory;

        /// <summary>
        /// The bytes of the current elements.
        /// </summary>
        public Span<byte> SlicedSpan => _list.SlicedSpan;

        /// <summary>
        /// UNSAFE: Gets the raw pointer to do operations without boundary checks
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal T* GetPointer_Unsafe()
        {
            return _list.GetPointer_Unsafe();
        }

        internal void EnsureCapacity(int length)
        {
            _list.EnsureCapacity(length, _memoryAllocator);
        }

        /// <summary>
        /// Appends a value to the end of the list, growing it by one.
        /// </summary>
        /// <param name="value">The value to append.</param>
        public void Add(T value)
        {
            _list.Add(value, _memoryAllocator);
        }

        /// <summary>
        /// Appends a run of elements copied from another list to the end of this one.
        /// </summary>
        /// <param name="list">The list to copy elements from.</param>
        /// <param name="index">The zero based index in <paramref name="list"/> to start copying from.</param>
        /// <param name="count">The number of elements to copy.</param>
        public void AddRangeFrom(PrimitiveList<T> list, int index, int count)
        {
            _list.AddRangeFrom(in list._list, index, count, _memoryAllocator);
        }

        /// <summary>
        /// Inserts a value at the given index, shifting every element at or after that index up by one.
        /// </summary>
        /// <param name="index">The zero based index to insert at.</param>
        /// <param name="value">The value to insert.</param>
        public void InsertAt(int index, T value)
        {
            _list.InsertAt(index, value, _memoryAllocator);
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
            _list.InsertRangeFrom(index, in other._list, start, count, _memoryAllocator);
        }

        /// <summary>
        /// Inserts the same value <paramref name="count"/> times at the given index, shifting existing elements up to make room.
        /// </summary>
        /// <param name="index">The zero based index to insert at.</param>
        /// <param name="value">The value to insert repeatedly.</param>
        /// <param name="count">The number of copies to insert.</param>
        public void InsertStaticRange(int index, T value, int count)
        {
            _list.InsertStaticRange(index, value, count, _memoryAllocator);
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
            _list.InsertFrom(in other._list, in sortedLookup, in insertPositions, in lookupNullIndex, _memoryAllocator);
        }

        /// <summary>
        /// Batch delete elements at the specified sorted indices.
        /// This is more efficient than calling RemoveAt repeatedly because it processes
        /// contiguous blocks of retained data in a single left-to-right sweep.
        /// </summary>
        /// <param name="targets">A span of sorted indices (ascending) of elements to delete.</param>
        public void DeleteBatch(ReadOnlySpan<int> targets)
        {
            _list.DeleteBatch(targets, _memoryAllocator);
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
            _list.InsertFrom(keys, sortedLookup, insertPositions, _memoryAllocator);
        }

        /// <summary>
        /// Opens a gap of <paramref name="count"/> elements at the given index by shifting the elements at or after it up,
        /// without writing any values into the gap. The list grows by <paramref name="count"/>.
        /// </summary>
        /// <param name="index">The zero based index at which to open the gap.</param>
        /// <param name="count">The number of element slots to open.</param>
        public void MoveAtIndex(int index, int count)
        {
            _list.MoveAtIndex(index, count, _memoryAllocator);
        }

        /// <summary>
        /// Removes the element at the given index and shifts all elements above it down.
        /// </summary>
        /// <param name="index">The zero based index of the element to remove.</param>
        public void RemoveAt(int index)
        {
            _list.RemoveAt(index, _memoryAllocator);
        }

        /// <summary>
        /// Removes a run of elements starting at the given index and shifts all elements above the range down.
        /// </summary>
        /// <param name="index">The zero based index of the first element to remove.</param>
        /// <param name="count">The number of elements to remove.</param>
        public void RemoveRange(int index, int count)
        {
            _list.RemoveRange(index, count, _memoryAllocator);
        }

        /// <summary>
        /// Gets the element at the given index.
        /// </summary>
        /// <param name="index">The zero based index.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public T Get(int index)
        {
            return _list.Get(index);
        }

        /// <summary>
        /// Gets a reference to the element at the given index, allowing it to be read or modified in place.
        /// </summary>
        /// <param name="index">The zero based index.</param>
        public ref T GetRef(scoped in int index)
        {
            return ref _list.GetRef(in index);
        }

        /// <summary>
        /// Overwrites the element at the given index.
        /// </summary>
        /// <param name="index">The zero based index.</param>
        /// <param name="value">The new value.</param>
        public void Update(in int index, in T value)
        {
            _list.Update(in index, in value);
        }

        /// <summary>
        /// Gets or sets the element at the given index.
        /// </summary>
        /// <param name="index">The zero based index.</param>
        public T this[int index]
        {
            get
            {
                return _list.Get(index);
            }
            set
            {
                _list.Update(index, value);
            }
        }

        /// <summary>
        /// The number of elements in the list.
        /// </summary>
        public int Count => _list.Count;

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                _list.Dispose(_memoryAllocator);
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
            for (var i = 0; i < Count; i++)
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
            _list.Clear();
        }

        /// <summary>
        /// Creates a deep copy of the list, allocating new backing memory from the given allocator.
        /// </summary>
        /// <param name="memoryAllocator">The allocator used for the copy's backing memory.</param>
        /// <returns>A new list with the same elements.</returns>
        public PrimitiveList<T> Copy(IMemoryAllocator memoryAllocator)
        {
            return new PrimitiveList<T>(_list.Copy(memoryAllocator), memoryAllocator);
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
            return _list.BinarySearch(value);
        }

        /// <summary>
        /// Sets the element count directly, without allocating or clearing memory. The caller must ensure the backing
        /// memory already holds that many valid elements.
        /// </summary>
        /// <param name="newLength">The new element count.</param>
        public void SetLength(int newLength)
        {
            _list.SetLength(newLength);
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
            _list.GetPrefixSumByteSizes(indices, sizes);
        }
    }
}
