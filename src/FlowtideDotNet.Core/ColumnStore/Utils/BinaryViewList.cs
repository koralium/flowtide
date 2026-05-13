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

using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace FlowtideDotNet.Core.ColumnStore.Utils
{
    /// <summary>
    /// A list that stores binary data following the Apache Arrow binary-view (BinaryView / StringView) specification.
    /// Values of 12 bytes or fewer are stored inline inside the 16-byte view struct.
    /// Larger values are stored in a single backing data buffer and referenced by (bufferIndex=0, offset, length).
    /// <para>
    /// Compared to <see cref="BinaryList"/> this trades offset-array maintenance for a compaction step when
    /// fragmentation grows too large; all other public operations mirror <see cref="BinaryList"/> exactly.
    /// </para>
    /// </summary>
    internal unsafe sealed class BinaryViewList : IDisposable
    {
        [StructLayout(LayoutKind.Explicit, Size = 16)]
        internal struct ArrowBinaryView
        {
            /// <summary>Byte length of the value.</summary>
            [FieldOffset(0)]
            public int Length;

            // ---- inline path (Length <= 12) ----
            [FieldOffset(4)]
            public fixed byte InlineData[12];

            // ---- out-of-line path (Length > 12) ----
            [FieldOffset(4)]
            public fixed byte PrefixBytes[4];   // first 4 bytes of value for fast prefix comparison

            [FieldOffset(4)]
            public uint PrefixInt;              // same 4 bytes as a uint for comparison

            [FieldOffset(8)]
            public int BufferIndex;             // always 0 in this implementation (single buffer)

            [FieldOffset(12)]
            public int Offset;                  // byte offset into the backing data buffer

            public readonly bool IsInline => Length <= 12;
        }

        // View-slot array (one ArrowBinaryView per logical element)
        private PrimitiveList<ArrowBinaryView> _views;

        // Backing data buffer for out-of-line values
        private void* _data;
        private int _dataCapacity;          // allocated byte capacity
        private IMemoryOwner<byte>? _memoryOwner;

        // Append pointer – next byte to write in the data buffer
        private int _insertPointer;

        // Fragmentation tracking – bytes consumed by logically removed out-of-line slots
        private int _deletedDataSize;

        private bool _disposedValue;
        private readonly IMemoryAllocator _memoryAllocator;

        internal int InsertPointer => _insertPointer;
        internal int DeletedDataSize => _deletedDataSize;

        public BinaryViewList(IMemoryAllocator memoryAllocator)
        {
            _views = new PrimitiveList<ArrowBinaryView>(memoryAllocator);
            _memoryAllocator = memoryAllocator;
        }

        public BinaryViewList(IMemoryAllocator memoryAllocator, int initialRowCapacity, int initialDataCapacity)
        {
            _memoryAllocator = memoryAllocator;
            _views = new PrimitiveList<ArrowBinaryView>(memoryAllocator, initialRowCapacity);
            if (initialDataCapacity > 0)
            {
                _memoryOwner = _memoryAllocator.Allocate(initialDataCapacity, 64);
                _data = _memoryOwner.Memory.Pin().Pointer;
                _dataCapacity = _memoryOwner.Memory.Length;
            }
        }

        /// <summary>
        /// Creates a <see cref="BinaryViewList"/> from pre-existing memory.
        /// Takes ownership of both memory owners; they will be disposed when this list is disposed.
        /// </summary>
        /// <param name="viewMemory">Memory containing the serialized <see cref="ArrowBinaryView"/> array.</param>
        /// <param name="viewCount">Number of logical elements (views) in <paramref name="viewMemory"/>.</param>
        /// <param name="dataMemory">Memory containing the backing data for out-of-line values, or null if all values are inline.</param>
        /// <param name="memoryAllocator">Allocator to use for future mutations.</param>
        public BinaryViewList(
            IMemoryOwner<byte> viewMemory, 
            int viewCount, 
            IMemoryOwner<byte>? dataMemory, 
            IMemoryAllocator memoryAllocator,
            int deletedSize,
            int insertPointer = -1)
        {
            _memoryAllocator = memoryAllocator;
            _views = new PrimitiveList<ArrowBinaryView>(viewMemory, viewCount, memoryAllocator);

            _insertPointer = insertPointer;
            _deletedDataSize = deletedSize;
            if (dataMemory != null && _insertPointer == -1)
            {
                _memoryOwner = dataMemory;
                _data = dataMemory.Memory.Pin().Pointer;
                _dataCapacity = dataMemory.Memory.Length;

                // Calculate _insertPointer as the end of the last out-of-line value.
                int maxEnd = 0;
                for (int i = 0; i < viewCount; i++)
                {
                    var view = _views[i];
                    if (!view.IsInline)
                    {
                        int end = view.Offset + view.Length;
                        if (end > maxEnd)
                            maxEnd = end;
                    }
                }
                _insertPointer = maxEnd;
            }
        }

        public int Count => _views.Count;

        public ReadOnlyMemory<byte> ViewsMemory => _views.SlicedMemory;

        public ReadOnlyMemory<byte> DataMemory => _memoryOwner == null ? ReadOnlyMemory<byte>.Empty : _memoryOwner.Memory.Slice(0, _insertPointer);

        private Span<byte> DataSpan => new Span<byte>(_data, _dataCapacity);

        /// <summary>
        /// Ensures the backing data buffer has room for at least <paramref name="requiredBytes"/> bytes total
        /// (where <paramref name="requiredBytes"/> is the new value of <see cref="_insertPointer"/> after the
        /// caller appends its data).
        /// <para>
        /// When a realloc would be needed and there is fragmentation (<see cref="_deletedDataSize"/> &gt; 0),
        /// we allocate a new buffer sized to <c>liveBytes + extra</c> (not <c>insertPointer + extra</c>) and
        /// compact-copy only the live bytes into it. This avoids copying dead bytes that a plain Realloc would
        /// drag along, and eliminates fragmentation for free.
        /// </para>
        /// </summary>
        private void EnsureDataCapacity(int requiredBytes)
        {
            if (_dataCapacity >= requiredBytes)
                return;

            if (_memoryOwner == null)
            {
                // First allocation – no existing data to worry about.
                int newCapacity = Math.Max(requiredBytes * 2, 64);
                _memoryOwner = _memoryAllocator.Allocate(newCapacity, 64);
                _data = _memoryOwner.Memory.Pin().Pointer;
                _dataCapacity = _memoryOwner.Memory.Length;
                return;
            }

            if (_deletedDataSize > 0)
            {
                // A Realloc would memcpy the full _insertPointer bytes (including holes).
                // Instead, allocate a buffer sized to just the live bytes + the extra we need,
                // and compact-copy only live data into it — same number of writes, smaller result.
                int liveBytes = _insertPointer - _deletedDataSize;
                int extraNeeded = requiredBytes - _insertPointer;   // bytes the caller still needs to append
                int newCapacity = Math.Max((liveBytes + extraNeeded) * 2, 64);

                var newMemoryOwner = _memoryAllocator.Allocate(newCapacity, 64);
                var newDataPtr = (byte*)newMemoryOwner.Memory.Pin().Pointer;
                int newInsertPointer = 0;

                for (int i = 0; i < _views.Count; i++)
                {
                    var view = _views[i];
                    if (view.IsInline)
                        continue;

                    new ReadOnlySpan<byte>((byte*)_data + view.Offset, view.Length)
                        .CopyTo(new Span<byte>(newDataPtr + newInsertPointer, view.Length));
                    view.Offset = newInsertPointer;
                    newInsertPointer += view.Length;
                    _views[i] = view;
                }

                _memoryOwner.Dispose();
                _memoryOwner = newMemoryOwner;
                _data = newDataPtr;
                _dataCapacity = newMemoryOwner.Memory.Length;
                _insertPointer = newInsertPointer;
                _deletedDataSize = 0;
            }
            else
            {
                // No fragmentation — a plain Realloc is optimal (may grow in-place).
                int newCapacity = Math.Max(requiredBytes * 2, 64);
                _memoryOwner = _memoryAllocator.Realloc(_memoryOwner, newCapacity, 64);
                _data = _memoryOwner.Memory.Pin().Pointer;
                _dataCapacity = _memoryOwner.Memory.Length;
            }
        }

        /// <summary>
        /// Shrinks the backing data buffer when it is very large relative to used data.
        /// Compact() already allocates a fresh right-sized buffer, so no further Realloc is needed.
        /// </summary>
        private void CheckDataSizeReduction()
        {
            int used = _insertPointer - _deletedDataSize;
            var multiplied = (used << 1) + (used >> 1); // used * 1.5
            if (multiplied < _dataCapacity && _dataCapacity > 256 && _memoryOwner != null)
            {
                Compact();
            }
        }

        /// <summary>
        /// Builds an out-of-line view slot: copies <paramref name="data"/> into the backing buffer and
        /// returns the populated <see cref="ArrowBinaryView"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ArrowBinaryView WriteOutOfLine(ReadOnlySpan<byte> data)
        {
            Debug.Assert(data.Length > 12);

            EnsureDataCapacity(_insertPointer + data.Length);
            int offset = _insertPointer;
            data.CopyTo(DataSpan.Slice(offset));

            var view = new ArrowBinaryView
            {
                Length = data.Length,
                BufferIndex = 0,
                Offset = offset
            };
            // Copy the first 4 bytes into the prefix for fast comparison
            for (int i = 0; i < 4; i++)
                view.PrefixBytes[i] = data[i];

            _insertPointer += data.Length;
            return view;
        }

        /// <summary>
        /// Builds an inline view slot (≤ 12 bytes).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ArrowBinaryView WriteInline(ReadOnlySpan<byte> data)
        {
            Debug.Assert(data.Length <= 12);
            var view = new ArrowBinaryView { Length = data.Length };
            for (int i = 0; i < data.Length; i++)
                view.InlineData[i] = data[i];
            return view;
        }

        /// <summary>
        /// Returns the span for the value stored at position <paramref name="view"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private Span<byte> GetSpanForViewRef(ref ArrowBinaryView view)
        {
            if (view.IsInline)
            {
                // Inline bytes start at byte-offset 4 inside the 16-byte struct (after the int Length field).
                return new Span<byte>((byte*)Unsafe.AsPointer(ref view) + 4, view.Length);
            }
            return DataSpan.Slice(view.Offset, view.Length);
        }

        /// <summary>
        /// Returns the value at the given index as a ReadOnlyMemory.
        /// Returns a slice referencing the exact memory region (either inside the views array or the data buffer).
        /// </summary>
        public ReadOnlyMemory<byte> GetMemory(in int index)
        {
            var view = _views.Get(index);
            if (view.IsInline)
            {
                // Return a memory reference directly into the inline struct array
                // The InlineData array starts at byte offset 4 inside the 16-byte ArrowBinaryView struct.
                return _views.Memory.Slice(index * 16 + 4, view.Length);
            }
            if (_memoryOwner == null)
            {
                return ReadOnlyMemory<byte>.Empty;
            }
            return _memoryOwner.Memory.Slice(view.Offset, view.Length);
        }

        /// <summary>Appends a value to the end of the list.</summary>
        public void Add(ReadOnlySpan<byte> data)
        {
            _views.Add(data.Length <= 12 ? WriteInline(data) : WriteOutOfLine(data));
        }

        /// <summary>Appends an empty (zero-length) value.</summary>
        public void AddEmpty()
        {
            _views.Add(new ArrowBinaryView { Length = 0 });
        }

        /// <summary>Inserts a value at the specified index.</summary>
        public void Insert(int index, ReadOnlySpan<byte> data)
        {
            if ((uint)index > (uint)_views.Count)
                throw new ArgumentOutOfRangeException(nameof(index));

            _views.InsertAt(index, data.Length <= 12 ? WriteInline(data) : WriteOutOfLine(data));
        }

        /// <summary>Inserts an empty (zero-length) value at the specified index.</summary>
        public void InsertEmpty(in int index)
        {
            if ((uint)index > (uint)_views.Count)
                throw new ArgumentOutOfRangeException(nameof(index));
            _views.InsertAt(index, new ArrowBinaryView { Length = 0 });
        }

        /// <summary>Replaces the value at the specified index.</summary>
        public void UpdateAt(int index, ReadOnlySpan<byte> data)
        {
            if ((uint)index >= (uint)_views.Count)
                throw new ArgumentOutOfRangeException(nameof(index));

            var old = _views[index];
            if (!old.IsInline)
                _deletedDataSize += old.Length;

            _views[index] = data.Length <= 12 ? WriteInline(data) : WriteOutOfLine(data);
        }

        /// <summary>Removes the value at the specified index.</summary>
        public void RemoveAt(int index)
        {
            if ((uint)index >= (uint)_views.Count)
                throw new ArgumentOutOfRangeException(nameof(index));

            var view = _views[index];
            if (!view.IsInline)
                _deletedDataSize += view.Length;

            _views.RemoveAt(index);
            CheckDataSizeReduction();
        }

        /// <summary>Removes a contiguous range of values starting at <paramref name="index"/>.</summary>
        public void RemoveRange(int index, int count)
        {
            if (count == 0) return;
            if ((uint)index >= (uint)_views.Count || index + count > _views.Count)
                throw new ArgumentOutOfRangeException(nameof(index));

            for (int i = index; i < index + count; i++)
            {
                var v = _views[i];
                if (!v.IsInline)
                    _deletedDataSize += v.Length;
            }
            _views.RemoveRange(index, count);
            CheckDataSizeReduction();
        }

        /// <summary>Returns the value at the specified index as a <see cref="Span{Byte}"/>.</summary>
        public Span<byte> Get(in int index)
        {
            ref var view = ref _views.GetRef(index);
            return GetSpanForViewRef(ref view);
        }

        /// <summary>
        /// Returns raw pointer information for the value at the specified index.
        /// The returned pointer is only valid until the next mutation that triggers a realloc.
        /// </summary>
        public BinaryInfo GetBinaryInfo(in int index)
        {
            ref var view = ref _views.GetRef(index);
            if (view.IsInline)
            {
                var ptr = (byte*)Unsafe.AsPointer(ref view) + 4;
                return new BinaryInfo(ptr, view.Length);
            }
            return new BinaryInfo((byte*)_data + view.Offset, view.Length);
        }

        /// <summary>Returns byte size consumed by elements in the range [start, end] (inclusive), including per-element overhead.</summary>
        public int GetByteSize(int start, int end)
        {
            int total = 0;
            for (int i = start; i <= end; i++)
                total += _views[i].Length + sizeof(int);   // match BinaryList: data bytes + 4-byte offset entry
            return total;
        }

        public int GetByteSize()
        {
            return _insertPointer - _deletedDataSize + _views.Count * sizeof(int);
        }

        /// <summary>
        /// Accumulates per-element byte sizes into <paramref name="sizes"/> (prefix-sum style, adding to existing values).
        /// </summary>
        public void GetPrefixSumByteSizes(ReadOnlySpan<int> indices, Span<int> sizes)
        {
            int length = indices.Length;
            ref int indicesHead = ref MemoryMarshal.GetReference(indices);
            ref int sizesHead = ref MemoryMarshal.GetReference(sizes);

            int sum = 0;
            for (int i = 0; i < length; i++)
            {
                int idx = Unsafe.Add(ref indicesHead, i);
                sum += _views[idx].Length + sizeof(int);
                Unsafe.Add(ref sizesHead, i) += sum;
            }
        }

        /// <summary>
        /// Copies a contiguous range from another <see cref="BinaryViewList"/> and inserts it at <paramref name="index"/>.
        /// Uses a single bulk shift of the view array instead of per-element InsertAt.
        /// </summary>
        public void InsertRangeFrom(int index, BinaryViewList other, int start, int count)
        {
            if (count == 0) return;

            // Pre-calculate total out-of-line bytes so we can do a single EnsureDataCapacity call.
            // This avoids compaction mid-loop which would invalidate offsets for already-written data.
            int totalOutOfLineBytes = 0;
            for (int i = 0; i < count; i++)
            {
                var src = other._views[start + i];
                if (!src.IsInline)
                    totalOutOfLineBytes += src.Length;
            }
            if (totalOutOfLineBytes > 0)
                EnsureDataCapacity(_insertPointer + totalOutOfLineBytes);

            // Open a gap in the view array with a single bulk shift.
            _views.MoveAtIndex(index, count);

            // Write each new view directly into the gap.
            for (int i = 0; i < count; i++)
            {
                var src = other._views[start + i];
                if (src.IsInline)
                {
                    _views[index + i] = src;
                }
                else
                {
                    var srcSpan = new ReadOnlySpan<byte>((byte*)other._data + src.Offset, src.Length);
                    _views[index + i] = WriteOutOfLine(srcSpan);
                }
            }
        }

        /// <summary>Inserts <paramref name="count"/> empty (zero-length) values at <paramref name="index"/>.</summary>
        public void InsertNullRange(int index, int count)
        {
            var empty = new ArrowBinaryView { Length = 0 };
            _views.InsertStaticRange(index, empty, count);
        }

        /// <summary>
        /// Batch-inserts selected elements from <paramref name="other"/> at specific positions in this list.
        /// Mirrors <see cref="BinaryList.InsertFrom"/>.
        /// </summary>
        public void InsertFrom(
            ref readonly BinaryViewList other,
            ref readonly ReadOnlySpan<int> sortedLookup,
            ref readonly ReadOnlySpan<int> insertPositions,
            in int lookupNullIndex)
        {
            Debug.Assert(sortedLookup.Length == insertPositions.Length);
            int otherCount = sortedLookup.Length;
            if (otherCount == 0) return;

            // Pre-calculate total out-of-line bytes so we can do a single EnsureDataCapacity call.
            // This prevents compaction mid-loop from disposing data written by earlier iterations.
            int totalOutOfLineBytes = 0;
            for (int i = 0; i < otherCount; i++)
            {
                int oIdx = sortedLookup[i];
                if (oIdx != lookupNullIndex)
                {
                    var src = other._views[oIdx];
                    if (!src.IsInline)
                        totalOutOfLineBytes += src.Length;
                }
            }
            if (totalOutOfLineBytes > 0)
                EnsureDataCapacity(_insertPointer + totalOutOfLineBytes);

            int oldCount = _views.Count;

            // Extend the views array to its final size.
            // MoveAtIndex at position oldCount shifts zero elements — it just grows capacity and length.
            _views.MoveAtIndex(oldCount, otherCount);

            // Right-to-left sweep directly on the views span — zero intermediate allocation.
            // This is the same algorithm as PrimitiveList.InsertFrom, inlined here so we can
            // build each view on-the-fly instead of pre-building an array.
            var viewSpan = _views.Span;
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

                    viewSpan.Slice(currentReadIdx, elementsToMove)
                            .CopyTo(viewSpan.Slice(currentWriteIdx, elementsToMove));
                }

                // Build and place the new view directly
                int oIdx = sortedLookup[i];
                currentWriteIdx--;

                if (oIdx == lookupNullIndex)
                {
                    viewSpan[currentWriteIdx] = default; // Length = 0
                }
                else
                {
                    var src = other._views[oIdx];
                    if (src.IsInline)
                    {
                        viewSpan[currentWriteIdx] = src;
                    }
                    else
                    {
                        var srcSpan = new ReadOnlySpan<byte>((byte*)other._data + src.Offset, src.Length);
                        viewSpan[currentWriteIdx] = WriteOutOfLine(srcSpan);
                    }
                }

                currentReadIdx = targetInsertIdx;
            }
        }

        /// <summary>
        /// Batch-deletes elements at the sorted ascending indices in <paramref name="targets"/>.
        /// More efficient than repeated <see cref="RemoveAt"/> calls.
        /// </summary>
        public void DeleteBatch(ReadOnlySpan<int> targets)
        {
            int deleteCount = targets.Length;
            if (deleteCount == 0) return;

            Debug.Assert(deleteCount <= Count);

            // Accumulate deleted data size before delegating to PrimitiveList
            for (int i = 0; i < deleteCount; i++)
            {
                var v = _views[targets[i]];
                if (!v.IsInline)
                    _deletedDataSize += v.Length;
            }

            _views.DeleteBatch(targets);
            CheckDataSizeReduction();
        }

        /// <summary>Creates a deep copy of this list using the provided allocator.
        /// Does not mutate the source list.</summary>
        public BinaryViewList Copy(IMemoryAllocator memoryAllocator)
        {
            // Calculate the live byte count so the copy's data buffer is compacted.
            int liveBytes = _insertPointer - _deletedDataSize;
            var copy = new BinaryViewList(memoryAllocator, _views.Count, Math.Max(liveBytes, 0));

            // Compact-copy out-of-line data into the copy's buffer without mutating this list.
            if (liveBytes > 0)
            {
                int newInsertPointer = 0;
                for (int i = 0; i < _views.Count; i++)
                {
                    var view = _views[i];
                    if (view.IsInline)
                    {
                        copy._views.Add(view);
                        continue;
                    }

                    new ReadOnlySpan<byte>((byte*)_data + view.Offset, view.Length)
                        .CopyTo(new Span<byte>((byte*)copy._data + newInsertPointer, view.Length));
                    view.Offset = newInsertPointer;
                    newInsertPointer += view.Length;
                    copy._views.Add(view);
                }
                copy._insertPointer = newInsertPointer;
            }
            else
            {
                // All data is inline or list is empty.
                for (int i = 0; i < _views.Count; i++)
                    copy._views.Add(_views[i]);
            }

            return copy;
        }

        /// <summary>Removes all elements.</summary>
        public void Clear()
        {
            // Mark all out-of-line data as deleted (or just reset the pointer directly)
            _views.Clear();
            _insertPointer = 0;
            _deletedDataSize = 0;
        }

        /// <summary>
        /// Compacts the backing data buffer by eliminating gaps left by removed/updated values.
        /// After compaction all out-of-line view offsets are updated to point into the new dense buffer.
        /// </summary>
        public void Compact()
        {
            if (_deletedDataSize == 0 || _views.Count == 0)
                return;

            int usedBytes = _insertPointer - _deletedDataSize;
            if (usedBytes <= 0)
            {
                _insertPointer = 0;
                _deletedDataSize = 0;
                return;
            }

            // Allocate a fresh buffer for the compacted data.
            var newMemoryOwner = _memoryAllocator.Allocate(usedBytes, 64);
            var newDataPtr = (byte*)newMemoryOwner.Memory.Pin().Pointer;
            int newInsertPointer = 0;

            for (int i = 0; i < _views.Count; i++)
            {
                var view = _views[i];
                if (view.IsInline)
                    continue;

                // Copy the out-of-line bytes and rewrite the offset.
                var src = new ReadOnlySpan<byte>((byte*)_data + view.Offset, view.Length);
                src.CopyTo(new Span<byte>(newDataPtr + newInsertPointer, view.Length));
                view.Offset = newInsertPointer;
                newInsertPointer += view.Length;
                _views[i] = view;
            }

            _memoryOwner?.Dispose();
            _memoryOwner = newMemoryOwner;
            _data = newDataPtr;
            _dataCapacity = newMemoryOwner.Memory.Length;
            _insertPointer = newInsertPointer;
            _deletedDataSize = 0;
        }

        private void DisposeCore(bool disposing)
        {
            if (!_disposedValue)
            {
                if (disposing)
                {
                    _views.Dispose();
                }
                if (_memoryOwner != null)
                {
                    _memoryOwner.Dispose();
                    _memoryOwner = null;
                    _data = null;
                }
                _disposedValue = true;
            }
        }

        ~BinaryViewList()
        {
            DisposeCore(disposing: false);
        }

        public void Dispose()
        {
            DisposeCore(disposing: true);
            GC.SuppressFinalize(this);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void* GetViewsPointer_Unsafe()
        {
            return _views.GetPointer_Unsafe();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void* GetDataPointer_Unsafe()
        {
            return _data;
        }
    }
}
