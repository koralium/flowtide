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
using System.Runtime.InteropServices;

namespace FlowtideDotNet.Core.ColumnStore.Utils
{

    internal unsafe struct BinaryInfo
    {
        public readonly byte* data;
        public readonly int length;

        public Span<byte> Span => new Span<byte>(data, length);

        public BinaryInfo(byte* data, int length)
        {
            this.data = data;
            this.length = length;
        }
    }

    /// <summary>
    /// Helper list that stores binary data and their offsets.
    /// This follows apache arrow on how to store binary data.
    /// This means that it does not store references to the binary data, but instead stores them directly in the array.
    /// This list allows inserting data and removing data where it correctly recalculates offsets.
    /// Mutable struct: hold it in exactly one field, mutate only through that field or a ref local,
    /// and never copy it — a realloc in a copy frees the memory the original still points at.
    /// Memory views over the data are minted by the owning column, which is the MemoryManager.
    /// The struct does not store an allocator; every call that can allocate or free takes the owner's
    /// allocator, and all calls for a given list must use the same one.
    /// </summary>
    [NonCopyable]
    internal unsafe struct BinaryList
    {
        // Memory objects
        private FlowtideMemory _memory;

        // List specific members
        private IntList _offsets;

        // Used bytes in the data buffer; the arrow layout keeps this equal to the last offset,
        // so it is derived instead of mirrored. Zero when unallocated or disposed.
        private readonly int DataByteLength => _offsets.Count == 0 ? 0 : _offsets.Get(_offsets.Count - 1);

        public readonly Memory<byte> OffsetMemory => _offsets.Memory;

        public readonly Span<byte> OffsetSpan => _offsets.SlicedSpan;

        public readonly Span<byte> DataSpan => new Span<byte>(_memory.Pointer, DataByteLength);

        /// <summary>
        /// The full data buffer including unused capacity, used by the owning column's GetSpan.
        /// </summary>
        internal readonly Span<byte> CapacitySpan => _memory.Span;

        public readonly int Count => _offsets.Count - 1;

        private readonly Span<byte> AccessSpan => _memory.Span;

        public BinaryList(IMemoryAllocator memoryAllocator)
        {
            _offsets.Add(0, memoryAllocator);
        }

        public BinaryList(IMemoryAllocator memoryAllocator, int initialRowCapacity, int initialDataCapacity)
        {
            _offsets = new IntList(initialRowCapacity + 1, memoryAllocator);
            _offsets.Add(0, memoryAllocator);
            _memory = memoryAllocator.AllocateMemory(initialDataCapacity);
        }

        /// <summary>
        /// Create a binary list from existing memory.
        /// If any changes are made to the list that exceeds the current memory, a new memory block will be allocated that is used only for the list.
        /// </summary>
        /// <param name="offsetMemory"></param>
        /// <param name="offsetLength"></param>
        /// <param name="dataMemory"></param>
        /// <param name="memoryAllocator"></param>
        public BinaryList(FlowtideMemory offsetMemory, int offsetLength, FlowtideMemory dataMemory)
        {
            _offsets = new IntList(offsetMemory, offsetLength);
            _memory = dataMemory;
        }

        public readonly void* GetDataPointer_Unsafe()
        {
            return _memory.Pointer;
        }

        public readonly void* GetOffsetPointer_Unsafe()
        {
            return _offsets.GetPointer_Unsafe();
        }

        /// <summary>
        /// Gets the data buffer position of an element, for the owning column to mint a Memory view from.
        /// </summary>
        internal readonly (int offset, int length) GetOffsetAndLength(in int index)
        {
            var offset = _offsets.Get(index);
            return (offset, _offsets.Get(index + 1) - offset);
        }

        private void EnsureCapacity(int length, IMemoryAllocator memoryAllocator)
        {
            if (_memory.Length < length)
            {
                var allocLength = length * 2;
                if (allocLength < 64)
                {
                    allocLength = 64;
                }
                memoryAllocator.Realloc(ref _memory, allocLength);
            }
        }

        private void CheckSizeReduction(IMemoryAllocator memoryAllocator)
        {
            var length = DataByteLength;
            var multipleid = (length << 1) + (length >> 1);
            if (multipleid < _memory.Length && _memory.Length > 256)
            {
                Debug.Assert(!_memory.IsNull);
                memoryAllocator.Realloc(ref _memory, length);
            }
        }

        /// <summary>
        /// Add binary data as an element to the list.
        /// </summary>
        /// <param name="data"></param>
        public void Add(ReadOnlySpan<byte> data, IMemoryAllocator memoryAllocator)
        {
            var totalBytes = DataByteLength;
            EnsureCapacity(totalBytes + data.Length, memoryAllocator);
            data.CopyTo(AccessSpan.Slice(totalBytes));
            _offsets.Add(totalBytes + data.Length, memoryAllocator);
        }

        public void AddEmpty(IMemoryAllocator memoryAllocator)
        {
            _offsets.Add(DataByteLength, memoryAllocator);
        }

        public void UpdateAt(int index, ReadOnlySpan<byte> data, IMemoryAllocator memoryAllocator)
        {
            var offset = _offsets.Get(index);
            var endOffset = _offsets.Get(index + 1);
            var length = endOffset - offset;
            if (length == data.Length)
            {
                data.CopyTo(AccessSpan.Slice(offset));
            }
            else
            {
                var difference = data.Length - length;
                var totalBytes = DataByteLength;
                EnsureCapacity(totalBytes + difference, memoryAllocator);
                var span = AccessSpan;
                span.Slice(offset + length, totalBytes - offset - length).CopyTo(span.Slice(offset + data.Length));
                data.CopyTo(span.Slice(offset));
                _offsets.Update(index + 1, offset + data.Length, difference);
            }
        }

        /// <summary>
        /// Insert binary data at a specfic index.
        /// If this is not inserted at the end, a copy will be done of all elements larger than this index.
        /// </summary>
        /// <param name="index"></param>
        /// <param name="data"></param>
        public void Insert(int index, ReadOnlySpan<byte> data, IMemoryAllocator memoryAllocator)
        {
            if (index == Count)
            {
                Add(data, memoryAllocator);
                return;
            }

            var totalBytes = DataByteLength;
            EnsureCapacity(totalBytes + data.Length, memoryAllocator);
            // Get the offset of the current element at the location
            var offset = _offsets.Get(index);

            // Take out the length that all bytes must be moved
            var toMove = data.Length;
            var span = AccessSpan;

            // Move all elements after the index
            span.Slice(offset, totalBytes - offset).CopyTo(span.Slice(offset + toMove));

            // Insert data of the new element
            data.CopyTo(span.Slice(offset));

            // Add the offset and add the size to all offsets above this one.
            _offsets.InsertAt(index, offset, toMove, memoryAllocator);
        }

        public void InsertEmpty(in int index, IMemoryAllocator memoryAllocator)
        {
            if (index == Count)
            {
                AddEmpty(memoryAllocator);
                return;
            }

            var offset = _offsets.Get(index);
            _offsets.InsertAt(index, offset, memoryAllocator);
        }

        public void RemoveAt(int index, IMemoryAllocator memoryAllocator)
        {
            // Check if are removing the last element
            if (index == _offsets.Count - 1)
            {
                _offsets.RemoveAt(index, memoryAllocator);
                CheckSizeReduction(memoryAllocator);
                return;
            }
            else
            {
                var totalBytes = DataByteLength;
                var offset = _offsets.Get(index);
                var length = _offsets.Get(index + 1) - offset;
                // Remove the offset and negate the length of all elements above this index.
                _offsets.RemoveAt(index, -length, memoryAllocator);

                var span = AccessSpan;

                // Move all elements after the index
                span.Slice(offset + length, totalBytes - offset - length).CopyTo(span.Slice(offset));
                CheckSizeReduction(memoryAllocator);
            }
        }

        public void RemoveRange(int index, int count, IMemoryAllocator memoryAllocator)
        {
            var totalBytes = DataByteLength;
            var offset = _offsets.Get(index);
            var length = _offsets.Get(index + count) - offset;

            _offsets.RemoveRange(index, count, -length, memoryAllocator);

            var span = AccessSpan;

            // Move all elements after the index
            span.Slice(offset + length, totalBytes - offset - length).CopyTo(span.Slice(offset));
            CheckSizeReduction(memoryAllocator);
        }

        public readonly Span<byte> Get(in int index)
        {
            var offset = _offsets.Get(index);
            return AccessSpan.Slice(offset, _offsets.Get(index + 1) - offset);
        }

        /// <summary>
        /// Returns the underlying information, the raw array and index and offset.
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public readonly BinaryInfo GetBinaryInfo(in int index)
        {
            var offset = _offsets.Get(index);
            return new BinaryInfo(((byte*)_memory.Pointer) + offset, _offsets.Get(index + 1) - offset);
        }

        // No IDisposable: a using-declared struct variable is read-only and mutating calls on it
        // would run on defensive copies. Free zeroes _memory, so double dispose is a no-op.
        public void Dispose(IMemoryAllocator memoryAllocator)
        {
            _offsets.Dispose(memoryAllocator);
            if (!_memory.IsNull)
            {
                memoryAllocator.Free(ref _memory);
            }
        }

        public void Clear(IMemoryAllocator memoryAllocator)
        {
            _offsets.Clear();
            _offsets.Add(0, memoryAllocator);
        }

        public readonly int GetByteSize(int start, int end)
        {
            var startOffset = _offsets.Get(start);
            var endOffset = _offsets.Get(end + 1);
            return endOffset - startOffset + ((end - start + 1) * sizeof(int));
        }

        /// <summary>
        /// Fetches sizes for each index and adds them to the sizes span. 
        /// This is used to calculate the size of a batch of elements in bytes.
        /// It adds instead of replaces so it can check multiple columns
        /// </summary>
        /// <param name="indices"></param>
        /// <param name="sizes"></param>
        public readonly void GetPrefixSumByteSizes(ReadOnlySpan<int> indices, Span<int> sizes)
        {
            int length = indices.Length;

            ref int indicesHead = ref MemoryMarshal.GetReference(indices);
            ref int sizesHead = ref MemoryMarshal.GetReference(sizes);
            int sum = 0;
            for (int i = 0; i < length; i++)
            {
                int idx = Unsafe.Add(ref indicesHead, i);

                int startOffset = _offsets.Get(idx);
                int endOffset = _offsets.Get(idx + 1);
                sum += endOffset - startOffset + sizeof(int);
                Unsafe.Add(ref sizesHead, i) += sum;
            }
        }

        public void InsertRangeFrom(int index, in BinaryList binaryList, int start, int count, IMemoryAllocator memoryAllocator)
        {
            var totalBytes = DataByteLength;
            var offsetToInsertAt = _offsets.Get(index);
            var offsetToCopyStart = binaryList._offsets.Get(start);
            var offsetToCopyEnd = binaryList._offsets.Get(start + count);
            var toCopyLength = offsetToCopyEnd - offsetToCopyStart;
            EnsureCapacity(totalBytes + toCopyLength, memoryAllocator);
            var span = AccessSpan;
            // Move all data up to free space for the insert
            span.Slice(offsetToInsertAt, totalBytes - offsetToInsertAt).CopyTo(span.Slice(offsetToInsertAt + toCopyLength));
            // Copy the data
            binaryList.AccessSpan.Slice(offsetToCopyStart, toCopyLength).CopyTo(span.Slice(offsetToInsertAt));
            var offsetDifference = offsetToInsertAt - offsetToCopyStart;
            _offsets.InsertRangeFrom(index, binaryList._offsets, start, count, toCopyLength, offsetDifference, memoryAllocator);
        }

        public void InsertNullRange(int index, int count, IMemoryAllocator memoryAllocator)
        {
            var offsetToInsertAt = _offsets.Get(index);
            _offsets.InsertRangeStaticValue(index, count, offsetToInsertAt, memoryAllocator);
        }

        public readonly BinaryList Copy(IMemoryAllocator memoryAllocator)
        {
            var dataSpan = DataSpan;
            var dataMemoryCopy = memoryAllocator.AllocateMemory(dataSpan.Length);
            dataSpan.CopyTo(dataMemoryCopy.Span);
            var offsetSpan = OffsetSpan;
            var offsetMemoryCopy = memoryAllocator.AllocateMemory(offsetSpan.Length);
            offsetSpan.CopyTo(offsetMemoryCopy.Span);

            return new BinaryList(offsetMemoryCopy, _offsets.Count, dataMemoryCopy);
        }

        /// <summary>
        /// Special case insert that allows inserting a range of data from another binary list at specific positions.
        /// This is used when merging two lists together more memory efficiently than inserting each element one by one.
        /// </summary>
        /// <param name="other">The other binary list to insert data from.</param>
        /// <param name="sortedLookup">A span containing the sorted indices of the elements to insert from the other list.</param>
        /// <param name="insertPositions">A span containing the positions at which to insert the elements in the current list.</param>
        public void InsertFrom(ref readonly BinaryList other, ref readonly ReadOnlySpan<int> sortedLookup, ref readonly ReadOnlySpan<int> insertPositions, in int lookupNullIndex, IMemoryAllocator memoryAllocator)
        {
            Debug.Assert(sortedLookup.Length == insertPositions.Length);
            int otherCount = sortedLookup.Length;
            if (otherCount == 0) return;

            // Calculate total bytes only for the elements being inserted
            int totalNewBytes = 0;
            for (int i = 0; i < otherCount; i++)
            {
                int oIdx = sortedLookup[i];
                if (oIdx != lookupNullIndex)
                {
                    totalNewBytes += other._offsets.Get(oIdx + 1) - other._offsets.Get(oIdx);
                }
            }


            int oldDataCount = Count;
            int oldTotalBytes = DataByteLength;

            // Ensure we have enough binary data capacity for the new total size after insertion
            this.EnsureCapacity(oldTotalBytes + totalNewBytes, memoryAllocator);

            // Ensure the offsets have enough capacity for the new total count after insertion (old count + new count)
            this._offsets.EnsureCapacity(oldDataCount + otherCount + 1, memoryAllocator);
            this._offsets.IncreaseLength(otherCount);

            // Fetch out spans to not have to access properties multiple times in the loop
            var selfData = this.AccessSpan;
            var otherData = other.AccessSpan;
            var selfOffsets = this._offsets.AccessSpan;

            int currentReadRow = oldDataCount;
            int currentWriteDataPtr = oldTotalBytes + totalNewBytes;
            int currentReadDataPtr = oldTotalBytes;
            int runningByteDelta = totalNewBytes;

            for (int i = otherCount - 1; i >= 0; i--)
            {
                int targetInsertIdx = insertPositions[i];
                int rowsToMove = currentReadRow - targetInsertIdx;

                if (rowsToMove > 0)
                {
                    int blockStartByteOffset = selfOffsets[targetInsertIdx];
                    int blockSizeInBytes = currentReadDataPtr - blockStartByteOffset;

                    currentWriteDataPtr -= blockSizeInBytes;
                    currentReadDataPtr -= blockSizeInBytes;

                    selfData.Slice(currentReadDataPtr, blockSizeInBytes)
                            .CopyTo(selfData.Slice(currentWriteDataPtr, blockSizeInBytes));

                    IntList.MoveIndex(selfOffsets, targetInsertIdx, i + 1, rowsToMove, runningByteDelta);
                }

                int oIdx = sortedLookup[i];
                int oSize = 0;
                
                if (oIdx != lookupNullIndex)
                {
                    int oStart = other._offsets.Get(oIdx);
                    int oEnd = other._offsets.Get(oIdx + 1);
                    oSize = oEnd - oStart;

                    // Copy the data
                    currentWriteDataPtr -= oSize;
                    otherData.Slice(oStart, oSize).CopyTo(selfData.Slice(currentWriteDataPtr));
                }

                // Update the running delta
                runningByteDelta -= oSize;

                // Set the actual offset value for this new item
                // It sits exactly at the currentWriteDataPtr we just used
                selfOffsets[targetInsertIdx + i] = currentWriteDataPtr;

                // Move the read tracker to the left of the block we just processed
                currentReadRow = targetInsertIdx;
            }

            selfOffsets[oldDataCount + otherCount] = oldTotalBytes + totalNewBytes;
        }

        /// <summary>
        /// Batch delete elements at the specified sorted indices.
        /// This is more efficient than calling RemoveAt repeatedly because it processes
        /// contiguous blocks of retained data in a single left-to-right sweep.
        /// </summary>
        /// <param name="targets">A span of sorted indices (ascending) of elements to delete.</param>
        public void DeleteBatch(ReadOnlySpan<int> targets, IMemoryAllocator memoryAllocator)
        {
            int deleteCount = targets.Length;
            if (deleteCount == 0) return;

            Debug.Assert(deleteCount <= Count);

            int oldCount = Count;
            var selfData = AccessSpan;
            var selfOffsets = _offsets.AccessSpan;

            int writeDataPtr = 0;
            int currentSourceRow = 0;
            int cumulativeBytesRemoved = 0;

            for (int i = 0; i < deleteCount; i++)
            {
                int targetIdx = targets[i];

                // Copy the retained block before this deletion target
                if (targetIdx > currentSourceRow)
                {
                    int blockStart = selfOffsets[currentSourceRow];
                    int blockEnd = selfOffsets[targetIdx];
                    int blockSize = blockEnd - blockStart;

                    if (blockSize > 0 && writeDataPtr != blockStart)
                    {
                        selfData.Slice(blockStart, blockSize).CopyTo(selfData.Slice(writeDataPtr));
                    }

                    // Write compacted offsets for the retained rows
                    for (int r = currentSourceRow; r < targetIdx; r++)
                    {
                        selfOffsets[r - i] = selfOffsets[r] - cumulativeBytesRemoved;
                    }

                    writeDataPtr += blockSize;
                }

                // Account for the bytes of the deleted element
                int deletedStart = selfOffsets[targetIdx];
                int deletedEnd = selfOffsets[targetIdx + 1];
                int deletedSize = deletedEnd - deletedStart;
                cumulativeBytesRemoved += deletedSize;

                currentSourceRow = targetIdx + 1;
            }

            // Copy the remaining block after the last deletion target
            if (currentSourceRow < oldCount)
            {
                int blockStart = selfOffsets[currentSourceRow];
                int blockEnd = selfOffsets[oldCount]; // sentinel offset
                int blockSize = blockEnd - blockStart;

                if (blockSize > 0 && writeDataPtr != blockStart)
                {
                    selfData.Slice(blockStart, blockSize).CopyTo(selfData.Slice(writeDataPtr));
                }

                for (int r = currentSourceRow; r <= oldCount; r++)
                {
                    selfOffsets[r - deleteCount] = selfOffsets[r] - cumulativeBytesRemoved;
                }

                writeDataPtr += blockSize;
            }
            else
            {
                // All trailing elements were deleted; write the final sentinel offset
                selfOffsets[oldCount - deleteCount] = writeDataPtr;
            }

            _offsets.RemoveRange(oldCount + 1 - deleteCount, deleteCount, memoryAllocator);
            CheckSizeReduction(memoryAllocator);
        }
    }
}
