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
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Storage.Persistence.Reservoir.Internal
{
    /// <summary>
    /// Contains information for a new checkpoint
    /// 
    /// It also contains the code to return the serialized data as a pipereader
    /// </summary>
    internal class BlobNewCheckpoint : PipeReader, IDisposable
    {
        private const int HeaderSize = 192;

        private readonly PrimitiveList<long> _upsertPageIds;
        private readonly PrimitiveList<ulong> _upsertPageFileIds;
        private readonly PrimitiveList<int> _upsertPageOffsets;
        private readonly PrimitiveList<int> _upsertPageSizes;
        private readonly PrimitiveList<uint> _upsertPageCrc32;

        private readonly PrimitiveList<long> _deletedPageIds;

        // File information
        // Tracks how many pages are no longer active in the file
        // This is used to determine if a file is no longer active or if it can be
        // Rewritten to remove the inactive pages
        private readonly PrimitiveList<ulong> _changedFileIds;
        private readonly PrimitiveList<int> _changedFilePageCounts;
        private readonly PrimitiveList<int> _changedFileNonActivePageCounts;
        private readonly PrimitiveList<long> _changedFileAddedAtVersion;
        private readonly PrimitiveList<int> _changedFileSize;
        private readonly PrimitiveList<int> _changedFileDeletedSize;
        private readonly PrimitiveList<ulong> _changedFileCrc64;


        private readonly PrimitiveList<ulong> _deletedFileIds;
        private readonly PrimitiveList<long> _deletedFileAtVersion;
        private readonly IMemoryAllocator memoryAllocator;
        private ulong _nextFileId;

        // Read specific fields
        private SequencePosition _advancedPosition;
        private BufferSegment _headerData;
        private BufferSegment _head;
        private BufferSegment _end;
        private BufferSegment? _compressedSegment;
        private int endIndex;

        private ulong _crc64;
        private bool disposedValue;

        public ReadOnlySequence<byte> WrittenData => new ReadOnlySequence<byte>(_head, 0, _end, endIndex);

        public PrimitiveList<ulong> ChangedFileCrc64 => _changedFileCrc64;

        public ulong Crc64 => _crc64;

        public BufferSegment Head => _head;
        public BufferSegment End => _end;
        public int EndIndex => endIndex;

        public BlobNewCheckpoint(MemoryPool<byte> memoryPool, IMemoryAllocator memoryAllocator)
        {
            _upsertPageIds = new PrimitiveList<long>(memoryAllocator);
            _upsertPageFileIds = new PrimitiveList<ulong>(memoryAllocator);
            _upsertPageOffsets = new PrimitiveList<int>(memoryAllocator);
            _upsertPageSizes = new PrimitiveList<int>(memoryAllocator);
            _upsertPageCrc32 = new PrimitiveList<uint>(memoryAllocator);
            _deletedPageIds = new PrimitiveList<long>(memoryAllocator);

            _changedFileIds = new PrimitiveList<ulong>(memoryAllocator);
            _changedFilePageCounts = new PrimitiveList<int>(memoryAllocator);
            _changedFileNonActivePageCounts = new PrimitiveList<int>(memoryAllocator);
            _changedFileSize = new PrimitiveList<int>(memoryAllocator);
            _changedFileDeletedSize = new PrimitiveList<int>(memoryAllocator);
            _changedFileAddedAtVersion = new PrimitiveList<long>(memoryAllocator);
            _changedFileCrc64 = new PrimitiveList<ulong>(memoryAllocator);

            _deletedFileIds = new PrimitiveList<ulong>(memoryAllocator);
            _deletedFileAtVersion = new PrimitiveList<long>(memoryAllocator);

            // Create a segment for the header
            _headerData = new BufferSegment(memoryPool.Rent(HeaderSize), HeaderSize);
            _headerData.End = HeaderSize;
            _head = _headerData;
            _end = _headerData;
            endIndex = HeaderSize;
            this.memoryAllocator = memoryAllocator;
        }

        public void SetNextFileId(ulong nextFileId)
        {
            _nextFileId = nextFileId;
        }

        public void UpdateAllFileIds(ulong newFileId)
        {
            for (int i = 0; i < _upsertPageFileIds.Count; i++)
            {
                _upsertPageFileIds[i] = newFileId;
            }
        }

        /// <summary>
        /// Finalize the header file for writing
        /// This creates all the buffer segments, and writes the header information
        /// </summary>
        public void FinishForWriting()
        {
            var upsertPageIdsOffset = _end.RunningIndex + endIndex;
            var pageIdSegment = new BufferSegment(_upsertPageIds.SlicedMemory);
            _end.SetNext(pageIdSegment);
            _end = pageIdSegment;
            endIndex = pageIdSegment.Length;

            var upsertPageFileIdsOffset = _end.RunningIndex + endIndex;
            var upsertPageFileIdsSegment = new BufferSegment(_upsertPageFileIds.SlicedMemory);
            _end.SetNext(upsertPageFileIdsSegment);
            _end = upsertPageFileIdsSegment;
            endIndex = upsertPageFileIdsSegment.Length;

            var upsertPageFileOffsetsOffset = _end.RunningIndex + endIndex;
            var upsertPageFileOffsetsSegment = new BufferSegment(_upsertPageOffsets.SlicedMemory);
            _end.SetNext(upsertPageFileOffsetsSegment);
            _end = upsertPageFileOffsetsSegment;
            endIndex = upsertPageFileOffsetsSegment.Length;

            var upsertPageSizesOffset = _end.RunningIndex + endIndex;
            var upsertPageSizesOffsetSegment = new BufferSegment(_upsertPageSizes.SlicedMemory);
            _end.SetNext(upsertPageSizesOffsetSegment);
            _end = upsertPageSizesOffsetSegment;
            endIndex = upsertPageSizesOffsetSegment.Length;

            var upsertPageCrc32sOffset = _end.RunningIndex + endIndex;
            var upsertPageCrc32sSegment = new BufferSegment(_upsertPageCrc32.SlicedMemory);
            _end.SetNext(upsertPageCrc32sSegment);
            _end = upsertPageCrc32sSegment;
            endIndex = upsertPageCrc32sSegment.Length;

            var deletedPageIdsOffset = _end.RunningIndex + endIndex;
            var deletedPageIdsSegment = new BufferSegment(_deletedPageIds.SlicedMemory);
            _end.SetNext(deletedPageIdsSegment);
            _end = deletedPageIdsSegment;
            endIndex = deletedPageIdsSegment.Length;

            var updatedFileIdsOffset = _end.RunningIndex + endIndex;
            var updatedFileIdsSegment = new BufferSegment(_changedFileIds.SlicedMemory);
            _end.SetNext(updatedFileIdsSegment);
            _end = updatedFileIdsSegment;
            endIndex = updatedFileIdsSegment.Length;

            var updatedFilePageCountOffset = _end.RunningIndex + endIndex;
            var updatedFilePageCountSegment = new BufferSegment(_changedFilePageCounts.SlicedMemory);
            _end.SetNext(updatedFilePageCountSegment);
            _end = updatedFilePageCountSegment;
            endIndex = updatedFilePageCountSegment.Length;

            var updatedFileNonActivePageCountOffset = _end.RunningIndex + endIndex;
            var updatedFileNonActivePageCountSegment = new BufferSegment(_changedFileNonActivePageCounts.SlicedMemory);
            _end.SetNext(updatedFileNonActivePageCountSegment);
            _end = updatedFileNonActivePageCountSegment;
            endIndex = updatedFileNonActivePageCountSegment.Length;

            var updatedFileSizeOffset = _end.RunningIndex + endIndex;
            var updatedFileSizeSegment = new BufferSegment(_changedFileSize.SlicedMemory);
            _end.SetNext(updatedFileSizeSegment);
            _end = updatedFileSizeSegment;
            endIndex = updatedFileSizeSegment.Length;

            var updatedFileDeletedSizeOffset = _end.RunningIndex + endIndex;
            var updatedFileDeletedSizeSegment = new BufferSegment(_changedFileDeletedSize.SlicedMemory);
            _end.SetNext(updatedFileDeletedSizeSegment);
            _end = updatedFileDeletedSizeSegment;
            endIndex = updatedFileDeletedSizeSegment.Length;

            var updatedFileAddedAtVersionOffset = _end.RunningIndex + endIndex;
            var updatedFileAddedAtVersionSegment = new BufferSegment(_changedFileAddedAtVersion.SlicedMemory);
            _end.SetNext(updatedFileAddedAtVersionSegment);
            _end = updatedFileAddedAtVersionSegment;
            endIndex = updatedFileAddedAtVersionSegment.Length;

            var updatedFileCrc64Offset = _end.RunningIndex + endIndex;
            var updatedFileCrc64Segment = new BufferSegment(_changedFileCrc64.SlicedMemory);
            _end.SetNext(updatedFileCrc64Segment);
            _end = updatedFileCrc64Segment;
            endIndex = updatedFileCrc64Segment.Length;

            var deletedFileIdsOffset = _end.RunningIndex + endIndex;
            var deletedFileIdsSegment = new BufferSegment(_deletedFileIds.SlicedMemory);
            _end.SetNext(deletedFileIdsSegment);
            _end = deletedFileIdsSegment;
            endIndex = deletedFileIdsSegment.Length;

            var deletedFileAtVersionOffset = _end.RunningIndex + endIndex;
            var deletedFileAtVersionSegment = new BufferSegment(_deletedFileAtVersion.SlicedMemory);
            _end.SetNext(deletedFileAtVersionSegment);
            _end = deletedFileAtVersionSegment;
            endIndex = deletedFileAtVersionSegment.Length;

            var headerData = _headerData.AvailableMemory.Span;

            // Write magic number
            BinaryPrimitives.WriteInt32LittleEndian(headerData, MagicNumbers.CheckpointFileMagicNumber);
            headerData = headerData.Slice(4);

            // Write version
            BinaryPrimitives.WriteInt16LittleEndian(headerData, 1);
            // Next 2 bytes are reserved, so we skip 2
            headerData = headerData.Slice(4);

            // Write counts
            BinaryPrimitives.WriteInt64LittleEndian(headerData, _upsertPageIds.Count);
            headerData = headerData.Slice(8);

            BinaryPrimitives.WriteInt64LittleEndian(headerData, _deletedPageIds.Count);
            headerData = headerData.Slice(8);

            BinaryPrimitives.WriteInt64LittleEndian(headerData, _changedFileIds.Count);
            headerData = headerData.Slice(8);

            BinaryPrimitives.WriteInt64LittleEndian(headerData, _deletedFileIds.Count);
            headerData = headerData.Slice(8);

            // Write offsets
            BinaryPrimitives.WriteInt64LittleEndian(headerData, upsertPageIdsOffset);
            headerData = headerData.Slice(8);

            BinaryPrimitives.WriteInt64LittleEndian(headerData, upsertPageFileIdsOffset);
            headerData = headerData.Slice(8);

            BinaryPrimitives.WriteInt64LittleEndian(headerData, upsertPageFileOffsetsOffset);
            headerData = headerData.Slice(8);

            BinaryPrimitives.WriteInt64LittleEndian(headerData, upsertPageSizesOffset);
            headerData = headerData.Slice(8);

            BinaryPrimitives.WriteInt64LittleEndian(headerData, upsertPageCrc32sOffset);
            headerData = headerData.Slice(8);

            BinaryPrimitives.WriteInt64LittleEndian(headerData, deletedPageIdsOffset);
            headerData = headerData.Slice(8);

            // Changes files
            BinaryPrimitives.WriteInt64LittleEndian(headerData, updatedFileIdsOffset);
            headerData = headerData.Slice(8);

            BinaryPrimitives.WriteInt64LittleEndian(headerData, updatedFilePageCountOffset);
            headerData = headerData.Slice(8);

            BinaryPrimitives.WriteInt64LittleEndian(headerData, updatedFileNonActivePageCountOffset);
            headerData = headerData.Slice(8);

            BinaryPrimitives.WriteInt64LittleEndian(headerData, updatedFileSizeOffset);
            headerData = headerData.Slice(8);

            BinaryPrimitives.WriteInt64LittleEndian(headerData, updatedFileDeletedSizeOffset);
            headerData = headerData.Slice(8);

            BinaryPrimitives.WriteInt64LittleEndian(headerData, updatedFileAddedAtVersionOffset);
            headerData = headerData.Slice(8);

            BinaryPrimitives.WriteInt64LittleEndian(headerData, updatedFileCrc64Offset);
            headerData = headerData.Slice(8);

            // Deleted files
            BinaryPrimitives.WriteInt64LittleEndian(headerData, deletedFileIdsOffset);
            headerData = headerData.Slice(8);

            BinaryPrimitives.WriteInt64LittleEndian(headerData, deletedFileAtVersionOffset);
            headerData = headerData.Slice(8);

            // Next file id for data files
            BinaryPrimitives.WriteUInt64LittleEndian(headerData, _nextFileId);
            headerData = headerData.Slice(8);
        }

        public void CompressData()
        {
            using ZstdCompression compressor = new ZstdCompression(memoryAllocator, 3, (int)WrittenData.Length, 8);
            foreach(var segment in WrittenData)
            {
                compressor.Write(segment.Span);
            }
            var result = compressor.Complete();
            var headerSpan = result.memoryOwner.Memory.Span;
            BinaryPrimitives.WriteInt32LittleEndian(headerSpan, MagicNumbers.CompressedZstdCheckpointFileMagicNumber);
            headerSpan = headerSpan.Slice(4);
            BinaryPrimitives.WriteInt32LittleEndian(headerSpan, (int)WrittenData.Length);

            _compressedSegment = new BufferSegment(result.memoryOwner, result.writtenLength + 8);

            var disposeSegment = _head;
            while (disposeSegment != null)
            {
                var next = disposeSegment._next;
                disposeSegment.Dispose();
                disposeSegment = next;
            }

            _head = _compressedSegment;
            _end = _compressedSegment;
            endIndex = _compressedSegment.Length;
        }

        public void RecalculateCrc64()
        {
            System.IO.Hashing.Crc64 crc64 = new System.IO.Hashing.Crc64();
            foreach (var segment in WrittenData)
            {
                crc64.Append(segment.Span);
            }
            _crc64 = crc64.GetCurrentHashAsUInt64();
        }

        public void AddDeletedPageId(long pageId)
        {
            _deletedPageIds.Add(pageId);
        }

        public void AddFileInformation(FileInformation fileInformation)
        {
            _changedFileIds.Add(fileInformation.FileId);
            _changedFilePageCounts.Add(fileInformation.PageCount);
            _changedFileNonActivePageCounts.Add(fileInformation.NonActivePageCount);
            _changedFileSize.Add(fileInformation.FileSize);
            _changedFileDeletedSize.Add(fileInformation.DeletedSize);
            _changedFileAddedAtVersion.Add(fileInformation.AddedAtVersion);
            _changedFileCrc64.Add(fileInformation.Crc64);
        }

        public void AddDeletedFileId(DeletedFileInfo deletedFileInfo)
        {
            _deletedFileIds.Add(deletedFileInfo.fileId);
            _deletedFileAtVersion.Add(deletedFileInfo.deletedAtVersion);
        }

        public void AddUpsertPages(ConcurrentDictionary<long, PageFileLocation> pageFileLocations)
        {
            if (_upsertPageFileIds.Count != 0)
            {
                throw new InvalidOperationException("Upsert pages have already been added to the checkpoint, passing the dictionary should only be used for snapshots");
            }

            var sortedKeys = pageFileLocations.Keys.OrderBy(x => x).ToArray();

            // Ensure capacity
            _upsertPageFileIds.EnsureCapacity(sortedKeys.Length);
            _upsertPageIds.EnsureCapacity(sortedKeys.Length);
            _upsertPageOffsets.EnsureCapacity(sortedKeys.Length);
            _upsertPageSizes.EnsureCapacity(sortedKeys.Length);
            _upsertPageCrc32.EnsureCapacity(sortedKeys.Length);

            foreach (var pageId in sortedKeys)
            {
                var location = pageFileLocations[pageId];

                if (location.Size < 0)
                {
                    throw new InvalidOperationException($"Page file location for page id {pageId} has invalid size {location.Size}");
                }

                _upsertPageIds.Add(pageId);
                _upsertPageFileIds.Add(location.FileId);
                _upsertPageOffsets.Add(location.Offset);
                _upsertPageSizes.Add(location.Size);
                _upsertPageCrc32.Add(location.Crc32);
            }
        }

        [SkipLocalsInit]
        public unsafe void AddUpsertPages(
            PrimitiveList<long> pageIds, 
            PrimitiveList<ulong> fileIds,
            PrimitiveList<int> pageOffsets,
            PrimitiveList<int> pageSizes,
            PrimitiveList<uint> crc32s)
        {
            if (pageIds.Count < 128)
            {
                Span<int> indices = stackalloc int[pageIds.Count];
                AddUpsertPages_Internal(indices, pageIds, fileIds, pageOffsets, pageSizes, crc32s);
            }
            else
            {
                int[] indices = new int[pageIds.Count];
                AddUpsertPages_Internal(indices, pageIds, fileIds, pageOffsets, pageSizes, crc32s);
            }
        }

        private readonly struct IndiceComparer : IComparer<int>
        {
            private readonly PrimitiveList<long> pageIds;

            public IndiceComparer(PrimitiveList<long> pageIds)
            {
                this.pageIds = pageIds;
            }
            public int Compare(int x, int y)
            {
                return pageIds[x].CompareTo(pageIds[y]);
            }
        }

        private void AddUpsertPages_Internal(
            Span<int> indices,
            PrimitiveList<long> pageIds,
            PrimitiveList<ulong> fileIds,
            PrimitiveList<int> pageOffsets,
            PrimitiveList<int> pageSizes,
            PrimitiveList<uint> pageCrc32s)
        {
            // Sort the page ids
            int count = pageIds.Count;
            for (int i = 0; i < count; i++)
            {
                indices[i] = i;
            }
            indices.Sort(new IndiceComparer(pageIds));

            // Ensure capacity
            _upsertPageFileIds.EnsureCapacity(_upsertPageFileIds.Count + indices.Length);
            _upsertPageIds.EnsureCapacity(_upsertPageIds.Count + indices.Length);
            _upsertPageOffsets.EnsureCapacity(_upsertPageOffsets.Count + indices.Length);
            _upsertPageSizes.EnsureCapacity(_upsertPageSizes.Count + indices.Length);
            _upsertPageCrc32.EnsureCapacity(_upsertPageCrc32.Count + indices.Length);

            int top = _upsertPageFileIds.Count;

            _upsertPageFileIds.SetLength(_upsertPageFileIds.Count + indices.Length);
            _upsertPageIds.SetLength(_upsertPageIds.Count + indices.Length);
            _upsertPageOffsets.SetLength(_upsertPageOffsets.Count + indices.Length);
            _upsertPageSizes.SetLength(_upsertPageSizes.Count + indices.Length);
            _upsertPageCrc32.SetLength(_upsertPageCrc32.Count + indices.Length);


            var ids = _upsertPageIds.Span;
            var files = _upsertPageFileIds.Span;
            var offsets = _upsertPageOffsets.Span;
            var sizes = _upsertPageSizes.Span;
            var crc32s = _upsertPageCrc32.Span;

            // Start at the end to minimize the amount of memory copies
            for (int i = indices.Length - 1; i >= 0; i--)
            {
                // Binary search the position
                var index = indices[i];
                var position = ids.Slice(0, top).BinarySearch(pageIds[index]);

                if (position >= 0)
                {
                    throw new InvalidOperationException("Page id already exists in the checkpoint.");
                }
                position = ~position;

                var elementIndex = position + i;
                if (top - position > 0)
                {
                    // move forward the elements above with 'i' count.
                    ids.Slice(position, top - position).CopyTo(ids.Slice(elementIndex + 1));
                    files.Slice(position, top - position).CopyTo(files.Slice(elementIndex + 1));
                    offsets.Slice(position, top - position).CopyTo(offsets.Slice(elementIndex + 1));
                    sizes.Slice(position, top - position).CopyTo(sizes.Slice(elementIndex + 1));
                    crc32s.Slice(position, top - position).CopyTo(crc32s.Slice(elementIndex + 1));
                }

                if (pageSizes[index] < 0)
                {
                    throw new InvalidOperationException($"Page size for page id {pageIds[index]} is invalid with size {pageSizes[index]}");
                }

                // Insert new element
                _upsertPageIds[elementIndex] = pageIds[index];
                _upsertPageFileIds[elementIndex] = fileIds[index];
                _upsertPageOffsets[elementIndex] = pageOffsets[index];
                _upsertPageSizes[elementIndex] = pageSizes[index];
                _upsertPageCrc32[elementIndex] = pageCrc32s[index];

                // Set the new top value top limit how much is copied
                top = position;
            }
        }

        public override bool TryRead(out ReadResult result)
        {
            var data = WrittenData;
            if (data.End.Equals(_advancedPosition))
            {
                result = new ReadResult(ReadOnlySequence<byte>.Empty, false, true);
                return false;
            }
            result = new ReadResult(data.Slice(_advancedPosition), false, true);
            return true;
        }

        public override ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default)
        {
            TryRead(out var result);
            return ValueTask.FromResult(result);
        }

        public override void AdvanceTo(SequencePosition consumed)
        {
            var obj = consumed.GetObject();

            if (obj is byte[] byteArr && byteArr.Length == 0)
            {
                // Nothing was read, so we don't advance
                return;
            }

            _advancedPosition = consumed;
        }

        public override void AdvanceTo(SequencePosition consumed, SequencePosition examined)
        {
            _advancedPosition = consumed;
        }

        public override void CancelPendingRead()
        {
        }

        public override void Complete(Exception? exception = null)
        {
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                _upsertPageIds.Dispose();
                _upsertPageFileIds.Dispose();
                _upsertPageOffsets.Dispose();
                _upsertPageSizes.Dispose();
                _upsertPageCrc32.Dispose();
                _deletedPageIds.Dispose();

                _changedFileIds.Dispose();
                _changedFilePageCounts.Dispose();
                _changedFileNonActivePageCounts.Dispose();
                _changedFileAddedAtVersion.Dispose();
                _changedFileSize.Dispose();
                _changedFileDeletedSize.Dispose();
                _changedFileCrc64.Dispose();

                _deletedFileIds.Dispose();
                _deletedFileAtVersion.Dispose();

                // Dispose all segments
                var segment = _head;
                while (segment != null)
                {
                    var next = segment._next;
                    segment.Dispose();
                    segment = next;
                }

                disposedValue = true;
            }
        }

        ~BlobNewCheckpoint()
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
    }
}
