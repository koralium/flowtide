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
using System.IO.Pipelines;

namespace FlowtideDotNet.Storage.Persistence.Reservoir.Internal
{
    /// <summary>
    /// Represents a file that is a combination of multiple blob files. 
    /// This allows for writing multiple blob files and then combining them into a single file without copying the data.
    /// 
    /// This is used to gather multiple small files into a single file to reduce the number of files on disk and improve read performance.
    /// </summary>
    internal class MergedBlobFileWriter : PagesFile, IFileWithSequence
    {
        private const int HeaderSize = 64;
        private readonly MemoryPool<byte> _memoryPool;
        private readonly IMemoryAllocator _memoryAllocator;
        private PrimitiveList<long> _pageIds;
        private PrimitiveList<int> _pageOffset;
        private PrimitiveList<uint> _crc32s;
        private int _globalOffset;
        private int endIndex;
        private bool _finished;
        private ulong _crc64;

        private bool _addingSequences = false;
        private IMemoryOwner<byte>? _sequencesMemory;
        private int _sequencesOffset;

        public List<BlobFileWriter> _files = new List<BlobFileWriter>();

        // Read fields
        private SequencePosition _advancedPosition;
        private BufferSegment _headerData;
        private BufferSegment _head;
        private BufferSegment _end;
        private BufferSegment _pageIdsSegment;
        private BufferSegment _pageOffsetsSegment;

        public BufferSegment Head => _head;
        public BufferSegment End => _end;
        public BufferSegment HeaderData => _headerData;

        public int EndIndex => endIndex;
        


        private bool disposedValue;

        public ReadOnlySequence<byte> WrittenData => new ReadOnlySequence<byte>(_head, 0, _end, endIndex);

        public MergedBlobFileWriter(MemoryPool<byte> memoryPool, IMemoryAllocator memoryAllocator)
        {
            _pageIds = new PrimitiveList<long>(memoryAllocator);
            _pageOffset = new PrimitiveList<int>(memoryAllocator);
            _crc32s = new PrimitiveList<uint>(memoryAllocator);
            _headerData = new BufferSegment(memoryPool.Rent(HeaderSize), HeaderSize);
            _headerData.End = HeaderSize;
            _head = _headerData;
            _end = _headerData;
            _pageIdsSegment = new BufferSegment(_pageIds.SlicedMemory);
            _pageOffsetsSegment = new BufferSegment(_pageOffset.SlicedMemory);
            _end.SetNext(_pageIdsSegment);
            _end = _pageIdsSegment;
            _end.SetNext(_pageOffsetsSegment);
            _end = _pageOffsetsSegment;
            _memoryPool = memoryPool;
            _memoryAllocator = memoryAllocator;
        }

        public void StartAddingSequences(int estimatedCount)
        {
            if (_addingSequences)
            {
                throw new InvalidOperationException("Already adding sequences");
            }
            _sequencesMemory = _memoryAllocator.Allocate(16 * 1024 * estimatedCount, 64);
            _addingSequences = true;
            _sequencesOffset = 0;
        }

        public void AddSequence(long pageId, uint crc32, ReadOnlySequence<byte> data)
        {
            if (_finished)
            {
                throw new InvalidOperationException("Cannot add a blob file after the merged file has been finished");
            }
            if (!_addingSequences)
            {
                throw new InvalidOperationException("Must call StartAddingSequences before adding sequences");
            }
            if (_sequencesMemory == null)
            {
                throw new InvalidOperationException("Must call StartAddingSequences before adding sequences");
            }
            _finished = false;
            

            if (data.Length > (_sequencesMemory.Memory.Length - _sequencesOffset))
            {
                var newMemoryLength = Math.Max(_sequencesMemory.Memory.Length * 2, _sequencesOffset + (int)data.Length);
                _sequencesMemory = _memoryAllocator.Realloc(_sequencesMemory, newMemoryLength, 64);
            }

            var sequencesMemory =_sequencesMemory.Memory;
            var pointer = data.Start;
            while (data.TryGet(ref pointer, out var mem))
            {
                mem.CopyTo(sequencesMemory.Slice(_sequencesOffset));
                _sequencesOffset += mem.Length;
            }

            _pageIds.Add(pageId);
            _pageOffset.Add(_globalOffset);
            _crc32s.Add(crc32);
            _globalOffset += (int)data.Length;
        }

        public void FinishAddingSequences()
        {
            if (!_addingSequences)
            {
                throw new InvalidOperationException("Must call StartAddingSequences before finishing adding sequences");
            }
            if (_sequencesMemory == null)
            {
                throw new InvalidOperationException("No sequences memory allocated");
            }
            var segment = new BufferSegment(_sequencesMemory, _sequencesOffset);
            _end.SetNext(segment);
            _end = segment;
            endIndex = segment.Length;
            _addingSequences = false;
        }

        public void AddBlobFile(BlobFileWriter blobFileWriter)
        {
            if (_finished)
            {
                throw new InvalidOperationException("Cannot add a blob file after the merged file has been finished");
            }
            _finished = false;
            blobFileWriter.FinishDataOnly();
            _files.Add(blobFileWriter);

            // Link the new file's data segments to the end of the current data
            var segment = blobFileWriter.DataStartSegment;
            while (segment != null)
            {
                var clone = segment.CloneWithoutNextNoOwnership();
                _end.SetNext(clone);
                endIndex = clone.End;
                _end = clone;
                if (segment.Next != null)
                {
                    if (segment.Next is BufferSegment bufferSegment)
                    {
                        segment = bufferSegment;
                    }
                    else
                    {
                        throw new InvalidOperationException("Unexpected segment type");
                    }
                }
                else
                {
                    segment = null;
                }
            }

            for (int i = 0; i < blobFileWriter.PageIds.Count; i++)
            {
                _pageIds.Add(blobFileWriter.PageIds[i]);
                _pageOffset.Add(blobFileWriter.PageOffsets[i] + _globalOffset);
                _crc32s.Add(blobFileWriter.Crc32s[i]);
            }

            _globalOffset += blobFileWriter.WrittenLength;
        }

        public override PrimitiveList<long> PageIds => _pageIds;

        public override PrimitiveList<int> PageOffsets => _pageOffset;

        public override PrimitiveList<uint> Crc32s => _crc32s;

        public override int FileSize => (int)WrittenData.Length;

        public override ulong Crc64 => _crc64;

        public void Finish()
        {
            if (_pageIds.Count > 0)
            {
                // We only add offset if there is any page
                _pageOffset.Add(_globalOffset);
            }
            
            _pageIdsSegment.UpdateMemory_Unsafe(_pageIds.SlicedMemory);
            _pageOffsetsSegment.UpdateMemory_Unsafe(_pageOffset.SlicedMemory);
            _head.UpdateRunningIndices();

            var pageIdsOffset = _pageIdsSegment.RunningIndex;
            var pageOffsetsOffset = _pageOffsetsSegment.RunningIndex;
            var idsAndOffsetsOffset = (int)(_pageOffsetsSegment.RunningIndex + _pageOffsetsSegment.Length);

            // Update page offsets
            for (int i = 0; i < _pageOffset.Count; i++)
            {
                _pageOffset[i] = _pageOffset[i] + idsAndOffsetsOffset;
            }

            var headerData = _headerData.AvailableMemory.Span;

            // Write magic number
            BinaryPrimitives.WriteInt32LittleEndian(headerData, MagicNumbers.DataFileMagicNumber);
            headerData = headerData.Slice(4);

            // Write version
            BinaryPrimitives.WriteInt16LittleEndian(headerData, 1);
            headerData = headerData.Slice(2);

            // Write flags
            BinaryPrimitives.WriteInt16LittleEndian(headerData, 0);
            headerData = headerData.Slice(2);

            // Write page count
            BinaryPrimitives.WriteInt32LittleEndian(headerData, _pageIds.Count);
            headerData = headerData.Slice(4);

            // Write offset to page ids
            BinaryPrimitives.WriteInt32LittleEndian(headerData, (int)pageIdsOffset);
            headerData = headerData.Slice(4);

            // Write offset to page offsets
            BinaryPrimitives.WriteInt32LittleEndian(headerData, (int)pageOffsetsOffset);
            headerData = headerData.Slice(4);

            // Write offset to page data start
            BinaryPrimitives.WriteInt32LittleEndian(headerData, idsAndOffsetsOffset);
            _finished = true;

            RecalculateCrc64();
        }

        public void RecalculateCrc64()
        {
            // Calculate crc64 of the entire file
            System.IO.Hashing.Crc64 crc64 = new System.IO.Hashing.Crc64();
            foreach (var segment in WrittenData)
            {
                crc64.Append(segment.Span);
            }
            _crc64 = crc64.GetCurrentHashAsUInt64();
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
            _advancedPosition = default;
        }

        public override void Complete(Exception? exception = null)
        {
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    foreach(var file in _files)
                    {
                        file.Return();
                    }
                }

                _pageIds.Dispose();
                _pageOffset.Dispose();
                _crc32s.Dispose();
                _headerData.Dispose();

                // Dispose segments since there may be segments from sequences here
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

        ~MergedBlobFileWriter()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: false);
        }

        public override void Return()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        public override void DoneWriting()
        {
            foreach(var file in _files)
            {
                file.DoneWriting();
            }
        }
    }
}
