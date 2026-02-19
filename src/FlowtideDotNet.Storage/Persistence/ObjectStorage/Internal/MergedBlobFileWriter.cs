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
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage.Internal
{
    /// <summary>
    /// Represents a file that is a combination of multiple blob files. 
    /// This allows for writing multiple blob files and then combining them into a single file without copying the data.
    /// 
    /// This is used to gather multiple small files into a single file to reduce the number of files on disk and improve read performance.
    /// </summary>
    internal class MergedBlobFileWriter : PagesFile
    {
        private const int HeaderSize = 64;
        private PrimitiveList<long> _pageIds;
        private PrimitiveList<int> _pageOffset;
        private int _globalOffset;
        private int endIndex;
        private bool _finished;

        public List<BlobFileWriter> _files = new List<BlobFileWriter>();

        // Read fields
        private SequencePosition _advancedPosition;
        private BufferSegment _headerData;
        private BufferSegment _head;
        private BufferSegment _end;
        private BufferSegment _pageIdsSegment;
        private BufferSegment _pageOffsetsSegment;
        


        private bool disposedValue;

        public ReadOnlySequence<byte> WrittenData => new ReadOnlySequence<byte>(_head, 0, _end, endIndex);

        public MergedBlobFileWriter(MemoryPool<byte> memoryPool, IMemoryAllocator memoryAllocator)
        {
            _pageIds = new PrimitiveList<long>(memoryAllocator);
            _pageOffset = new PrimitiveList<int>(memoryAllocator);
            _headerData = new BufferSegment(memoryPool.Rent(HeaderSize));
            _headerData.End = HeaderSize;
            _head = _headerData;
            _end = _headerData;
            _pageIdsSegment = new BufferSegment(_pageIds.SlicedMemory);
            _pageOffsetsSegment = new BufferSegment(_pageOffset.SlicedMemory);
            _end.SetNext(_pageIdsSegment);
            _end = _pageIdsSegment;
            _end.SetNext(_pageOffsetsSegment);
            _end = _pageOffsetsSegment;
        }

        public void AddSequence(long pageId, ReadOnlySequence<byte> data)
        {
            if (_finished)
            {
                throw new InvalidOperationException("Cannot add a blob file after the merged file has been finished");
            }
            _finished = false;
            var pointer = data.Start;

            while(data.TryGet(ref pointer, out var mem))
            {
                // We need to take a copy of the memory since it can be re-used
                var segment = new BufferSegment(mem.ToArray());
                _end.SetNext(segment);
                _end = segment;
                endIndex = segment.Length;
            }
            _pageIds.Add(pageId);
            _pageOffset.Add(_globalOffset);
            _globalOffset += (int)data.Length;
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
                var clone = segment.CloneWithoutNext();
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
            }

            _globalOffset += blobFileWriter.WrittenLength;
        }

        public override PrimitiveList<long> PageIds => _pageIds;

        public override PrimitiveList<int> PageOffsets => _pageOffset;

        public override int FileSize => (int)WrittenData.Length;

        public void Finish()
        {
            _pageOffset.Add(_globalOffset);
            
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

            //var pageIdsOffset = _end.RunningIndex + endIndex;
            //var pageIdSegment = new BufferSegment(_pageIds.SlicedMemory);
            //_end.SetNext(pageIdSegment);
            //_end = pageIdSegment;
            //endIndex = pageIdSegment.Length;

            //var pageOffsetOffset = _end.RunningIndex + endIndex;
            //var offsetSegment = new BufferSegment(_pageOffset.SlicedMemory);
            //_end.SetNext(offsetSegment);
            //_end = offsetSegment;
            //endIndex = offsetSegment.Length;

            var headerData = _headerData.AvailableMemory.Span;

            // Write version
            BinaryPrimitives.WriteInt16LittleEndian(headerData, 1);
            headerData = headerData.Slice(4);

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
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects)
                    foreach(var file in _files)
                    {
                        file.Dispose();
                    }
                }

                _pageIds.Dispose();
                _pageOffset.Dispose();

                disposedValue = true;
            }
        }

        ~MergedBlobFileWriter()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: false);
        }

        public override void Dispose()
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
