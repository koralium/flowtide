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
using FlowtideDotNet.Storage.Exceptions;
using FlowtideDotNet.Storage.Memory;
using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Reflection.PortableExecutable;
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage.Internal
{
    internal class BlobFileWriter : PagesFile, IBufferWriter<byte>
    {
        private const int HeaderSize = 64;

        private readonly Action<PagesFile> doneFunc;
        private readonly MemoryPool<byte> _memoryPool;
        private const int InitialSegmentSize = 4 * 1024 * 1024;
        private BufferSegment _headerData;
        private BufferSegment _head;
        private BufferSegment _end;
        private int endIndex = 0;
        private int _writtenBytes = 0;
        private PrimitiveList<long> _pageIds;
        private PrimitiveList<int> _pageOffset;
        private SequencePosition _advancedPosition;
        private bool disposedValue;
        private BufferSegment _dataStart;
        private bool _finished = false;
        private readonly object _lock = new object();

        public BlobFileWriter(Action<PagesFile> doneFunc, MemoryPool<byte> memoryPool, IMemoryAllocator memoryAllocator)
        {
            this.doneFunc = doneFunc;
            this._memoryPool = memoryPool;
            _headerData = new BufferSegment(memoryPool.Rent(HeaderSize));
            _headerData.End = HeaderSize;
            _head = _headerData;
            var firstDataSegment = new BufferSegment(memoryPool.Rent(InitialSegmentSize));
            _dataStart = firstDataSegment;
            _head.SetNext(firstDataSegment);
            _end = firstDataSegment;
            _pageIds = new PrimitiveList<long>(memoryAllocator);
            _pageOffset = new PrimitiveList<int>(memoryAllocator);
            _advancedPosition = WrittenData.Start;
        }

        public ReadOnlySequence<byte> WrittenData => new ReadOnlySequence<byte>(_head, 0, _end, endIndex);

        public BufferSegment CurrentSegment => _end;
        public int CurrentIndex => endIndex;

        public BufferSegment DataStartSegment => _dataStart;

        public int WrittenLength => _writtenBytes;

        public override PrimitiveList<long> PageIds => _pageIds;

        public override PrimitiveList<int> PageOffsets => _pageOffset;

        public ReadOnlySequence<byte> Write(long key, SerializableObject value)
        {
            lock (_lock)
            {
                var position = WrittenLength;
                var startSegment = CurrentSegment;
                var segmentPosition = CurrentIndex;
                value.Serialize(this);
                var endSegment = CurrentSegment;
                var endSegmentPosition = CurrentIndex;
                _pageIds.Add(key);
                _pageOffset.Add(position + HeaderSize);
                return new ReadOnlySequence<byte>(startSegment, segmentPosition, endSegment, endSegmentPosition);
            }
        }
        
        public void Advance(int count)
        {
            endIndex += count;
            _writtenBytes += count;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureCapacity(ref readonly int sizeHint)
        {
            if (_finished) 
            { 
                throw new InvalidOperationException("Cannot write to BlobFileWriter after it has been finished.");
            }
            if (endIndex + sizeHint >= _end.AvailableMemory.Length)
            {
                var newSegment = new BufferSegment(_memoryPool.Rent(Math.Max(sizeHint, InitialSegmentSize)));
                _end.SetNext(newSegment);
                _end.End = endIndex;
                _end = newSegment;
                endIndex = 0;
            }
        }

        public void FinishDataOnly()
        {
            _finished = true;
            _end.End = endIndex;
        }

        public void Finish()
        {
            lock (_lock)
            {
                if (_finished)
                {
                    throw new FlowtidePersistentStorageException("Tried to finish a file already finished");
                }
                _finished = true;
                _end.End = endIndex;
                var pageIdsOffset = _end.RunningIndex + endIndex;
                var pageIdSegment = new BufferSegment(_pageIds.SlicedMemory);
                _end.SetNext(pageIdSegment);
                _end = pageIdSegment;
                endIndex = pageIdSegment.Length;

                var pageOffsetOffset = _end.RunningIndex + endIndex;
                var offsetSegment = new BufferSegment(_pageOffset.SlicedMemory);
                _end.SetNext(offsetSegment);
                _end = offsetSegment;
                endIndex = offsetSegment.Length;

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
                BinaryPrimitives.WriteInt32LittleEndian(headerData, (int)pageOffsetOffset);
                headerData = headerData.Slice(4);

                // Write offset to page data start
                BinaryPrimitives.WriteInt32LittleEndian(headerData, 64);
            }
        }

        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            EnsureCapacity(in sizeHint);
            return _end.AvailableMemory.Slice(endIndex);
        }

        public Span<byte> GetSpan(int sizeHint = 0)
        {
            EnsureCapacity(in sizeHint);
            return _end.AvailableMemory.Span.Slice(endIndex);
        }

        public override bool TryRead(out ReadResult result)
        {
            var data = WrittenData;
            if (data.End.Equals(_advancedPosition))
            {
                result = new ReadResult(ReadOnlySequence<byte>.Empty, false, true);
                return false;
            }
            result = new ReadResult(WrittenData.Slice(_advancedPosition), false, true);
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
                var segment = _head;
                while (segment != null)
                {
                    var next = segment._next;
                    segment.Dispose();
                    segment = next;
                }

                _pageIds.Dispose();
                _pageOffset.Dispose();

                disposedValue = true;
            }
        }

        ~BlobFileWriter()
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
            doneFunc(this);
        }
    }
}
