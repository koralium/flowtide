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

using Microsoft.Win32.SafeHandles;
using System.Buffers;
using System.IO.Pipelines;

namespace FlowtideDotNet.Connector.Files.Internal.CsvFiles.Parser
{
    public sealed class FilePipeReader : PipeReader
    {
        private readonly SafeFileHandle _fileHandle;
        private readonly ArrayPool<byte> _pool;
        private readonly int _bufferSize;

        private PipeSegment? _activeHead;
        private PipeSegment? _activeTail;
        private PipeSegment? _freeSegments;

        private int _headIndex;
        private long _fileOffset;
        private bool _isCompleted;
        private bool _needsMoreData;

        public FilePipeReader(SafeFileHandle fileHandle, int bufferSize = 4 * 1024 * 1024)
        {
            _fileHandle = fileHandle;
            _bufferSize = bufferSize;
            _pool = ArrayPool<byte>.Shared;
            _fileOffset = 0;
            _isCompleted = false;
            _needsMoreData = true;
        }

        private PipeSegment AllocateSegment()
        {
            if (_freeSegments != null)
            {
                var segment = _freeSegments;
                _freeSegments = segment._next;
                segment.Reset();
                return segment;
            }
            return new PipeSegment(_pool, _bufferSize);
        }

        public override async ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default)
        {
            if (_isCompleted && _activeHead == null)
            {
                return new ReadResult(default, isCanceled: false, isCompleted: true);
            }

            if (!_needsMoreData && _activeHead != null)
            {
                return new ReadResult(CreateSequence(), isCanceled: false, isCompleted: false);
            }

            var newSegment = AllocateSegment();
            Memory<byte> targetBuffer = newSegment.AvailableMemory.Slice(newSegment.End);

            int bytesRead = await RandomAccess.ReadAsync(_fileHandle, targetBuffer, _fileOffset, cancellationToken);

            if (bytesRead == 0)
            {
                _isCompleted = true;
                newSegment.ReturnToPool();
            }
            else
            {
                _fileOffset += bytesRead;
                newSegment.Advance(bytesRead);

                if (bytesRead < targetBuffer.Length)
                {
                    _isCompleted = true;
                }

                if (_activeTail != null)
                {
                    _activeTail.SetNext(newSegment);
                    _activeTail = newSegment;
                }
                else
                {
                    _activeHead = _activeTail = newSegment;
                    _headIndex = 0;
                }
            }

            _needsMoreData = false;
            return new ReadResult(CreateSequence(), isCanceled: false, isCompleted: _isCompleted);
        }

        // ─── THE NEW SKIP MAGIC ──────────────────────────────────────────

        public void SkipForward(long bytesToSkip)
        {
            if (bytesToSkip <= 0) return;

            long bufferedBytes = GetBufferedBytes();

            if (bytesToSkip <= bufferedBytes)
            {
                // The junk data is already in RAM. Just advance our cursors over it.
                AdvanceInternal(bytesToSkip);
            }
            else
            {
                // The junk data extends past our memory. 
                // 1. Calculate what is still on the disk.
                long bytesStillOnDisk = bytesToSkip - bufferedBytes;

                // 2. Trash all current memory buffers (they only contain junk anyway)
                ClearActiveSegments();

                // 3. Jump the physical NVMe file pointer!
                _fileOffset += bytesStillOnDisk;

                // 4. Force the next ReadAsync to pull fresh data from the new offset
                _needsMoreData = true;

                // If we hit EOF via skip, we should ideally check file length, 
                // but the next ReadAsync returning 0 bytes will handle it naturally.
            }
        }

        private long GetBufferedBytes()
        {
            long count = 0;
            var current = _activeHead;
            bool isFirst = true;

            while (current != null)
            {
                count += isFirst ? (current.Length - _headIndex) : current.Length;
                isFirst = false;
                current = (PipeSegment?)current.Next;
            }
            return count;
        }

        private void AdvanceInternal(long bytesToConsume)
        {
            long remaining = bytesToConsume;

            while (_activeHead != null && remaining > 0)
            {
                int availableInHead = _activeHead.Length - _headIndex;

                if (remaining >= availableInHead)
                {
                    // Consume this entire segment
                    remaining -= availableInHead;

                    var oldHead = _activeHead;
                    _activeHead = (PipeSegment?)oldHead.Next;
                    _headIndex = 0;

                    oldHead.Reset();
                    oldHead._next = _freeSegments;
                    _freeSegments = oldHead;

                    if (_activeHead == null)
                    {
                        _activeTail = null;
                        _needsMoreData = true;
                    }
                }
                else
                {
                    // Consume partial segment
                    _headIndex += (int)remaining;
                    remaining = 0;
                }
            }
        }

        // ─────────────────────────────────────────────────────────────────

        public override void AdvanceTo(SequencePosition consumed) => AdvanceTo(consumed, consumed);

        public override void AdvanceTo(SequencePosition consumed, SequencePosition examined)
        {
            var consumedSegment = (PipeSegment)consumed.GetObject()!;
            int consumedIndex = consumed.GetInteger();

            var examinedSegment = (PipeSegment)examined.GetObject()!;
            int examinedIndex = examined.GetInteger();

            _needsMoreData = (_activeTail != null && examinedSegment == _activeTail && examinedIndex == _activeTail.Length);

            while (_activeHead != null && _activeHead != consumedSegment)
            {
                var oldHead = _activeHead;
                _activeHead = (PipeSegment?)oldHead.Next;

                oldHead.Reset();
                oldHead._next = _freeSegments;
                _freeSegments = oldHead;
            }

            _headIndex = consumedIndex;

            if (_activeHead != null && _headIndex == _activeHead.Length)
            {
                var oldHead = _activeHead;
                _activeHead = (PipeSegment?)oldHead.Next;
                _headIndex = 0;

                oldHead.Reset();
                oldHead._next = _freeSegments;
                _freeSegments = oldHead;

                if (_activeHead == null) _activeTail = null;
            }
        }

        private ReadOnlySequence<byte> CreateSequence()
        {
            if (_activeHead == null || _activeTail == null) return default;
            return new ReadOnlySequence<byte>(_activeHead, _headIndex, _activeTail, _activeTail.Length);
        }

        private void ClearActiveSegments()
        {
            var current = _activeHead;
            while (current != null)
            {
                var next = (PipeSegment?)current.Next;
                current.Reset();
                current._next = _freeSegments;
                _freeSegments = current;
                current = next;
            }
            _activeHead = _activeTail = null;
            _headIndex = 0;
        }

        public override bool TryRead(out ReadResult result) => throw new NotSupportedException();
        public override void CancelPendingRead() { }

        public override void Complete(Exception? exception = null)
        {
            ClearActiveSegments();

            var currentFree = _freeSegments;
            while (currentFree != null)
            {
                var next = currentFree._next;
                currentFree.ReturnToPool();
                currentFree = next;
            }
            _freeSegments = null;
        }
    }
}
