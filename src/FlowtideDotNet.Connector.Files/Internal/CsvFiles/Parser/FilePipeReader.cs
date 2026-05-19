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

        // Sequence State
        private PipeSegment? _activeHead;
        private PipeSegment? _activeTail;
        private PipeSegment? _freeSegments;

        // Cursors
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

            // Fallback: Allocate a new segment
            return new PipeSegment(_pool, _bufferSize);
        }

        public override async ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default)
        {
            if (_isCompleted)
            {
                return new ReadResult(CreateSequence(), isCanceled: false, isCompleted: true);
            }

            if (!_needsMoreData)
            {
                return new ReadResult(CreateSequence(), isCanceled: false, isCompleted: false);
            }

            if (_activeTail == null || _activeTail.Length == _activeTail.AvailableMemory.Length)
            {
                var newSegment = AllocateSegment();

                if (_activeTail != null)
                {
                    _activeTail.SetNext(newSegment);
                    _activeTail = newSegment;
                }
                else
                {
                    _activeHead = _activeTail = newSegment;
                    _headIndex = 0; // Reset read cursor for the new head
                }
            }

            Memory<byte> targetBuffer = _activeTail.AvailableMemory.Slice(_activeTail.End);

            int bytesRead = await RandomAccess.ReadAsync(_fileHandle, targetBuffer, _fileOffset, cancellationToken);

            if (bytesRead == 0)
            {
                _isCompleted = true;
            }
            else
            {
                _fileOffset += bytesRead;
                _activeTail.Advance(bytesRead);
                if (bytesRead < targetBuffer.Length)
                {
                    _isCompleted = true;
                }
            }

            return new ReadResult(CreateSequence(), isCanceled: false, isCompleted: _isCompleted);
        }

        public override void AdvanceTo(SequencePosition consumed)
        {
            AdvanceTo(consumed, consumed);
        }

        public override void AdvanceTo(SequencePosition consumed, SequencePosition examined)
        {
            var consumedSegment = (PipeSegment)consumed.GetObject()!;
            int consumedIndex = consumed.GetInteger();

            var examinedSegment = (PipeSegment)examined.GetObject()!;
            int examinedIndex = examined.GetInteger();

            _needsMoreData = (_activeTail != null && examinedSegment == _activeTail && examinedIndex == _activeTail.Length);

            // 2. NOW detach and recycle everything strictly BEFORE the consumed segment 
            while (_activeHead != null && _activeHead != consumedSegment)
            {
                var oldHead = _activeHead;
                _activeHead = (PipeSegment?)oldHead.Next;

                oldHead.Reset();
                oldHead._next = _freeSegments;
                _freeSegments = oldHead;
            }

            _headIndex = consumedIndex;

            // 3. Immediate cleanup of a fully consumed head
            if (_activeHead != null && _headIndex == _activeHead.Length)
            {
                var oldHead = _activeHead;
                _activeHead = (PipeSegment?)oldHead.Next;
                _headIndex = 0;

                oldHead.Reset();
                oldHead._next = _freeSegments;
                _freeSegments = oldHead;

                if (_activeHead == null)
                {
                    _activeTail = null;
                }
            }
        }

        private ReadOnlySequence<byte> CreateSequence()
        {
            if (_activeHead == null || _activeTail == null)
            {
                return default;
            }

            return new ReadOnlySequence<byte>(
                _activeHead,
                _headIndex,
                _activeTail,
                _activeTail.Length
            );
        }

        public override bool TryRead(out ReadResult result) => throw new NotSupportedException();
        public override void CancelPendingRead() { }
        public override void Complete(Exception? exception = null)
        {
            // Clean up the remaining active segments and free list
            var current = _activeHead;
            while (current != null)
            {
                var next = (PipeSegment?)current.Next;
                current.ReturnToPool();
                current = next;
            }

            current = _freeSegments;
            while (current != null)
            {
                var next = current._next;
                current.ReturnToPool();
                current = next;
            }

            _activeHead = _activeTail = _freeSegments = null;
        }
    }
}
