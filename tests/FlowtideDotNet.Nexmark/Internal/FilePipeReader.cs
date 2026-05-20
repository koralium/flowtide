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
    public sealed class FilePipeReader : PipeReader, IDisposable
    {
        private readonly SafeFileHandle _fileHandle;
        private readonly ArrayPool<byte> _pool;
        private readonly int _bufferSize;
        private readonly CancellationTokenSource _internalCts;

        // Sequence State (Only contains fully completed reads)
        private PipeSegment? _activeHead;
        private PipeSegment? _activeTail;
        private PipeSegment? _freeSegments;

        // Prefetch State (The background worker)
        private ValueTask<int> _prefetchTask;
        private PipeSegment? _prefetchSegment;
        private Memory<byte> _prefetchTargetBuffer;
        private bool _hasPrefetchStarted;

        // Cursors
        private int _headIndex;
        private long _fileOffset;
        private bool _isCompleted;
        private bool _needsMoreData;
        private bool _isDisposed;

        public FilePipeReader(SafeFileHandle fileHandle, int bufferSize = 4 * 1024 * 1024)
        {
            _fileHandle = fileHandle;
            _bufferSize = bufferSize;
            _pool = ArrayPool<byte>.Shared;
            _fileOffset = 0;
            _isCompleted = false;
            _needsMoreData = true;
            _hasPrefetchStarted = false;
            _internalCts = new CancellationTokenSource();
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
            ObjectDisposedException.ThrowIf(_isDisposed, this);

            if (_isCompleted && _activeHead == null)
            {
                return new ReadResult(default, isCanceled: false, isCompleted: true);
            }

            if (!_hasPrefetchStarted && !_isCompleted)
            {
                StartNextPrefetch();
            }

            if (!_needsMoreData && _activeHead != null)
            {
                return new ReadResult(CreateSequence(), isCanceled: false, isCompleted: false);
            }

            if (_hasPrefetchStarted)
            {
                int bytesRead;
                var completedSegment = _prefetchSegment!;

                try
                {
                    // Link the user's cancellation token with our internal one
                    using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _internalCts.Token);

                    // Note: RandomAccess.ReadAsync might ignore cancellation if it's deeply queued in the OS, 
                    // but this ensures we try to abort at the framework level.
                    bytesRead = await _prefetchTask.AsTask().WaitAsync(linkedCts.Token);
                }
                catch (Exception)
                {
                    // If it fails or cancels, the memory is unsafe. We abort and safely orphan/clean it.
                    _hasPrefetchStarted = false;
                    _prefetchSegment = null;
                    _isCompleted = true;

                    // Return to pool to prevent leak. 
                    // (If the OS is still holding it, we rely on the internal Complete() logic or process crash anyway)
                    completedSegment.ReturnToPool();
                    throw;
                }

                _hasPrefetchStarted = false;
                _prefetchSegment = null;

                if (bytesRead == 0)
                {
                    _isCompleted = true;
                    completedSegment.Reset();
                    completedSegment._next = _freeSegments;
                    _freeSegments = completedSegment;
                }
                else
                {
                    _fileOffset += bytesRead;
                    completedSegment.Advance(bytesRead);

                    if (bytesRead < _prefetchTargetBuffer.Length)
                    {
                        _isCompleted = true;
                    }

                    if (_activeTail != null)
                    {
                        _activeTail.SetNext(completedSegment);
                        _activeTail = completedSegment;
                    }
                    else
                    {
                        _activeHead = _activeTail = completedSegment;
                        _headIndex = 0;
                    }
                }
            }

            if (!_isCompleted && !_hasPrefetchStarted)
            {
                StartNextPrefetch();
            }

            return new ReadResult(CreateSequence(), isCanceled: false, isCompleted: _isCompleted && !_hasPrefetchStarted);
        }

        private void StartNextPrefetch()
        {
            _prefetchSegment = AllocateSegment();
            _prefetchTargetBuffer = _prefetchSegment.AvailableMemory.Slice(_prefetchSegment.End);

            _prefetchTask = RandomAccess.ReadAsync(_fileHandle, _prefetchTargetBuffer, _fileOffset, _internalCts.Token);
            _hasPrefetchStarted = true;
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

        public override void CancelPendingRead()
        {
            _internalCts.Cancel();
        }

        public override void Complete(Exception? exception = null)
        {
            if (_isCompleted) return;
            _isCompleted = true;

            // 1. Cancel any ongoing OS read immediately
            _internalCts.Cancel();

            // 2. Safe async handoff for the prefetch memory
            if (_hasPrefetchStarted)
            {
                var orphanedTask = _prefetchTask;
                var orphanedSegment = _prefetchSegment;

                _hasPrefetchStarted = false;
                _prefetchSegment = null;

                // Fire-and-forget background task to wait for the OS to release the memory
                Task.Run(async () =>
                {
                    try
                    {
                        await orphanedTask;
                    }
                    catch
                    {
                        // Ignore cancellation/I/O errors from the aborted task
                    }
                    finally
                    {
                        // ONLY return to the pool once we are 100% certain the OS is done with the pointer
                        orphanedSegment?.ReturnToPool();
                    }
                });
            }

            // 3. Synchronous cleanup for memory we know the OS isn't touching
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

        public void Dispose()
        {
            if (!_isDisposed)
            {
                Complete();
                _internalCts.Dispose();
                _isDisposed = true;
            }
        }
    }
}
