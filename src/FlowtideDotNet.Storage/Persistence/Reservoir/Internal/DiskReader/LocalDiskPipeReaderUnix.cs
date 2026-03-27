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
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.Reservoir.Internal.DiskReader
{
    internal class LocalDiskPipeReaderUnix : PipeReader, IDisposable
    {
        private readonly LocalDiskReaderUnix localDiskReader;
        private readonly IMemoryAllocator memoryAllocator;
        private int _readFromFileIndex = 0;
        private bool _hasReadAll;
        private BufferSegment? _head;
        private BufferSegment? _end;
        private int _endIndex = 0;
        private SequencePosition _advancedPosition;
        private bool disposedValue;

        private IMemoryOwner<byte>? _freeMemory;

        public ReadOnlySequence<byte> WrittenData => GetWrittenData();

        public LocalDiskPipeReaderUnix(LocalDiskReaderUnix localDiskReader, IMemoryAllocator memoryAllocator)
        {
            this.localDiskReader = localDiskReader;
            this.memoryAllocator = memoryAllocator;
        }

        public ReadOnlySequence<byte> GetWrittenData()
        {
            if (_head == null || _end == null)
            {
                return ReadOnlySequence<byte>.Empty;
            }
            return new ReadOnlySequence<byte>(_head, 0, _end, _endIndex);
        }

        public override void AdvanceTo(SequencePosition consumed)
        {
            Debug.Assert(_head != null);

            var obj = consumed.GetObject();

            if (obj is BufferSegment bufferSegment)
            {
                while (bufferSegment != _head && _head != null)
                {
                    var next = _head._next;
                    if (_freeMemory == null)
                    {
                        _freeMemory = _head.MemoryOwner;
                        // Remove memory from ownership in the segment so we can reuse it
                        _head.ClearOwner();
                    }
                    _head.Dispose();

                    _head = next;
                }
            }

            _advancedPosition = consumed;
        }

        public override void AdvanceTo(SequencePosition consumed, SequencePosition examined)
        {
            Debug.Assert(_head != null);
            var obj = consumed.GetObject();

            if (obj is BufferSegment bufferSegment)
            {
                while (bufferSegment != _head && _head != null)
                {
                    var next = _head._next;
                    _head.Dispose();
                    _head = next;
                }
            }

            _advancedPosition = consumed;
        }

        public override void CancelPendingRead()
        {
        }

        public override void Complete(Exception? exception = null)
        {
            Dispose();
        }

        public override ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default)
        {
            if (TryRead(out var result))
            {
                return ValueTask.FromResult<ReadResult>(result);
            }
            throw new InvalidOperationException("Could not read from file");
        }

        public override bool TryRead(out ReadResult result)
        {
            if (_hasReadAll)
            {
                result = new ReadResult(WrittenData.Slice(_advancedPosition), isCanceled: false, isCompleted: true);
                return true;
            }

            IMemoryOwner<byte> buffer;
            if (_freeMemory != null)
            {
                buffer = _freeMemory;
                _freeMemory = null;
            }
            else
            {
                buffer = memoryAllocator.Allocate(localDiskReader.Alignment * 4, localDiskReader.Alignment);
            }

            if (_head == null)
            {
                _head = new BufferSegment(buffer, buffer.Memory.Length);
                _end = _head;
            }
            else
            {
                Debug.Assert(_end != null, "end was null even though it should not after head is set.");
                var segment = new BufferSegment(buffer, buffer.Memory.Length);
                _end.SetNext(segment);
                _end = segment;
                
            }

            var readBytes = localDiskReader.ReadToBuffer(_readFromFileIndex, buffer.Memory);
            _readFromFileIndex += readBytes;
            _endIndex = readBytes;
            if (readBytes < buffer.Memory.Length)
            {
                // We have read all the data from the file, we can complete the reader
                _hasReadAll = true;
            }

            result = new ReadResult(WrittenData.Slice(_advancedPosition), isCanceled: false, isCompleted: _hasReadAll);
            return true;
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

                if (_freeMemory != null)
                {
                    _freeMemory.Dispose();
                    _freeMemory = null;
                }

                disposedValue = true;
            }
        }

        ~LocalDiskPipeReaderUnix()
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
