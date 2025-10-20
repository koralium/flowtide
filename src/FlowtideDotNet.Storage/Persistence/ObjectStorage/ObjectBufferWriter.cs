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

using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage
{
    /// <summary>
    /// Buffer writer that allows writing to a buffer that will be written to object storage.
    /// It is possible to read from the buffer after writing but before flushing to storage.
    /// </summary>
    internal class ObjectBufferWriter : PipeReader, IBufferWriter<byte>
    {
        private const int InitialSegmentSize = 4 * 1024 * 1024;
        private BufferSegment _head;
        private BufferSegment _end;
        private int endIndex = 0;
        private object _lock = new object();
        public ObjectBufferWriter()
        {
            _head = new BufferSegment(MemoryPool<byte>.Shared.Rent(InitialSegmentSize));
            _end = _head;
        }

        public void Advance(int count)
        {
            endIndex += count;
        }

        public BufferSegment CurrentSegment => _end;
        public int CurrentIndex => endIndex;

        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            EnsureCapacity(sizeHint);
            return _end.AvailableMemory.Slice(endIndex);
        }

        private void EnsureCapacity(int sizeHint)
        {
            if (endIndex + sizeHint > _end.AvailableMemory.Length)
            {
                var newSegment = new BufferSegment(MemoryPool<byte>.Shared.Rent(Math.Max(sizeHint, InitialSegmentSize)));
                _end.SetNext(newSegment);
                _end = newSegment;
                _end.End = endIndex;
                endIndex = 0;
            }
        }

        public void Write(long key, ReadOnlySpan<byte> value)
        {
            Span<byte> span;
            BufferSegment segment;
            lock (_lock)
            {
                // Ensure that there is enough size for the data
                EnsureCapacity(value.Length);
                segment = _end;
                span = _end.AvailableMemory.Span.Slice(endIndex);
                endIndex += value.Length;
            }

            // Copy data is done outside of the lock to minimize lock time
            // This allows parallel copies to happen
            value.CopyTo(span);
        }

        public Span<byte> GetSpan(int sizeHint = 0)
        {
            EnsureCapacity(sizeHint);
            return _end.AvailableMemory.Span.Slice(endIndex);
        }

        public override bool TryRead(out ReadResult result)
        {
            if (_head.Next != null)
            {
                result = new ReadResult(new ReadOnlySequence<byte>(_head.AvailableMemory), false, true);
                return true;
            }
            result = default;
            return false;
        }

        public override ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public override void AdvanceTo(SequencePosition consumed)
        {
            throw new NotImplementedException();
        }

        public override void AdvanceTo(SequencePosition consumed, SequencePosition examined)
        {
            var obj = consumed.GetObject();
            var index = consumed.GetInteger();
            throw new NotImplementedException();
        }

        public override void CancelPendingRead()
        {
            throw new NotImplementedException();
        }

        public override void Complete(Exception? exception = null)
        {
            throw new NotImplementedException();
        }
    }
}
