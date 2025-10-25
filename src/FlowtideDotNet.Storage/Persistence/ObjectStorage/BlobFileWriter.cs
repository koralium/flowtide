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
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Persistence.ObjectStorage
{
    internal class BlobFileWriter : IBufferWriter<byte>
    {
        private readonly MemoryPool<byte> _memoryPool;
        private const int InitialSegmentSize = 4 * 1024 * 1024;
        private BufferSegment _head;
        private BufferSegment _end;
        private int endIndex = 0;
        private int _writtenBytes = 0;

        public BlobFileWriter(MemoryPool<byte> memoryPool)
        {
            this._memoryPool = memoryPool;
            _head = new BufferSegment(memoryPool.Rent(InitialSegmentSize));
            _end = _head;
        }

        public ReadOnlySequence<byte> WrittenData => new ReadOnlySequence<byte>(_head, 0, _end, endIndex);

        public BufferSegment CurrentSegment => _end;
        public int CurrentIndex => endIndex;

        public int WrittenLength => _writtenBytes;

        public void Advance(int count)
        {
            endIndex += count;
            _writtenBytes += count;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureCapacity(ref readonly int sizeHint)
        {
            if (endIndex + sizeHint > _end.AvailableMemory.Length)
            {
                var newSegment = new BufferSegment(_memoryPool.Rent(Math.Max(sizeHint, InitialSegmentSize)));
                _end.SetNext(newSegment);
                _end = newSegment;
                _end.End = endIndex;
                endIndex = 0;
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
    }
}
