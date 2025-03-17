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
using System.Buffers;
using System.Diagnostics;

namespace FlowtideDotNet.Storage.FileCache.Internal
{
    internal class FileCacheBufferWriter : IBufferWriter<byte>, IDisposable
    {
        private readonly IMemoryAllocator memoryAllocator;
        private readonly int sectorSize;
        private IMemoryOwner<byte>? _memory;
        private int _position;

        public Memory<byte> Memory => SliceMemory();

        public int Position => _position;

        private Memory<byte> SliceMemory()
        {
            var alignedLength = (_position + sectorSize - 1) / sectorSize * sectorSize;
            return _memory?.Memory.Slice(0, alignedLength) ?? Memory<byte>.Empty;
        }

        public FileCacheBufferWriter(IMemoryAllocator memoryAllocator, int sectorSize)
        {
            this.memoryAllocator = memoryAllocator;
            this.sectorSize = sectorSize;
        }
        public void Advance(int count)
        {
            _position += count;
        }

        public void Reset(int estimatedSize)
        {
            var alignedLength = (estimatedSize + sectorSize - 1) / sectorSize * sectorSize;
            if (_memory == null)
            {
                _memory = memoryAllocator.Allocate(alignedLength, sectorSize);
            }
            else if (_memory.Memory.Length < alignedLength)
            {
                _memory = memoryAllocator.Realloc(_memory, alignedLength, sectorSize);
            }
            _position = 0;
        }

        public void Dispose()
        {
            if (_memory != null)
            {
                _memory.Dispose();
            }
        }

        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            Debug.Assert(_memory != null);
            if (sizeHint < 4096)
            {
                sizeHint = 4096;
            }
            var newLength = _position + sizeHint;
            var alignedLength = (newLength + sectorSize - 1) / sectorSize * sectorSize;
            if (_memory.Memory.Length < alignedLength)
            {
                _memory = memoryAllocator.Realloc(_memory, alignedLength,  sectorSize);
            }
            return _memory.Memory.Slice(_position);
        }

        public Span<byte> GetSpan(int sizeHint = 0)
        {
            return GetMemory(sizeHint).Span;
        }
    }
}
