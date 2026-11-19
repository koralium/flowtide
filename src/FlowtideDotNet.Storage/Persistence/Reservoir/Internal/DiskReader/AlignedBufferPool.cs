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
using System.Collections.Concurrent;

namespace FlowtideDotNet.Storage.Persistence.Reservoir.Internal.DiskReader
{
    /// <summary>
    /// Aligned buffers for direct reads, shared by one manager's files.
    /// </summary>
    internal sealed class AlignedBufferPool : IDisposable
    {
        public const int Alignment = 4096;
        private const int MinimumSize = 64 * 1024;
        // A larger buffer is freed after use
        private const int LargestKept = 1024 * 1024;

        private readonly IMemoryAllocator _memoryAllocator;
        private readonly ConcurrentBag<IMemoryOwner<byte>> _buffers = new ConcurrentBag<IMemoryOwner<byte>>();
        private readonly int _maxKept = Environment.ProcessorCount * 2;
        private int _kept;
        private volatile bool _disposed;

        public AlignedBufferPool(IMemoryAllocator memoryAllocator)
        {
            _memoryAllocator = memoryAllocator;
        }

        public IMemoryOwner<byte> Rent(int size)
        {
            // Too large to keep, so take none from the pool
            if (size > LargestKept)
            {
                return _memoryAllocator.Allocate((size + Alignment - 1) / Alignment * Alignment, Alignment);
            }
            if (_buffers.TryTake(out var buffer))
            {
                Interlocked.Decrement(ref _kept);
                if (buffer.Memory.Length >= size)
                {
                    return buffer;
                }
                buffer.Dispose();
            }
            int wanted = Math.Max(size, MinimumSize);
            return _memoryAllocator.Allocate((wanted + Alignment - 1) / Alignment * Alignment, Alignment);
        }

        public void Return(IMemoryOwner<byte> buffer)
        {
            if (_disposed || buffer.Memory.Length > LargestKept)
            {
                buffer.Dispose();
                return;
            }
            if (Interlocked.Increment(ref _kept) > _maxKept)
            {
                Interlocked.Decrement(ref _kept);
                buffer.Dispose();
                return;
            }
            _buffers.Add(buffer);
            // A dispose may have emptied the bag in between
            Interlocked.MemoryBarrier();
            if (_disposed && _buffers.TryTake(out var late))
            {
                late.Dispose();
            }
        }

        public void Dispose()
        {
            _disposed = true;
            Interlocked.MemoryBarrier();
            while (_buffers.TryTake(out var buffer))
            {
                buffer.Dispose();
            }
        }
    }
}
