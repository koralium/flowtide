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
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Memory
{
    public class NativeBufferWriter : IBufferWriter<byte>, IDisposable
    {
        private readonly IMemoryAllocator _memoryAllocator;
        private int _currentLength;
        private int _index;
        private IMemoryOwner<byte>? _memoryOwner;
        private bool disposedValue;

        public Memory<byte> WrittenMemory => GetWrittenMemory();

        public Span<byte> WrittenSpan => GetWrittenSpan();

        public NativeBufferWriter(IMemoryAllocator memoryAllocator)
        {
            this._memoryAllocator = memoryAllocator;
            _currentLength = 0;
            _index = 0;
        }

        public void Advance(int count)
        {
            _index += count;
        }

        [MemberNotNull(nameof(_memoryOwner))]
        private void EnsureSize(int sizeHint)
        {
            if (_memoryOwner == null)
            {
                if (sizeHint < 64)
                {
                    sizeHint = 64;
                }
                _memoryOwner = _memoryAllocator.Allocate(sizeHint, 64);
            }
            else
            {
                if (_currentLength - _index < sizeHint)
                {
                    _memoryOwner = _memoryAllocator.Realloc(_memoryOwner, _currentLength + sizeHint, 64);
                }
            }
        }

        public Memory<byte> GetMemory(int sizeHint = 0)
        {
            EnsureSize(sizeHint);
            return _memoryOwner.Memory.Slice(_index, sizeHint);
        }

        public Span<byte> GetSpan(int sizeHint = 0)
        {
            EnsureSize(sizeHint);
            return _memoryOwner.Memory.Span.Slice(_index, sizeHint);
        }

        private Memory<byte> GetWrittenMemory()
        {
            return _memoryOwner?.Memory.Slice(0, _index) ?? Memory<byte>.Empty;
        }

        private Span<byte> GetWrittenSpan()
        {
            if (_memoryOwner == null)
            {
                return Span<byte>.Empty;
            }
            return _memoryOwner.Memory.Span.Slice(0, _index);
        }

        public void Reset()
        {
            _index = 0;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (_memoryOwner != null)
                {
                    _memoryOwner.Dispose();
                    _memoryOwner = null;
                }
                disposedValue = true;
            }
        }

        ~NativeBufferWriter()
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
