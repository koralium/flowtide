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

using FlowtideDotNet.Core.ColumnStore.Memory;
using Google.Protobuf.Reflection;
using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Utils
{
    /// <summary>
    /// A list of longs that can be used in a column store.
    /// All values are stored in little endian to allow for fast serialization and deserialization.
    /// Uses native memory aligned to 64 bytes to allow for fast avx operations.
    /// </summary>
    internal unsafe class NativeLongList : IDisposable
    {
        private void* _data;
        private int _dataLength;
        private int _length;
        private bool _disposedValue;
        private readonly IMemoryAllocator _memoryAllocator;
        private IFlowtideMemoryOwner? _memoryOwner;

        public NativeLongList(IMemoryAllocator memoryAllocator)
        {
            _data = null;
            _memoryAllocator = memoryAllocator;
        }

        public Memory<byte> Memory => _memoryOwner?.Memory ?? new Memory<byte>();

        public NativeLongList(void* data, int dataLength, int length, IMemoryAllocator memoryAllocator)
        {
            _data = data;
            _dataLength = dataLength;
            _length = length;
            _memoryAllocator = memoryAllocator;
        }

        private void EnsureCapacity(int length)
        {
            if (_dataLength < length)
            {
                var newLength = length * 2;
                if (newLength < 64)
                {
                    newLength = 64;
                }
                var allocSize = newLength * sizeof(long);

                if (_memoryOwner == null)
                {
                    _memoryOwner = _memoryAllocator.Allocate(allocSize, 64);
                    _data = _memoryOwner.Memory.Pin().Pointer;
                }
                else
                {
                    _memoryOwner.Reallocate(allocSize);
                    _data = _memoryOwner.Memory.Pin().Pointer;
                }
                _dataLength = newLength;
            }
        }

        private Span<long> AccessSpan => new Span<long>(_data, _dataLength);

        public void Add(long value)
        {
            EnsureCapacity(_length + 1);
            if (BitConverter.IsLittleEndian)
            {
                AccessSpan[_length++] = value;
            }
            else
            {
                AccessSpan[_length++] = BinaryPrimitives.ReverseEndianness(value);
            }
        }

        public void InsertAt(int index, long value)
        {
            if (index == _length)
            {
                Add(value);
                return;
            }
            
            EnsureCapacity(_length + 1);
            var span = AccessSpan;
            span.Slice(index, _length - index).CopyTo(span.Slice(index + 1, _length - index));
            if (BitConverter.IsLittleEndian)
            {
                span[index] = value;
            }
            else
            {
                span[index] = BinaryPrimitives.ReverseEndianness(value);
            }
            _length++;
        }

        public void RemoveAt(int index)
        {
            var span = AccessSpan;
            span.Slice(index + 1, _length - index - 1).CopyTo(span.Slice(index, _length - index - 1));
            _length--;
        }

        public void RemoveRange(int index, int count)
        {
            var span = AccessSpan;
            var length = _length - index - count;
            span.Slice(index + count, length).CopyTo(span.Slice(index));
            _length -= count;
        }

        public long Get(in int index)
        {
            var span = AccessSpan;
            if (BitConverter.IsLittleEndian)
            {
                return span[index];
            }
            else
            {
                return BinaryPrimitives.ReverseEndianness(span[index]);
            }
        }

        public void Update(in int index, in long value)
        {
            var span = AccessSpan;
            if (BitConverter.IsLittleEndian)
            {
                span[index] = value;
            }
            else
            {
                span[index] = BinaryPrimitives.ReverseEndianness(value);
            }
        }

        public long this[int index]
        {
            get
            {
                return Get(index);
            }
            set
            {
                Update(index, value);
            }
        }

        public int Count => _length;

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposedValue)
            {
                if (_memoryOwner != null)
                {
                    _memoryOwner.Dispose();
                    _memoryOwner = null;
                    _data = null;
                }
                
                _disposedValue = true;
            }
        }

        ~NativeLongList()
        {
            Dispose(disposing: false);
        }

        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
