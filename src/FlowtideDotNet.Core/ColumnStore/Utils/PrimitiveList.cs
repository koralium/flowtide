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
using System;
using System.Buffers.Binary;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Utils
{
    internal unsafe class PrimitiveList<T> : IDisposable
        where T: unmanaged
    {
        private void* _data;
        private int _dataLength;
        private int _length;
        private bool _disposedValue;
        private readonly IMemoryAllocator _memoryAllocator;
        private IMemoryOwner<byte>? _memoryOwner;

        public PrimitiveList(IMemoryAllocator memoryAllocator)
        {
            _data = null;
            _memoryAllocator = memoryAllocator;
        }

        public PrimitiveList(IMemoryOwner<byte> memory, int length, IMemoryAllocator memoryAllocator)
        {
            _memoryOwner = memory;
            _data = _memoryOwner.Memory.Pin().Pointer;
            _dataLength = memory.Memory.Length / sizeof(T);
            _length = length;
            _memoryAllocator = memoryAllocator;
        }

        public Memory<byte> Memory => _memoryOwner?.Memory ?? new Memory<byte>();

        public PrimitiveList(void* data, int dataLength, int length, IMemoryAllocator memoryAllocator)
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
                var allocSize = newLength * sizeof(T);

                if (_memoryOwner == null)
                {
                    _memoryOwner = _memoryAllocator.Allocate(allocSize, 64);
                    _data = _memoryOwner.Memory.Pin().Pointer;
                }
                else
                {
                    var newMemory = _memoryAllocator.Allocate(allocSize, 64);
                    var newPtr = newMemory.Memory.Pin().Pointer;
                    NativeMemory.Copy(_data, newPtr, (nuint)(_dataLength * sizeof(T)));
                    _data = newPtr;
                    _memoryOwner.Dispose();
                    _memoryOwner = newMemory;
                }
                _dataLength = newLength;
            }
        }

        private Span<T> AccessSpan => new Span<T>(_data, _dataLength);

        public void Add(T value)
        {
            EnsureCapacity(_length + 1);
            AccessSpan[_length++] = value;
        }

        public void InsertAt(int index, T value)
        {
            if (index == _length)
            {
                Add(value);
                return;
            }

            EnsureCapacity(_length + 1);
            var span = AccessSpan;
            span.Slice(index, _length - index).CopyTo(span.Slice(index + 1, _length - index));
            span[index] = value;
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

        public T Get(in int index)
        {
            var span = AccessSpan;
            return span[index];
        }

        public void Update(in int index, in T value)
        {
            AccessSpan[index] = value;
        }

        public T this[int index]
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

        ~PrimitiveList()
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
