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
using System.Collections;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace FlowtideDotNet.Core.ColumnStore.Utils
{
    internal unsafe class TypeList : IDisposable, IReadOnlyList<sbyte>
    {
        private void* _data;
        private int _dataLength;
        private int _length;
        private bool _disposedValue;
        private readonly IMemoryAllocator _memoryAllocator;
        private IMemoryOwner<byte>? _memoryOwner;
        private int _rentCounter;

        public TypeList(IMemoryAllocator memoryAllocator)
        {
            _data = null;
            _memoryAllocator = memoryAllocator;
        }

        public TypeList(IMemoryOwner<byte> memory, int length, IMemoryAllocator memoryAllocator)
        {
            _memoryOwner = memory;
            _data = _memoryOwner.Memory.Pin().Pointer;
            _dataLength = memory.Memory.Length / sizeof(sbyte);
            _length = length;
            _memoryAllocator = memoryAllocator;
        }

        public Span<sbyte> Span => new Span<sbyte>(_data, _length);

        public Memory<byte> Memory => _memoryOwner?.Memory ?? new Memory<byte>();

        public Memory<byte> SlicedMemory => _memoryOwner?.Memory.Slice(0, _length * sizeof(sbyte)) ?? new Memory<byte>();

        public TypeList(void* data, int dataLength, int length, IMemoryAllocator memoryAllocator)
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
                var allocSize = newLength * sizeof(sbyte);

                if (_memoryOwner == null)
                {
                    _memoryOwner = _memoryAllocator.Allocate(allocSize, 64);
                    _data = _memoryOwner.Memory.Pin().Pointer;
                }
                else
                {
                    _memoryOwner = _memoryAllocator.Realloc(_memoryOwner, allocSize, 64);
                    _data = _memoryOwner.Memory.Pin().Pointer;
                }
                _dataLength = newLength;
            }
        }

        private Span<sbyte> AccessSpan => new Span<sbyte>(_data, _dataLength);

        public void Add(sbyte value)
        {
            EnsureCapacity(_length + 1);
            AccessSpan[_length++] = value;
        }

        public void AddRangeFrom(TypeList list, int index, int count)
        {
            EnsureCapacity(_length + count);
            var span = AccessSpan;
            var sourceSpan = list.AccessSpan;
            sourceSpan.Slice(index, count).CopyTo(span.Slice(_length, count));
            _length += count;
        }

        public void InsertAt(int index, sbyte value)
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

        public void InsertRangeFrom(int index, TypeList other, int start, int count)
        {
            EnsureCapacity(_length + count);
            var span = AccessSpan;
            var sourceSpan = other.AccessSpan;
            span.Slice(index, _length - index).CopyTo(span.Slice(index + count, _length - index));
            sourceSpan.Slice(start, count).CopyTo(span.Slice(index, count));
            _length += count;
        }

        public void InsertRangeFrom(int index, TypeList other, int start, int count, Span<sbyte> mapping, int typeCount)
        {
            EnsureCapacity(_length + count);
            var span = AccessSpan;
            var sourceSpan = other.AccessSpan;

            // Copy existing data
            span.Slice(index, _length - index).CopyTo(span.Slice(index + count, _length - index));

            int i = 0;
            if (Avx2.IsSupported && typeCount <= 8)
            {
                // Code that runs it using avx

                fixed (sbyte* pDest = span)
                fixed (sbyte* pSource = sourceSpan)
                fixed (sbyte* pMapping = mapping)
                {
                    Vector128<sbyte> mappingVector = Vector128.Load(pMapping);

                    int vectorSize = Vector128<sbyte>.Count;
                    
                    for (; i <= count - vectorSize; i += vectorSize)
                    {
                        Vector128<sbyte> sourceVector = Vector128.Load(pSource + start + i);
                        Vector128<sbyte> resultVector = Avx.Shuffle(mappingVector, sourceVector);
                        Avx.Store(pDest + index + i, resultVector);
                    }
                }
            }

            // Insert the new data
            for (; i < count; i++)
            {
                span[index + i] = mapping[sourceSpan[start + i]];
            }
            _length += count;
        }

        public void InsertStaticRange(int index, sbyte value, int count)
        {
            EnsureCapacity(_length + count);
            var span = AccessSpan;
            span.Slice(index, _length - index).CopyTo(span.Slice(index + count, _length - index));
            for (var i = 0; i < count; i++)
            {
                span[index + i] = value;
            }
            _length += count;
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

        public sbyte Get(in int index)
        {
            var span = AccessSpan;
            return span[index];
        }

        public ref sbyte GetRef(scoped in int index)
        {
            var span = AccessSpan;
            return ref span[index];
        }

        public void Update(in int index, in sbyte value)
        {
            AccessSpan[index] = value;
        }

        public sbyte this[int index]
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

        ~TypeList()
        {
            Dispose(disposing: false);
        }

        public void Dispose()
        {
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        private IEnumerable<sbyte> GetEnumerable()
        {
            for (var i = 0; i < _length; i++)
            {
                yield return Get(i);
            }
        }

        public IEnumerator<sbyte> GetEnumerator()
        {
            return GetEnumerable().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerable().GetEnumerator();
        }

        public void Rent(int count)
        {
            Interlocked.Add(ref _rentCounter, count);
        }

        public void Return()
        {
            var result = Interlocked.Decrement(ref _rentCounter);
            if (result <= 0)
            {
                Dispose();
            }
        }

        public void Clear()
        {
            _length = 0;
        }

        public TypeList Copy(IMemoryAllocator memoryAllocator)
        {
            var mem = SlicedMemory;
            var newMem = memoryAllocator.Allocate(mem.Length, 64);
            mem.Span.CopyTo(newMem.Memory.Span);

            return new TypeList(newMem, _length, memoryAllocator);
        }
    }
}
