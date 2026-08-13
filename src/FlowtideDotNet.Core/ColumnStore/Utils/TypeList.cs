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
        private int _length;
        private bool _disposedValue;
        private readonly IMemoryAllocator _memoryAllocator;
        private FlowtideMemory _memory;
        private int _rentCounter;

        // Capacity in elements, derived from the block.
        private int DataLength => _memory.Length;

        public TypeList(IMemoryAllocator memoryAllocator)
        {
            _memoryAllocator = memoryAllocator;
        }

        public TypeList(IMemoryAllocator memoryAllocator, int initialCapacity)
        {
            _memoryAllocator = memoryAllocator;
            EnsureCapacity(initialCapacity);
        }

        public TypeList(FlowtideMemory memory, int length, IMemoryAllocator memoryAllocator)
        {
            _memory = memory;
            _length = length;
            _memoryAllocator = memoryAllocator;
        }

        public Span<sbyte> Span => new Span<sbyte>(_memory.Pointer, _length);

        public Memory<byte> Memory => GetViewMemory();

        public Memory<byte> SlicedMemory => GetViewMemory().Slice(0, _length * sizeof(sbyte));

        public Span<byte> SlicedSpan => new Span<byte>(_memory.Pointer, _length * sizeof(sbyte));

        // We create a new view per call, only cold paths need it.
        private Memory<byte> GetViewMemory()
        {
            if (_memory.IsNull)
            {
                return new Memory<byte>();
            }
            return new NativeMemoryView(_memory.Pointer, _memory.Length).Memory;
        }

        private void EnsureCapacity(int length)
        {
            if (DataLength < length)
            {
                var newLength = length * 2;
                if (newLength < 64)
                {
                    newLength = 64;
                }
                _memoryAllocator.Realloc(ref _memory, newLength * sizeof(sbyte));
            }
        }

        private Span<sbyte> AccessSpan => new Span<sbyte>(_memory.Pointer, DataLength);

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
                if (!_memory.IsNull)
                {
                    _memoryAllocator.Free(ref _memory);
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
            var slicedSpan = SlicedSpan;
            var newMem = memoryAllocator.AllocateMemory(slicedSpan.Length);
            slicedSpan.CopyTo(newMem.Span);

            return new TypeList(newMem, _length, memoryAllocator);
        }
    }
}
