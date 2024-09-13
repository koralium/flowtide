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
using FlowtideDotNet.Storage.Memory;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Utils
{

    internal unsafe struct BinaryInfo
    {
        public readonly byte* data;
        public readonly int length;

        public Span<byte> Span => new Span<byte>(data, length);

        public BinaryInfo(byte* data, int length)
        {   
            this.data = data;
            this.length = length;
        }
    }

    /// <summary>
    /// Helper list that stores binary data and their offsets.
    /// This follows apache arrow on how to store binary data.
    /// This means that it does not store references to the binary data, but instead stores them directly in the array.
    /// This list allows inserting data and removing data where it correctly recalculates offsets.
    /// </summary>
    internal unsafe class BinaryList : IDisposable
    {
        // Memory objects
        private void* _data;
        private int _dataLength;
        private IMemoryOwner<byte>? _memoryOwner;

        // List specific members
        private IntList _offsets;
        private int _length;

        // Dispose value
        private bool disposedValue;
        private readonly IMemoryAllocator _memoryAllocator;

        public Memory<byte> OffsetMemory => _offsets.Memory;

        public Memory<byte> DataMemory => _memoryOwner?.Memory ?? new Memory<byte>();

        public int Count => _offsets.Count - 1;

        private Span<byte> AccessSpan => new Span<byte>(_data, _dataLength);

        public BinaryList(IMemoryAllocator memoryAllocator)
        {
            _offsets = new IntList(memoryAllocator);
            _offsets.Add(0);
            _data = null;
            _memoryAllocator = memoryAllocator;
        }

        /// <summary>
        /// Create a binary list from existing memory.
        /// If any changes are made to the list that exceeds the current memory, a new memory block will be allocated that is used only for the list.
        /// </summary>
        /// <param name="offsetMemory"></param>
        /// <param name="offsetLength"></param>
        /// <param name="dataMemory"></param>
        /// <param name="memoryAllocator"></param>
        public BinaryList(IMemoryOwner<byte> offsetMemory, int offsetLength, IMemoryOwner<byte> dataMemory, IMemoryAllocator memoryAllocator)
        {
            _offsets = new IntList(offsetMemory, offsetLength, memoryAllocator);
            _data = dataMemory.Memory.Pin().Pointer;
            _memoryAllocator = memoryAllocator;
            _memoryOwner = dataMemory;
            var lastoffset = _offsets.Get(offsetLength - 1);
            _length = lastoffset;
            _dataLength = dataMemory.Memory.Length;
        }

        private void EnsureCapacity(int length)
        {
            if (_dataLength < length)
            {
                var allocLength = length * 2;
                if (allocLength < 64)
                {
                    allocLength = 64;
                }
                if (_memoryOwner == null)
                {
                    _memoryOwner = _memoryAllocator.Allocate(allocLength, 64);
                    _data = _memoryOwner.Memory.Pin().Pointer;
                }
                else
                {
                    var newMemory = _memoryAllocator.Allocate(allocLength, 64);
                    var newPtr = newMemory.Memory.Pin().Pointer;
                    NativeMemory.Copy(_data, newPtr, (nuint)(_dataLength));
                    _data = newPtr;
                    _memoryOwner.Dispose();
                    _memoryOwner = newMemory;
                }
                _dataLength = allocLength;
            }
        }

        /// <summary>
        /// Add binary data as an element to the list.
        /// </summary>
        /// <param name="data"></param>
        public void Add(Span<byte> data)
        {
            //var currentOffset = _length;
            EnsureCapacity(_length + data.Length);
            data.CopyTo(AccessSpan.Slice(_length));
            _length += data.Length;
            _offsets.Add(_length);
        }

        public void AddEmpty()
        {
            var currentOffset = _length;
            _offsets.Add(currentOffset);
        }

        public void UpdateAt(int index, Span<byte> data)
        {
            var offset = _offsets.Get(index);
            var endOffset = _offsets.Get(index + 1);
            var length = endOffset - offset;
            if (length == data.Length)
            {
                data.CopyTo(AccessSpan.Slice(offset));
            }
            else
            {
                var difference = data.Length - length;
                EnsureCapacity(_length + difference);
                var span = AccessSpan;
                span.Slice(offset + length, _length - offset - length).CopyTo(span.Slice(offset + data.Length));
                data.CopyTo(span.Slice(offset));
                _length += difference;
                _offsets.Update(index + 1, offset + data.Length, difference);
            }   
        }

        /// <summary>
        /// Insert binary data at a specfic index.
        /// If this is not inserted at the end, a copy will be done of all elements larger than this index.
        /// </summary>
        /// <param name="index"></param>
        /// <param name="data"></param>
        public void Insert(int index, Span<byte> data)
        {
            if (index == _offsets.Count)
            {
                Add(data);
                return;
            }

            EnsureCapacity(_length + data.Length);
            // Get the offset of the current element at the location
            var offset = _offsets.Get(index);

            // Take out the length that all bytes must be moved
            var toMove =  data.Length;
            var span = AccessSpan;

            // Move all elements after the index
            span.Slice(offset, _length - offset).CopyTo(span.Slice(offset + toMove));
            
            // Insert data of the new element
            data.CopyTo(span.Slice(offset));

            // Add the offset and add the size to all offsets above this one.
            _offsets.InsertAt(index, offset, toMove);
            
            _length += data.Length;
        }

        public void InsertEmpty(in int index)
        {
            if (index == _offsets.Count)
            {
                AddEmpty();
                return;
            }

            var offset = _offsets.Get(index);
            _offsets.InsertAt(index, offset);
        }

        public void RemoveAt(int index)
        {
            // Check if are removing the last element
            if (index == _offsets.Count - 1)
            {
                var offset = _offsets.Get(index);
                var length = _length - offset;
                _offsets.RemoveAt(index);
                _length -= length;
                return;
            }
            else
            {
                var offset = _offsets.Get(index);
                var length = _offsets.Get(index + 1) - offset;
                // Remove the offset and negate the length of all elements above this index.
                _offsets.RemoveAt(index, -length);
                
                var span = AccessSpan;

                // Move all elements after the index
                span.Slice(offset + length, _length - offset - length).CopyTo(span.Slice(offset));
                _length -= length;
            }
        }

        public Span<byte> Get(in int index)
        {
            var offset = _offsets.Get(index);
            return AccessSpan.Slice(offset, _offsets.Get(index + 1) - offset);
            
        }

        public Memory<byte> GetMemory(in int index)
        {
            if (_memoryOwner == null)
            {
                return Memory<byte>.Empty;
            }
            var offset = _offsets.Get(index);
            return _memoryOwner.Memory.Slice(offset, _offsets.Get(index + 1) - offset);
        }

        /// <summary>
        /// Returns the underlying information, the raw array and index and offset.
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public BinaryInfo GetBinaryInfo(in int index)
        {
            var offset = _offsets.Get(index);
            return new BinaryInfo(((byte*)_data) + offset, _offsets.Get(index + 1) - offset);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects)
                    _offsets.Dispose();
                }
                if (_memoryOwner != null)
                {
                    _memoryOwner.Dispose();
                    _memoryOwner = null;
                    _data = null;
                }
                // TODO: free unmanaged resources (unmanaged objects) and override finalizer
                // TODO: set large fields to null
                disposedValue = true;
            }
        }

        ~BinaryList()
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

        public void Clear()
        {
            _offsets.Clear();
            _offsets.Add(0);
            _length = 0;
        }
    }
}
