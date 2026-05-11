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

using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Utils
{
    internal unsafe class BinaryViewList
    {
        [StructLayout(LayoutKind.Explicit, Size = 16)]
        private unsafe struct ArrowStringView
        {
            [FieldOffset(0)]
            public int Length;

            [FieldOffset(4)]
            public fixed byte InlineData[12];

            [FieldOffset(4)]
            public fixed byte PrefixBytes[4];

            [FieldOffset(4)]
            public uint PrefixInt;

            [FieldOffset(8)]
            public int BufferIndex;

            [FieldOffset(12)]
            public int Offset;

            public bool IsInline => Length <= 12;
        }

        private PrimitiveList<ArrowStringView> _offets;
        private void* _data;
        private int _dataLength;
        private IMemoryOwner<byte>? _memoryOwner;

        /// <summary>
        /// Insert pointer do the data
        /// </summary>
        private int _insertPointer = 0;

        /// <summary>
        /// Keeps track of deleted data, if we should run compaction
        /// </summary>
        private int _deletedDataSize = 0;
        private readonly IMemoryAllocator _memoryAllocator;

        public BinaryViewList(IMemoryAllocator memoryAllocator)
        {
            _offets = new PrimitiveList<ArrowStringView>(memoryAllocator);
            this._memoryAllocator = memoryAllocator;
        }

        public void Add(ReadOnlySpan<byte> data)
        {
            var offset = new ArrowStringView
            {
                Length = data.Length
            };
            if (data.Length <= 12)
            {
                for (int i = 0; i < data.Length; i++)
                {
                    offset.InlineData[i] = data[i];
                }
            }
            else
            {
                offset.BufferIndex = 0;
                offset.Offset = _insertPointer;
                _insertPointer += data.Length;
                if (_insertPointer > _dataLength)
                {
                    // Need to allocate more memory
                    var newLength = Math.Max(_dataLength * 2, _insertPointer);
                    var newMemoryOwner = _memoryAllocator.Allocate(newLength, 64);
                    var newDataPtr = (byte*)newMemoryOwner.Memory.Pin().Pointer;
                    if (_data != null)
                    {
                        Buffer.MemoryCopy(_data, newDataPtr, newLength, _dataLength);
                        _memoryOwner!.Dispose();
                    }
                    _data = newDataPtr;
                    _dataLength = newLength;
                    _memoryOwner = newMemoryOwner;
                }
                for (int i = 0; i < data.Length; i++)
                {
                    ((byte*)_data)[offset.Offset + i] = data[i];
                }
            }
            _offets.Add(offset);
        }

        public void Insert(int index, ReadOnlySpan<byte> data)
        {
            if (index < 0 || index > _offets.Count)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }
            // Insert is basically add and move the data
            if (data.Length <= 12)
            {
                var offset = new ArrowStringView
                {
                    Length = data.Length
                };
                for (int i = 0; i < data.Length; i++)
                {
                    offset.InlineData[i] = data[i];
                }
                _offets.InsertAt(index, offset);
            }
            else
            {
                var offset = new ArrowStringView
                {
                    Length = data.Length,
                    BufferIndex = 0,
                    Offset = _insertPointer
                };
                _insertPointer += data.Length;

                if (_insertPointer > _dataLength)
                {
                    // Need to allocate more memory
                    var newLength = Math.Max(_dataLength * 2, _insertPointer);
                    var newMemoryOwner = _memoryAllocator.Allocate(newLength, 64);
                    var newDataPtr = (byte*)newMemoryOwner.Memory.Pin().Pointer;
                    if (_data != null)
                    {
                        Buffer.MemoryCopy(_data, newDataPtr, newLength, _dataLength);
                        _memoryOwner!.Dispose();
                    }
                    _data = newDataPtr;
                    _dataLength = newLength;
                    _memoryOwner = newMemoryOwner;
                }

                for (int i = 0; i < data.Length; i++)
                {
                    ((byte*)_data)[offset.Offset + i] = data[i];
                }
                _offets.InsertAt(index, offset);
            }
        }

        public void RemoveAt(int index)
        {
            if (index < 0 || index >= _offets.Count)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }
            var offset = _offets[index];
            if (!offset.IsInline)
            {
                _deletedDataSize += offset.Length;
            }
            _offets.RemoveAt(index);
        }

        public void UpdateAt(int index, ReadOnlySpan<byte> data)
        {
            if (index < 0 || index >= _offets.Count)
            {
                throw new ArgumentOutOfRangeException(nameof(index));
            }
            var offset = _offets[index];
            if (data.Length <= 12)
            {
                if (!offset.IsInline)
                {
                    _deletedDataSize += offset.Length;
                }
                var newOffset = new ArrowStringView
                {
                    Length = data.Length
                };
                for (int i = 0; i < data.Length; i++)
                {
                    newOffset.InlineData[i] = data[i];
                }
                _offets[index] = newOffset;
            }
            else
            {
                if (!offset.IsInline)
                {
                    _deletedDataSize += offset.Length;
                }
                var newOffset = new ArrowStringView
                {
                    Length = data.Length,
                    BufferIndex = 0,
                    Offset = _insertPointer
                };
                _insertPointer += data.Length;
                if (_insertPointer > _dataLength)
                {
                    // Need to allocate more memory
                    var newLength = Math.Max(_dataLength * 2, _insertPointer);
                    var newMemoryOwner = _memoryAllocator.Allocate(newLength, 64);
                    var newDataPtr = (byte*)newMemoryOwner.Memory.Pin().Pointer;
                    if (_data != null)
                    {
                        Buffer.MemoryCopy(_data, newDataPtr, newLength, _dataLength);
                        _memoryOwner!.Dispose();
                    }
                    _data = newDataPtr;
                    _dataLength = newLength;
                    _memoryOwner = newMemoryOwner;
                }
                for (int i = 0; i < data.Length; i++)
                {
                    ((byte*)_data)[newOffset.Offset + i] = data[i];
                }
                _offets[index] = newOffset;
            }
        }

        public void Compact()
        {
            if (_deletedDataSize == 0)
            {
                return;
            }
            var newMemoryOwner = _memoryAllocator.Allocate(_dataLength - _deletedDataSize, 64);
            var newDataPtr = (byte*)newMemoryOwner.Memory.Pin().Pointer;
            int newInsertPointer = 0;
            for (int i = 0; i < _offets.Count; i++)
            {
                var offset = _offets[i];
                if (offset.IsInline)
                {
                    continue;
                }
                for (int j = 0; j < offset.Length; j++)
                {
                    ((byte*)newDataPtr)[newInsertPointer + j] = ((byte*)_data)[offset.Offset + j];
                }
                offset.Offset = newInsertPointer;
                newInsertPointer += offset.Length;
                _offets[i] = offset;
            }
            _memoryOwner!.Dispose();
            _data = newDataPtr;
            _dataLength = newMemoryOwner.Memory.Length;
            _memoryOwner = newMemoryOwner;
            _insertPointer = newInsertPointer;
            _deletedDataSize = 0;
        }
    }
}
