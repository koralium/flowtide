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

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Core.ColumnStore.Serialization.CustomTypes;
using FlowtideDotNet.Core.ColumnStore.Serialization.Serializer;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO.Hashing;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.DataColumns
{
    internal class GuidColumn : IDataColumn
    {
        private const int GuidSize = 16;
        private readonly PrimitiveList<Guid> _values;
        private bool disposedValue;

        public GuidColumn(IMemoryAllocator memoryAllocator)
        {
            _values = new PrimitiveList<Guid>(memoryAllocator);
        }

        public GuidColumn(IMemoryOwner<byte> memory, int length, IMemoryAllocator memoryAllocator)
        {
            _values = new PrimitiveList<Guid>(memory, length, memoryAllocator);
        }

        internal GuidColumn(PrimitiveList<Guid> values)
        {
            _values = values;
        }

        public int Count => _values.Count;

        public ArrowTypeId Type => ArrowTypeId.Guid;

        public StructHeader StructHeader => throw new NotImplementedException();

        public int Add<T>(in T value) where T : IDataValue
        {
            var index = _values.Count;
            if (value.Type == ArrowTypeId.Null)
            {
                _values.Add(Guid.Empty);
            }
            else
            {
                _values.Add(value.AsGuid);
            }
            return index;
        }

        public void AddToHash(in int index, ReferenceSegment? child, NonCryptographicHashAlgorithm hashAlgorithm)
        {
            Span<byte> buffer = stackalloc byte[16];
            if (!_values[index].TryWriteBytes(buffer))
            {
                throw new InvalidOperationException("Could not write guid to hash.");
            }
            hashAlgorithm.Append(buffer);
        }

        public void AddToNewList<T>(in T value) where T : IDataValue
        {
            throw new NotImplementedException();
        }

        public void Clear()
        {
            _values.Clear();
        }

        public int CompareTo<T>(in int index, in T value, in ReferenceSegment? child, in BitmapList? validityList) where T : IDataValue
        {
            if (validityList != null &&
                !validityList.Get(index))
            {
                if (value.Type == ArrowTypeId.Null)
                {
                    return 0;
                }
                return -1;
            }
            else if (value.Type == ArrowTypeId.Null)
            {
                return 1;
            }
            return _values[index].CompareTo(value.AsGuid);
        }

        public int CompareTo(in IDataColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            Debug.Assert(_values != null);

            if (otherColumn is GuidColumn guidColumn)
            {
                Debug.Assert(guidColumn._values != null);
                return _values.Get(thisIndex).CompareTo(guidColumn._values.Get(otherIndex));
            }
            throw new NotImplementedException();
        }

        public IDataColumn Copy(IMemoryAllocator memoryAllocator)
        {
            return new GuidColumn(_values.Copy(memoryAllocator));
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _values.Dispose();
                }
                disposedValue = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        public int EndNewList()
        {
            throw new NotImplementedException();
        }

        public int GetByteSize(int start, int end)
        {
            return (end - start + 1) * GuidSize;
        }

        public int GetByteSize()
        {
            return Count * GuidSize;
        }

        public SerializationEstimation GetSerializationEstimate()
        {
            return new SerializationEstimation(1, 1, GetByteSize());
        }

        public ArrowTypeId GetTypeAt(in int index, in ReferenceSegment? child)
        {
            return ArrowTypeId.Guid;
        }

        public IDataValue GetValueAt(in int index, in ReferenceSegment? child)
        {
            return new GuidValue(_values[index]);
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer, in ReferenceSegment? child)
        {
            dataValueContainer._type = ArrowTypeId.Guid;
            dataValueContainer._guidValue = new GuidValue(_values[index]);
        }

        public void InsertAt<T>(in int index, in T value) where T : IDataValue
        {
            if (value.Type == ArrowTypeId.Null)
            {
                _values.InsertAt(index, Guid.Empty);
            }
            else
            {
                _values.InsertAt(index, value.AsGuid);
            }
        }

        public void InsertNullRange(int index, int count)
        {
            _values.InsertStaticRange(index, Guid.Empty, count);
        }

        public void InsertRangeFrom(int index, IDataColumn other, int start, int count, BitmapList? validityList)
        {
            if (other is GuidColumn guidColumn)
            {
                _values.InsertRangeFrom(index, guidColumn._values, start, count);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void RemoveAt(in int index)
        {
            _values.RemoveAt(index);
        }

        public void RemoveRange(int start, int count)
        {
            _values.RemoveRange(start, count);
        }

        public (int, int) SearchBoundries<T>(in T dataValue, in int start, in int end, in ReferenceSegment? child, bool desc) where T : IDataValue
        {
            if (desc)
            {
                return BoundarySearch.SearchBoundries(_values, dataValue.AsGuid, start, end, GuidComparerDesc.Instance);
            }
            return BoundarySearch.SearchBoundries(_values, dataValue.AsGuid, start, end, GuidComparer.Instance);
        }

        public (IArrowArray, IArrowType) ToArrowArray(ArrowBuffer nullBuffer, int nullCount)
        {
            var buffers = new ArrowBuffer[2]
            {
                nullBuffer,
                new ArrowBuffer(_values.SlicedMemory)
            };
            var array = new FixedSizeBinaryArray(new ArrayData(GuidType.Default, Count, nullCount, 0, buffers));
            return (array, GuidType.Default);
        }

        public int Update<T>(in int index, in T value) where T : IDataValue
        {
            _values.Update(index, value.AsGuid);
            return index;
        }

        public void WriteToJson(ref readonly Utf8JsonWriter writer, in int index)
        {
            writer.WriteStringValue(_values.Get(index));
        }

        void IDataColumn.AddBuffers(ref ArrowSerializer arrowSerializer)
        {
            arrowSerializer.AddBufferForward(_values.SlicedMemory.Length);
        }

        void IDataColumn.AddFieldNodes(ref ArrowSerializer arrowSerializer, in int nullCount)
        {
            arrowSerializer.CreateFieldNode(Count, nullCount);
        }

        int IDataColumn.CreateSchemaField(ref ArrowSerializer arrowSerializer, int emptyStringPointer, Span<int> pointerStack)
        {
            var extensionKeyPointer = arrowSerializer.CreateStringUtf8("ARROW:extension:name"u8);
            var extensionValuePointer = arrowSerializer.CreateStringUtf8("flowtide.guid"u8);
            var typePointer = arrowSerializer.AddFixedSizeBinaryType(16);
            pointerStack[0] = arrowSerializer.CreateKeyValue(extensionKeyPointer, extensionValuePointer);
            var customMetadataPointer = arrowSerializer.CreateCustomMetadataVector(pointerStack.Slice(0, 1));
            return arrowSerializer.CreateField(emptyStringPointer, true, Serialization.ArrowType.FixedSizeBinary, typePointer, custom_metadataOffset: customMetadataPointer);
        }

        void IDataColumn.WriteDataToBuffer(ref ArrowDataWriter dataWriter)
        {
            dataWriter.WriteArrowBuffer(_values.SlicedMemory.Span);
        }
    }
}
