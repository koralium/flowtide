﻿// Licensed under the Apache License, Version 2.0 (the "License")
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
using Apache.Arrow.Types;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Core.ColumnStore.Serialization.Serializer;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using System;
using System.Collections.Generic;
using System.IO.Hashing;
using System.Linq;
using System.Text;
using System.Text.Json;

namespace FlowtideDotNet.Core.ColumnStore.DataColumns
{
    internal class NullColumn : IDataColumn
    {
        private int _count;
        public int Count => _count;

        public ArrowTypeId Type => ArrowTypeId.Null;

        public StructHeader StructHeader => throw new NotImplementedException();

        public NullColumn()
        {
        }

        public NullColumn(int count)
        {
            _count = count;
        }

        public int Add<T>(in T value) where T : IDataValue
        {
            return _count++;
        }

        public int CompareTo<T>(in int index, in T value, in ReferenceSegment? child, in BitmapList? validityList) where T : IDataValue
        {
            return 0;
        }

        public int CompareTo(in IDataColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            throw new NotImplementedException();
        }

        public IDataValue GetValueAt(in int index, in ReferenceSegment? child)
        {
            return NullValue.Instance;
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer, in ReferenceSegment? child)
        {
            dataValueContainer._type = ArrowTypeId.Null;
        }

        public void InsertAt<T>(in int index, in T value) where T : IDataValue
        {
            _count++;
        }

        public void RemoveAt(in int index)
        {
            _count--;
        }

        public (int, int) SearchBoundries<T>(in T dataValue, in int start, in int end, in ReferenceSegment? child, bool desc) where T : IDataValue
        {
            throw new NotImplementedException();
        }

        public (IArrowArray, IArrowType) ToArrowArray(ArrowBuffer nullBuffer, int nullCount)
        {
            return (new Apache.Arrow.NullArray(_count), new NullType());
        }

        public int Update<T>(in int index, in T value) where T : IDataValue
        {
            return index;
        }

        public void Dispose()
        {
            // Not required for null column
        }

        public ArrowTypeId GetTypeAt(in int index, in ReferenceSegment? child)
        {
            return ArrowTypeId.Null;
        }

        public void Clear()
        {
            _count = 0;
        }

        public void AddToNewList<T>(in T value) where T : IDataValue
        {
            throw new NotImplementedException();
        }

        public int EndNewList()
        {
            throw new NotImplementedException();
        }

        public void RemoveRange(int start, int count)
        {
            _count -= count;
        }

        public int GetByteSize(int start, int end)
        {
            return 0;
        }

        public int GetByteSize()
        {
            return 0;
        }

        public void InsertRangeFrom(int index, IDataColumn other, int start, int count, BitmapList? validityList)
        {
            _count += count;
        }

        public void InsertNullRange(int index, int count)
        {
            _count += count;
        }

        public void WriteToJson(ref readonly Utf8JsonWriter writer, in int index)
        {
            writer.WriteNullValue();
        }

        public IDataColumn Copy(IMemoryAllocator memoryAllocator)
        {
            return new NullColumn(_count);
        }

        public void AddToHash(in int index, ReferenceSegment? child, NonCryptographicHashAlgorithm hashAlgorithm)
        {
            hashAlgorithm.Append(ByteArrayUtils.nullBytes);
        }

        int IDataColumn.CreateSchemaField(ref ArrowSerializer arrowSerializer, int emptyStringPointer, Span<int> pointerStack)
        {
            var typePointer = arrowSerializer.AddNullType();
            return arrowSerializer.CreateField(emptyStringPointer, true, Serialization.ArrowType.Null, typePointer);
        }

        public SerializationEstimation GetSerializationEstimate()
        {
            return new SerializationEstimation(1, 0, sizeof(int));
        }

        void IDataColumn.AddFieldNodes(ref ArrowSerializer arrowSerializer, in int nullCount)
        {
            arrowSerializer.CreateFieldNode(_count, _count);
        }

        void IDataColumn.AddBuffers(ref ArrowSerializer arrowSerializer)
        {
        }

        void IDataColumn.WriteDataToBuffer(ref ArrowDataWriter dataWriter)
        {
        }
    }
}
