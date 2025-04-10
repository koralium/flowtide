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
using Apache.Arrow.Types;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Core.ColumnStore.Serialization.Serializer;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using System.Text.Json;

namespace FlowtideDotNet.Core.ColumnStore
{
    public interface IDataColumn : IDisposable
    {
        int Count { get; }

        ArrowTypeId Type { get; }

        int CompareTo<T>(in int index, in T value, in ReferenceSegment? child, in BitmapList? validityList)
            where T : IDataValue;

        int CompareTo(in IDataColumn otherColumn, in int thisIndex, in int otherIndex);

        int Add<T>(in T value)
            where T : IDataValue;

        IDataValue GetValueAt(in int index, in ReferenceSegment? child);

        void GetValueAt(in int index, in DataValueContainer dataValueContainer, in ReferenceSegment? child);

        int Update<T>(in int index, in T value)
            where T : IDataValue;

        (int, int) SearchBoundries<T>(in T dataValue, in int start, in int end, in ReferenceSegment? child, bool desc)
            where T : IDataValue;

        void RemoveAt(in int index);

        void InsertAt<T>(in int index, in T value)
            where T : IDataValue;

        (IArrowArray, IArrowType) ToArrowArray(ArrowBuffer nullBuffer, int nullCount);

        ArrowTypeId GetTypeAt(in int index, in ReferenceSegment? child);

        void Clear();

        void AddToNewList<T>(in T value) where T : IDataValue;

        int EndNewList();

        void RemoveRange(int start, int count);

        int GetByteSize(int start, int end);

        int GetByteSize();

        void InsertRangeFrom(int index, IDataColumn other, int start, int count, BitmapList? validityList);

        /// <summary>
        /// Inserts null on all elements in the range.
        /// Used as an optimization to not have to insert nulls one by one.
        /// </summary>
        /// <param name="index"></param>
        /// <param name="count"></param>
        void InsertNullRange(int index, int count);

        void WriteToJson(ref readonly Utf8JsonWriter writer, in int index);

        IDataColumn Copy(IMemoryAllocator memoryAllocator);

        internal int CreateSchemaField(ref ArrowSerializer arrowSerializer, int emptyStringPointer, Span<int> pointerStack);

        internal void AddFieldNodes(ref ArrowSerializer arrowSerializer, in int nullCount);

        internal void AddBuffers(ref ArrowSerializer arrowSerializer);

        internal void WriteDataToBuffer(ref ArrowDataWriter dataWriter);

        SerializationEstimation GetSerializationEstimate();

        /// <summary>
        /// Only used for struct column, added for ease of access.
        /// Returns the header of a struct (column names).
        /// </summary>
        StructHeader StructHeader { get; }
    }
}
