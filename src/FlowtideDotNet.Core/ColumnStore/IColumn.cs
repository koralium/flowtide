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
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Core.ColumnStore.Serialization.Serializer;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using System.Text.Json;
using System.IO.Hashing;

namespace FlowtideDotNet.Core.ColumnStore
{
    public interface IColumn : IDisposable
    {
        int Count { get; }

        ArrowTypeId Type { get; }

        internal IDataColumn DataColumn { get; }

        void Add<T>(in T value)
            where T : IDataValue;

        void InsertAt<T>(in int index, in T value)
            where T : IDataValue;

        void UpdateAt<T>(in int index, in T value)
            where T : IDataValue;

        void RemoveAt(in int index);

        ArrowTypeId GetTypeAt(in int index, in ReferenceSegment? child);

        IDataValue GetValueAt(in int index, in ReferenceSegment? child);

        void GetValueAt(in int index, in DataValueContainer dataValueContainer, in ReferenceSegment? child);

        int CompareTo<T>(in int index, in T dataValue, in ReferenceSegment? child)
            where T : IDataValue;

        int CompareTo(in IColumn otherColumn, in int thisIndex, in int otherIndex);

        (int, int) SearchBoundries<T>(in T value, in int start, in int end, in ReferenceSegment? child, bool desc = false)
            where T : IDataValue;

        (IArrowArray, IArrowType) ToArrowArray();

        SerializationEstimation GetSerializationEstimate();

        internal int CreateSchemaField(ref ArrowSerializer arrowSerializer, int emptyStringPointer, Span<int> pointerStack);

        internal void AddFieldNodes(ref ArrowSerializer arrowSerializer);

        internal void AddBuffers(ref ArrowSerializer arrowSerializer);

        internal void WriteDataToBuffer(ref ArrowDataWriter dataWriter);

        void Rent(int count);

        void Return();

        void RemoveRange(in int index, in int count);

        int GetByteSize();

        int GetByteSize(int start, int end);

        void InsertRangeFrom(int index, IColumn otherColumn, int start, int count);

        void WriteToJson(ref readonly Utf8JsonWriter writer, in int index);

        Column Copy(IMemoryAllocator memoryAllocator);

        void AddToHash(in int index, ReferenceSegment? child, NonCryptographicHashAlgorithm hashAlgorithm);
    }
}
