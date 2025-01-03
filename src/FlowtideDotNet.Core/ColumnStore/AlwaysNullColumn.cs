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
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    /// <summary>
    /// Column that responds null always when fetching out values
    /// Used only to fill in missing columns in a row
    /// </summary>
    internal class AlwaysNullColumn : IColumn
    {
        public static AlwaysNullColumn Instance = new AlwaysNullColumn();
        public int Count => 0;

        IDataColumn IColumn.DataColumn => throw new NotSupportedException();

        public ArrowTypeId Type => ArrowTypeId.Null;

        public void Add<T>(in T value) where T : IDataValue
        {
            throw new NotSupportedException();
        }

        public int CompareTo<T>(in int index, in T dataValue, in ReferenceSegment? child) where T : IDataValue
        {
            return 0;
        }

        public int CompareTo(in IColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            return 0;
        }

        public void Dispose()
        {
        }

        public int GetByteSize()
        {
            return 0;
        }

        public int GetByteSize(int start, int end)
        {
            return 0;
        }

        public ArrowTypeId GetTypeAt(in int index, in ReferenceSegment? child)
        {
            return ArrowTypeId.Null;
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
            throw new NotSupportedException();
        }

        public void InsertRangeFrom(int index, IColumn otherColumn, int start, int count)
        {
            throw new NotSupportedException();
        }

        public void RemoveAt(in int index)
        {
            throw new NotSupportedException();
        }

        public void RemoveRange(in int index, in int count)
        {
            throw new NotSupportedException();
        }

        public void Rent(int count)
        {
        }

        public void Return()
        {
        }

        public (int, int) SearchBoundries<T>(in T value, in int start, in int end, in ReferenceSegment? child, bool desc = false) where T : IDataValue
        {
            throw new NotSupportedException();
        }

        public (IArrowArray, IArrowType) ToArrowArray()
        {
            throw new NotSupportedException();
        }

        public void UpdateAt<T>(in int index, in T value) where T : IDataValue
        {
            throw new NotSupportedException();
        }

        public void WriteToJson(ref readonly Utf8JsonWriter writer, in int index)
        {
            throw new NotSupportedException();
        }

        public Column Copy(IMemoryAllocator memoryAllocator)
        {
            throw new NotSupportedException();
        }
    }
}
