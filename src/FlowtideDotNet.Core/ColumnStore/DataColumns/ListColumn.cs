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

using Apache.Arrow.Types;
using Apache.Arrow;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Core.ColumnStore.Memory;
using FlowtideDotNet.Substrait.Expressions;

namespace FlowtideDotNet.Core.ColumnStore
{
    public class ListColumn : IDataColumn
    {
        private readonly Column _internalColumn;
        private readonly IntList _offsets;

        public int Count => _offsets.Count - 1;

        public ArrowTypeId Type => ArrowTypeId.List;

        public ListColumn()
        {
            _internalColumn = new Column();
            _offsets = new IntList(new NativeMemoryAllocator());
            _offsets.Add(0);
        }

        public int Add(in IDataValue value)
        {
            var list = value.AsList;

            var currentOffset = Count;
            var listLength = list.Count;
            for (int i = 0; i < listLength; i++)
            {
                _internalColumn.Add(list.GetAt(i));
            }
            _offsets.Add(_internalColumn.Count);

            return currentOffset;
        }

        public int Add<T>(in T value) where T : IDataValue
        {
            var currentOffset = Count;
            if (value.Type == ArrowTypeId.Null)
            {
                _offsets.Add(_internalColumn.Count);
                return currentOffset;
            }

            var list = value.AsList;
            var listLength = list.Count;
            for (int i = 0; i < listLength; i++)
            {
                _internalColumn.Add(list.GetAt(i));
            }
            _offsets.Add(_internalColumn.Count);

            return currentOffset;
        }

        public int BinarySearch(in IDataValue dataValue)
        {
            throw new NotImplementedException();
        }

        public int BinarySearch(in IDataValue dataValue, in int start, in int end)
        {
            throw new NotImplementedException();
        }

        public int CompareTo(in IDataColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            throw new NotImplementedException();
        }

        public int CompareTo<T>(in int index, in T value, in ReferenceSegment? child, in BitmapList? validityList) where T : IDataValue
        {
            throw new NotImplementedException();
        }

        public IDataValue GetValueAt(in int index, in ReferenceSegment? child)
        {
            return new ReferenceListValue(_internalColumn, _offsets.Get(index), _offsets.Get(index + 1));
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer, in ReferenceSegment? child)
        {
            throw new NotImplementedException();
        }

        public (int, int) SearchBoundries<T>(in T dataValue, in int start, in int end, in ReferenceSegment? child) 
            where T : IDataValue
        {
            throw new NotImplementedException();
        }

        public int Update(in int index, in IDataValue value)
        {
            return Add(value);
        }

        public int Update<T>(in int index, in T value) where T : IDataValue
        {
            throw new NotImplementedException();
        }

        public void RemoveAt(in int index)
        {
            throw new NotImplementedException();
        }

        public void InsertAt<T>(in int index, in T value) where T : IDataValue
        {
            throw new NotImplementedException();
        }

        public (IArrowArray, IArrowType) ToArrowArray(Apache.Arrow.ArrowBuffer nullBuffer, int nullCount)
        {
            var (arr, type) = _internalColumn.ToArrowArray();
            var listType = new ListType(type);
            var offsetBuffer = new ArrowBuffer(_offsets.Memory);
            return (new Apache.Arrow.ListArray(listType, Count, offsetBuffer, arr, nullBuffer, nullCount), listType);
        }
    }
}
