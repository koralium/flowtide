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

using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.ColumnStore.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    public class StringColumn : IDataColumn
    {
        private static SpanByteComparer s_spanByteComparer = new SpanByteComparer();
        private BinaryList _binaryList = new BinaryList();

        public int Count => _binaryList.Count;

        public ArrowTypeId Type => ArrowTypeId.String;

        public int Add<T>(in T value) where T : IDataValue
        {
            var index = _binaryList.Count;
            if (value.Type == ArrowTypeId.Null)
            {
                _binaryList.AddEmpty();
                return index;
            }
            _binaryList.Add(value.AsString.Span);
            return index;
        }

        public int CompareTo<T>(in int index, in T value) where T : IDataValue
        {
            return s_spanByteComparer.Compare(_binaryList.Get(index), value.AsString.Span);
        }

        public int CompareTo(in IDataColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            if (otherColumn is StringColumn stringColumn)
            {
                return s_spanByteComparer.Compare(_binaryList.Get(thisIndex), stringColumn._binaryList.Get(otherIndex));
            }
            throw new NotImplementedException();
        }

        public IDataValue GetValueAt(in int index)
        {
            return new StringValue(_binaryList.Get(in index).ToArray());
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer)
        {
            dataValueContainer._type = ArrowTypeId.String;
            dataValueContainer._stringValue = new StringValue(_binaryList.Get(in index).ToArray());
        }

        public void InsertAt<T>(in int index, in T value) where T : IDataValue
        {
            if (value.Type == ArrowTypeId.Null)
            {
                _binaryList.InsertEmpty(index);
                return;
            }
            _binaryList.Insert(index, value.AsString.Span);
        }

        public void RemoveAt(in int index)
        {
            _binaryList.RemoveAt(index);
        }

        public (int, int) SearchBoundries<T>(in T dataValue, in int start, in int end) where T : IDataValue
        {
            return BoundarySearch.SearchBoundries(_binaryList, dataValue.AsString.Span, start, end, s_spanByteComparer);
        }

        public int Update<T>(in int index, in T value) where T : IDataValue
        {
            if (value.Type == ArrowTypeId.Null)
            {
                _binaryList.UpdateAt(index, Span<byte>.Empty);
                return index;
            }
            _binaryList.UpdateAt(index, value.AsString.Span);
            return index;
        }
    }
}
