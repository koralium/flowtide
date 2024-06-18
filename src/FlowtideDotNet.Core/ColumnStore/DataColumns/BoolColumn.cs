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
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Substrait.Expressions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    internal class BoolColumn : IDataColumn
    {
        private List<bool> _data = new();

        public int Count => _data.Count;

        public ArrowTypeId Type => ArrowTypeId.Boolean;

        public int Add<T>(in T value) where T : IDataValue
        {
            var index = _data.Count;

            if (value.Type == ArrowTypeId.Null)
            {
                _data.Add(false);
                return index;
            }

            if (value.AsBool)
            {
                _data.Add(true);
            }
            else
            {
                _data.Add(false);
            }
            return index;
        }

        public int BinarySearch(in IDataValue dataValue)
        {
            throw new NotImplementedException();
        }

        public int BinarySearch(in IDataValue dataValue, in int start, in int end)
        {
            throw new NotImplementedException();
        }

        public int CompareToStrict(in int index, in IDataValue value)
        {
            throw new NotImplementedException();
        }

        public int CompareTo(in IDataColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            throw new NotImplementedException();
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
            return _data[index].CompareTo(value.AsBool);
        }

        public IDataValue GetValueAt(in int index, in ReferenceSegment? child)
        {
            return new BoolValue(_data[index]);
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer, in ReferenceSegment? child)
        {
            dataValueContainer._type = ArrowTypeId.Boolean;
            dataValueContainer._boolValue = new BoolValue(_data[index]);
        }

        public (int, int) SearchBoundries<T>(in T dataValue, in int start, in int end, in ReferenceSegment? child)
            where T : IDataValue
        {
            var val = dataValue.AsBool;
            return BoundarySearch.SearchBoundries<bool>(_data, val, start, end, BoolComparer.Instance);
        }

        public int Update(in int index, in IDataValue value)
        {
            if (value.AsBool)
            {
                _data[index] = true;
            }
            else
            {
                _data[index] = false;
            }
            return index;
        }

        public int Update<T>(in int index, in T value) where T : IDataValue
        {
            if (value.AsBool)
            {
                _data[index] = true;
            }
            else
            {
                _data[index] = false;
            }
            return index;
        }

        public void RemoveAt(in int index)
        {
            throw new NotImplementedException();
        }

        public void InsertAt<T>(in int index, in T value) where T : IDataValue
        {
            if (value.Type == ArrowTypeId.Null)
            {
                _data.Insert(index, false);
                return;
            }
            if (value.AsBool)
            {                 
                _data.Insert(index, true);
            }
            else
            {
                _data.Insert(index, false);
            } 
        }

        public (IArrowArray, IArrowType) ToArrowArray(ArrowBuffer nullBuffer, int nullCount)
        {
            throw new NotImplementedException();
        }
    }
}
