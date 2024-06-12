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

using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.DataColumns
{
    /// <summary>
    /// Union column is is used to handle multiple types in a single column.
    /// 
    /// The union column does not store all data in sorted order.
    /// Only the offsets will be in a sorted order.
    /// Therefore it must be compacted before serializing.
    /// </summary>
    internal class UnionColumn : IDataColumn
    {
        private readonly List<ArrowTypeId> _typeList;
        private readonly List<int> _offsets;
        private readonly IDataColumn[] _valueColumns;

        /// <summary>
        /// Counter that checks how many deletes have happpened.
        /// When this gets too high, the column should be compacted.
        /// </summary>
        private int _deletesCounter;

        /// <summary>
        /// Counter that keeps track of how many out of order inserts have happened.
        /// Too out of order will degrade performance since there wont be as many cache hits.
        /// </summary>
        private int outOfOrderCounter;

        public UnionColumn()
        {
            _typeList = new List<ArrowTypeId>();
            _offsets = new List<int>();
            _valueColumns = new IDataColumn[35];
        }


        private void CheckArrayExist(in ArrowTypeId type)
        {
            var typeByte = (byte)type;
            if (_valueColumns[typeByte] == null)
            {
                switch (type)
                {
                    case ArrowTypeId.Int64:
                        _valueColumns[typeByte] = new Int64Column();
                        break;
                    case ArrowTypeId.String:
                        _valueColumns[typeByte] = new StringColumn();
                        break;
                    case ArrowTypeId.Boolean:
                        _valueColumns[typeByte] = new BoolColumn();
                        break;
                    case ArrowTypeId.Double:
                        _valueColumns[typeByte] = new DoubleColumn();
                        break;
                    case ArrowTypeId.List:
                        _valueColumns[typeByte] = new ListColumn();
                        break;
                    case ArrowTypeId.Binary:
                        _valueColumns[typeByte] = new BinaryColumn();
                        break;
                    case ArrowTypeId.Map:
                        _valueColumns[typeByte] = new MapColumn();
                        break;
                    case ArrowTypeId.Decimal128:
                        _valueColumns[typeByte] = new DecimalColumn();
                        break;
                    default:
                        throw new NotImplementedException();
                }
            }
        }

        public void InsertAt<T>(in int index, in T value) where T: IDataValue
        {
            if (index != _typeList.Count)
            {
                outOfOrderCounter++;
            }
            if (value.Type == ArrowTypeId.Null)
            {
                _typeList.Add(ArrowTypeId.Null);
                _offsets.Add(0);
                return;
            }

            var typeByte = (byte)value.Type;
            CheckArrayExist(value.Type);
            _typeList.Insert(index, value.Type);
            var valueColumn = _valueColumns[typeByte];
            var offset = valueColumn.Add(in value);
            _offsets.Insert(index, offset);
        }

        public int Add<T>(in T value) where T : IDataValue
        {
            int index = _typeList.Count;
            InsertAt(_offsets.Count, in value);
            return index;
        }

        public int BinarySearch(in IDataValue dataValue, in int start, in int end)
        {
            throw new NotImplementedException();
        }

        public int CompareTo<T>(in int index, in T value) where T : IDataValue
        {
            var type = _typeList[index];

            if (type == value.Type)
            {
                var valueColumn = _valueColumns[(byte)type];
                return valueColumn.CompareTo(_offsets[index], in value);
            }
            else
            {
                return type - value.Type;
            }
        }

        public int CompareTo(in IDataColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            var thisType = _typeList[thisIndex];
            if (otherColumn is UnionColumn unionColumn)
            {
                var otherType = unionColumn._typeList[otherIndex];

                if (thisType == otherType)
                {
                    if (thisType == ArrowTypeId.Null)
                    {
                        return 0;
                    }
                    var thisValueColumn = _valueColumns[(byte)thisType];
                    var otherValueColumn = unionColumn._valueColumns[(byte)otherType];
                    return thisValueColumn.CompareTo(in otherValueColumn, _offsets[thisIndex], unionColumn._offsets[otherIndex]);
                }
                else
                {
                    return thisType - otherType;
                }
            }
            else
            {
                var value = otherColumn.GetValueAt(otherIndex);
                return CompareTo(thisIndex, value);
            }
        }

        public IDataValue GetValueAt(in int index)
        {
            var type = _typeList[index];
            if (type == ArrowTypeId.Null)
            {
                return new NullValue();
            }
            var valueColumn = _valueColumns[(byte)type];
            return valueColumn.GetValueAt(_offsets[index]);
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer)
        {
            var type = _typeList[index];
            var valueColumn = _valueColumns[(byte)type];
            valueColumn.GetValueAt(_offsets[index], dataValueContainer);
        }

        public (int, int) SearchBoundries<T>(in T dataValue, in int start, in int end) where T : IDataValue
        {
            return BoundarySearch.SearchBoundriesForColumn(this, in dataValue, in start, in end);
        }

        public int Update<T>(in int index, in T value) where T : struct, IDataValue
        {
            var currentType = _typeList[index];

            if (currentType != value.Type)
            {
                CheckArrayExist(value.Type);
                var typeByte = (byte)value.Type;
                _typeList[index] = value.Type;
                var valueColumn = _valueColumns[typeByte];
                _offsets[index] = valueColumn.Add(in value);
                _deletesCounter++;
            }
            else
            {
                // Same type
                var valueColumn = _valueColumns[(byte)currentType];
                var currentOffset = _offsets[index];
                var newOffset = valueColumn.Update(_offsets[index], in value);

                // Check if the offset has changed, then treat it as a insert and a delete.
                if (currentOffset != newOffset)
                {
                    _offsets[index] = newOffset;
                    _deletesCounter++;
                }
            }
            return index;
        }

        public void RemoveAt(in int index)
        {
            _deletesCounter++;
            _typeList.RemoveAt(index);
            _offsets.RemoveAt(index);
        }
    }
}
