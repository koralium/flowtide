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
using SqlParser.Ast;
using System.Runtime.InteropServices;

namespace FlowtideDotNet.Core.ColumnStore
{
    /// <summary>
    /// Represents a column with data.
    /// A column can contain multiple types of data.
    /// </summary>
    public class Column
    {
        /// <summary>
        /// Contains what type of data is stored in each element.
        /// </summary>
        private readonly List<ArrowTypeId> _typeList;
        private readonly List<int> _offsets;
        private readonly IDataColumn[] _valueColumns;
        private int _typesCounter;

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

        public Column()
        {
            _typeList = [];
            _offsets = [];
            _valueColumns = new IDataColumn[34];
        }

        public int Count => _typeList.Count;

        public void InsertAt<T>(in int index, in T value)
            where T: struct, IDataValue
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

        public void InsertAt(in int index, in IDataValue value)
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
            var offset = valueColumn.Add(value);
            _offsets.Insert(index, offset);
        }

        private void CheckArrayExist(in ArrowTypeId type)
        {
            var typeByte = (byte)type;
            if (_valueColumns[typeByte] == null)
            {
                _typesCounter++;
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

        public void UpdateAt<T>(in int index, in T value)
            where T: struct, IDataValue
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
        }

        public void Add<T>(in T value)
            where T: struct, IDataValue
        {
            InsertAt(_offsets.Count, in value);
        }

        public void Add(in IDataValue value)
        {
            InsertAt(_offsets.Count, in value);
        }

        public void Remove(in int index)
        {
            // For deletes, only the type list and offsets are updated.
            // The data will still be in the data column, but it will be ignored.
            // Compaction will remove the data.
            _deletesCounter++;
            _typeList.RemoveAt(index);
            _offsets.RemoveAt(index);
        }

        public int BinarySarch(in IDataValue value, int start, int end)
        {
            var type = (byte)value.Type;
            var valueColumn = _valueColumns[type];
            if (valueColumn == null)
            {
                return ~0;
            }
            return valueColumn.BinarySearch(value, start, end);
        }

        public (int, int) SearchBoundries<T>(in T value, in int start, in int end)
            where T: IDataValue
        {
            if (_typesCounter == 1 && outOfOrderCounter == 0)
            {
                // only one type in the column, so we can use the binary search inside of the type array.
                // It is also in order, so its safe to run the entire search inside of the type array.
                var type = (byte)value.Type;
                
                return _valueColumns[type].SearchBoundries(in value, in start, in end);
            }
            else
            {
                // Multiple types so a binary search will be done outside using all types.
                return BoundarySearch.SearchBoundriesForColumn(this, in value, in start, in end);
            }
        }

        public int CompareTo(in Column otherColumn, in int thisIndex, in int otherIndex)
        {
            var thisType = _typeList[thisIndex];
            var otherType = otherColumn._typeList[otherIndex];

            if (thisType == otherType)
            {
                if (thisType == ArrowTypeId.Null)
                {
                    return 0;
                }
                var thisValueColumn = _valueColumns[(byte)thisType];
                var otherValueColumn = otherColumn._valueColumns[(byte)otherType];
                return thisValueColumn.CompareToStrict(in otherValueColumn,_offsets[thisIndex], otherColumn._offsets[otherIndex]);
            }
            else
            {
                return thisType - otherType;
            }
        }

        // Generics to avoid boxing
        public int CompareTo<T>(in int index, in T dataValue)
            where T : IDataValue
        {
            var type = _typeList[index];

            if (type == dataValue.Type)
            {
                var valueColumn = _valueColumns[(byte)type];
                return valueColumn.CompareToStrict(_offsets[index], in dataValue);
            }
            else
            {
                return type - dataValue.Type;
            }
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer)
        {
            var type = _typeList[index];
            var valueColumn = _valueColumns[(byte)type];
            valueColumn.GetValueAt(_offsets[index], dataValueContainer);
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
    }
}
