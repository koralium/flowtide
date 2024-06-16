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
using FlowtideDotNet.Core.ColumnStore.Memory;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.ColumnStore.Utils;
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
        private readonly PrimitiveList<sbyte> _typeList;
        private readonly IntList _offsets;
        private readonly List<IDataColumn> _valueColumns;
        private readonly sbyte[] _typeIds;
        private int _typeCounter;

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

        public int Count => _typeList.Count;

        public ArrowTypeId Type => ArrowTypeId.Union;

        public UnionColumn()
        {
            _typeIds = new sbyte[35]; //35 types exist
            _typeList = new PrimitiveList<sbyte>(new NativeMemoryAllocator());
            _offsets = new IntList(new NativeMemoryAllocator());
            _valueColumns = new List<IDataColumn>()
            {
                new NullColumn()
            };
        }


        private void CheckArrayExist(in ArrowTypeId type)
        {
            var typeByte = (byte)type;
            if (_typeIds[typeByte] == 0)
            {
                _typeIds[typeByte] = (sbyte)_valueColumns.Count;
                switch (type)
                {
                    case ArrowTypeId.Int64:
                        _valueColumns.Add(new Int64Column());
                        break;
                    case ArrowTypeId.String:
                        _valueColumns.Add(new StringColumn());
                        break;
                    case ArrowTypeId.Boolean:
                        _valueColumns.Add(new BoolColumn());
                        break;
                    case ArrowTypeId.Double:
                        _valueColumns.Add(new DoubleColumn());
                        break;
                    case ArrowTypeId.List:
                        _valueColumns.Add(new ListColumn());
                        break;
                    case ArrowTypeId.Binary:
                        _valueColumns.Add(new BinaryColumn());
                        break;
                    case ArrowTypeId.Map:
                        _valueColumns.Add(new MapColumn());
                        break;
                    case ArrowTypeId.Decimal128:
                        _valueColumns.Add(new DecimalColumn());
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
                _typeList.Add((sbyte)ArrowTypeId.Null);
                _valueColumns[0].Add(value);
                _offsets.Add(0);
                return;
            }

            var typeByte = (byte)value.Type;
            CheckArrayExist(value.Type);
            var arrayIndex = _typeIds[typeByte];
            _typeList.InsertAt(index, arrayIndex);
            var valueColumn = _valueColumns[arrayIndex];
            var offset = valueColumn.Add(in value);
            _offsets.InsertAt(index, offset);
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

            if (type == (sbyte)value.Type)
            {
                var valueColumnIndex = _typeIds[(byte)type];
                var valueColumn = _valueColumns[valueColumnIndex];
                return valueColumn.CompareTo(_offsets.Get(index), in value);
            }
            else
            {
                return type - (sbyte)value.Type;
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
                    if (thisType == 0)
                    {
                        return 0;
                    }
                    var thisValueColumnIndex = _typeIds[(byte)thisType];
                    var otherValueColumnIndex = unionColumn._typeIds[(byte)otherType];
                    var thisValueColumn = _valueColumns[thisValueColumnIndex];
                    var otherValueColumn = unionColumn._valueColumns[otherValueColumnIndex];
                    return thisValueColumn.CompareTo(in otherValueColumn, _offsets.Get(thisIndex), unionColumn._offsets.Get(otherIndex));
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
            if (type == 0)
            {
                return new NullValue();
            }
            var valueColumnIndex = _typeIds[(byte)type];
            var valueColumn = _valueColumns[valueColumnIndex];
            return valueColumn.GetValueAt(_offsets.Get(index));
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer)
        {
            var type = _typeList[index];
            var valueColumnIndex = _typeIds[(byte)type];
            var valueColumn = _valueColumns[valueColumnIndex];
            valueColumn.GetValueAt(_offsets.Get(index), dataValueContainer);
        }

        public (int, int) SearchBoundries<T>(in T dataValue, in int start, in int end) where T : IDataValue
        {
            return BoundarySearch.SearchBoundriesForColumn(this, in dataValue, in start, in end);
        }

        public int Update<T>(in int index, in T value) where T : IDataValue
        {
            var currentType = _typeList[index];

            if (currentType != (sbyte)value.Type)
            {
                CheckArrayExist(value.Type);
                var typeByte = (byte)value.Type;
                _typeList[index] = (sbyte)value.Type;
                var valueColumnIndex = _typeIds[typeByte];
                var valueColumn = _valueColumns[valueColumnIndex];
                _offsets.Update(index, valueColumn.Add(in value));
                _deletesCounter++;
            }
            else
            {
                // Same type
                var valueColumnIndex = _typeIds[(byte)currentType];
                var valueColumn = _valueColumns[valueColumnIndex];
                var currentOffset = _offsets.Get(index);
                var newOffset = valueColumn.Update(_offsets.Get(index), in value);

                // Check if the offset has changed, then treat it as a insert and a delete.
                if (currentOffset != newOffset)
                {
                    _offsets.Update(index, newOffset);
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

        public (IArrowArray, IArrowType) ToArrowArray(ArrowBuffer nullBuffer, int nullCount)
        {
            if (_deletesCounter > 0 || outOfOrderCounter > 0)
            {
                // Need to rebuild the arrays before converting to arrow.
            }
            List<Field> fields = new List<Field>();
            List<int> typeIds = new List<int>();
            List<IArrowArray> childArrays = new List<IArrowArray>();
            for (int i = 0; i < _valueColumns.Count; i++)
            {
                if (_valueColumns[i] != null)
                {
                    var (arrowArray, arrowType) = _valueColumns[i].ToArrowArray(nullBuffer, nullCount);
                    typeIds.Add((int)arrowType.TypeId);
                    fields.Add(new Field("", arrowType, true));
                    childArrays.Add(arrowArray);
                }
            }
            var unionType = new UnionType(fields, typeIds, UnionMode.Dense);
            var typeIdsBuffer = new ArrowBuffer(_typeList.Memory);
            var offsetBuffer = new ArrowBuffer(_offsets.Memory);
            return (new DenseUnionArray(unionType, Count, childArrays, typeIdsBuffer, offsetBuffer), unionType);
        }
    }
}
