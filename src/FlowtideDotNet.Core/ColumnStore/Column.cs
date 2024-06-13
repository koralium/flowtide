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

using FlowtideDotNet.Core.ColumnStore.DataColumns;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    /// <summary>
    /// Represents a column of data.
    /// 
    /// It has the following state machine:
    /// Start -> Null column
    /// Add null value -> Null column
    /// Add value -> Value column (single type)
    /// Add different type -> Union column
    /// </summary>
    public class Column
    {
        private static readonly IDataValue NullValue = new NullValue();

        private int _nullCounter;
        private IDataColumn? _dataColumn;

        /// <summary>
        /// Null vailidity bitmap must be at this level and not in the data column
        /// since a column could start as a null column without any data.
        /// The type of data that is stored is then unknown.
        /// </summary>
        private BitmapList _nullList;

        /// <summary>
        /// The type of data the column is storing, starts with null
        /// </summary>
        private ArrowTypeId _type = ArrowTypeId.Null;

        public Column()
        {
            _nullList = new BitmapList();    
        }

        public int Count
        {
            get
            {
                if (_type == ArrowTypeId.Null)
                {
                    return _nullCounter;
                }
                return _dataColumn!.Count;
            }
        }

        private IDataColumn CreateArray(in ArrowTypeId type)
        {
            switch (type)
            {
                case ArrowTypeId.Int64:
                    return new Int64Column();
                case ArrowTypeId.String:
                    return new StringColumn();
                case ArrowTypeId.Boolean:
                    return new BoolColumn();
                case ArrowTypeId.Double:
                    return new DoubleColumn();
                case ArrowTypeId.List:
                    return new ListColumn();
                case ArrowTypeId.Binary:
                    return new BinaryColumn();
                case ArrowTypeId.Map:
                    return new MapColumn();
                case ArrowTypeId.Decimal128:
                    return new DecimalColumn();
                case ArrowTypeId.Union:
                    return new UnionColumn();
                default:
                    throw new NotImplementedException();
            }
        }

        public void Add<T>(in T value)
            where T : IDataValue
        {
            if (value.Type != _type)
            {
                if (_type == ArrowTypeId.Null)
                {
                    _dataColumn = CreateArray(value.Type);
                    _type = value.Type;

                    // Add null values as undefined values to the array.
                    for (var i = 0; i < _nullCounter; i++)
                    {
                        _dataColumn.Add(in NullValue);
                    }

                    _dataColumn.Add(value);
                    return;
                }
                if (_type == ArrowTypeId.Union)
                {
                    // Handle union
                    _dataColumn!.Add(value);
                    return;
                }
                var index = _dataColumn!.Count;
                if (value.Type == ArrowTypeId.Null)
                {
                    // Increase null counter
                    _nullCounter++;
                    // Set null bit
                    _nullList.Set(index);
                    // Add undefined value to value array
                    _dataColumn.Add(in NullValue);
                }
                else
                {
                    // Convert from single buffer to union buffer
                    var unionColumn = ConvertToUnion();
                    _dataColumn = unionColumn;
                    _nullCounter = 0;
                    _dataColumn.Add(value);
                }
            }
            // Same type
            else
            {
                // Check if the current buffer is a null buffer
                if (_type == ArrowTypeId.Null)
                {
                    var index = _nullCounter;
                    _nullCounter++;
                    _nullList.Set(index);
                }
                else
                {
                    _dataColumn!.Add(value);
                }
            }
        }

        private UnionColumn ConvertToUnion()
        {
            DataValueContainer dataValueContainer = new DataValueContainer();
            var unionColumn = new UnionColumn();
            for (int i = 0; i < Count; i++)
            {
                GetValueAt(in i, in dataValueContainer);
                unionColumn.Add(dataValueContainer);
            }
            return unionColumn;
        }

        public void InsertAt<T>(in int index, in T value)
            where T: IDataValue
        {
            if (value.Type != _type)
            {
                if (_type == ArrowTypeId.Null)
                {
                    _dataColumn = CreateArray(value.Type);
                    _type = value.Type;

                    // Add null values as undefined values to the array.
                    for (var i = 0; i < _nullCounter; i++)
                    {
                        _dataColumn.Add(in NullValue);
                    }

                    if (_nullCounter > 0)
                    {
                        _nullList.InsertAt(index, false);
                    }

                    _dataColumn.InsertAt(index, value);
                    return;
                }
                if (_type == ArrowTypeId.Union)
                {
                    // Handle union
                    _dataColumn!.InsertAt(index, value);
                    return;
                }
                if (value.Type == ArrowTypeId.Null)
                {
                    // Increase null counter
                    _nullCounter++;
                    // Set null bit
                    _nullList.Set(index);
                    // Add undefined value to value array
                    _dataColumn!.InsertAt(index, in NullValue);
                }
                else
                {
                    // Convert from single buffer to union buffer
                    var unionColumn = ConvertToUnion();
                    _dataColumn = unionColumn;
                    _nullCounter = 0;
                    _dataColumn.Add(value);
                }
            }
            // Same type
            else
            {
                // Check if the current buffer is a null buffer
                if (_type == ArrowTypeId.Null)
                {
                    _nullCounter++;
                    _nullList.Set(index);
                }
                else
                {
                    if (_nullCounter > 0)
                    {
                        _nullList.InsertAt(index, false);
                    }
                    _dataColumn!.InsertAt(index, value);
                }
            }
        }

        public void UpdateAt<T>(in int index, in T value)
            where T : IDataValue
        {
            if (_nullCounter > 0 &&
                    _nullList.Get(index))
            {
                if (value.Type == ArrowTypeId.Null)
                {
                    return;
                }
                _nullList.Unset(index);
                _nullCounter--;
            }
            if (value.Type == ArrowTypeId.Null)
            {
                _nullList.Set(index);
                _nullCounter++;
            }
            _dataColumn!.Update<T>(index, value);
        }

        public void RemoveAt(in int index)
        {
            if (_nullCounter > 0 &&
                _nullList.Get(index))
            {
                _nullList.RemoveAt(index);
                _nullCounter--;
            }
            else
            {
                _dataColumn!.RemoveAt(in index);
            }
        }

        public IDataValue GetValueAt(in int index)
        {
            try
            {
                if (_nullCounter > 0 &&
                _nullList.Get(index))
                {
                    return NullValue;
                }
                return _dataColumn!.GetValueAt(index);
            }
            catch(Exception e)
            {
                throw;
            }
            
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer)
        {
            if (_nullCounter > 0 &&
                _nullList.Get(index))
            {
                dataValueContainer._type = ArrowTypeId.Null;
            }
            _dataColumn!.GetValueAt(in index, in dataValueContainer);
        }

        public int CompareTo<T>(in int index, in T dataValue)
            where T : IDataValue
        {
            if (dataValue.Type == _type)
            {
                if (_type == ArrowTypeId.Null)
                {
                    return 0;
                }
                return _dataColumn!.CompareTo(index, in dataValue);
            }
            else if (_type == ArrowTypeId.Union)
            {
                return _dataColumn!.CompareTo(index, dataValue);
            }
            else
            {
                return _type - dataValue.Type;
            }
        }

        public int CompareTo(in Column otherColumn, in int thisIndex, in int otherIndex)
        {
            if (otherColumn._type == _type)
            {
                if (_type == ArrowTypeId.Null)
                {
                    return 0;
                }
                return _dataColumn!.CompareTo(otherColumn._dataColumn!, thisIndex, otherIndex);
            }
            // Check if any of the columns are unions, if so fetch the value and compare it
            else if (_type == ArrowTypeId.Union || otherColumn._type == ArrowTypeId.Union)
            {
                var otherValue = otherColumn.GetValueAt(otherIndex);
                return CompareTo(thisIndex, otherValue);
            }
            else
            {
                return _type - otherColumn._type;
            }
        }

        public (int, int) SearchBoundries<T>(in T value, in int start, in int end)
            where T : IDataValue
        {
            if (_type == value.Type)
            {
                if (_type == ArrowTypeId.Null)
                {
                    if (_nullCounter > 0)
                    {
                        return (0, _nullCounter);
                    }
                    return (~0, ~0);
                }
                // TODO: Check if there is any null values, if so null bitmap must be passed in.
                return _dataColumn!.SearchBoundries(in value, in start, in end);
            }
            else if (_type == ArrowTypeId.Union)
            {
                return _dataColumn!.SearchBoundries(in value, in start, in end);
            }
            else if (_nullCounter > 0 && value.Type == ArrowTypeId.Null)
            {
                // TODO: Must pass the validity bitmap to the search function
                return _dataColumn!.SearchBoundries(in value, in start, in end);
            }
            else
            {
                var compareValue = _type.CompareTo(value.Type);
                if (compareValue < 0)
                {
                    var index = Count;
                    return (~index, ~index);
                }
                else
                {
                    return (~0, ~0);
                }
            }
        }
    }
}
