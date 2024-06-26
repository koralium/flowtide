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
using FlowtideDotNet.Core.ColumnStore.DataColumns;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Memory;
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
    /// <summary>
    /// Represents a column of data.
    /// 
    /// It has the following state machine:
    /// Start -> Null column
    /// Add null value -> Null column
    /// Add value -> Value column (single type)
    /// Add different type -> Union column
    /// </summary>
    public class Column : IColumn
    {
        private readonly IMemoryAllocator _memoryAllocator;
        private int _nullCounter;
        private IDataColumn? _dataColumn;

        /// <summary>
        /// Null vailidity bitmap must be at this level and not in the data column
        /// since a column could start as a null column without any data.
        /// The type of data that is stored is then unknown.
        /// </summary>
        private BitmapList _validityList;

        /// <summary>
        /// The type of data the column is storing, starts with null
        /// </summary>
        private ArrowTypeId _type = ArrowTypeId.Null;
        private bool disposedValue;
        private int _rentCounter;

        public Column(IMemoryAllocator memoryAllocator)
        {
            _memoryAllocator = memoryAllocator;
            _validityList = new BitmapList(memoryAllocator);    
        }

        internal Column(int nullCounter, IDataColumn? dataColumn, BitmapList validityList, ArrowTypeId type, IMemoryAllocator memoryAllocator)
        {
            _nullCounter = nullCounter;
            _dataColumn = dataColumn;
            _validityList = validityList;
            _type = type;
            _memoryAllocator = memoryAllocator;
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

        public ArrowTypeId Type => _type;
        IDataColumn IColumn.DataColumn =>  _dataColumn!;

        private IDataColumn CreateArray(in ArrowTypeId type)
        {
            switch (type)
            {
                case ArrowTypeId.Int64:
                    return new Int64Column(_memoryAllocator);
                case ArrowTypeId.String:
                    return new StringColumn(_memoryAllocator);
                case ArrowTypeId.Boolean:
                    return new BoolColumn(_memoryAllocator);
                case ArrowTypeId.Double:
                    return new DoubleColumn(_memoryAllocator);
                case ArrowTypeId.List:
                    return new ListColumn(_memoryAllocator);
                case ArrowTypeId.Binary:
                    return new BinaryColumn(_memoryAllocator);
                case ArrowTypeId.Map:
                    return new MapColumn(_memoryAllocator);
                case ArrowTypeId.Decimal128:
                    return new DecimalColumn(_memoryAllocator);
                case ArrowTypeId.Union:
                    return new UnionColumn(_memoryAllocator);
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
                    if (_nullCounter > 0)
                    {
                        for (var i = 0; i < _nullCounter; i++)
                        {
                            _dataColumn.Add(in NullValue.Instance);
                        }
                        _validityList.Set(Count);
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
                    CheckNullInitialization();
                    // Increase null counter
                    _nullCounter++;
                    // Set null bit
                    _validityList.Unset(index);
                    // Add undefined value to value array
                    _dataColumn.Add(in NullValue.Instance);
                }
                else
                {
                    // Convert from single buffer to union buffer
                    var unionColumn = ConvertToUnion();
                    _type = ArrowTypeId.Union;
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
                    CheckNullInitialization();
                    var index = _nullCounter;
                    _nullCounter++;
                    _validityList.Unset(index);
                }
                else
                {
                    if (_nullCounter > 0)
                    {
                        _validityList.Set(_dataColumn!.Count);
                    }
                    _dataColumn!.Add(value);
                }
            }
        }

        private void CheckNullInitialization()
        {
            if (_nullCounter == 0)
            {
                for (int i = 0; i < Count; i++)
                {
                    _validityList.Set(i);
                }
            }
        }

        private UnionColumn ConvertToUnion()
        {
            DataValueContainer dataValueContainer = new DataValueContainer();
            var unionColumn = new UnionColumn(_memoryAllocator);
            for (int i = 0; i < Count; i++)
            {
                GetValueAt(in i, in dataValueContainer, default);
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
                        _dataColumn.Add(in NullValue.Instance);
                    }

                    if (_nullCounter > 0)
                    {
                        _validityList.InsertAt(index, true);
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
                    CheckNullInitialization();
                    // Increase null counter
                    _nullCounter++;
                    // Set null bit
                    _validityList.InsertAt(index, false);
                    // Add undefined value to value array
                    _dataColumn!.InsertAt(index, in NullValue.Instance);
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
                    _validityList.InsertAt(index, false);
                }
                else
                {
                    if (_nullCounter > 0)
                    {
                        _validityList.InsertAt(index, true);
                    }
                    _dataColumn!.InsertAt(index, value);
                }
            }
        }

        public void UpdateAt<T>(in int index, in T value)
            where T : IDataValue
        {
            if (_nullCounter > 0 &&
                    !_validityList.Get(index))
            {
                if (value.Type == ArrowTypeId.Null)
                {
                    return;
                }
                _validityList.Set(index);
                _nullCounter--;
            }
            if (value.Type == ArrowTypeId.Null)
            {
                _validityList.Unset(index);
                _nullCounter++;
            }
            _dataColumn!.Update<T>(index, value);
        }

        public void RemoveAt(in int index)
        {
            if (_nullCounter > 0)
            {
                if (!_validityList.Get(index))
                {
                    _nullCounter--;
                }
                _validityList.RemoveAt(index);
            }
            if (_dataColumn != null)
            {
                _dataColumn!.RemoveAt(in index);
            }
        }

        public IDataValue GetValueAt(in int index, in ReferenceSegment? child)
        {
            if (_nullCounter > 0 &&
            !_validityList.Get(index))
            {
                return NullValue.Instance;
            }
            return _dataColumn!.GetValueAt(index, child);
        }

        public ArrowTypeId GetTypeAt(in int index, in ReferenceSegment? child)
        {
            if (_type == ArrowTypeId.Union)
            {
                return _dataColumn!.GetTypeAt(index, child);
            }
            if (_nullCounter > 0 &&
            !_validityList.Get(index))
            {
                return ArrowTypeId.Null;
            }
            return _type;
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer, in ReferenceSegment? child)
        {
            if (_nullCounter > 0 &&
                !_validityList.Get(index))
            {
                dataValueContainer._type = ArrowTypeId.Null;
                return;
            }
            _dataColumn!.GetValueAt(in index, in dataValueContainer, child);
        }

        public int CompareTo<T>(in int index, in T dataValue, in ReferenceSegment? child)
            where T : IDataValue
        {
            if (dataValue.Type == _type)
            {
                if (_type == ArrowTypeId.Null)
                {
                    return 0;
                }
                return _dataColumn!.CompareTo(index, in dataValue, child, _nullCounter > 0 ? _validityList : default);
            }
            else if (_type == ArrowTypeId.Union)
            {
                return _dataColumn!.CompareTo(index, dataValue, child, default);
            }
            else
            {
                return _type - dataValue.Type;
            }
        }

        public int CompareTo(in IColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            if (otherColumn.Type == _type)
            {
                if (_type == ArrowTypeId.Null)
                {
                    return 0;
                }
                return _dataColumn!.CompareTo(otherColumn.DataColumn!, thisIndex, otherIndex);
            }
            // Check if any of the columns are unions, if so fetch the value and compare it
            else if (_type == ArrowTypeId.Union || otherColumn.Type == ArrowTypeId.Union)
            {
                var otherValue = otherColumn.GetValueAt(otherIndex, default);
                return CompareTo(thisIndex, otherValue, default);
            }
            else
            {
                return _type - otherColumn.Type;
            }
        }

        public (int, int) SearchBoundries<T>(in T value, in int start, in int end, in ReferenceSegment? child)
            where T : IDataValue
        {
            if (_type == value.Type)
            {
                if (_type == ArrowTypeId.Null)
                {
                    if (_nullCounter > 0)
                    {
                        return (start, end);
                    }
                    return (~start, ~start);
                }
                // TODO: Check if there is any null values, if so null bitmap must be passed in.
                if (_nullCounter > 0)
                {
                    return BoundarySearch.SearchBoundriesForDataColumn(in _dataColumn!, in value, in start, end, child, _validityList);
                }
                return _dataColumn!.SearchBoundries(in value, in start, in end, child);
            }
            else if (_type == ArrowTypeId.Union)
            {
                return _dataColumn!.SearchBoundries(in value, in start, in end, child);
            }
            else if (_type == ArrowTypeId.Null)
            {
                var compareValue = _type.CompareTo(value.Type);
                var index = end + 1;
                return compareValue < 0 ? (~index, ~index) : (~start, ~start);
            }
            else if (child != null)
            {
                if (_nullCounter > 0)
                {
                    return BoundarySearch.SearchBoundriesForDataColumn(in _dataColumn!, in value, in start, end, child, _validityList);
                }
                return _dataColumn!.SearchBoundries(in value, in start, in end, child);
            }
            else if (_nullCounter > 0 && value.Type == ArrowTypeId.Null)
            {
                // TODO: Must pass the validity bitmap to the search function
                return BoundarySearch.SearchBoundriesForDataColumn(in _dataColumn!, in value, in start, end, child, _validityList);
                //return _dataColumn!.SearchBoundries(in value, in start, in end, child);
            }
            else
            {
                var compareValue = _type.CompareTo(value.Type);
                if (compareValue < 0)
                {
                    var index = end + 1;
                    return (~index, ~index);
                }
                else
                {
                    return (~start, ~start);
                }
            }
        }

        public (IArrowArray, IArrowType) ToArrowArray()
        {
            if (_type == ArrowTypeId.Null)
            {
                return (new Apache.Arrow.NullArray(Count), NullType.Default);
            }

            var nullBuffer = new ArrowBuffer(_validityList.Memory);
            return _dataColumn!.ToArrowArray(nullBuffer, _nullCounter);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                _validityList.Dispose();
                if (_dataColumn != null)
                {
                    _dataColumn.Dispose();
                }
                // TODO: free unmanaged resources (unmanaged objects) and override finalizer
                // TODO: set large fields to null
                disposedValue = true;
            }
        }

        public void Rent()
        {
            Interlocked.Increment(ref _rentCounter);
        }

        public void Return()
        {
            var result = Interlocked.Decrement(ref _rentCounter);

            if (result <= 0)
            {
                Dispose();
            }
        }

        ~Column()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: false);
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
