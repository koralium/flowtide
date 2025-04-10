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
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Core.ColumnStore.Serialization.Serializer;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using System.Collections;
using System.Diagnostics;
using System.Text.Json;

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
    public class Column : IColumn, IReadOnlyList<IDataValue>
    {
        private IMemoryAllocator? _memoryAllocator;
        private int _nullCounter;
        private IDataColumn? _dataColumn;

        /// <summary>
        /// Null vailidity bitmap must be at this level and not in the data column
        /// since a column could start as a null column without any data.
        /// The type of data that is stored is then unknown.
        /// </summary>
        private BitmapList? _validityList;

        /// <summary>
        /// The type of data the column is storing, starts with null
        /// </summary>
        private ArrowTypeId _type = ArrowTypeId.Null;
        private bool disposedValue;
        private int _rentCounter;

        public int ByteSize => GetByteSize();

        public Column()
        {

        }

        internal void Assign(IMemoryAllocator memoryAllocator)
        {
            _memoryAllocator = memoryAllocator;
            _dataColumn = null;
            _nullCounter = 0;
            _type = ArrowTypeId.Null;
            _rentCounter = 0;
            _validityList = BitmapListFactory.Get(memoryAllocator);
            disposedValue = false;
        }

        internal void Assign(int nullCounter, IDataColumn? dataColumn, BitmapList validityList, ArrowTypeId type, IMemoryAllocator memoryAllocator)
        {
            _nullCounter = nullCounter;
            _dataColumn = dataColumn;
            _validityList = validityList;
            _type = type;
            _memoryAllocator = memoryAllocator;
            _rentCounter = 0;
            disposedValue = false;
        }

        public static Column Create(IMemoryAllocator memoryAllocator)
        {
            return ColumnFactory.Get(memoryAllocator);
        }

        public static Column Create(int nullCounter, IDataColumn? dataColumn, BitmapList validityList, ArrowTypeId type, IMemoryAllocator memoryAllocator)
        {
            return ColumnFactory.Get(nullCounter, dataColumn, validityList, type, memoryAllocator);
        }

        public Column(IMemoryAllocator memoryAllocator)
        {
            _memoryAllocator = memoryAllocator;
            _validityList = BitmapListFactory.Get(memoryAllocator);
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
        IDataColumn IColumn.DataColumn => _dataColumn!;

        internal StructHeader StructHeader => _dataColumn!.StructHeader;

        StructHeader? IColumn.StructHeader => StructHeader;

        public IDataValue this[int index] => GetValueAt(index, default);

        /// <summary>
        /// Used only for debugging
        /// </summary>
        /// <returns></returns>
        internal int GetValidityListCount()
        {
            Debug.Assert(_validityList != null);
            return _validityList.Count;
        }

        private IDataColumn CreateArray<T>(in T value)
            where T : IDataValue
        {
            var type = value.Type;
            Debug.Assert(_memoryAllocator != null);
            switch (type)
            {
                case ArrowTypeId.Struct:
                    return new StructColumn(value.AsStructValue.Header, _memoryAllocator);
                default:
                    return CreateArrayByType(type);
            }
        }

        private IDataColumn CreateArrayByType(in ArrowTypeId type)
        {
            Debug.Assert(_memoryAllocator != null);
            switch (type)
            {
                case ArrowTypeId.Int64:
                    return new IntegerColumn(_memoryAllocator);
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
                case ArrowTypeId.Timestamp:
                    return new TimestampTzColumn(_memoryAllocator);
                default:
                    throw new NotImplementedException();
            }
        }

        private bool CompareValueType<T>(in T value)
            where T : IDataValue
        {
            if (value.Type != _type)
            {
                return false;
            }
            if (_type == ArrowTypeId.Struct)
            {
                return StructHeader.Equals(value.AsStructValue.Header);
            }
            return true;
        }

        public void Add<T>(in T value)
            where T : IDataValue
        {
            Debug.Assert(_validityList != null);
            if (!CompareValueType(value))
            {
                if (_type == ArrowTypeId.Null)
                {
                    _dataColumn = CreateArray(value);
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
                    var previousColumn = _dataColumn;
                    _dataColumn = unionColumn;
                    previousColumn.Dispose();
                    _validityList.Clear();
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
            Debug.Assert(_validityList != null);
            if (_nullCounter == 0)
            {
                _validityList.InsertTrueInRange(0, Count);
            }
        }

        private UnionColumn ConvertToUnion()
        {
            Debug.Assert(_memoryAllocator != null);
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
            where T : IDataValue
        {
            Debug.Assert(_validityList != null);
            if (!CompareValueType(value))
            {
                if (_type == ArrowTypeId.Null)
                {
                    _dataColumn = CreateArray(value);
                    _type = value.Type;

                    // Add null values as undefined values to the array.
                    _dataColumn.InsertNullRange(0, _nullCounter);

                    if (_nullCounter > 0)
                    {
                        _validityList.Unset(Count - 1);
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
                    var previousColumn = _dataColumn;
                    _dataColumn = unionColumn;
                    if (previousColumn != null)
                    {
                        previousColumn.Dispose();
                    }
                    _type = ArrowTypeId.Union;
                    _validityList.Clear();
                    _nullCounter = 0;
                    _dataColumn.InsertAt(index, value);
                }
            }
            // Same type
            else
            {
                // Check if the current buffer is a null buffer
                if (_type == ArrowTypeId.Null)
                {
                    _nullCounter++;
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
            Debug.Assert(_validityList != null);
            if (_type == ArrowTypeId.Union)
            {
                _dataColumn!.Update<T>(index, value);
            }
            else if (value.Type == ArrowTypeId.Null)
            {
                // If it is null and being updated to null nothing needs to be updated
                if (_type != ArrowTypeId.Null)
                {
                    CheckNullInitialization();
                    if (_validityList.Get(index))
                    {
                        _validityList.Unset(index);
                        _nullCounter++;
                    }
                }
            }
            else
            {
                if (_type == ArrowTypeId.Null)
                {
                    _dataColumn = CreateArray(value);
                    _type = value.Type;
                    for (var i = 0; i < _nullCounter; i++)
                    {
                        _dataColumn.Add(in NullValue.Instance);
                    }
                    _validityList.Unset(Count - 1);
                }
                if (!CompareValueType(value))
                {
                    var unionColumn = ConvertToUnion();
                    _type = ArrowTypeId.Union;
                    var previousColumn = _dataColumn;
                    _dataColumn = unionColumn;
                    if (previousColumn != null)
                    {
                        previousColumn.Dispose();
                    }
                    _validityList.Clear();
                    _nullCounter = 0;
                    _dataColumn.Update<T>(index, value);
                }
                else
                {
                    _dataColumn!.Update<T>(index, value);

                    // Set to not null
                    if (_nullCounter > 0 &&
                        !_validityList.Get(index))
                    {
                        _validityList.Set(index);
                        _nullCounter--;
                    }
                }
            }
        }

        public void RemoveAt(in int index)
        {
            Debug.Assert(_validityList != null);
            if (_nullCounter > 0)
            {
                if (_type == ArrowTypeId.Null)
                {
                    _nullCounter--;
                }
                else
                {
                    if (!_validityList.Get(index))
                    {
                        _nullCounter--;
                    }
                    _validityList.RemoveAt(index);
                }
            }
            if (_dataColumn != null)
            {
                _dataColumn!.RemoveAt(in index);
            }
        }

        public void RemoveRange(in int index, in int count)
        {
            Debug.Assert(_validityList != null);
            if (_nullCounter > 0)
            {
                if (_type == ArrowTypeId.Null)
                {
                    _nullCounter -= count;
                }
                else
                {
                    _nullCounter -= _validityList.CountFalseInRange(index, count);
                    _validityList.RemoveRange(index, count);
                }
            }
            if (_dataColumn != null)
            {
                _dataColumn!.RemoveRange(index, count);
            }
        }

        public IDataValue GetValueAt(in int index, in ReferenceSegment? child)
        {
            Debug.Assert(_validityList != null);
            if (_nullCounter > 0)
            {
                if (_type == ArrowTypeId.Null)
                {
                    return NullValue.Instance;
                }
                else if (!_validityList.Get(index))
                {
                    return NullValue.Instance;
                }
            }
            return _dataColumn!.GetValueAt(index, child);
        }

        public ArrowTypeId GetTypeAt(in int index, in ReferenceSegment? child)
        {
            Debug.Assert(_validityList != null);
            if (_type == ArrowTypeId.Union)
            {
                return _dataColumn!.GetTypeAt(index, child);
            }
            if (_nullCounter > 0)
            {
                if (_type == ArrowTypeId.Null)
                {
                    return ArrowTypeId.Null;
                }
                else if (!_validityList.Get(index))
                {
                    return ArrowTypeId.Null;
                }
            }
            return _type;
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer, in ReferenceSegment? child)
        {
            Debug.Assert(_validityList != null);
            if (_nullCounter > 0)
            {
                if (_type == ArrowTypeId.Null)
                {
                    dataValueContainer._type = ArrowTypeId.Null;
                    return;
                }
                else if (!_validityList.Get(index))
                {
                    dataValueContainer._type = ArrowTypeId.Null;
                    return;
                }
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
                if (_nullCounter > 0 && dataValue.IsNull)
                {
                    if (_validityList!.Get(index))
                    {
                        return 1;
                    }
                    return 0;
                }
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

        public (int, int) SearchBoundries<T>(in T value, in int start, in int end, in ReferenceSegment? child, bool desc = false)
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
                    if (!desc)
                    {
                        return BoundarySearch.SearchBoundriesForDataColumn(in _dataColumn!, in value, in start, end, child, _validityList);
                    }
                    else
                    {
                        return BoundarySearch.SearchBoundriesForDataColumnDesc(in _dataColumn!, in value, in start, end, child, _validityList);
                    }

                }
                return _dataColumn!.SearchBoundries(in value, in start, in end, child, desc);
            }
            else if (_type == ArrowTypeId.Union)
            {
                return _dataColumn!.SearchBoundries(in value, in start, in end, child, desc);
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
                return _dataColumn!.SearchBoundries(in value, in start, in end, child, desc);
            }
            else if (_nullCounter > 0 && value.Type == ArrowTypeId.Null)
            {
                if (!desc)
                {
                    return BoundarySearch.SearchBoundriesForDataColumn(in _dataColumn!, in value, in start, end, child, _validityList);
                }
                else
                {
                    return BoundarySearch.SearchBoundriesForDataColumnDesc(in _dataColumn!, in value, in start, end, child, _validityList);
                }
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
            Debug.Assert(_validityList != null);
            if (_type == ArrowTypeId.Null)
            {
                return (new Apache.Arrow.NullArray(Count), NullType.Default);
            }

            var nullBuffer = new ArrowBuffer(_validityList.MemorySlice);
            return _dataColumn!.ToArrowArray(nullBuffer, _nullCounter);
        }

        protected virtual void Dispose(bool disposing)
        {
            Debug.Assert(_validityList != null);
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

                if (disposing)
                {
                    ColumnFactory.Return(this);
                }
            }
        }

        public void Rent(int count)
        {
            Interlocked.Add(ref _rentCounter, count);
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

        public void Clear()
        {
            if (_nullCounter > 0)
            {
                Debug.Assert(_validityList != null);
                _validityList.Clear();
                _nullCounter = 0;
            }
            if (_dataColumn != null)
            {
                _dataColumn.Clear();
            }
        }

        public void AddToNewList<T>(in T value) where T : IDataValue
        {
            if (_type == ArrowTypeId.List)
            {
                _dataColumn!.AddToNewList(value);
            }
            else if (_type == ArrowTypeId.Null)
            {
                CheckNullInitialization();
                _dataColumn = CreateArray(ListValue.Empty);
                _type = ArrowTypeId.List;

                // Add null values as undefined values to the array.
                for (var i = 0; i < _nullCounter; i++)
                {
                    _dataColumn.Add(in NullValue.Instance);
                }
                _dataColumn.AddToNewList(value);
            }
            else if (_type == ArrowTypeId.Union)
            {
                _dataColumn!.AddToNewList(value);
            }
            else
            {
                var unionColumn = ConvertToUnion();
                _type = ArrowTypeId.Union;
                var previousColumn = _dataColumn;
                _dataColumn = unionColumn;
                if (previousColumn != null)
                {
                    previousColumn.Dispose();
                }
                if (_validityList != null)
                {
                    _validityList.Clear();
                }
                _nullCounter = 0;
                _dataColumn.AddToNewList(value);
            }

        }

        public int EndNewList()
        {
            if (_type == ArrowTypeId.List)
            {
                return _dataColumn!.EndNewList();
            }
            else if (_type == ArrowTypeId.Null)
            {
                CheckNullInitialization();
                _dataColumn = CreateArray(ListValue.Empty);
                _type = ArrowTypeId.List;

                // Add null values as undefined values to the array.
                for (var i = 0; i < _nullCounter; i++)
                {
                    _dataColumn.Add(in NullValue.Instance);
                }
                return _dataColumn!.EndNewList();
            }
            else if (_type == ArrowTypeId.Union)
            {
                return _dataColumn!.EndNewList();
            }
            else
            {
                var unionColumn = ConvertToUnion();
                _type = ArrowTypeId.Union;
                var previousColumn = _dataColumn;
                _dataColumn = unionColumn;
                if (previousColumn != null)
                {
                    previousColumn.Dispose();
                }
                if (_validityList != null)
                {
                    _validityList.Clear();
                }
                _nullCounter = 0;
                return _dataColumn!.EndNewList();
            }
        }

        internal int GetNullCount()
        {
            return _nullCounter;
        }

        public int GetByteSize(int start, int end)
        {
            if (_type == ArrowTypeId.Null)
            {
                return 0;
            }

            return _dataColumn!.GetByteSize(start, end) + _validityList!.GetByteSize(start, end);
        }

        public int GetByteSize()
        {
            if (_type == ArrowTypeId.Null)
            {
                return 0;
            }
            return _dataColumn!.GetByteSize() + _validityList!.GetByteSize(0, Count - 1);
        }

        private bool CompareOtherColumnType(Column otherColumn)
        {
            if (_type == otherColumn.Type)
            {
                if (_type == ArrowTypeId.Struct)
                {
                    if (this.StructHeader.Equals(otherColumn.StructHeader))
                    {
                        return true;
                    }
                    return false;
                }
                return true;
            }
            return false;
        }

        public void InsertRangeFrom(int index, IColumn otherColumn, int start, int count)
        {
            if (otherColumn is Column column)
            {
                
                if (CompareOtherColumnType(column))
                {
                    if (_type == ArrowTypeId.Null)
                    {
                        // Both columns are null, just add null count
                        _nullCounter += count;
                        return;
                    }
                    if (_type == ArrowTypeId.Union)
                    {
                        Debug.Assert(_dataColumn != null);
                        Debug.Assert(column._dataColumn != null);
                        _dataColumn.InsertRangeFrom(index, column._dataColumn, start, count, column._nullCounter > 0 ? column._validityList : default);
                        return;
                    }

                    if (_nullCounter > 0 || column._nullCounter > 0)
                    {
                        if (_nullCounter > 0 && column._nullCounter > 0)
                        {
                            Debug.Assert(_validityList != null);
                            Debug.Assert(column._validityList != null);

                            _validityList!.InsertRangeFrom(index, column._validityList!, start, count);
                            _nullCounter += column._validityList.CountFalseInRange(start, count);
                        }
                        else if (_nullCounter > 0)
                        {
                            Debug.Assert(_validityList != null);

                            // Set entire range as not null, no need to update null counter since nothing is null in the copy
                            _validityList.InsertTrueInRange(index, count);
                        }
                        else // Incoming contain null but this column does not
                        {
                            Debug.Assert(column._validityList != null);
                            Debug.Assert(_validityList != null);

                            // check so all existing values are set as not null
                            CheckNullInitialization();

                            // Insert the range from the other columns validity list
                            _validityList.InsertRangeFrom(index, column._validityList!, start, count);
                            // Count how many are null in the range
                            _nullCounter = column._validityList.CountFalseInRange(start, count);
                        }
                    }

                    Debug.Assert(_dataColumn != null);
                    Debug.Assert(column._dataColumn != null);
                    // Insert the actual data
                    _dataColumn.InsertRangeFrom(index, column._dataColumn, start, count, column._nullCounter > 0 ? column._validityList : default);
                }
                else
                {
                    if (_type == ArrowTypeId.Null)
                    {
                        Debug.Assert(_validityList != null);
                        if (otherColumn.Type == ArrowTypeId.Struct)
                        {
                            if (otherColumn.StructHeader == null)
                            {
                                throw new InvalidOperationException("Struct header is null, cannot create struct column.");
                            }
                            Debug.Assert(_memoryAllocator != null);
                            _dataColumn = new StructColumn(otherColumn.StructHeader.Value, _memoryAllocator);
                        }
                        else
                        {
                            _dataColumn = CreateArrayByType(otherColumn.Type);
                        }
                        
                        _type = otherColumn.Type;

                        if (_type == ArrowTypeId.Union)
                        {
                            _dataColumn.InsertNullRange(0, _nullCounter);
                            _validityList.Clear();
                            _nullCounter = 0;
                        }

                        // Add null values as undefined values to the array.
                        if (_nullCounter > 0)
                        {
                            _dataColumn.InsertNullRange(0, _nullCounter);
                            _validityList.Unset(Count - 1);
                        }
                        // Check if we need to copy over null values
                        if (column._nullCounter > 0 || _nullCounter > 0)
                        {
                            if (column._nullCounter > 0)
                            {
                                Debug.Assert(column._validityList != null);
                                _validityList.InsertRangeFrom(index, column._validityList!, start, count);
                                _nullCounter += column._validityList.CountFalseInRange(start, count);
                            }
                            else
                            {
                                _validityList.InsertTrueInRange(index, count);
                            }
                        }
                    }
                    else if (column.Type == ArrowTypeId.Null)
                    {
                        Debug.Assert(_dataColumn != null);
                        if (_type != ArrowTypeId.Union)
                        {
                            CheckNullInitialization();
                            Debug.Assert(_validityList != null);
                            _validityList.InsertFalseInRange(index, count);
                            _nullCounter += count;
                        }
                        _dataColumn.InsertNullRange(index, count);
                        return;
                    }
                    else if (_type != ArrowTypeId.Union)
                    {
                        Debug.Assert(_validityList != null);
                        Debug.Assert(_dataColumn != null);

                        // Convert the column into a union column since the types differ and this is not a union column
                        var unionColumn = ConvertToUnion();
                        _type = ArrowTypeId.Union;
                        var previousColumn = _dataColumn;
                        _dataColumn = unionColumn;
                        previousColumn.Dispose();
                        _validityList.Clear();
                        _nullCounter = 0;
                    }

                    Debug.Assert(_dataColumn != null);
                    Debug.Assert(column._dataColumn != null);

                    // Insert the data into the union column
                    _dataColumn.InsertRangeFrom(index, column._dataColumn, start, count, column._nullCounter > 0 ? column._validityList : default);
                }
            }
            else
            {
                throw new NotImplementedException("Insert range from does not yet work from a column with offset.");
            }
        }

        private IEnumerable<IDataValue> GetEnumerable()
        {
            for (var i = 0; i < Count; i++)
            {
                yield return GetValueAt(i, default);
            }
        }

        public IEnumerator<IDataValue> GetEnumerator()
        {
            return GetEnumerable().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerable().GetEnumerator();
        }

        public void WriteToJson(ref readonly Utf8JsonWriter writer, in int index)
        {
            Debug.Assert(_validityList != null);

            if (_nullCounter > 0)
            {
                if (_type == ArrowTypeId.Null)
                {
                    writer.WriteNullValue();
                    return;
                }
                if (!_validityList.Get(index))
                {
                    writer.WriteNullValue();
                    return;
                }
            }

            _dataColumn!.WriteToJson(in writer, index);
        }

        public Column Copy(IMemoryAllocator memoryAllocator)
        {
            return new Column(_nullCounter, _dataColumn?.Copy(memoryAllocator), _validityList!.Copy(memoryAllocator), _type, memoryAllocator);
        }

        internal int CreateSchemaField(ref ArrowSerializer arrowSerializer, int emptyStringPointer, Span<int> pointerStack)
        {
            if (_type == ArrowTypeId.Null)
            {
                var nullTypePointer = arrowSerializer.AddNullType();
                return arrowSerializer.CreateField(emptyStringPointer, true, Serialization.ArrowType.Null, nullTypePointer);
            }

            return _dataColumn!.CreateSchemaField(ref arrowSerializer, emptyStringPointer, pointerStack);
        }

        int IColumn.CreateSchemaField(ref ArrowSerializer arrowSerializer, int emptyStringPointer, Span<int> pointerStack)
        {
            return CreateSchemaField(ref arrowSerializer, emptyStringPointer, pointerStack);
        }

        public SerializationEstimation GetSerializationEstimate()
        {
            if (_type == ArrowTypeId.Null)
            {
                return new SerializationEstimation(1, 0, sizeof(int));
            }

            var estimate = _dataColumn!.GetSerializationEstimate();
            return new SerializationEstimation(estimate.fieldNodeCount, 1 + estimate.bufferCount, estimate.bodyLength + _validityList!.GetByteSize(0, Count - 1));
        }

        void IColumn.AddFieldNodes(ref ArrowSerializer arrowSerializer)
        {
            AddFieldNodes(ref arrowSerializer);
        }

        internal void AddFieldNodes(ref ArrowSerializer arrowSerializer)
        {
            if (_type == ArrowTypeId.Null)
            {
                arrowSerializer.CreateFieldNode(_nullCounter, _nullCounter);
                return;
            }

            _dataColumn!.AddFieldNodes(ref arrowSerializer, _nullCounter);
        }

        void IColumn.AddBuffers(ref ArrowSerializer arrowSerializer)
        {
            AddBuffers(ref arrowSerializer);
        }

        internal void AddBuffers(ref ArrowSerializer arrowSerializer)
        {
            if (_type == ArrowTypeId.Null)
            {
                return;
            }
            if (_type != ArrowTypeId.Union)
            {
                // Union does not have a validity bitmap in apache arrow
                arrowSerializer.AddBufferForward(_validityList!.MemorySlice.Length);
            }

            _dataColumn!.AddBuffers(ref arrowSerializer);
        }

        void IColumn.WriteDataToBuffer(ref ArrowDataWriter dataWriter)
        {
            this.WriteDataToBuffer(ref dataWriter);
        }

        internal void WriteDataToBuffer(ref ArrowDataWriter dataWriter)
        {
            if (_type == ArrowTypeId.Null)
            {
                return;
            }

            if (_type != ArrowTypeId.Union)
            {
                // Union does not have a validity bitmap in apache arrow
                if (_nullCounter == 0)
                {
                    // write an empty buffer if there is no null values
                    dataWriter.WriteArrowBuffer(Span<byte>.Empty);
                }
                else
                {
                    dataWriter.WriteArrowBuffer(_validityList!.MemorySlice.Span);
                }
            }

            _dataColumn!.WriteDataToBuffer(ref dataWriter);
        }
    }
}
