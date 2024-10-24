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
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.Serialization.CustomTypes;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    internal class DecimalColumn : IDataColumn
    {
        private PrimitiveList<decimal> _values;
        private bool disposedValue;

        public int Count => _values.Count;

        public ArrowTypeId Type => ArrowTypeId.Decimal128;

        public DecimalColumn(IMemoryAllocator memoryAllocator)
        {
            _values = new PrimitiveList<decimal>(memoryAllocator);
        }

        public DecimalColumn(IMemoryOwner<byte> memory, int length, IMemoryAllocator memoryAllocator)
        {
            _values = new PrimitiveList<decimal>(memory, length, memoryAllocator);   
        }

        public int Add<T>(in T value) where T : IDataValue
        {
            var index = _values.Count;
            if (value.Type == ArrowTypeId.Null)
            {
                _values.Add(0);
            }
            else
            {
                _values.Add(value.AsDecimal);
            }
            return index;
        }

        public int CompareTo(in IDataColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            Debug.Assert(_values != null);

            if (otherColumn is DecimalColumn decimalColumn)
            {
                Debug.Assert(decimalColumn._values != null);
                return _values.Get(thisIndex).CompareTo(decimalColumn._values.Get(otherIndex));
            }
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
            return _values[index].CompareTo(value.AsDecimal);
        }

        public IDataValue GetValueAt(in int index, in ReferenceSegment? child)
        {
            return new DecimalValue(_values[index]);
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer, in ReferenceSegment? child)
        {
            dataValueContainer._type = ArrowTypeId.Decimal128;
            dataValueContainer._decimalValue = new DecimalValue(_values[index]);
        }

        public (int, int) SearchBoundries<T>(in T dataValue, in int start, in int end, in ReferenceSegment? child, bool desc) 
            where T : IDataValue
        {
            if (desc)
            {
                return BoundarySearch.SearchBoundries(_values, dataValue.AsDecimal, start, end, DecimalComparerDesc.Instance);
            }
            return BoundarySearch.SearchBoundries(_values, dataValue.AsDecimal, start, end, DecimalComparer.Instance);
        }

        public int Update<T>(in int index, in T value) where T : IDataValue
        {
            _values.Update(index, value.AsDecimal);
            return index;
        }

        public void RemoveAt(in int index)
        {
            _values.RemoveAt(index);
        }

        public void InsertAt<T>(in int index, in T value) where T : IDataValue
        {
            if (value.Type == ArrowTypeId.Null)
            {
                _values.InsertAt(index, 0);
            }
            else
            {
                _values.InsertAt(index, value.AsDecimal);
            }
        }

        public (IArrowArray, IArrowType) ToArrowArray(ArrowBuffer nullBuffer, int nullCount)
        {
            var buffers = new ArrowBuffer[2]
            {
                nullBuffer,
                new ArrowBuffer(_values.Memory)
            };
            var array = new FixedSizeBinaryArray(new ArrayData(FloatingPointDecimalType.Default, Count, nullCount, 0, buffers));
            return (array, FloatingPointDecimalType.Default);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _values.Dispose();
                }
                disposedValue = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        public ArrowTypeId GetTypeAt(in int index, in ReferenceSegment? child)
        {
            return ArrowTypeId.Decimal128;
        }

        public void Clear()
        {
            _values.Clear();
        }

        public void AddToNewList<T>(in T value) where T : IDataValue
        {
            throw new NotImplementedException();
        }

        public int EndNewList()
        {
            throw new NotImplementedException();
        }

        public void RemoveRange(int start, int count)
        {
            _values.RemoveRange(start, count);
        }

        public int GetByteSize(int start, int end)
        {
            return (end - start + 1) * sizeof(decimal);
        }

        public int GetByteSize()
        {
            return Count * sizeof(decimal);
        }

        public void InsertRangeFrom(int index, IDataColumn other, int start, int count, BitmapList? validityList)
        {
            if (other is DecimalColumn decimalColumn)
            {
                _values.InsertRangeFrom(index, decimalColumn._values, start, count);
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        public void InsertNullRange(int index, int count)
        {
            _values.InsertStaticRange(index, 0, count);
        }
    }
}
