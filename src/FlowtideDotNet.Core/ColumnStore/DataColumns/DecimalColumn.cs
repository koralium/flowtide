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
using FlowtideDotNet.Core.ColumnStore.Utils;
using FlowtideDotNet.Substrait.Expressions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    internal class DecimalColumn : IDataColumn
    {
        private List<decimal> _values;

        public int Count => _values.Count;

        public ArrowTypeId Type => ArrowTypeId.Decimal128;

        public DecimalColumn()
        {
            _values = new List<decimal>();
        }

        public int Add<T>(in T value) where T : IDataValue
        {
            var index = _values.Count;
            _values.Add(value.AsDecimal);
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
            return new DecimalValue(_values[index]);
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer, in ReferenceSegment? child)
        {
            dataValueContainer._decimalValue = new DecimalValue(_values[index]);
        }

        public (int, int) SearchBoundries<T>(in T dataValue, in int start, in int end, in ReferenceSegment? child) 
            where T : IDataValue
        {
            throw new NotImplementedException();
        }

        public int Update(in int index, in IDataValue value)
        {
            throw new NotImplementedException();
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

        public (IArrowArray, IArrowType) ToArrowArray(ArrowBuffer nullBuffer, int nullCount)
        {
            throw new NotImplementedException();
        }
    }
}
