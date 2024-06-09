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

        public DecimalColumn()
        {
            _values = new List<decimal>();
        }

        public int Add(in IDataValue value)
        {
            var index = _values.Count;
            _values.Add(value.AsDecimal);
            return index;
        }

        public int Add<T>(in T value) where T : struct, IDataValue
        {
            var index = _values.Count;
            _values.Add(value.AsDecimal);
            return index;
        }

        public int CompareToStrict(in int index, in IDataValue value)
        {
            throw new NotImplementedException();
        }

        public int CompareToStrict(in IDataColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            throw new NotImplementedException();
        }

        public int CompareToStrict<T>(in int index, in T value) where T : struct, IDataValue
        {
            throw new NotImplementedException();
        }

        public IDataValue GetValueAt(in int index)
        {
            throw new NotImplementedException();
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer)
        {
            dataValueContainer._decimalValue = new DecimalValue(_values[index]);
        }

        public int Update(in int index, in IDataValue value)
        {
            throw new NotImplementedException();
        }

        public int Update<T>(in int index, in T value) where T : struct, IDataValue
        {
            throw new NotImplementedException();
        }
    }
}
