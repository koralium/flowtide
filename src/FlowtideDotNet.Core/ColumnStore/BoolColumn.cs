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
    internal class BoolColumn : IDataColumn
    {
        private BitmapArray _bitmapArray = new();
        private int _count;
        public int Add(in IDataValue value)
        {
            if (value.AsBool)
            {
                _bitmapArray.Set(_count);
            }
            else
            {
                _bitmapArray.Clear(_count);
            }
            return _count++;
        }

        public int Add<T>(in T value) where T : struct, IDataValue
        {
            if (value.AsBool)
            {
                _bitmapArray.Set(_count);
            }
            else
            {
                _bitmapArray.Clear(_count);
            }
            return _count++;
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
            return new BoolValue(_bitmapArray.IsSet(index));
        }

        public void GetValueAt(in int index, in DataValueContainer dataValueContainer)
        {
            dataValueContainer._type = ArrowTypeId.Boolean;
            dataValueContainer._boolValue = new BoolValue(_bitmapArray.IsSet(index));
        }

        public int Update(in int index, in IDataValue value)
        {
            if (value.AsBool)
            {
                _bitmapArray.Set(index);
            }
            else
            {
                _bitmapArray.Clear(index);
            }
            return index;
        }

        public int Update<T>(in int index, in T value) where T : struct, IDataValue
        {
            throw new NotImplementedException();
        }
    }
}
