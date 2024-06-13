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
    public class BinaryColumn : AbstractBinaryColumn
    {
        public override ArrowTypeId Type => ArrowTypeId.Binary;

        public override int CompareToStrict(in int index, in IDataValue value)
        {
            throw new NotImplementedException();
        }

        public override int CompareTo(in IDataColumn otherColumn, in int thisIndex, in int otherIndex)
        {
            throw new NotImplementedException();
        }

        public override IDataValue GetValueAt(in int index)
        {
            if (index + 1 < _offsets.Count)
            {
                return new BinaryValue(_data, _offsets[index], _offsets[index + 1]);
            }
            else
            {
                return new BinaryValue(_data, _offsets[index], _length);
            }
        }

        public override void GetValueAt(in int index, in DataValueContainer dataValueContainer)
        {
            throw new NotImplementedException();
        }

        public override int Update(in int index, in IDataValue value)
        {
            return Add(value);
        }

        public override int Add<T>(in T value)
        {
            var span = value.AsBinary;
            return Add(span);
        }
    }
}
