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
    public interface IDataColumn
    {
        int CompareToStrict<T>(in int index, in T value)
            where T: struct, IDataValue;

        int CompareToStrict(in IDataColumn otherColumn, in int thisIndex, in int otherIndex);

        int Add<T>(in T value)
            where T: struct, IDataValue;

        int Add(in IDataValue value);

        IDataValue GetValueAt(in int index);

        void GetValueAt(in int index, in DataValueContainer dataValueContainer);

        int Update<T>(in int index, in T value)
            where T: struct, IDataValue;

        int BinarySearch(in IDataValue dataValue, int start, int end);
    }
}
