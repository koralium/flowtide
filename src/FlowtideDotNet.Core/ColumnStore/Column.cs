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
using FlowtideDotNet.Core.ColumnStore.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    internal class Column
    {
        private int _nullCounter;
        private IDataColumn _dataColumn;
        private BitmapList _nullList;
        /// <summary>
        /// The type of data the column is storing, starts with null
        /// </summary>
        private ArrowTypeId _type = ArrowTypeId.Null;

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
            //if (value.Type != _type)
            //{
            //    if (_type == ArrowTypeId.Null)
            //    {
            //        _dataColumn = CreateArray(value.Type);
            //    }
            //    if (_type == ArrowTypeId.Union)
            //    {
            //        // Handle union
            //        return;
            //    }
            //    var index = _dataColumn.Count;
            //    if (value.Type == ArrowTypeId.Null)
            //    {
            //        _nullList.Set()
            //    }
            //}
            //else
            //{

            //}
        }
    }
}
