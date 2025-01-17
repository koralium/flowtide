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

using FlowtideDotNet.Core.ColumnStore.ObjectConverter.Encoders;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.ObjectConverter.Converters
{
    internal class Int64Converter : IObjectColumnConverter
    {
        private readonly ObjectConverterColumnInfo columnInfo;

        public Int64Converter(ObjectConverterColumnInfo columnInfo)
        {
            this.columnInfo = columnInfo;
        }

        public object Deserialize(IColumn column, int index)
        {
            var value = column.GetValueAt(index, default);
            return Convert.ChangeType(value.AsLong, columnInfo.Type);
        }

        public void Serialize(object obj, IColumn destination)
        {
            var data = Convert.ToInt64(columnInfo.GetFunc(obj));
            destination.Add(new Int64Value(data));
        }

        public IDataValue SerializeDataValue(object obj)
        {
            var data = Convert.ToInt64(columnInfo.GetFunc(obj));
            return new Int64Value(data);
        }
    }
}
