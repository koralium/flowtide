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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Substrait.Type;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.Files
{
    public class FileExtraColumn
    {
        public FileExtraColumn(string columnName, SubstraitBaseType dataType, Func<string, long, Dictionary<string, string>, IDataValue> getValueFunction)
        {
            ColumnName = columnName;
            DataType = dataType;
            GetValueFunction = getValueFunction;
        }

        public string ColumnName { get; }
        public SubstraitBaseType DataType { get; }
        public Func<string, long, Dictionary<string, string>, IDataValue> GetValueFunction { get; }
    }
}
