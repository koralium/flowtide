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

using FlowtideDotNet.Substrait.Sql;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.TestFramework.Internal
{
    internal class TestDataTableProvider : ITableProvider
    {
        private readonly string _tableName;
        private readonly TestDataTable _table;

        public TestDataTableProvider(string tableName, TestDataTable table)
        {
            this._tableName = tableName;
            this._table = table;
        }

        public bool TryGetTableInformation(IReadOnlyList<string> tableName, [NotNullWhen(true)] out TableMetadata? tableMetadata)
        {
            if (string.Join(".", tableName) == this._tableName)
            {
                tableMetadata = new TableMetadata(_tableName, _table.GetSchema());
                return true;
            }
            tableMetadata = null;
            return false;
        }
    }
}
