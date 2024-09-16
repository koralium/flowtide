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
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.AcceptanceTests.Internal
{
    internal class DatasetTableProvider : ITableProvider
    {
        private readonly MockDatabase mockDatabase;

        public DatasetTableProvider(MockDatabase mockDatabase)
        {
            this.mockDatabase = mockDatabase;
        }

        public bool TryGetTableInformation(string tableName, [NotNullWhen(true)] out TableMetadata? tableMetadata)
        {
            if (mockDatabase.Tables.TryGetValue(tableName, out var mockTable))
            {
                tableMetadata = new TableMetadata(tableName, new Substrait.Type.NamedStruct()
                {
                    Names = mockTable.Columns.ToList(),
                    Struct = new Substrait.Type.Struct()
                    {
                        Types = mockTable.Types.ToList()
                    }
                });
                return true;
            }
            tableMetadata = null;
            return false;
        }
    }
}
