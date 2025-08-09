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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Type;
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Connector.DeltaLake.Internal
{
    /// <summary>
    /// Allows sql metadata to be retrieved from a delta lake storage location
    /// </summary>
    internal class DeltaLakeSqlProvider : ITableProvider
    {
        private readonly DeltaLakeOptions deltaLakeOptions;
        //private HashSet<string>? tableNames;

        private Dictionary<string, TableMetadata> tableMetadataLookup;
        private HashSet<string> nonExistingTables;

        public DeltaLakeSqlProvider(DeltaLakeOptions deltaLakeOptions)
        {
            this.deltaLakeOptions = deltaLakeOptions;
            tableMetadataLookup = new Dictionary<string, TableMetadata>();
            nonExistingTables = new HashSet<string>();
        }

        private async Task<TableMetadata?> GetTableMetadata(string tableName)
        {
            var table = await DeltaTransactionReader.ReadTable(deltaLakeOptions.StorageLocation, tableName);

            if (table == null)
            {
                return null;
            }

            List<string> columnNames = new List<string>();
            List<SubstraitBaseType> types = new List<SubstraitBaseType>();

            foreach (var column in table.Schema.Fields)
            {
                columnNames.Add(column.Name);
                types.Add(DeltaToSubstraitTypeVisitor.Instance.Visit(column.Type));
            }

            var schema = new NamedStruct()
            {
                Names = columnNames,
                Struct = new Struct()
                {
                    Types = types
                }
            };

            return new TableMetadata(tableName, schema);
        }

        public bool TryGetTableInformation(IReadOnlyList<string> tableName, [NotNullWhen(true)] out TableMetadata? tableMetadata)
        {
            string path = string.Join("/", tableName);

            if (nonExistingTables.Contains(path))
            {
                tableMetadata = null;
                return false;
            }

            if (tableMetadataLookup.TryGetValue(path, out var table))
            {
                tableMetadata = table;
                return true;
            }
            var result = GetTableMetadata(path).Result;
            if (result != null)
            {
                tableMetadataLookup.Add(path, result);
                tableMetadata = result;
                return true;
            }
            nonExistingTables.Add(path);

            tableMetadata = null;
            return false;
        }
    }
}
