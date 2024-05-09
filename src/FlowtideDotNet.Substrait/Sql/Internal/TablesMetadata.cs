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

using FlowtideDotNet.Substrait.Type;
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Substrait.Sql.Internal
{
    internal class TablesMetadata
    {
        /// <summary>
        /// Cache of all known tables
        /// </summary>
        private readonly Dictionary<string, TableMetadata> _tables;

        /// <summary>
        /// List of table providers to use to get table information
        /// </summary>
        private readonly List<ITableProvider> _tableProviders;

        public TablesMetadata()
        {
            _tables = new Dictionary<string, TableMetadata>(StringComparer.OrdinalIgnoreCase);
            _tableProviders = new List<ITableProvider>();
        }

        public void AddTable(string name, NamedStruct schema)
        {
            _tables.Add(name, new TableMetadata(name, schema));
        }

        public void AddTableProvider(ITableProvider tableProvider)
        {
            _tableProviders.Add(tableProvider);
        }

        public bool TryGetTable(string tableName, [NotNullWhen(true)] out TableMetadata? tableMetadata)
        {
            if (_tables.TryGetValue(tableName, out tableMetadata))
            {
                return true;
            }
            foreach(var tableProvider in _tableProviders)
            {
                if (tableProvider.TryGetTableInformation(tableName, out tableMetadata))
                {
                    _tables.TryAdd(tableName, tableMetadata);
                    return true;
                }
            }
            tableMetadata = default;
            return false;
        }
    }
}
