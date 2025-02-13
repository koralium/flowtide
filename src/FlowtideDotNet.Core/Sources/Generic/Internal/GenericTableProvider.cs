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

namespace FlowtideDotNet.Core.Sources.Generic.Internal
{
    internal class GenericTableProvider<T> : ITableProvider
    {
        private readonly TableMetadata _tableMetadata;

        public GenericTableProvider(string name)
        {
            var properties = typeof(T).GetProperties();
            var columnNames = properties.Select(x =>
            {
                return x.Name;
            }).ToList();

            columnNames.Add("__key");

            _tableMetadata = new TableMetadata(name, new Substrait.Type.NamedStruct()
            {
                Names = columnNames
            });
        }

        public bool TryGetTableInformation(string tableName, [NotNullWhen(true)] out TableMetadata? tableMetadata)
        {
            if (tableName.Equals(_tableMetadata.Name, StringComparison.OrdinalIgnoreCase))
            {
                tableMetadata = _tableMetadata;
                return true;
            }
            else
            {
                tableMetadata = null;
                return false;
            }
        }
    }
}
