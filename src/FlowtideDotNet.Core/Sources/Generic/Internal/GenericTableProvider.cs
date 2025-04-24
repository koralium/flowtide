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

using FlowtideDotNet.Core.ColumnStore.ObjectConverter;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Type;
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Core.Sources.Generic.Internal
{
    internal class GenericTableProvider<T> : ITableProvider
    {
        private readonly TableMetadata _tableMetadata;

        public GenericTableProvider(string name)
        {
            var schema = BatchConverter.GetBatchConverter(typeof(T)).GetSchema();
            schema.Names.Add("__key");
            if (schema.Struct != null)
            {
                schema.Struct.Types.Add(new StringType());
            }
            
            _tableMetadata = new TableMetadata(name, schema);
        }

        public bool TryGetTableInformation(IReadOnlyList<string> tableName, [NotNullWhen(true)] out TableMetadata? tableMetadata)
        {
            var fullName = string.Join(".", tableName);
            if (fullName.Equals(_tableMetadata.Name, StringComparison.OrdinalIgnoreCase))
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
