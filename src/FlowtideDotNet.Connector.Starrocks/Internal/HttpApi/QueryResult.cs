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

namespace FlowtideDotNet.Connector.StarRocks.Internal.HttpApi
{
    internal class QueryResult
    {
        public IReadOnlyList<SchemaField> Schema { get; }

        public IAsyncEnumerable<IReadOnlyList<object?>> Rows { get; }

        public QueryResult(IReadOnlyList<SchemaField> schema, IAsyncEnumerable<IReadOnlyList<object?>> rows)
        {
            Schema = schema;
            Rows = rows;
        }

        public int FieldIndex(string name)
        {
            for (int i = 0; i < Schema.Count; i++)
            {
                if (Schema[i].Name.Equals(name, StringComparison.OrdinalIgnoreCase))
                {
                    return i;
                }
            }
            return -1;
        }
    }
}
