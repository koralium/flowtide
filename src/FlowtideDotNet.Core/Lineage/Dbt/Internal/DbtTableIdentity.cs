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

namespace FlowtideDotNet.Core.Lineage.Dbt.Internal
{
    // One dbt node per resolved table, ordinal equality.
    internal readonly record struct DbtTableIdentity(string Namespace, string? Database, string Schema, string Identifier)
    {
        public IEnumerable<string> NonEmptyParts()
        {
            if (!string.IsNullOrEmpty(Database))
            {
                yield return Database;
            }
            if (!string.IsNullOrEmpty(Schema))
            {
                yield return Schema;
            }
            yield return Identifier;
        }
    }
}
