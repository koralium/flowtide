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

namespace FlowtideDotNet.Core.Lineage.Dbt
{
    /// <summary>
    /// Physical database, schema and identifier of a dbt node.
    /// </summary>
    public sealed class DbtRelation
    {
        /// <summary>
        /// Creates a relation, normalizing empty parts.
        /// </summary>
        /// <param name="database">Database name, empty becomes null.</param>
        /// <param name="schema">Schema name, null becomes empty.</param>
        /// <param name="identifier">Table name, must not be empty.</param>
        public DbtRelation(string? database, string? schema, string identifier)
        {
            ArgumentException.ThrowIfNullOrEmpty(identifier);
            Database = string.IsNullOrEmpty(database) ? null : database;
            Schema = schema ?? string.Empty;
            Identifier = identifier;
        }

        /// <summary>
        /// Database name, null when unknown.
        /// </summary>
        public string? Database { get; }

        /// <summary>
        /// Schema name, never null.
        /// </summary>
        public string Schema { get; }

        /// <summary>
        /// Physical table name.
        /// </summary>
        public string Identifier { get; }
    }
}
