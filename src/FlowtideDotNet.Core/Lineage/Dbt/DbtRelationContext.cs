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
    /// Table passed to the relation resolver.
    /// </summary>
    public sealed class DbtRelationContext
    {
        internal DbtRelationContext(string @namespace, string tableName, IReadOnlyList<string> nameParts, DbtRelation defaultRelation)
        {
            Namespace = @namespace;
            TableName = tableName;
            NameParts = nameParts;
            DefaultRelation = defaultRelation;
        }

        /// <summary>
        /// Lineage namespace reported by the connector.
        /// </summary>
        public string Namespace { get; }

        /// <summary>
        /// Table name reported by the connector.
        /// </summary>
        public string TableName { get; }

        /// <summary>
        /// Physical name parts, catalog prefix stripped.
        /// </summary>
        public IReadOnlyList<string> NameParts { get; }

        /// <summary>
        /// Relation from the built-in rules and namespace mappings.
        /// </summary>
        public DbtRelation DefaultRelation { get; }
    }
}
