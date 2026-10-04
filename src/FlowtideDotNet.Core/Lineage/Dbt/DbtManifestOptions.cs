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

using FlowtideDotNet.Core.Lineage.Dbt.Internal;

namespace FlowtideDotNet.Core.Lineage.Dbt
{
    /// <summary>
    /// Options for the mock dbt manifest.
    /// </summary>
    public sealed class DbtManifestOptions
    {
        private readonly Dictionary<string, DbtNamespaceMapping> _namespaceMappings = new Dictionary<string, DbtNamespaceMapping>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Package name and unique id prefix.
        /// </summary>
        public string ProjectName { get; set; } = "flowtide";

        /// <summary>
        /// Dialect of the mock SQL and adapter type.
        /// </summary>
        public DbtSqlDialect SqlDialect { get; set; } = DbtSqlDialect.Postgres;

        /// <summary>
        /// Namespaces left out, full or before "://".
        /// </summary>
        public ISet<string> ExcludedNamespaces { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase) {  };

        /// <summary>
        /// Asks connectors for their table schema at build.
        /// </summary>
        public bool IncludeConnectorSchema { get; set; } = true;

        /// <summary>
        /// Longest wait for expected streams before serving.
        /// </summary>
        public TimeSpan WarmupTimeout { get; set; } = TimeSpan.FromMinutes(2);

        /// <summary>
        /// Overrides a relation, null keeps the default.
        /// </summary>
        public Func<DbtRelationContext, DbtRelation?>? RelationResolver { get; set; }

        /// <summary>
        /// Sets the database and default schema of a namespace.
        /// </summary>
        /// <param name="namespace">Full namespace or the part before "://".</param>
        /// <param name="database">Database for one and two part names.</param>
        /// <param name="defaultSchema">Schema for one part names.</param>
        /// <returns>The same options instance.</returns>
        public DbtManifestOptions MapNamespace(string @namespace, string? database = null, string? defaultSchema = null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(@namespace);
            _namespaceMappings[@namespace] = new DbtNamespaceMapping(database, defaultSchema);
            return this;
        }

        internal IReadOnlyDictionary<string, DbtNamespaceMapping> NamespaceMappings => _namespaceMappings;
    }
}
