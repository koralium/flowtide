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

using System.Collections.Frozen;

namespace FlowtideDotNet.Core.Lineage.Dbt.Internal
{
    // Frozen copy, later option changes are ignored.
    internal sealed class DbtGeneratorSettings
    {
        private DbtGeneratorSettings(
            string projectName,
            DbtSqlDialectInfo dialect,
            FrozenSet<string> excludedNamespaces,
            FrozenDictionary<string, DbtNamespaceMapping> namespaceMappings,
            Func<DbtRelationContext, DbtRelation?>? relationResolver,
            bool includeConnectorSchema,
            TimeSpan warmupTimeout)
        {
            ProjectName = projectName;
            Dialect = dialect;
            ExcludedNamespaces = excludedNamespaces;
            NamespaceMappings = namespaceMappings;
            RelationResolver = relationResolver;
            IncludeConnectorSchema = includeConnectorSchema;
            WarmupTimeout = warmupTimeout;
        }

        public string ProjectName { get; }

        public DbtSqlDialectInfo Dialect { get; }

        public FrozenSet<string> ExcludedNamespaces { get; }

        public FrozenDictionary<string, DbtNamespaceMapping> NamespaceMappings { get; }

        public Func<DbtRelationContext, DbtRelation?>? RelationResolver { get; }

        public bool IncludeConnectorSchema { get; }

        public TimeSpan WarmupTimeout { get; }

        public static DbtGeneratorSettings Create(DbtManifestOptions options)
        {
            ArgumentNullException.ThrowIfNull(options);
            var projectName = string.IsNullOrWhiteSpace(options.ProjectName) ? DbtConstants.DefaultProjectName : DbtNodeNamer.Sanitize(options.ProjectName);
            return new DbtGeneratorSettings(
                projectName,
                DbtSqlDialectInfo.For(options.SqlDialect),
                options.ExcludedNamespaces.ToFrozenSet(StringComparer.OrdinalIgnoreCase),
                options.NamespaceMappings.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase),
                options.RelationResolver,
                options.IncludeConnectorSchema,
                options.WarmupTimeout);
        }
    }
}
