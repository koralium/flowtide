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

using FlowtideDotNet.Core.Lineage.Internal;
using System.Collections.ObjectModel;

namespace FlowtideDotNet.Core.Lineage.Dbt.Internal
{
    // One instance per generation, the cache never outlives it.
    internal sealed class DbtRelationResolution
    {
        private readonly DbtGeneratorSettings _settings;
        private readonly Dictionary<(string, string, string), DbtTableIdentity?> _cache = new Dictionary<(string, string, string), DbtTableIdentity?>();

        public DbtRelationResolution(DbtGeneratorSettings settings)
        {
            _settings = settings;
        }

        public static string ShortNamespace(string @namespace)
        {
            return LineageRelationNames.ShortNamespace(@namespace);
        }

        public bool IsExcluded(string @namespace)
        {
            return _settings.ExcludedNamespaces.Contains(@namespace) || _settings.ExcludedNamespaces.Contains(ShortNamespace(@namespace));
        }

        // Null when the namespace is excluded.
        public DbtTableIdentity? Resolve(string @namespace, string tableName, IReadOnlyList<string> nameParts)
        {
            var key = (@namespace, tableName, string.Join('\u001f', nameParts));
            if (_cache.TryGetValue(key, out var cached))
            {
                return cached;
            }

            DbtTableIdentity? identity = null;
            if (!IsExcluded(@namespace))
            {
                var relation = ResolveRelation(@namespace, tableName, nameParts);
                identity = new DbtTableIdentity(@namespace, relation.Database, relation.Schema, relation.Identifier);
            }
            _cache.Add(key, identity);
            return identity;
        }

        private DbtRelation ResolveRelation(string @namespace, string tableName, IReadOnlyList<string> nameParts)
        {
            try
            {
                var defaultRelation = GetDefaultRelation(@namespace, tableName, nameParts);
                if (_settings.RelationResolver == null)
                {
                    return defaultRelation;
                }
                var parts = new ReadOnlyCollection<string>(nameParts.ToArray());
                return _settings.RelationResolver(new DbtRelationContext(@namespace, tableName, parts, defaultRelation)) ?? defaultRelation;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to resolve the dbt relation for namespace '{@namespace}' table '{tableName}'.", ex);
            }
        }

        private DbtRelation GetDefaultRelation(string @namespace, string tableName, IReadOnlyList<string> nameParts)
        {
            var shortNamespace = ShortNamespace(@namespace);
            if (!_settings.NamespaceMappings.TryGetValue(@namespace, out var mapping))
            {
                _settings.NamespaceMappings.TryGetValue(shortNamespace, out mapping);
            }

            var (database, schema, identifier) = LineageRelationNames.Split(shortNamespace, tableName, nameParts, mapping?.Database, mapping?.DefaultSchema);
            return new DbtRelation(database, schema, identifier);
        }
    }
}
