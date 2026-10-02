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
            var index = @namespace.IndexOf("://", StringComparison.Ordinal);
            return index < 0 ? @namespace : @namespace.Substring(0, index);
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

            var database = mapping?.Database;
            var defaultSchema = mapping?.DefaultSchema ?? GetBuiltInDefaultSchema(shortNamespace) ?? string.Empty;
            if (IsFlat(shortNamespace))
            {
                return new DbtRelation(database, defaultSchema, tableName);
            }

            var parts = nameParts.Count > 0 ? nameParts : [tableName];
            var count = parts.Count;
            return count switch
            {
                1 => new DbtRelation(database, defaultSchema, parts[0]),
                2 => new DbtRelation(database, parts[0], parts[1]),
                3 => new DbtRelation(parts[0], parts[1], parts[2]),
                _ => new DbtRelation(string.Join(".", parts.Take(count - 2)), parts[count - 2], parts[count - 1])
            };
        }

        // Topics and indexes keep dots inside one identifier.
        private static bool IsFlat(string shortNamespace)
        {
            return shortNamespace.Equals("elasticsearch", StringComparison.OrdinalIgnoreCase) ||
                shortNamespace.StartsWith("kafka", StringComparison.OrdinalIgnoreCase);
        }

        private static string? GetBuiltInDefaultSchema(string shortNamespace)
        {
            return shortNamespace.Equals("mssql", StringComparison.OrdinalIgnoreCase) ? "dbo" : null;
        }
    }
}
