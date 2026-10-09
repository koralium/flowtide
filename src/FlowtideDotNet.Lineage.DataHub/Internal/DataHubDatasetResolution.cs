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

namespace FlowtideDotNet.Lineage.DataHub.Internal
{
    internal sealed class DataHubResolvedDataset
    {
        public required string Urn { get; init; }

        public required string Platform { get; init; }

        // Name with the platform instance prefix.
        public required string QualifiedName { get; init; }

        public string? PlatformInstance { get; init; }

        public required string Env { get; init; }

        public required bool IncludeMetadata { get; init; }

        public required bool LowercaseColumns { get; init; }

        public required string Namespace { get; init; }

        public required string TableName { get; init; }
    }

    // One instance per generation, the cache never outlives it.
    internal sealed class DataHubDatasetResolution
    {
        private readonly DataHubSettings _settings;
        private readonly Dictionary<(string, string, string), DataHubResolvedDataset?> _cache = new Dictionary<(string, string, string), DataHubResolvedDataset?>();

        public DataHubDatasetResolution(DataHubSettings settings)
        {
            _settings = settings;
        }

        // Null when the namespace is excluded.
        public DataHubResolvedDataset? Resolve(string @namespace, string tableName, IReadOnlyList<string> nameParts)
        {
            var key = (@namespace, tableName, string.Join('\u001f', nameParts));
            if (_cache.TryGetValue(key, out var cached))
            {
                return cached;
            }

            DataHubResolvedDataset? dataset = null;
            if (!IsExcluded(@namespace))
            {
                dataset = ResolveDataset(@namespace, tableName, nameParts);
            }
            _cache.Add(key, dataset);
            return dataset;
        }

        private bool IsExcluded(string @namespace)
        {
            return _settings.ExcludedNamespaces.Contains(@namespace) || _settings.ExcludedNamespaces.Contains(LineageRelationNames.ShortNamespace(@namespace));
        }

        private DataHubResolvedDataset ResolveDataset(string @namespace, string tableName, IReadOnlyList<string> nameParts)
        {
            var shortNamespace = LineageRelationNames.ShortNamespace(@namespace);
            if (!_settings.NamespaceMappings.TryGetValue(@namespace, out var mapping))
            {
                _settings.NamespaceMappings.TryGetValue(shortNamespace, out mapping);
            }

            try
            {
                var defaultDataset = GetDefaultDataset(shortNamespace, tableName, nameParts, mapping);
                var dataset = defaultDataset;
                if (_settings.DatasetResolver != null)
                {
                    var parts = new ReadOnlyCollection<string>(nameParts.ToArray());
                    dataset = _settings.DatasetResolver(new DataHubDatasetContext(@namespace, tableName, parts, defaultDataset)) ?? defaultDataset;
                }

                var env = dataset.Env == null ? _settings.Env : DataHubSettings.NormalizeEnv(dataset.Env);
                var qualifiedName = dataset.PlatformInstance == null ? dataset.Name : dataset.PlatformInstance + "." + dataset.Name;
                // DataHub lowercases the instance prefix with the name, a resolver result is used as is.
                if (mapping?.LowercaseNames == true && ReferenceEquals(dataset, defaultDataset))
                {
                    qualifiedName = qualifiedName.ToLowerInvariant();
                }
                return new DataHubResolvedDataset()
                {
                    Urn = DataHubUrns.Dataset(dataset.Platform, qualifiedName, env),
                    Platform = dataset.Platform,
                    QualifiedName = qualifiedName,
                    PlatformInstance = dataset.PlatformInstance,
                    Env = env,
                    IncludeMetadata = mapping?.IncludeDatasetMetadata ?? _settings.IncludeDatasetMetadata,
                    LowercaseColumns = mapping?.LowercaseColumns ?? false,
                    Namespace = @namespace,
                    TableName = tableName
                };
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to resolve the DataHub dataset for namespace '{@namespace}' table '{tableName}'.", ex);
            }
        }

        private DataHubDataset GetDefaultDataset(string shortNamespace, string tableName, IReadOnlyList<string> nameParts, DataHubNamespaceMapping? mapping)
        {
            string name;
            if (LineageRelationNames.IsFlat(shortNamespace))
            {
                name = tableName;
            }
            else
            {
                var (database, schema, identifier) = LineageRelationNames.Split(shortNamespace, tableName, nameParts, mapping?.Database, mapping?.DefaultSchema);
                name = string.Join(".", new[] { database, schema, identifier }.Where(x => !string.IsNullOrEmpty(x)));
            }
            if (mapping?.LowercaseNames == true)
            {
                name = name.ToLowerInvariant();
            }
            return new DataHubDataset(mapping?.Platform ?? GetBuiltInPlatform(shortNamespace), name, mapping?.PlatformInstance, mapping?.Env ?? _settings.Env);
        }

        // Connector namespaces that differ from the DataHub platform id.
        private static string GetBuiltInPlatform(string shortNamespace)
        {
            var platform = shortNamespace.ToLowerInvariant();
            return platform switch
            {
                "postgresql" => "postgres",
                "delta_table" => "delta-lake",
                _ => platform
            };
        }
    }
}
