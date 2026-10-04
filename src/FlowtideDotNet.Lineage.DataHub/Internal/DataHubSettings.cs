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

namespace FlowtideDotNet.Lineage.DataHub.Internal
{
    internal sealed record DataHubNamespaceMapping(
        string? Platform,
        string? PlatformInstance,
        string? Env,
        string? Database,
        string? DefaultSchema,
        bool LowercaseNames,
        bool LowercaseColumns,
        bool? IncludeDatasetMetadata);

    // Frozen copy, later option changes are ignored.
    internal sealed class DataHubSettings
    {
        // DataHub FabricType values.
        private static readonly FrozenSet<string> s_environments = new[]
        {
            "PROD", "DEV", "TEST", "QA", "UAT", "EI", "PRE", "STG", "NON_PROD", "CORP", "RVW", "PRD", "TST", "SIT", "SBX", "SANDBOX", "CERT"
        }.ToFrozenSet(StringComparer.Ordinal);

        private DataHubSettings(
            string env,
            FrozenSet<string> excludedNamespaces,
            FrozenDictionary<string, DataHubNamespaceMapping> namespaceMappings,
            bool includeConnectorSchema,
            bool includeDatasetMetadata,
            TimeSpan warmupTimeout,
            Func<DataHubDatasetContext, DataHubDataset?>? datasetResolver,
            Func<DataHubEntityContext, IEnumerable<DataHubAspect>?>? aspectProvider,
            bool includePlatformInfo,
            string? platformLogoUrl)
        {
            Env = env;
            ExcludedNamespaces = excludedNamespaces;
            NamespaceMappings = namespaceMappings;
            IncludeConnectorSchema = includeConnectorSchema;
            IncludeDatasetMetadata = includeDatasetMetadata;
            WarmupTimeout = warmupTimeout;
            DatasetResolver = datasetResolver;
            AspectProvider = aspectProvider;
            IncludePlatformInfo = includePlatformInfo;
            PlatformLogoUrl = platformLogoUrl;
        }

        public string Env { get; }

        public FrozenSet<string> ExcludedNamespaces { get; }

        public FrozenDictionary<string, DataHubNamespaceMapping> NamespaceMappings { get; }

        public bool IncludeConnectorSchema { get; }

        public bool IncludeDatasetMetadata { get; }

        public TimeSpan WarmupTimeout { get; }

        public Func<DataHubDatasetContext, DataHubDataset?>? DatasetResolver { get; }

        public Func<DataHubEntityContext, IEnumerable<DataHubAspect>?>? AspectProvider { get; }

        public bool IncludePlatformInfo { get; }

        public string? PlatformLogoUrl { get; }

        public static DataHubSettings Create(DataHubLineageOptions options)
        {
            ArgumentNullException.ThrowIfNull(options);
            var mappings = options.Namespaces.ToFrozenDictionary(
                x => x.Key,
                x => new DataHubNamespaceMapping(
                    string.IsNullOrWhiteSpace(x.Value.Platform) ? null : x.Value.Platform,
                    string.IsNullOrWhiteSpace(x.Value.PlatformInstance) ? null : x.Value.PlatformInstance,
                    x.Value.Env == null ? null : NormalizeEnv(x.Value.Env),
                    x.Value.Database,
                    x.Value.DefaultSchema,
                    x.Value.LowercaseNames,
                    x.Value.LowercaseColumns,
                    x.Value.IncludeDatasetMetadata),
                StringComparer.OrdinalIgnoreCase);
            return new DataHubSettings(
                NormalizeEnv(options.Env),
                options.ExcludedNamespaces.ToFrozenSet(StringComparer.OrdinalIgnoreCase),
                mappings,
                options.IncludeConnectorSchema,
                options.IncludeDatasetMetadata,
                options.WarmupTimeout,
                options.DatasetResolver,
                options.AspectProvider,
                options.IncludePlatformInfo,
                string.IsNullOrWhiteSpace(options.PlatformLogoUrl) ? null : options.PlatformLogoUrl);
        }

        // DataHub rejects aspects with an unknown FabricType.
        public static string NormalizeEnv(string env)
        {
            ArgumentNullException.ThrowIfNull(env);
            var normalized = env.ToUpperInvariant();
            if (!s_environments.Contains(normalized))
            {
                throw new ArgumentException($"'{env}' is not a DataHub environment, use one of {string.Join(", ", s_environments.Order(StringComparer.Ordinal))}.", nameof(env));
            }
            return normalized;
        }
    }
}
