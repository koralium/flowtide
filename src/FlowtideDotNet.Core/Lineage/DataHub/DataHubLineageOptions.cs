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

namespace FlowtideDotNet.Core.Lineage.DataHub
{
    /// <summary>
    /// Options for the DataHub lineage endpoint.
    /// </summary>
    public sealed class DataHubLineageOptions
    {
        private readonly Dictionary<string, DataHubNamespaceOptions> _namespaces = new Dictionary<string, DataHubNamespaceOptions>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Environment of datasets, flows and jobs.
        /// </summary>
        public string Env { get; set; } = "PROD";

        /// <summary>
        /// Namespaces left out, full or before "://".
        /// </summary>
        public ISet<string> ExcludedNamespaces { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "console", "blackhole", "test" };

        /// <summary>
        /// Asks connectors for their table schema at build.
        /// </summary>
        public bool IncludeConnectorSchema { get; set; } = true;

        /// <summary>
        /// Serves status and schema of every dataset in the lineage.
        /// </summary>
        public bool IncludeDatasetMetadata { get; set; } = true;

        /// <summary>
        /// Longest wait for expected streams before serving.
        /// </summary>
        public TimeSpan WarmupTimeout { get; set; } = TimeSpan.FromMinutes(2);

        /// <summary>
        /// Overrides a dataset, null keeps the default.
        /// </summary>
        public Func<DataHubDatasetContext, DataHubDataset?>? DatasetResolver { get; set; }

        /// <summary>
        /// Adds aspects to an entity, replacing built-in ones with the same name.
        /// </summary>
        public Func<DataHubEntityContext, IEnumerable<DataHubAspect>?>? AspectProvider { get; set; }

        /// <summary>
        /// Configures the DataHub naming of a namespace.
        /// </summary>
        /// <param name="namespace">Full namespace or the part before "://".</param>
        /// <param name="configure">Applied to the namespace options, repeated calls add up.</param>
        /// <returns>The same options instance.</returns>
        public DataHubLineageOptions MapNamespace(string @namespace, Action<DataHubNamespaceOptions> configure)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(@namespace);
            ArgumentNullException.ThrowIfNull(configure);
            if (!_namespaces.TryGetValue(@namespace, out var options))
            {
                options = new DataHubNamespaceOptions();
                _namespaces.Add(@namespace, options);
            }
            configure(options);
            return this;
        }

        internal IReadOnlyDictionary<string, DataHubNamespaceOptions> Namespaces => _namespaces;
    }
}
