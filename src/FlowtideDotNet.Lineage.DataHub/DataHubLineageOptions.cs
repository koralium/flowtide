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

namespace FlowtideDotNet.Lineage.DataHub
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
        public ISet<string> ExcludedNamespaces { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

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
        /// Serves the check functions of the streams as assertions with their latest status.
        /// </summary>
        public bool IncludeChecks { get; set; } = true;

        /// <summary>
        /// Raises an incident while a check fails and resolves it when the check passes, needs <see cref="IncludeChecks"/>.
        /// </summary>
        public bool RaiseIncidents { get; set; }

        /// <summary>
        /// Priority of raised incidents, and severity of failing checks with critical and high both high.
        /// </summary>
        public DataHubIncidentPriority IncidentPriority { get; set; } = DataHubIncidentPriority.Medium;

        /// <summary>
        /// Overrides the priority and severity of a check, null keeps <see cref="IncidentPriority"/>.
        /// </summary>
        public Func<DataHubIncidentContext, DataHubIncidentPriority?>? IncidentPriorityResolver { get; set; }

        /// <summary>
        /// Serves a run on each data job per stream, or per substream, with its latest state.
        /// </summary>
        public bool IncludeRuns { get; set; } = true;

        /// <summary>
        /// Serves the flowtide data platform with its display name and logo.
        /// </summary>
        public bool IncludePlatformInfo { get; set; } = true;

        /// <summary>
        /// Logo of the flowtide data platform, null for none.
        /// </summary>
        public string? PlatformLogoUrl { get; set; } = "https://raw.githubusercontent.com/koralium/flowtide/main/logo/flowtidelogo.svg";

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
