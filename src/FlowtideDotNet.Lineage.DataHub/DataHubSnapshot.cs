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

using FlowtideDotNet.Lineage.DataHub.Internal;

namespace FlowtideDotNet.Lineage.DataHub
{
    /// <summary>
    /// Every entity served to DataHub at one point in time.
    /// </summary>
    public sealed class DataHubSnapshot
    {
        private readonly Dictionary<string, byte[]> _entities;
        private readonly Dictionary<string, DataHubAssertion> _assertions;
        private readonly DataHubCheckStatusTable? _checkStatuses;
        private readonly long _version;
        private readonly Dictionary<string, (DataHubAssertion Assertion, DataHubIncidentInfo Info)> _incidents;
        private readonly HashSet<string> _removedIncidents;
        private readonly Dictionary<string, DataHubRunInfo> _runs;

        internal DataHubSnapshot(
            IReadOnlyList<string> urns,
            Dictionary<string, byte[]> entities,
            Dictionary<string, DataHubAssertion>? assertions = null,
            DataHubCheckStatusTable? checkStatuses = null,
            long version = 0,
            Dictionary<string, (DataHubAssertion Assertion, DataHubIncidentInfo Info)>? incidents = null,
            HashSet<string>? removedIncidents = null,
            Dictionary<string, DataHubRunInfo>? runs = null)
        {
            Urns = urns;
            _entities = entities;
            _assertions = assertions ?? new Dictionary<string, DataHubAssertion>(StringComparer.Ordinal);
            _checkStatuses = checkStatuses;
            _version = version;
            _incidents = incidents ?? new Dictionary<string, (DataHubAssertion Assertion, DataHubIncidentInfo Info)>(StringComparer.Ordinal);
            _removedIncidents = removedIncidents ?? new HashSet<string>(StringComparer.Ordinal);
            _runs = runs ?? new Dictionary<string, DataHubRunInfo>(StringComparer.Ordinal);
        }

        /// <summary>
        /// Entity urns in ordinal order, with the incident urn of every check that could raise one and the run urns of every stream before it starts.
        /// </summary>
        public IReadOnlyList<string> Urns { get; }

        /// <summary>
        /// Gets an entity as a GMS entitiesV2 response.
        /// </summary>
        /// <param name="urn">Entity urn, case sensitive.</param>
        /// <param name="utf8Json">UTF-8 encoded entity JSON.</param>
        /// <returns>False when the urn is not served, such as an incident whose check never failed or the run of a stream that has not started.</returns>
        /// <remarks>Reading an assertion or an incident publishes the latest check status, as an ingestion run does.</remarks>
        public bool TryGetEntity(string urn, out ReadOnlyMemory<byte> utf8Json)
        {
            ArgumentNullException.ThrowIfNull(urn);
            if (_entities.TryGetValue(urn, out var json))
            {
                utf8Json = json;
                return true;
            }
            // The run event follows the latest committed check status.
            if (_assertions.TryGetValue(urn, out var assertion))
            {
                var result = _checkStatuses?.Publish(assertion, _version);
                utf8Json = result == null
                    ? assertion.Builder.ToUtf8Json()
                    : assertion.Builder.ToUtf8Json("assertionRunEvent", w => DataHubAspectWriter.WriteAssertionRunEvent(w, urn, assertion.DatasetUrn, assertion.StreamName, result));
                return true;
            }
            // Same served result as the assertion, so the incident never disagrees with its run event.
            if (_checkStatuses != null && _incidents.TryGetValue(urn, out var incident))
            {
                return TryWriteIncident(urn, _checkStatuses.PublishIncident(urn, incident.Info, incident.Assertion, _version), out utf8Json);
            }
            if (_checkStatuses != null && _removedIncidents.Contains(urn))
            {
                return TryWriteIncident(urn, _checkStatuses.ResolveRemovedIncident(urn, _version), out utf8Json);
            }
            // A run is served once its stream started.
            if (_checkStatuses != null && _runs.TryGetValue(urn, out var run) && _checkStatuses.GetRun(run.Generation) is DataHubRunState state)
            {
                var builder = new DataHubEntityBuilder("dataProcessInstance", urn);
                builder.Set("dataProcessInstanceProperties", w => DataHubAspectWriter.WriteDataProcessInstanceProperties(w, run, state.CreatedMillis));
                builder.Set("dataProcessInstanceRelationships", w => DataHubAspectWriter.WriteDataProcessInstanceRelationships(w, run.ParentUrn));
                builder.Set("dataProcessInstanceInput", w => DataHubAspectWriter.WriteDataProcessInstanceDatasets(w, "inputs", run.Inputs));
                builder.Set("dataProcessInstanceOutput", w => DataHubAspectWriter.WriteDataProcessInstanceDatasets(w, "outputs", run.Outputs));
                builder.Set("dataPlatformInstance", w => DataHubAspectWriter.WriteDataPlatformInstance(w, DataHubUrns.Platform(DataHubUrns.Orchestrator), null));
                utf8Json = builder.ToUtf8Json("dataProcessInstanceRunEvent", w => DataHubAspectWriter.WriteDataProcessInstanceRunEvent(w, state));
                return true;
            }
            utf8Json = default;
            return false;
        }

        // An incident is served only once its check failed.
        private static bool TryWriteIncident(string urn, DataHubIncidentState? incident, out ReadOnlyMemory<byte> utf8Json)
        {
            if (incident == null)
            {
                utf8Json = default;
                return false;
            }
            var builder = new DataHubEntityBuilder("incident", urn);
            builder.Set("incidentInfo", w => DataHubAspectWriter.WriteIncidentInfo(w, incident));
            utf8Json = builder.ToUtf8Json();
            return true;
        }
    }
}
