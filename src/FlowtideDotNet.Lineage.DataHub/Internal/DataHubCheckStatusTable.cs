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

using FlowtideDotNet.Base.Engine;

namespace FlowtideDotNet.Lineage.DataHub.Internal
{
    // Outlives every snapshot, so a regenerated snapshot continues from the run events already served.
    // One lock orders every record and every read, so a result never mixes states and a newer time always means newer values.
    internal sealed class DataHubCheckStatusTable
    {
        private readonly TimeProvider _timeProvider;
        private readonly object _lock = new object();
        // Keyed by registration generation, a rebuild can give a check id to another check.
        private readonly Dictionary<long, Dictionary<string, DataHubCheckStatus>> _statuses = new Dictionary<long, Dictionary<string, DataHubCheckStatus>>();
        // Keyed by assertion urn.
        private readonly Dictionary<string, DataHubAssertionResult> _published = new Dictionary<string, DataHubAssertionResult>(StringComparer.Ordinal);
        // Keyed by incident urn, only incidents raised in this process.
        private readonly Dictionary<string, DataHubIncidentState> _incidents = new Dictionary<string, DataHubIncidentState>(StringComparer.Ordinal);
        // Keyed by registration generation, a rebuild starts a new run.
        private readonly Dictionary<long, DataHubRunState> _runs = new Dictionary<long, DataHubRunState>();
        // Registration version of the current snapshot.
        private long _version;
        private long _lastTimestamp;

        public DataHubCheckStatusTable(TimeProvider timeProvider)
        {
            _timeProvider = timeProvider;
        }

        // No snapshot of an earlier version publishes once a registration changed.
        public void Register(long version, long generation, long? replacedGeneration)
        {
            lock (_lock)
            {
                _version = version;
                _statuses.TryAdd(generation, new Dictionary<string, DataHubCheckStatus>(StringComparer.Ordinal));
                if (replacedGeneration is long replaced)
                {
                    _statuses.Remove(replaced);
                    _runs.Remove(replaced);
                }
            }
        }

        // An unchanged status keeps its timestamp, a replaced build reports nothing.
        public void Record(long generation, string checkId, CheckState state, long activeIssues, long failingRows)
        {
            lock (_lock)
            {
                if (!_statuses.TryGetValue(generation, out var statuses))
                {
                    return;
                }
                if (statuses.TryGetValue(checkId, out var current) &&
                    current.State == state && current.ActiveIssues == activeIssues && current.FailingRows == failingRows)
                {
                    return;
                }
                statuses[checkId] = new DataHubCheckStatus(state, activeIssues, failingRows, NextTimestamp());
            }
        }

        // Same mapping as the OpenLineage reporter: a start starts the run, a failure, a stop or a delete ends it.
        public void RecordStreamState(long generation, StreamStateValue state)
        {
            lock (_lock)
            {
                if (!_statuses.ContainsKey(generation))
                {
                    return;
                }
                _runs.TryGetValue(generation, out var current);
                var previous = current?.PreviousState ?? StreamStateValue.NotStarted;
                var started = current != null && current.ResultType == null;
                var cancelling = current?.Cancelling == true;
                string? resultType = state switch
                {
                    // A stop or delete during the start fails it on purpose, the stop or delete decides the result.
                    StreamStateValue.Failure when cancelling => null,
                    StreamStateValue.Failure => "FAILURE",
                    StreamStateValue.NotStarted when previous == StreamStateValue.Stopping => current?.StopFailed == true ? "FAILURE" : "SUCCESS",
                    StreamStateValue.NotStarted when previous == StreamStateValue.Failure && cancelling => "SUCCESS",
                    StreamStateValue.Deleted => "SKIPPED",
                    _ => null
                };
                if ((state == StreamStateValue.Starting || state == StreamStateValue.Running) && !started)
                {
                    var time = NextTimestamp();
                    current = new DataHubRunState(current?.CreatedMillis ?? time, time, null, time, state);
                }
                else if (resultType != null && started)
                {
                    current = current! with { ResultType = resultType, EventMillis = NextTimestamp(), PreviousState = state };
                }
                else if (current != null)
                {
                    // A start that went on after a cancellation was not stopped.
                    current = current with { PreviousState = state, Cancelling = cancelling && state != StreamStateValue.Starting && state != StreamStateValue.Running };
                }
                else
                {
                    return;
                }
                _runs[generation] = current;
            }
        }

        // Comes before the failure state, so the state can tell a cancelled start and a failed stop apart.
        public void RecordStreamFailure(long generation, bool cancellation)
        {
            lock (_lock)
            {
                if (!_runs.TryGetValue(generation, out var current) || current.ResultType != null)
                {
                    return;
                }
                if (current.PreviousState == StreamStateValue.Failure && !cancellation)
                {
                    // Kept running through a cancelled start, a failure while failing brings no further state change.
                    _runs[generation] = current with { ResultType = "FAILURE", EventMillis = NextTimestamp(), Cancelling = false };
                    return;
                }
                _runs[generation] = current.PreviousState == StreamStateValue.Stopping
                    ? current with { StopFailed = !cancellation }
                    : current with { Cancelling = cancellation };
            }
        }

        public DataHubRunState? GetRun(long generation)
        {
            lock (_lock)
            {
                return _runs.TryGetValue(generation, out var run) ? run : null;
            }
        }

        public DataHubAssertionResult? Publish(DataHubAssertion assertion, long version)
        {
            lock (_lock)
            {
                return PublishLocked(assertion, version);
            }
        }

        // Follows the served result of its assertion, so it never disagrees with the run event.
        public DataHubIncidentState? PublishIncident(string incidentUrn, DataHubIncidentInfo info, DataHubAssertion assertion, long version)
        {
            lock (_lock)
            {
                _incidents.TryGetValue(incidentUrn, out var current);
                // A newer snapshot may lack the check and could then never resolve what this one raises.
                if (version != _version)
                {
                    return current;
                }
                var served = PublishLocked(assertion, version);
                // The priority resolver can answer differently after a rebuild.
                if (current != null && current.Info != info)
                {
                    current = current with { Info = info };
                    _incidents[incidentUrn] = current;
                }
                if (served == null || (current != null && served.TimestampMillis <= current.FollowedMillis))
                {
                    return current;
                }
                var failed = served.State == CheckState.Failed;
                var time = served.TimestampMillis;
                if (current == null)
                {
                    if (!failed)
                    {
                        return null;
                    }
                    current = new DataHubIncidentState(info, true, time, time, time, false, time);
                }
                else if (failed != current.Active || current.CheckRemoved)
                {
                    current = current with
                    {
                        Active = failed,
                        StartedAtMillis = failed && !current.Active ? time : current.StartedAtMillis,
                        LastUpdatedMillis = time,
                        CheckRemoved = false,
                        FollowedMillis = time
                    };
                }
                else
                {
                    current = current with { FollowedMillis = time };
                }
                _incidents[incidentUrn] = current;
                return current;
            }
        }

        // A removed check never passes again, so its incident is resolved here. An obsolete snapshot may list a check that came back.
        public DataHubIncidentState? ResolveRemovedIncident(string incidentUrn, long version)
        {
            lock (_lock)
            {
                if (!_incidents.TryGetValue(incidentUrn, out var current) || !current.Active || version != _version)
                {
                    return current;
                }
                current = current with { Active = false, LastUpdatedMillis = NextTimestamp(), CheckRemoved = true };
                _incidents[incidentUrn] = current;
                // A returned check then publishes fresh, even with the values it had before.
                _published.Remove(current.Info.AssertionUrn);
                return current;
            }
        }

        public List<string> GetIncidentUrns()
        {
            lock (_lock)
            {
                return _incidents.Keys.ToList();
            }
        }

        // Only a newer status replaces the served result, a rebuild that drops a part never does.
        private DataHubAssertionResult? PublishLocked(DataHubAssertion assertion, long version)
        {
            var urn = assertion.Builder.Urn;
            _published.TryGetValue(urn, out var last);
            // An obsolete snapshot can miss the parts of a rebuild, it only serves what was published.
            if (version != _version)
            {
                return last;
            }
            // The priority resolver can answer differently after a rebuild, the result keeps its time.
            if (last != null && last.Priority != assertion.Priority)
            {
                last = last with { Priority = assertion.Priority };
                _published[urn] = last;
            }
            if (assertion.GetResult(GetLocked) is not DataHubAssertionResult computed)
            {
                return last;
            }
            var result = computed with { Priority = assertion.Priority };
            if (last != null &&
                (result.TimestampMillis <= last.TimestampMillis ||
                (last.State == result.State && last.ActiveIssues == result.ActiveIssues && last.FailingRows == result.FailingRows)))
            {
                return last;
            }
            _published[urn] = result;
            return result;
        }

        private DataHubCheckStatus? GetLocked(long generation, string checkId)
        {
            return _statuses.TryGetValue(generation, out var statuses) && statuses.TryGetValue(checkId, out var status) ? status : null;
        }

        // Strictly increasing, DataHub keeps one run event per timestamp and summarizes only newer ones.
        private long NextTimestamp()
        {
            var next = Math.Max(_timeProvider.GetUtcNow().ToUnixTimeMilliseconds(), _lastTimestamp + 1);
            _lastTimestamp = next;
            return next;
        }
    }
}
