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

namespace FlowtideDotNet.Lineage.DataHub.Internal
{
    // Built per snapshot, only the priority can differ between snapshots.
    internal sealed record DataHubIncidentInfo(string AssertionUrn, string DatasetUrn, string StreamName, string Title, DataHubIncidentPriority Priority);

    // Times change only with the state, so every pull serves the same aspect.
    internal sealed record DataHubIncidentState(
        DataHubIncidentInfo Info,
        bool Active,
        long CreatedMillis,
        long StartedAtMillis,
        long LastUpdatedMillis,
        bool CheckRemoved,
        long FollowedMillis);
}
