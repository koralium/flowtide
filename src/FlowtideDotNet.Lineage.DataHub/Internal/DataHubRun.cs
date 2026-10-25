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
    // One run per job and stream, or substream, so a restarted process takes the run over. The parent is the flow for one that writes no table.
    internal sealed record DataHubRunInfo(
        string Urn,
        string Name,
        string StreamName,
        string? SubstreamName,
        string ParentUrn,
        IReadOnlyList<string> Inputs,
        IReadOnlyList<string> Outputs,
        long Generation);

    // ResultType is null while the run is started. Times change only with the state.
    // Cancelling: a stop or delete aborted the start. StopFailed: the stop reported an error.
    internal sealed record DataHubRunState(
        long CreatedMillis,
        long StartedMillis,
        string? ResultType,
        long EventMillis,
        StreamStateValue PreviousState,
        bool Cancelling = false,
        bool StopFailed = false);
}
