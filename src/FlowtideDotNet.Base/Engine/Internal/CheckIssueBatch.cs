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

namespace FlowtideDotNet.Base.Engine.Internal
{
    /// <summary>
    /// The issue changes and status of one check, sealed at one checkpoint barrier or published at start.
    /// </summary>
    internal sealed class CheckIssueBatch
    {
        public required string CheckId { get; init; }

        /// <summary>
        /// The check's message template.
        /// </summary>
        public required string CheckName { get; init; }

        /// <summary>
        /// The checkpoint version of the barrier that sealed the batch.
        /// </summary>
        public required long Version { get; init; }

        /// <summary>
        /// Replaces the listener's view of the check, every change is then active.
        /// </summary>
        public required bool IsSnapshot { get; init; }

        /// <summary>
        /// The issue changes, empty when only the status is carried.
        /// </summary>
        public required IReadOnlyList<CheckIssueChange> Changes { get; init; }

        /// <summary>
        /// The check's state and counts, null when the status is not reported.
        /// </summary>
        public CheckStatus? Status { get; init; }

        /// <summary>
        /// Runs once the batch is published, also when no listener is registered.
        /// </summary>
        public Action? Committed { get; init; }
    }

    /// <summary>
    /// One issue that became active or stopped being active.
    /// </summary>
    internal readonly record struct CheckIssueChange(bool Active, KeyValuePair<string, object?>[] Tags);

    /// <summary>
    /// The state and counts of one check.
    /// </summary>
    internal readonly record struct CheckStatus(CheckState State, long ActiveIssues, long FailingRows)
    {
        /// <summary>
        /// The status of a check that has not been evaluated yet.
        /// </summary>
        public static CheckStatus NotEvaluated => new CheckStatus(CheckState.NotEvaluated, 0, 0);

        /// <summary>
        /// The status of an evaluated check with these counts.
        /// </summary>
        public static CheckStatus Evaluated(long activeIssues, long failingRows)
        {
            return new CheckStatus(activeIssues > 0 ? CheckState.Failed : CheckState.Passed, activeIssues, failingRows);
        }
    }
}
