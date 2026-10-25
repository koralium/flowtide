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

using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// What an attempt's inline discard deletion did; the first failure and the first kept reason are kept for its log line.
    /// </summary>
    internal readonly record struct InlineDiscardResult(int Deleted, int Absent, int Kept, int Failed, KeepReason FirstKept, Exception? FirstError);

    /// <summary>
    /// The inline fallback: the attempt deletes its own discarded outputs before it stages, each by its exact qualified name and as a file only, also on a stack the user did not vouch for.
    /// </summary>
    internal static class InlineDiscards
    {
        /// <summary>
        /// Tries every path through the executor's checks for an attempt's own outputs; counts a rejected target as kept and a failed delete as failed, and never throws.
        /// </summary>
        public static async Task<InlineDiscardResult> DeleteAsync(DeletionExecutor executor, IReadOnlyList<string> paths, DiscardCounters counters)
        {
            int deleted = 0, absent = 0, kept = 0, failed = 0;
            var firstKept = KeepReason.None;
            Exception? firstError = null;
            foreach (var path in paths)
            {
                DeleteOutcome outcome;
                try
                {
                    outcome = await executor.DeleteOwnAsync(path, CancellationToken.None);
                }
                catch (Exception e)
                {
                    outcome = DeleteOutcome.Fail(e);
                }
                counters.Add(outcome);
                switch (outcome.Status)
                {
                    case DeleteStatus.Deleted:
                        deleted++;
                        break;
                    case DeleteStatus.Absent:
                        absent++;
                        break;
                    case DeleteStatus.Kept:
                        kept++;
                        if (firstKept == KeepReason.None)
                        {
                            firstKept = outcome.Reason;
                        }
                        break;
                    default:
                        failed++;
                        firstError ??= outcome.Error;
                        break;
                }
            }
            return new InlineDiscardResult(deleted, absent, kept, failed, firstKept, firstError);
        }
    }
}
