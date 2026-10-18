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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints
{
    /// <summary>
    /// One listing of a table's log: its contiguous commit suffix, the classic checkpoints that can root a version, and commit lines.
    /// </summary>
    internal sealed class DeltaLogSegment
    {
        private readonly IFileStorage _storage;

        public DeltaLogSegment(IFileStorage storage, IOPath table, DeltaTransactionReader.LogListing listing)
        {
            _storage = storage;
            Table = table;
            Listing = listing;
        }

        public static async Task<DeltaLogSegment> ListAsync(IFileStorage storage, IOPath table, CancellationToken cancellationToken = default)
        {
            return new DeltaLogSegment(storage, table, await DeltaTransactionReader.ListLog(storage, table, cancellationToken));
        }

        public IOPath Table { get; }

        public DeltaTransactionReader.LogListing Listing { get; }

        public long Head => Listing.Head;

        // The first version of the unbroken run of commits that ends at the target, target + 1 when its own commit is missing
        public long SuffixStart(long target)
        {
            long start = target + 1;
            while (start > 0 && Listing.Commits.ContainsKey(start - 1))
            {
                start--;
            }
            return start;
        }

        // Every commit from 0 to the target is listed
        public bool HasHistoryFromZero(long target) => SuffixStart(target) == 0;

        /// <summary>
        /// Classic checkpoints at or below the bound with every commit after them up to the target listed, newest first.
        /// </summary>
        public IEnumerable<LogTransactionFile> RootCandidates(long target, long bound)
        {
            var start = SuffixStart(target);
            foreach (var checkpoint in Listing.Checkpoints)
            {
                if (checkpoint.Version > bound)
                {
                    continue;
                }
                // Older checkpoints need an even longer run of commits
                if (checkpoint.Version + 1 < start)
                {
                    yield break;
                }
                yield return checkpoint;
            }
        }

        /// <summary>
        /// The lines of a listed commit, null when it was removed after the listing.
        /// </summary>
        public async Task<BoundedLineReader?> OpenCommitAsync(long version, JobMemory? memory, int cap, bool skipOversizedCommitInfo, CancellationToken cancellationToken = default, bool streamOwnHeader = false)
        {
            var stream = await _storage.OpenRead(Listing.Commits[version].IOEntry.Path, cancellationToken);
            if (stream == null)
            {
                return null;
            }
            try
            {
                return new BoundedLineReader(stream, memory, cap, skipOversizedCommitInfo, ownsStream: true, streamOwnHeader);
            }
            catch
            {
                await stream.DisposeAsync();
                throw;
            }
        }
    }
}
