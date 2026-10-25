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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta
{
    /// <summary>
    /// Who wrote a commit and when: its stage id, adoption epoch and typed timestamp, from its first line.
    /// </summary>
    internal readonly record struct CommitProvenance(long Version, string? StageId, long? AdoptedAt, long? Timestamp)
    {
        /// <summary>
        /// A commit of this sink's lineage: a stage id and an epoch within [0, version].
        /// </summary>
        public bool IsOwn => !string.IsNullOrEmpty(StageId) && AdoptedAt is { } epoch && epoch >= 0 && epoch <= Version;

        public static CommitProvenance Unknown(long version) => new CommitProvenance(version, null, null, null);

        /// <summary>
        /// A commitInfo-only line carries the lineage and the timestamp independently, each only when its fields are neither duplicated nor mistyped.
        /// </summary>
        public static CommitProvenance From(long version, CommitHeader? header)
        {
            if (header == null || !header.IsCommitInfo)
            {
                return Unknown(version);
            }
            var lineage = (header.Flags & (HeaderFlags.InvalidStageId | HeaderFlags.InvalidAdoptedAt)) == 0
                && (header.DuplicateFields & (CommitHeader.StageIdBit | CommitHeader.AdoptedAtBit)) == 0;
            var timestamp = (header.Flags & HeaderFlags.InvalidTimestamp) == 0
                && (header.DuplicateFields & CommitHeader.TimestampBit) == 0;
            return new CommitProvenance(version, lineage ? header.StageId : null, lineage ? header.AdoptedAt : null, timestamp ? header.Timestamp : null);
        }

        /// <summary>
        /// For cleanup proofs: provenance only from a valid commitInfo line whose fixed fields are all well-typed and unique.
        /// </summary>
        public static CommitProvenance FromStrict(long version, CommitHeader? header)
        {
            if (header == null || !header.IsValid || !header.IsCommitInfo || header.Flags != HeaderFlags.None || header.DuplicateFields != 0)
            {
                return Unknown(version);
            }
            return new CommitProvenance(version, header.StageId, header.AdoptedAt, header.Timestamp);
        }

        /// <summary>
        /// The epoch of the next commit: inherited from an own head, otherwise the commit's own version.
        /// </summary>
        public static long NextEpoch(CommitProvenance? head, long nextVersion)
        {
            return head is { IsOwn: true } own ? own.AdoptedAt!.Value : nextVersion;
        }

        /// <summary>
        /// The next commit's timestamp: the clock, raised to one past the head's integer timestamp, saturating instead of wrapping.
        /// </summary>
        public static long NextTimestamp(long nowMs, CommitProvenance? head)
        {
            if (head?.Timestamp is not { } previous)
            {
                return nowMs;
            }
            var next = previous == long.MaxValue ? long.MaxValue : previous + 1;
            return Math.Max(nowMs, next);
        }
    }
}
