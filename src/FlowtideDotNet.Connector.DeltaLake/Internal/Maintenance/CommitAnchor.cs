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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// Whether a commit proves what an anchor needs, a finite set.
    /// </summary>
    internal enum AnchorCheck : byte
    {
        Proven = 0,
        // Above the listed head, so not published yet
        NotPublished = 1,
        // At or below the head but not listed, or gone when read
        Missing = 2,
        // The first line fails the strict proof or has no own stage id and epoch
        NotOwn = 3,
        // An own commit of another attempt
        StageMismatch = 4,
        // Its bytes are not the listed size
        Changed = 5,
        Preempted = 6,
        // The admission's counted reads were spent before it was read
        ReadsSpent = 7
    }

    /// <summary>
    /// A commit read in full: its content proof with strictly proven provenance, or why there is none.
    /// </summary>
    internal readonly record struct CommitProof(AnchorCheck Check, ContentProof? Proof, IssuanceStop Stop = IssuanceStop.None);

    /// <summary>
    /// The own commit cleanup is anchored to: table id, exact name, stage id, epoch, listed discriminator and the SHA-256 of its complete bytes.
    /// </summary>
    internal sealed class CommitAnchor
    {
        private CommitAnchor(string tableId, ContentProof proof, string stageId, long adoptedAt)
        {
            TableId = tableId;
            Proof = proof;
            StageId = stageId;
            AdoptedAt = adoptedAt;
        }

        /// <summary>
        /// metaData.id from the destination's effective metadata when the anchor was proven.
        /// </summary>
        public string TableId { get; }

        public ContentProof Proof { get; }

        public long Version => Proof.Version;

        /// <summary>
        /// The exact commit name, {version:D20}.json.
        /// </summary>
        public string Name => Proof.Path.Name;

        public string StageId { get; }

        // The epoch A of the anchor's own run of commits
        public long AdoptedAt { get; }

        public ObjectIdentity Listed => Proof.Listed;

        public byte[] Sha256 => Proof.Sha256;

        /// <summary>
        /// An anchor from a complete commit proof whose first line strictly proves an own stage id and epoch.
        /// </summary>
        public static AnchorCheck TryCreate(string tableId, ContentProof proof, string? expectedStageId, out CommitAnchor? anchor)
        {
            anchor = null;
            if (proof.IsCheckpoint || proof.Provenance is not { IsOwn: true } provenance)
            {
                return AnchorCheck.NotOwn;
            }
            if (expectedStageId != null && provenance.StageId != expectedStageId)
            {
                return AnchorCheck.StageMismatch;
            }
            anchor = new CommitAnchor(tableId, proof, provenance.StageId!, provenance.AdoptedAt!.Value);
            return AnchorCheck.Proven;
        }

        /// <summary>
        /// Reads a listed commit in full: SHA-256 of every byte, a strict proof of its first line, and a length equal to the listed size.
        /// </summary>
        public static async Task<CommitProof> ProveAsync(IOPath table, IFileStorage storage, LogEvidence log, long version, CleanupContext context)
        {
            if (version > log.Head)
            {
                return new CommitProof(AnchorCheck.NotPublished, null);
            }
            if (ContentProof.Find(log.Listing, version, false) is not { } file)
            {
                return new CommitProof(AnchorCheck.Missing, null);
            }
            var listed = file.IOEntry.Size;
            var read = await ProofReader.ReadAsync(storage, file.IOEntry.Path, version, listed, proveFirstLine: true, context);
            switch (read.Status)
            {
                case ProofStatus.Missing:
                    return new CommitProof(AnchorCheck.Missing, null);
                case ProofStatus.Preempted:
                    return new CommitProof(AnchorCheck.Preempted, null, read.Stop);
                case ProofStatus.ReadsSpent:
                    return new CommitProof(AnchorCheck.ReadsSpent, null);
            }
            if (listed != null && read.Length != listed)
            {
                return new CommitProof(AnchorCheck.Changed, null);
            }
            var proof = new ContentProof(file.IOEntry.Path, version, false, ObjectIdentity.Of(table, file), read.Length, read.Sha256!) { Provenance = read.Provenance };
            return new CommitProof(AnchorCheck.Proven, proof);
        }
    }
}
