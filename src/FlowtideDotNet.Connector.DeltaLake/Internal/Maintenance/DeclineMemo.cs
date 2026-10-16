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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded;
using Stowage;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    internal enum MemoCheck : byte
    {
        Holds,
        Changed,
        ExpiredWeak
    }

    /// <summary>
    /// A decline that holds while its dependencies keep their identity, so a stable decline downloads no root again.
    /// </summary>
    internal sealed class DeclineMemo
    {
        public static readonly TimeSpan WeakExpiry = TimeSpan.FromHours(6);

        // The memo stays within its bound however long the history: candidates as a digest, the diagnostic cut
        public const int MaxDetailLength = 1024;

        public static string? Cut(string? detail) => detail?.Length > MaxDetailLength ? detail.Substring(0, MaxDetailLength) : detail;

        private DeclineMemo(MaintenanceReason reason, DeclineDependency dependency, string? detail, byte[] candidatesDigest, ObjectIdentity? tailCommit,
            IReadOnlyList<ObjectIdentity> protocolCommits, bool weak, DateTimeOffset setAt, long checkedThrough)
        {
            Reason = reason;
            Dependency = dependency;
            Detail = Cut(detail);
            CandidatesDigest = candidatesDigest;
            TailCommit = tailCommit;
            ProtocolCommits = protocolCommits;
            Weak = weak;
            SetAt = setAt;
            CheckedThrough = checkedThrough;
        }

        public MaintenanceReason Reason { get; }

        public DeclineDependency Dependency { get; }

        public string? Detail { get; }

        // SHA-256 over the root candidates' identities in listing order
        public byte[] CandidatesDigest { get; }

        public ObjectIdentity? TailCommit { get; }

        // At most two: the tail commits holding the effective protocol and metaData of a protocol-dependent decline
        public IReadOnlyList<ObjectIdentity> ProtocolCommits { get; }

        // Some dependency was listed without a modification time or an MD5, so version and size alone identify it
        public bool Weak { get; }

        // On the options clock; a held decline keeps it
        public DateTimeOffset SetAt { get; }

        public long CheckedThrough { get; set; }

        /// <summary>
        /// The memo for a decline, or null for a class that is never memoized: output-dependent, transient or missing history.
        /// </summary>
        public static DeclineMemo? For(BuildResult result, long target, DateTimeOffset setAt)
        {
            if (result.Status != BuildStatus.Declined || result.Dependency == DeclineDependency.None)
            {
                return null;
            }
            var tail = result.Dependency == DeclineDependency.Tail ? result.TailCommit : null;
            var holders = result.Dependency == DeclineDependency.Protocol ? result.ProtocolCommits : Array.Empty<ObjectIdentity>();
            var weak = result.Candidates.Any(x => !x.Strong) || tail is { Strong: false } || result.TargetCommit is { Strong: false } || holders.Any(x => !x.Strong);
            return new DeclineMemo(result.Reason, result.Dependency, result.Detail, Digest(result.Candidates), tail, holders, weak, setAt, target);
        }

        public static byte[] Digest(IEnumerable<ObjectIdentity> identities)
        {
            using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
            foreach (var x in identities)
            {
                hash.AppendData(Encoding.UTF8.GetBytes($"{x.Table}\n{x.Version}\n{x.Size}\n{x.Modified?.UtcTicks}\n{x.Md5}\n"));
            }
            return hash.GetHashAndReset();
        }

        /// <summary>
        /// One listing, the commits after the checked version peeked for protocol and metaData actions, and no root download.
        /// </summary>
        public async Task<MemoCheck> CheckAsync(IFileStorage storage, IOPath table, long target, DateTimeOffset now, JobMemory memory, TransferAllowance? transfer, CancellationToken cancellationToken)
        {
            if (Weak && now - SetAt >= WeakExpiry)
            {
                return MemoCheck.ExpiredWeak;
            }
            var segment = await DeltaLogSegment.ListAsync(storage, table, cancellationToken);
            // Missing history is never memoized, so a commit missing since the last check clears the memo
            for (long version = Math.Max(CheckedThrough + 1, 0); version <= target; version++)
            {
                if (!segment.Listing.Commits.ContainsKey(version))
                {
                    return MemoCheck.Changed;
                }
            }
            // A new or replaced checkpoint, or a filled gap that makes an older one contiguous, changes the candidates
            if (!Digest(segment.RootCandidates(target, target).Select(x => ObjectIdentity.Of(table, x))).AsSpan().SequenceEqual(CandidatesDigest))
            {
                return MemoCheck.Changed;
            }
            // A same-version repair of a commit that holds the protocol or metaData changes its identity
            foreach (var holder in ProtocolCommits)
            {
                if (!segment.Listing.Commits.TryGetValue(holder.Version, out var holding) || ObjectIdentity.Of(table, holding) != holder)
                {
                    return MemoCheck.Changed;
                }
            }
            if (TailCommit != null && (!segment.Listing.Commits.TryGetValue(TailCommit.Version, out var commit) || ObjectIdentity.Of(table, commit) != TailCommit))
            {
                return MemoCheck.Changed;
            }
            if (Dependency == DeclineDependency.Protocol)
            {
                for (long version = CheckedThrough + 1; version <= target; version++)
                {
                    if (await ChangesProtocolAsync(segment, version, memory, transfer, cancellationToken))
                    {
                        return MemoCheck.Changed;
                    }
                }
            }
            return MemoCheck.Holds;
        }

        // Peeks each line's action name; a line the bounded reader refuses counts as a change, so the next build reports it
        private static async Task<bool> ChangesProtocolAsync(DeltaLogSegment segment, long version, JobMemory memory, TransferAllowance? transfer, CancellationToken cancellationToken)
        {
            try
            {
                // Stowage reads whole bodies, so the commit is admitted by its listed size until its reader is gone
                using var admitted = transfer != null ? await transfer.AdmitAsync(segment.Listing.Commits[version].IOEntry.Size ?? 0, cancellationToken) : null;
                using var lines = await segment.OpenCommitAsync(version, memory, BoundedCheckpointReader.ActionCap, skipOversizedCommitInfo: true, cancellationToken);
                if (lines == null)
                {
                    return true;
                }
                while (await lines.ReadLineAsync(cancellationToken))
                {
                    if (IsProtocolOrMetaData(lines.Line.Span))
                    {
                        return true;
                    }
                }
                return false;
            }
            catch (CheckpointDeclinedException)
            {
                return true;
            }
        }

        // Every top-level property, as the encoder reads them all
        private static bool IsProtocolOrMetaData(ReadOnlySpan<byte> line)
        {
            var reader = new Utf8JsonReader(line);
            try
            {
                if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
                {
                    return false;
                }
                while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
                {
                    if (reader.ValueTextEquals("protocol"u8) || reader.ValueTextEquals("metaData"u8))
                    {
                        return true;
                    }
                    reader.Read();
                    reader.Skip();
                }
                return false;
            }
            catch (JsonException)
            {
                return false;
            }
        }
    }
}
