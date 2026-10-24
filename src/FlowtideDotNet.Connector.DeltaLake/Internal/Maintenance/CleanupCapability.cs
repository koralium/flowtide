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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// Whether cleanup may act on a table at a version, a finite set for diagnostics.
    /// </summary>
    internal enum CapabilityCheck : byte
    {
        Capable = 0,
        // Protocol versions or feature lists this connector does not understand
        UnknownProtocol = 1,
        // A protected or catalog-managed variant
        ExcludedFeature = 2,
        // A feature in effect that is not on the allowlist
        UnsupportedFeature = 3,
        // In-commit timestamps are enabled
        IctEnabled = 4,
        // The in-commit timestamp setting is not a boolean
        IctMalformed = 5,
        // In-commit timestamp tracking without enablement, or a repeated setting
        IctInconsistent = 6,
        // The effective metaData has no table id
        NoTableId = 7,
        NoProtocolOrMetadata = 8,
        // No usable root and commits before the replay are missing
        MissingHistory = 9,
        // A root or commit line could not be read within the limits
        Unreadable = 10,
        // A listed object changed or vanished while it was read
        Changed = 11,
        // Falling back past unusable roots needs more reads than a fresh short admission allows
        OverReadLimit = 12,
        // The replay needs long admission; nothing was read
        NeedsLongAdmission = 13,
        // Issuance stopped while reading; nothing is kept
        Preempted = 14,
        // A storage error or a memory or disk cap; nothing is concluded
        Unavailable = 15,
        // The reads left fit a fresh admission but not what this one has left
        ReadsSpent = 16
    }

    /// <summary>
    /// The effective protocol and metadata facts cleanup relies on at one listed head, with the complete-content proofs of their sources.
    /// </summary>
    internal sealed class TableCapability
    {
        public required long Version { get; init; }

        /// <summary>
        /// metaData.id at the version, read from storage; never the worker's advisory id.
        /// </summary>
        public required string TableId { get; init; }

        public required DeltaProtocolAction Protocol { get; init; }

        public required IReadOnlyDictionary<string, string> Configuration { get; init; }

        public required IReadOnlySet<string> WriterFeatures { get; init; }

        // The root checkpoint or commit that held the effective protocol
        public required ContentProof ProtocolSource { get; init; }

        public required ContentProof MetaDataSource { get; init; }

        // The head commit as the replay read it, with its strictly proven provenance; null when a root at the head left nothing to replay
        public ContentProof? HeadProof { get; init; }

        // The sequence of the listing the facts were established at
        public long ListingSequence { get; init; }

        /// <summary>
        /// The distinct source objects, each to be rehashed before its facts are used again.
        /// </summary>
        public IEnumerable<ContentProof> Sources
        {
            get
            {
                yield return ProtocolSource;
                if (MetaDataSource.Path.Full != ProtocolSource.Path.Full)
                {
                    yield return MetaDataSource;
                }
            }
        }
    }

    internal sealed record CapabilityResult(CapabilityCheck Check, TableCapability? Capability = null, string? Detail = null)
    {
        // Proofs of the replayed commits that were asked for, with their strictly proven provenance
        public IReadOnlyDictionary<long, ContentProof> Proofs { get; init; } = new Dictionary<long, ContentProof>();

        public IssuanceStop Stop { get; init; }

        // Counted object reads the check made
        public long Reads { get; init; }

        // No history, and a root was skipped for a fixed memory or disk cap
        public bool RootOverCap { get; init; }

        // The memory or disk cap the check stopped at, None otherwise
        public MaintenanceReason Cap { get; init; }

        public MaintenanceReason Reason => CleanupCapability.ReasonOf(Check, Stop);
    }

    /// <summary>
    /// Whether cleanup may act on a table: a supported read and write protocol and metadata established from the destination, apart from any retention policy.
    /// </summary>
    internal static class CleanupCapability
    {
        public const string IctEnablement = "delta.enableInCommitTimestamps";
        private const string IctTracking = "delta.inCommitTimestamp";
        private const string CoordinatedCommitsPrefix = "delta.coordinatedCommits.";
        private const string CheckpointProtection = "delta.requireCheckpointProtectionBeforeVersion";

        public static MaintenanceReason ReasonOf(CapabilityCheck check, IssuanceStop stop) => check switch
        {
            CapabilityCheck.Capable or CapabilityCheck.NeedsLongAdmission or CapabilityCheck.ReadsSpent => MaintenanceReason.None,
            CapabilityCheck.UnknownProtocol => MaintenanceReason.UnknownProtocolVersion,
            CapabilityCheck.ExcludedFeature or CapabilityCheck.IctEnabled or CapabilityCheck.IctMalformed or CapabilityCheck.IctInconsistent => MaintenanceReason.ExcludedFeature,
            CapabilityCheck.UnsupportedFeature => MaintenanceReason.UnsupportedFeature,
            CapabilityCheck.MissingHistory => MaintenanceReason.MissingHistory,
            CapabilityCheck.OverReadLimit => MaintenanceReason.NoAdmissibleRoot,
            CapabilityCheck.Changed or CapabilityCheck.Unavailable => MaintenanceReason.Transient,
            CapabilityCheck.Preempted => stop == IssuanceStop.Deadline ? MaintenanceReason.JobDeadline : MaintenanceReason.None,
            _ => MaintenanceReason.ValidationFailed
        };

        /// <summary>
        /// The pure protocol and configuration rules: every feature in effect allowlisted, no protected or catalog-managed variant, in-commit timestamps off and consistent.
        /// </summary>
        public static CapabilityCheck CheckFeatures(DeltaProtocolAction protocol, IReadOnlyDictionary<string, string>? configuration, out IReadOnlySet<string>? features, out string? detail)
        {
            features = null;
            detail = null;
            IReadOnlySet<string> inEffect;
            try
            {
                inEffect = CheckpointCapability.WriterFeatures(protocol);
            }
            catch (CheckpointDeclinedException e)
            {
                detail = e.Message;
                return CapabilityCheck.UnknownProtocol;
            }
            // Every writer feature is checked, which is the write-protocol check vacuumProtocolCheck asks cleanup for
            foreach (var feature in inEffect.Order(StringComparer.Ordinal))
            {
                if (CheckpointCapability.Excluded.TryGetValue(feature, out var reason))
                {
                    detail = $"{feature}: {reason}";
                    return CapabilityCheck.ExcludedFeature;
                }
                if (!CheckpointCapability.Allowed.ContainsKey(feature))
                {
                    detail = $"writer feature {feature} is not on the allowlist";
                    return CapabilityCheck.UnsupportedFeature;
                }
            }
            if (configuration != null)
            {
                foreach (var key in configuration.Keys)
                {
                    if (key.StartsWith(CoordinatedCommitsPrefix, StringComparison.OrdinalIgnoreCase) || key.Equals(CheckpointProtection, StringComparison.OrdinalIgnoreCase))
                    {
                        detail = $"{key} marks a protected or catalog-managed table";
                        return CapabilityCheck.ExcludedFeature;
                    }
                }
            }
            var ict = CheckIct(configuration, out detail);
            if (ict != CapabilityCheck.Capable)
            {
                return ict;
            }
            features = inEffect;
            return CapabilityCheck.Capable;
        }

        // Enabled, malformed or inconsistent in-commit timestamps decline; keys compare without case
        private static CapabilityCheck CheckIct(IReadOnlyDictionary<string, string>? configuration, out string? detail)
        {
            detail = null;
            if (configuration == null)
            {
                return CapabilityCheck.Capable;
            }
            var enabled = false;
            var settings = 0;
            string? tracking = null;
            foreach (var (key, value) in configuration)
            {
                if (key.Equals(IctEnablement, StringComparison.OrdinalIgnoreCase))
                {
                    settings++;
                    if (value == null || !bool.TryParse(value.Trim(), out var parsed))
                    {
                        detail = $"{key} is '{value}'";
                        return CapabilityCheck.IctMalformed;
                    }
                    enabled |= parsed;
                }
                else if (key.StartsWith(IctEnablement, StringComparison.OrdinalIgnoreCase) || key.StartsWith(IctTracking, StringComparison.OrdinalIgnoreCase))
                {
                    tracking ??= key;
                }
            }
            if (enabled)
            {
                detail = "in-commit timestamps are enabled";
                return CapabilityCheck.IctEnabled;
            }
            if (settings > 1)
            {
                detail = $"{IctEnablement} is set {settings} times";
                return CapabilityCheck.IctInconsistent;
            }
            if (tracking != null)
            {
                detail = $"{tracking} is set while in-commit timestamps are not enabled";
                return CapabilityCheck.IctInconsistent;
            }
            return CapabilityCheck.Capable;
        }

        // The protocol and metaData leaves of a root, the only ones the check decodes
        private static CheckpointProjection Select(CheckpointProjection projection)
        {
            return projection.WithLeaves(projection.Leaves.Where(x => x.Role == ProjectedRole.Field
                && (x.Field is >= Leaf.ProtocolMinReader and <= Leaf.ProtocolWriterFeature || x.Field is >= Leaf.MetaId and <= Leaf.MetaConfigurationValue)).ToList());
        }

        /// <summary>
        /// Establishes the facts at the checked listing's head from the newest usable classic root and the contiguous commits after it, hashing every source; never throws for runtime failures.
        /// </summary>
        public static async Task<CapabilityResult> CheckAsync(CleanupConfiguration configuration, IFileStorage storage, LogEvidence log, CleanupContext context, IReadOnlyCollection<long>? prove = null, int lineCap = BoundedCheckpointReader.ActionCap)
        {
            var state = new ReplayState(context.Memory);
            try
            {
                var result = await CheckCoreAsync(configuration.Store.Table, storage, log, context, prove, lineCap, state);
                return result with { Reads = state.Reads };
            }
            catch (CleanupPreemptedException e)
            {
                return new CapabilityResult(CapabilityCheck.Preempted, Detail: e.Message) { Stop = e.Stop, Reads = state.Reads };
            }
            catch (OperationCanceledException) when (context.Stop.IsCancellationRequested)
            {
                return new CapabilityResult(CapabilityCheck.Preempted, Detail: "The worker stopped") { Stop = IssuanceStop.Stopped, Reads = state.Reads };
            }
            catch (CheckpointDeclinedException e) when (IsCap(e.Reason))
            {
                return new CapabilityResult(CapabilityCheck.Unavailable, Detail: e.Message) { Reads = state.Reads, Cap = e.Reason };
            }
            catch (CheckpointDeclinedException e)
            {
                return new CapabilityResult(CapabilityCheck.Unreadable, Detail: e.Message) { Reads = state.Reads };
            }
            catch (Exception e)
            {
                return new CapabilityResult(CapabilityCheck.Unavailable, Detail: $"{e.GetType().Name}: {e.Message}") { Reads = state.Reads };
            }
            finally
            {
                state.Dispose();
            }
        }

        // A resource limit, never a fact about the table
        internal static bool IsCap(MaintenanceReason reason) => reason is MaintenanceReason.ManagedMemoryCap or MaintenanceReason.NativeMemoryCap or MaintenanceReason.DiskCap;

        private static async Task<CapabilityResult> CheckCoreAsync(IOPath table, IFileStorage storage, LogEvidence log, CleanupContext context, IReadOnlyCollection<long>? prove, int lineCap,
            ReplayState state)
        {
            var head = log.Head;
            if (head < 0)
            {
                return new CapabilityResult(CapabilityCheck.NoProtocolOrMetadata, Detail: "The log has no commits");
            }
            var segment = new DeltaLogSegment(storage, table, log.Listing);
            var candidates = segment.RootCandidates(head, head).ToList();
            var fromZero = segment.HasHistoryFromZero(head);
            if (candidates.Count == 0 && !fromZero)
            {
                return new CapabilityResult(CapabilityCheck.MissingHistory, Detail: $"Commits before {segment.SuffixStart(head)} are missing and no classic checkpoint roots the head");
            }
            // Promoted before any read when even the newest root leaves too many commits
            var newest = candidates.Count > 0 ? candidates[0].Version : -1;
            var needed = (newest >= 0 ? 1 : 0) + head - newest;
            if (needed > context.ReadLimit)
            {
                return new CapabilityResult(CapabilityCheck.NeedsLongAdmission, Detail: $"The replay reads {head - newest} commits, more than the {context.ReadLimit} reads short admission allows");
            }

            var skipped = new List<string>();
            var cap = MaintenanceReason.None;
            long rootVersion = -1;
            foreach (var candidate in candidates)
            {
                var rest = 1 + head - candidate.Version;
                if (state.Reads + rest > context.ReadLimit)
                {
                    return new CapabilityResult(CapabilityCheck.OverReadLimit, Detail: string.Join("; ", skipped));
                }
                if (!TryRead(context, state, rest))
                {
                    return Spent(rest, context);
                }
                var root = await ReadRootAsync(table, storage, candidate, context, state);
                if (root.Changed != null)
                {
                    return new CapabilityResult(CapabilityCheck.Changed, Detail: root.Changed);
                }
                if (root.Skipped != null)
                {
                    cap = root.Cap == MaintenanceReason.None ? cap : root.Cap;
                    skipped.Add($"checkpoint {candidate.Version}: {root.Skipped}");
                    continue;
                }
                rootVersion = candidate.Version;
                break;
            }
            if (rootVersion < 0)
            {
                if (!fromZero)
                {
                    // A root skipped for a memory or disk cap leaves the table undecided
                    var capped = cap != MaintenanceReason.None;
                    return new CapabilityResult(capped ? CapabilityCheck.Unavailable : CapabilityCheck.MissingHistory,
                        Detail: $"No usable root and commits before {segment.SuffixStart(head)} are missing: {string.Join("; ", skipped)}")
                    { RootOverCap = capped, Cap = cap };
                }
                if (state.Reads + head + 1 > context.ReadLimit)
                {
                    return new CapabilityResult(CapabilityCheck.OverReadLimit, Detail: string.Join("; ", skipped));
                }
            }

            // The contiguous commits after the root, each hashed as it is read
            var proofs = new Dictionary<long, ContentProof>();
            using (var encoder = new TailRecordEncoder(context.Memory))
            {
                for (var version = rootVersion + 1; version <= head; version++)
                {
                    var file = log.Listing.Commits[version];
                    var proveLine = version == head || (prove != null && prove.Contains(version));
                    if (!TryRead(context, state, head - version + 1))
                    {
                        return Spent(head - version + 1, context);
                    }
                    var proof = await ReplayCommitAsync(table, storage, file, context, encoder, state, lineCap, proveLine);
                    if (proof == null)
                    {
                        return new CapabilityResult(CapabilityCheck.Changed, Detail: $"Commit {version} changed or vanished after the listing");
                    }
                    if (proveLine)
                    {
                        proofs[version] = proof;
                    }
                }
            }

            if (state.Protocol == null || state.MetaData == null)
            {
                return new CapabilityResult(CapabilityCheck.NoProtocolOrMetadata, Detail: $"No protocol or metaData at version {head}");
            }
            var protocol = CheckpointRecord.ToAction(state.Protocol).Protocol!;
            var metaData = CheckpointRecord.ToAction(state.MetaData).MetaData!;
            var configuration = (IReadOnlyDictionary<string, string>?)metaData.Configuration ?? new Dictionary<string, string>();
            var check = CheckFeatures(protocol, configuration, out var features, out var detail);
            if (check != CapabilityCheck.Capable)
            {
                return new CapabilityResult(check, Detail: detail);
            }
            if (string.IsNullOrEmpty(metaData.Id))
            {
                return new CapabilityResult(CapabilityCheck.NoTableId, Detail: $"metaData at version {head} has no id");
            }
            return new CapabilityResult(CapabilityCheck.Capable, new TableCapability()
            {
                Version = head,
                TableId = metaData.Id,
                Protocol = protocol,
                Configuration = configuration,
                WriterFeatures = features!,
                ProtocolSource = state.ProtocolSource!,
                MetaDataSource = state.MetaDataSource!,
                HeadProof = proofs.GetValueOrDefault(head),
                ListingSequence = log.Sequence
            })
            { Proofs = proofs };
        }

        // Counts one read when this admission still covers every read left, this one included
        private static bool TryRead(CleanupContext context, ReplayState state, long left)
        {
            if (left > context.ReadsRemaining || !context.TryCountRead())
            {
                return false;
            }
            state.Reads++;
            return true;
        }

        private static CapabilityResult Spent(long reads, CleanupContext context)
        {
            return new CapabilityResult(CapabilityCheck.ReadsSpent, Detail: $"The replay needs {reads} more reads and this admission has {context.ReadsRemaining} of its {context.ReadLimit} left");
        }

        // Cap: the memory or disk cap it was skipped for, None for its content
        private readonly record struct RootRead(string? Changed, string? Skipped, MaintenanceReason Cap = MaintenanceReason.None);

        // Downloads the root to job scratch while hashing it, then reads only its protocol and metaData
        private static async Task<RootRead> ReadRootAsync(IOPath table, IFileStorage storage, LogTransactionFile file, CleanupContext context, ReplayState state)
        {
            var listed = file.IOEntry.Size;
            var scratch = context.Scratch;
            if (listed > scratch.Ledger.Cap)
            {
                return new RootRead(null, $"{listed} bytes, over the {scratch.Ledger.Cap} byte disk cap", MaintenanceReason.DiskCap);
            }
            var download = scratch.NewPath(".capability.parquet");
            var buffer = Array.Empty<byte>();
            try
            {
                ProofRead read;
                using (await context.Transfer.AdmitAsync(listed ?? 0, context.Stop))
                {
                    buffer = context.Memory.Allocate(ProofReader.BufferSize);
                    await using var target = scratch.Create(download);
                    var source = await storage.OpenRead(file.IOEntry.Path, context.Stop);
                    if (source == null)
                    {
                        return new RootRead($"Checkpoint {file.Version} was removed after the listing", null);
                    }
                    await using var proof = new ProofStream(source, context, proveFirstLine: false);
                    int count;
                    while ((count = await proof.ReadAsync(buffer, context.Stop)) > 0)
                    {
                        await target.WriteAsync(buffer.AsMemory(0, count), context.Stop);
                    }
                    read = proof.Complete(file.Version);
                }
                if (listed != null && read.Length != listed)
                {
                    return new RootRead($"Checkpoint {file.Version} has {read.Length} bytes, the listing said {listed}", null);
                }
                // A torn root fails the scan like any other root out of the envelope
                byte[]? protocol = null;
                byte[]? metaData = null;
                try
                {
                    try
                    {
                        var scan = CheckpointPageScan.Scan(download, context.Memory, select: Select);
                        try
                        {
                            using var reader = new BoundedCheckpointReader(download, scan, context.Memory);
                            reader.Read(record =>
                            {
                                var kind = CheckpointRecord.Kind(record);
                                if (kind == RecordKind.Protocol)
                                {
                                    protocol = state.Copy(protocol, record);
                                }
                                else if (kind == RecordKind.MetaData)
                                {
                                    metaData = state.Copy(metaData, record);
                                }
                            }, context.Stop);
                        }
                        finally
                        {
                            context.Memory.Release(scan.ModelBytes);
                        }
                    }
                    catch (CheckpointDeclinedException e)
                    {
                        return new RootRead(null, e.Message, IsCap(e.Reason) ? e.Reason : MaintenanceReason.None);
                    }
                    if (protocol == null || metaData == null)
                    {
                        return new RootRead(null, "no protocol or metaData");
                    }
                    var source = new ContentProof(file.IOEntry.Path, file.Version, true, ObjectIdentity.Of(table, file), read.Length, read.Sha256!);
                    state.SetRoot(protocol, metaData, source);
                    // The state owns the copies now
                    protocol = null;
                    metaData = null;
                    return new RootRead(null, null);
                }
                finally
                {
                    state.Free(protocol);
                    state.Free(metaData);
                }
            }
            finally
            {
                if (buffer.Length > 0)
                {
                    context.Memory.Free(buffer);
                }
                scratch.Delete(download);
            }
        }

        // One commit through a hashing stream and the bounded line reader; null when it vanished or its length is not the listed size
        private static async Task<ContentProof?> ReplayCommitAsync(IOPath table, IFileStorage storage, LogTransactionFile file, CleanupContext context, TailRecordEncoder encoder,
            ReplayState state, int lineCap, bool proveLine)
        {
            if (context.CheckStop() is var stop && stop != IssuanceStop.None)
            {
                throw new CleanupPreemptedException(stop);
            }
            var listed = file.IOEntry.Size;
            ProofRead read;
            var protocol = false;
            var metaData = false;
            using (await context.Transfer.AdmitAsync(listed ?? 0, context.Stop))
            {
                var source = await storage.OpenRead(file.IOEntry.Path, context.Stop);
                if (source == null)
                {
                    return null;
                }
                var proof = new ProofStream(source, context, proveLine);
                BoundedLineReader lines;
                try
                {
                    lines = new BoundedLineReader(proof, context.Memory, lineCap, skipOversizedCommitInfo: true, ownsStream: true);
                }
                catch
                {
                    await proof.DisposeAsync();
                    throw;
                }
                using var owned = lines;
                try
                {
                    while (await lines.ReadLineAsync(context.Stop))
                    {
                        if (BoundedLineReader.IsBlank(lines.Line.Span))
                        {
                            continue;
                        }
                        encoder.Encode(lines.Line.Span, record =>
                        {
                            var kind = CheckpointRecord.Kind(record);
                            if (kind == RecordKind.Protocol)
                            {
                                state.KeepProtocol(record);
                                protocol = true;
                            }
                            else if (kind == RecordKind.MetaData)
                            {
                                state.KeepMetaData(record);
                                metaData = true;
                            }
                        });
                    }
                }
                catch (CheckpointDeclinedException) when (proof.Ended && listed != null && proof.BytesRead != listed)
                {
                    // A last line cut short by an object shorter than listed is a change, not an unreadable log
                    return null;
                }
                read = proof.Complete(file.Version);
            }
            if (listed != null && read.Length != listed)
            {
                return null;
            }
            var content = new ContentProof(file.IOEntry.Path, file.Version, false, ObjectIdentity.Of(table, file), read.Length, read.Sha256!) { Provenance = read.Provenance };
            if (protocol)
            {
                state.ProtocolSource = content;
            }
            if (metaData)
            {
                state.MetaDataSource = content;
            }
            return content;
        }

        /// <summary>
        /// The latest protocol and metaData records in charged copies, and the objects that held them.
        /// </summary>
        private sealed class ReplayState : IDisposable
        {
            private readonly JobMemory _memory;

            public ReplayState(JobMemory memory)
            {
                _memory = memory;
            }

            public byte[]? Protocol { get; private set; }

            public byte[]? MetaData { get; private set; }

            // Counted object reads: roots and commits
            public long Reads { get; set; }

            public ContentProof? ProtocolSource { get; set; }

            public ContentProof? MetaDataSource { get; set; }

            public void SetRoot(byte[] protocol, byte[] metaData, ContentProof source)
            {
                Free(Protocol);
                Free(MetaData);
                Protocol = protocol;
                MetaData = metaData;
                ProtocolSource = source;
                MetaDataSource = source;
            }

            public void KeepProtocol(ReadOnlySpan<byte> record) => Protocol = Copy(Protocol, record);

            public void KeepMetaData(ReadOnlySpan<byte> record) => MetaData = Copy(MetaData, record);

            // Allocated before the old copy is freed, so a refused allocation keeps the old one
            public byte[] Copy(byte[]? old, ReadOnlySpan<byte> record)
            {
                var copy = _memory.Allocate(record.Length);
                record.CopyTo(copy);
                Free(old);
                return copy;
            }

            public void Free(byte[]? copy)
            {
                if (copy != null)
                {
                    _memory.Free(copy);
                }
            }

            public void Dispose()
            {
                Free(Protocol);
                Free(MetaData);
                Protocol = null;
                MetaData = null;
            }
        }
    }
}
