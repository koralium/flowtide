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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.References;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// What the orphan sweep emitted: targets written, candidates protected, kept and over the cap.
    /// </summary>
    internal readonly record struct OrphanEmission(long Targets, long Protected, long Kept, long OverCap);

    /// <summary>
    /// The orphan sweep's protection after its barrier: live(O), the commits from O to L3's head and the stages L2 and L3 list.
    /// </summary>
    internal sealed class OrphanProtection : IDisposable
    {
        // Emitted candidates between stop checks
        private const int CheckEvery = 4096;

        private enum StageRead : byte
        {
            Read,
            // Gone before it was read
            Missing,
            // Not its listed size, unreadable, or the reads were spent
            Changed,
            // A line or path protection cannot use
            Declined
        }

        private readonly CleanupConfiguration _configuration;
        private readonly IFileStorage _storage;
        private readonly PathCanonicalizer _identity;
        private readonly CommitReferenceExtractor _extractor;
        // Stages gone before their read, resolved against L3
        private readonly List<IOEntry> _missing = new List<IOEntry>();

        // Storage is the run's meter; the set holds keys of the inventory's identity
        public OrphanProtection(CleanupConfiguration configuration, IFileStorage storage, PathCanonicalizer identity, CleanupContext context, long arenaCap)
        {
            _configuration = configuration;
            _storage = storage;
            _identity = identity;
            Set = new PathSet(context.Scratch, context.Memory, identity.Domain, PathRole.Protection, arenaCap);
            _extractor = new CommitReferenceExtractor(identity, Set, PathRole.Protection);
        }

        public PathSet Set { get; }

        /// <summary>
        /// One checked log listing above the published floor with its successor probed, then a clear fence and every commit from O; anything else ends the job.
        /// </summary>
        public static async Task<LogEvidence> ListAsync(CleanupConfiguration configuration, IFileStorage storage, long publishedFloor, long oldest, string name, RetentionCounters counters,
            CleanupContext context)
        {
            ListingCheck check;
            LogEvidence? log;
            try
            {
                (check, log) = await ListingEvidence.ReadLogAsync(configuration, storage, publishedFloor, context.Stop, context.Memory, () => AdvanceJob.StopIfIssuanceStopped(context));
            }
            catch (OperationCanceledException) when (context.Stop.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception e) when (e is not (CheckpointDeclinedException or CleanupPreemptedException))
            {
                throw AdvanceJob.Declined(MaintenanceReason.Transient, $"The orphan sweep's {name} failed: {e.GetType().Name}: {e.Message}");
            }
            counters.AddListing(check);
            if (check != ListingCheck.Passed)
            {
                throw AdvanceJob.Declined(MaintenanceReason.ListingIneligible, $"The orphan sweep's {name} failed its {check} check");
            }
            if (!log!.Fence.Clear)
            {
                throw AdvanceJob.Declined(MaintenanceReason.CleanupGate, $"The orphan sweep's {name} shows an advance's authority: {log.Fence.Detail}");
            }
            // A head below O holds no commit from O
            check = log.Head < oldest ? ListingCheck.RangeGap : log.CheckRange(oldest, log.Head);
            if (check != ListingCheck.Passed)
            {
                counters.AddDecline(check);
                throw AdvanceJob.Declined(MaintenanceReason.ListingIneligible, $"The orphan sweep's {name} does not hold every commit from {oldest} to {log.Head}");
            }
            return log;
        }

        /// <summary>
        /// The stage files a listing shows, in its checked order.
        /// </summary>
        public static List<IOEntry> StagesOf(LogEvidence log)
        {
            var stages = new List<IOEntry>();
            var entries = log.Entries;
            for (int i = 0; i < entries.Count; i++)
            {
                if (!entries[i].Path.IsFolder && ListingEvidence.TryGetStageVersion(entries[i].Name, out _))
                {
                    stages.Add(entries[i]);
                }
            }
            return stages;
        }

        /// <summary>
        /// The stages of a later listing an earlier one did not show; both are in the checked order, which is UTF-8 order.
        /// </summary>
        public static List<IOEntry> Unseen(List<IOEntry> later, List<IOEntry> earlier)
        {
            var unseen = new List<IOEntry>();
            var index = 0;
            foreach (var stage in later)
            {
                var full = stage.Path.Full;
                while (index < earlier.Count && ListingEvidence.CompareUtf8(earlier[index].Path.Full, full) < 0)
                {
                    index++;
                }
                if (index < earlier.Count && earlier[index].Path.Full == full)
                {
                    continue;
                }
                unseen.Add(stage);
            }
            return unseen;
        }

        /// <summary>
        /// Adds each stage's references, one stage at a time; a stage gone before its read waits for ResolveAsync, and any other failure ends the job.
        /// </summary>
        public async Task ReadStagesAsync(List<IOEntry> stages, CleanupContext context)
        {
            foreach (var stage in stages)
            {
                AdvanceJob.StopIfIssuanceStopped(context);
                switch (await ReadStageAsync(stage, context))
                {
                    case StageRead.Read:
                        break;
                    case StageRead.Missing:
                        _missing.Add(stage);
                        break;
                    case StageRead.Declined:
                        throw AdvanceJob.Declined(MaintenanceReason.PathRejected, $"A listed stage holds a reference protection cannot use: {_extractor.DeclineDetail}");
                    default:
                        throw AdvanceJob.Declined(MaintenanceReason.Transient, "A listed stage changed or could not be read");
                }
            }
        }

        // Every path one stage references, as one counted read
        private async Task<StageRead> ReadStageAsync(IOEntry entry, CleanupContext context)
        {
            if (!context.TryCountRead())
            {
                return StageRead.Changed;
            }
            try
            {
                using var admitted = await context.Transfer.AdmitAsync(entry.Size ?? 0, context.Stop);
                var source = await _storage.OpenRead(entry.Path, context.Stop);
                if (source == null)
                {
                    return StageRead.Missing;
                }
                var proof = new ProofStream(source, context, proveFirstLine: false);
                BoundedLineReader lines;
                try
                {
                    lines = new BoundedLineReader(proof, context.Memory, BoundedCheckpointReader.ActionCap, skipOversizedCommitInfo: true, ownsStream: true);
                }
                catch
                {
                    await proof.DisposeAsync();
                    throw;
                }
                using (lines)
                {
                    while (await lines.ReadLineAsync(context.Stop))
                    {
                        if (!_extractor.AddLine(lines.Line.Span))
                        {
                            // A stage written in place can end in a torn line
                            return await RestAsync(proof, context) == entry.Size ? StageRead.Declined : StageRead.Changed;
                        }
                    }
                }
                await _identity.ChargeOwedAsync(context.Stop, context);
                return entry.Size is { } listed && listed != proof.BytesRead ? StageRead.Changed : StageRead.Read;
            }
            catch (CheckpointDeclinedException e) when (!CleanupCapability.IsCap(e.Reason))
            {
                return StageRead.Declined;
            }
            catch (Exception e) when (e is not (CleanupPreemptedException or CheckpointDeclinedException) && !context.Stop.IsCancellationRequested)
            {
                return StageRead.Changed;
            }
        }

        // The stream's length once every byte left is read
        private static async Task<long> RestAsync(ProofStream proof, CleanupContext context)
        {
            var buffer = context.Memory.Allocate(ProofReader.BufferSize);
            try
            {
                while (await proof.ReadAsync(buffer, context.Stop) > 0)
                {
                }
            }
            finally
            {
                context.Memory.Free(buffer);
            }
            return proof.BytesRead;
        }

        /// <summary>
        /// Each stage gone before its read passes only as its own commit, listed in L3 and proven under the stage's id.
        /// </summary>
        public async Task ResolveAsync(LogEvidence l3, string tableId, CleanupContext context)
        {
            var table = _configuration.Store.Table;
            foreach (var stage in _missing)
            {
                AdvanceJob.StopIfIssuanceStopped(context);
                if (!ListingEvidence.TryGetStageId(stage.Name, out var version, out var id))
                {
                    throw AdvanceJob.Declined(MaintenanceReason.Transient, $"{stage.Name} vanished before its read and names no stage id");
                }
                CommitProof proof;
                try
                {
                    proof = await CommitAnchor.ProveAsync(table, _storage, l3, version, context);
                }
                catch (OperationCanceledException) when (context.Stop.IsCancellationRequested)
                {
                    throw;
                }
                catch (Exception e) when (e is not (CleanupPreemptedException or CheckpointDeclinedException))
                {
                    throw AdvanceJob.Declined(MaintenanceReason.Transient, $"The commit of a vanished stage at version {version} failed to read: {e.GetType().Name}: {e.Message}");
                }
                if (proof.Check == AnchorCheck.Preempted)
                {
                    AdvanceJob.StopIfIssuanceStopped(context);
                }
                var own = proof.Check == AnchorCheck.Proven ? CommitAnchor.TryCreate(tableId, proof.Proof!, id, out _) : proof.Check;
                if (own != AnchorCheck.Proven)
                {
                    throw AdvanceJob.Declined(MaintenanceReason.Transient, $"A stage at version {version} vanished before its read and its commit is {own}");
                }
            }
        }

        /// <summary>
        /// Adds live(O) from the checkpoint at O as L3 lists it and every commit from O to L3's head, each object's resolutions charged after it, then seals the set.
        /// </summary>
        public async Task BuildAsync(LogEvidence l3, long oldest, CleanupContext context)
        {
            var table = _configuration.Store.Table;
            if (oldest > 0)
            {
                var root = ContentProof.Find(l3.Listing, oldest, true) ?? throw AdvanceJob.Declined(MaintenanceReason.HistoryChanged, $"The oldest retained checkpoint {oldest} is not listed");
                AdvanceJob.StopIfIssuanceStopped(context);
                var read = await DestinationCheckpoint.ReadAsync(table, _storage, root, context.Scratch, context.Memory, context.Transfer, context, record =>
                {
                    if (CheckpointRecord.Kind(record) == RecordKind.Add)
                    {
                        _extractor.AddRecord(record);
                    }
                }, context.Stop);
                if (read.Status != DestinationStatus.Read)
                {
                    context.Stop.ThrowIfCancellationRequested();
                    if (read.Status == DestinationStatus.Preempted)
                    {
                        AdvanceJob.StopIfIssuanceStopped(context);
                    }
                    var reason = read.Status switch
                    {
                        DestinationStatus.Missing or DestinationStatus.Changed => MaintenanceReason.HistoryChanged,
                        DestinationStatus.Invalid => MaintenanceReason.OutOfEnvelope,
                        _ => MaintenanceReason.Transient
                    };
                    throw AdvanceJob.Declined(reason, $"Checkpoint {oldest} was not read: {read.Status}, {read.Detail}");
                }
                if (_extractor.Declined != PathRejection.None)
                {
                    throw AdvanceJob.Declined(MaintenanceReason.PathRejected, $"A protecting reference is unusable: {_extractor.DeclineDetail}");
                }
                await _identity.ChargeOwedAsync(context.Stop, context);
            }
            var commits = new List<IOEntry>();
            for (var version = oldest; version <= l3.Head; version++)
            {
                commits.Add(l3.Listing.Commits[version].IOEntry);
            }
            if (await AdvancePlanner.ReadAllAsync(_storage, commits, _extractor, context, MaintenanceReason.HistoryChanged, _identity) is { } declined)
            {
                throw AdvanceJob.Declined(declined.Reason, declined.Detail ?? "A protecting commit was not read");
            }
            Set.Seal(context.Stop);
        }

        /// <summary>
        /// Writes the candidates no protection holds, in key order, until the cap; the rest that could be submitted are only counted.
        /// </summary>
        public OrphanEmission Emit(PathSet candidates, DeletionExecutor executor, OrphanTargetsWriter writer, int max, RetentionCounters counters, CleanupContext context)
        {
            long kept = 0;
            long over = 0;
            long seen = 0;
            using var difference = PathSet.Difference(candidates, Set);
            while (difference.MoveNext())
            {
                if (++seen % CheckEvery == 0)
                {
                    AdvanceJob.StopIfIssuanceStopped(context);
                }
                var path = difference.DeletionPathText;
                var keep = executor.Check(executor.TargetOf(path));
                if (keep is KeepReason.StorageChanged or KeepReason.StoreUnsupported)
                {
                    throw AdvanceJob.Declined(MaintenanceReason.CleanupGate, $"No orphan can be submitted: {keep}");
                }
                if (keep != KeepReason.None)
                {
                    counters.AddOrphanKept(keep);
                    kept++;
                    continue;
                }
                if (writer.Count >= max)
                {
                    over++;
                    continue;
                }
                writer.Add(path, difference.Payload);
            }
            return new OrphanEmission(writer.Count, difference.Protected, kept + difference.Untargeted, over);
        }

        public void Dispose() => Set.Dispose();
    }
}
