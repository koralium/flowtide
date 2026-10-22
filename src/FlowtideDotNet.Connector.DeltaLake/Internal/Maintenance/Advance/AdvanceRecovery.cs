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
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance
{
    internal enum GroupState : byte
    {
        // A valid header and this table's started, every record agreeing
        Resumable,
        // Start evidence that no rule releases
        Suspended
    }

    /// <summary>
    /// A started advance no rule released: its boundary, id, state, validated records and, when resumable, its parts' bytes from its progress on.
    /// </summary>
    internal sealed record ControlGroup(long C, string AdvanceId, GroupState State, StartedRecord? Started, ProgressRecord? Progress, bool LogsComplete, long? PartBytesLeft)
    {
        // The validated header without its part descriptors, when resumable
        public HeaderRecord? Header { get; init; }

        // The newest validated progress record's name
        public string? ProgressName { get; init; }
    }

    /// <summary>
    /// The listing after recovery, the groups it left unresolved and the records it deleted.
    /// </summary>
    internal sealed record RecoveryResult(LogEvidence Log, IReadOnlyList<ControlGroup> Unresolved, int Deleted);

    /// <summary>
    /// Releases started advances only on validated evidence, tears their records down, scavenges unstarted leftovers and restores unresolved advances' demand.
    /// </summary>
    internal static class AdvanceRecovery
    {
        /// <summary>
        /// Decides each group of a checked listing by its own or a later completion, a valid header or an intact original range, deletes what is released and re-lists after any delete.
        /// </summary>
        public static async Task<RecoveryResult> RecoverAsync(CleanupConfiguration configuration, IFileStorage storage, LogEvidence log, CleanupEvidence evidence, CleanupContext context,
            RetentionCounters? counters = null)
        {
            var groups = Group(log.Listing.Controls);
            if (groups.Count == 0)
            {
                if (context.Capacity is { } ledger)
                {
                    Account(configuration, storage, log, evidence, context, ledger, Array.Empty<Records>(), Array.Empty<ControlGroup>());
                }
                return new RecoveryResult(log, Array.Empty<ControlGroup>(), 0);
            }
            foreach (var group in groups)
            {
                await ReadAsync(storage, group, context);
            }
            // This table's completions in groups whose records agree, the only ones that release or stay
            var dones = groups.Where(x => !x.Conflicted).SelectMany(x => x.ValidDones).Where(x => Ours(x.Record, evidence)).ToList();
            var released = new List<Records>();
            var unresolved = new List<ControlGroup>();
            foreach (var group in groups)
            {
                if (!group.Conflicted && group.ValidDones.Any(x => Ours(x.Record, evidence)))
                {
                    // Its own validated completion
                    released.Add(group);
                }
                else if (!group.Authority)
                {
                    // Other tables' completions go; conflicting ones stay
                    if (!group.Conflicted && group.ValidDones.Count > 0)
                    {
                        released.Add(group);
                    }
                }
                else if (dones.Any(x => x.Record.Association.AdvanceId != group.AdvanceId && x.Record.Association.C >= group.C))
                {
                    // A validated completion of another advance of this table at or above it
                    released.Add(group);
                }
                else if (Startable(group, evidence) && await HeaderValidAsync(storage, group, context))
                {
                    unresolved.Add(group.Unresolved(GroupState.Resumable));
                }
                else if (Startable(group, evidence) && group.LogsCompleteEntries.Count + group.ProgressEntries.Count + group.DoneEntries.Count == 0
                    && log.CheckRange(group.Started!.FirstJson, group.C) == ListingCheck.Passed && RootListed(log, group.Started))
                {
                    // A validated start whose original commits and root are all listed, with nothing after it
                    released.Add(group);
                }
                else
                {
                    unresolved.Add(group.Unresolved(GroupState.Suspended));
                }
            }
            if (context.Capacity is { } capacity)
            {
                Account(configuration, storage, log, evidence, context, capacity, released, unresolved);
            }
            // Never at a boundary that holds a record of unknown association
            var blocked = groups.Where(x => x.Unknown || x.Conflicted).Select(x => x.C).ToHashSet();
            var scavenged = groups.Where(x => x.Leftover && !blocked.Contains(x.C));
            // The newest validated completion of this table stays for late starts
            var kept = dones.OrderBy(x => x.Record.Association.C).ThenBy(x => x.Entry.Name, StringComparer.Ordinal).Select(x => x.Entry).LastOrDefault();
            var valid = released.SelectMany(x => x.ValidDones).Select(x => x.Entry).ToHashSet(ReferenceEqualityComparer.Instance);
            var phases = new[]
            {
                released.Concat(scavenged).SelectMany(x => x.Headers.Concat(x.Parts)).ToList(),
                released.SelectMany(x => x.ProgressEntries).ToList(),
                released.SelectMany(x => x.StartedEntries.Concat(x.LogsCompleteEntries)).ToList(),
                // Invalid copies before the valid completions that release them
                released.SelectMany(x => x.DoneEntries).Where(x => !ReferenceEquals(x, kept)).OrderBy(valid.Contains).ToList()
            };
            var deleted = await TearDownAsync(configuration, storage, phases, context, counters);
            if (deleted == 0)
            {
                return new RecoveryResult(log, unresolved, 0);
            }
            return new RecoveryResult(await AdvancePreparation.ListAsync(configuration, storage, context.Head, context), unresolved, deleted);
        }

        // By the table id preparation established, never the run's
        private static bool Ours(DoneRecord done, CleanupEvidence evidence) => string.Equals(done.TableId, evidence.TableId, StringComparison.Ordinal);

        // Every record valid and agreeing, with this table's start
        private static bool Startable(Records group, CleanupEvidence evidence) =>
            !group.Conflicted && !group.Unknown && group.Started != null && string.Equals(group.Started.TableId, evidence.TableId, StringComparison.Ordinal);

        // Without a commit at O, the classic checkpoint that roots it
        private static bool RootListed(LogEvidence log, StartedRecord started) => started.FirstJson == started.O || log.Listing.Checkpoints.Any(x => x.Version == started.O);

        // Unresolved advances hold what they have left, exempt from admission; released ones and those no listed record names hold nothing
        private static void Account(CleanupConfiguration configuration, IFileStorage storage, LogEvidence log, CleanupEvidence evidence, CleanupContext context, CapacityLedger capacity,
            IReadOnlyList<Records> released, IReadOnlyList<ControlGroup> unresolved)
        {
            var key = TableLeases.KeyOf(RawStorage.Unwrap(storage), configuration.Store.Table);
            foreach (var group in released)
            {
                capacity.Release(key, group.AdvanceId);
            }
            // The owner's and stopped owners' leases of advances no listed record names
            capacity.Retain(context.CapacityOwner, key, unresolved.Select(x => x.AdvanceId).ToHashSet());
            if (unresolved.Count == 0)
            {
                return;
            }
            var classes = LogClassifier.Classify(log);
            foreach (var group in unresolved)
            {
                var demand = CapacityEstimator.Advance(Remaining(configuration, log, classes, evidence, group, CapacityEstimator.DefaultLatency), capacity.RequestsPerSecond);
                capacity.Restore(context.CapacityOwner, key, group.AdvanceId, demand, group.State == GroupState.Resumable ? CapacityState.Runnable : CapacityState.Suspended);
            }
        }

        /// <summary>
        /// What an unresolved advance has left at a checked listing: its log targets until logs-complete, and its paths and parts from its newest progress on.
        /// </summary>
        internal static AdvanceDemandInputs Remaining(CleanupConfiguration configuration, LogEvidence log, LogClassification classes, CleanupEvidence evidence, ControlGroup group, TimeSpan latency)
        {
            var totals = group.Started?.Totals ?? new RecordTotals(0, 0, 0);
            var parts = Math.Max(0, totals.Parts - (group.Progress?.Next.Part ?? 0));
            // As the validated header describes the parts left, else their share of the started's bytes
            var partBytes = group.PartBytesLeft ?? (totals.Parts == 0 ? 0 : totals.PartBytes * parts / totals.Parts);
            var logTargets = group.LogsComplete ? 0 : LogTargets.Count(classes, group.C);
            return AdvanceDemand.Of(configuration, log, evidence, group.C, logTargets, Math.Max(0, totals.Emitted - (group.Progress?.Processed ?? 0)), parts, partBytes, latency);
        }

        // By boundary and advance id; a checked listing holds only parseable records
        private static List<Records> Group(IReadOnlyList<IOEntry> controls)
        {
            var groups = new Dictionary<(long, string), Records>();
            foreach (var entry in controls)
            {
                if (!ControlName.TryParse(entry.Name, out var name))
                {
                    continue;
                }
                if (!groups.TryGetValue((name.Boundary, name.AdvanceId), out var group))
                {
                    group = new Records(name.Boundary, name.AdvanceId);
                    groups.Add((name.Boundary, name.AdvanceId), group);
                }
                group.Of(name.Kind).Add(entry);
            }
            return groups.Values.ToList();
        }

        // Every started, logs-complete, progress and done body of the group, uncounted; started first, which progress is checked against
        private static async Task ReadAsync(IFileStorage storage, Records group, CleanupContext context)
        {
            foreach (var kind in new[] { ControlKind.Started, ControlKind.LogsComplete, ControlKind.Progress, ControlKind.Done })
            {
                foreach (var entry in group.Of(kind))
                {
                    AdvanceJob.StopIfIssuanceStopped(context);
                    using var bytes = await ControlRecords.ReadAsync(storage, entry.Path, kind, context.Memory, context.Stop);
                    group.Add(entry, kind, bytes.Bytes.Span);
                }
            }
        }

        // The exact header the group's records name, read whole with their full digest, decoded under its name, agreeing with the started and backing its progress
        private static async Task<bool> HeaderValidAsync(IFileStorage storage, Records group, CleanupContext context)
        {
            var association = group.Association!;
            var entry = group.Headers.FirstOrDefault(x => x.Name == association.HeaderName);
            if (entry == null || !ControlName.TryParse(entry.Name, out var name))
            {
                return false;
            }
            AdvanceJob.StopIfIssuanceStopped(context);
            using var bytes = await ControlRecords.ReadAsync(storage, entry.Path, ControlKind.Header, context.Memory, context.Stop);
            var descriptors = new List<PartDescriptor>();
            HeaderRecord? header = null;
            var valid = bytes.Status == RecordStatus.Valid && bytes.Sha256.AsSpan().SequenceEqual(association.HeaderSha256)
                && ControlRecords.DecodeHeader(bytes.Bytes.Span, out header, descriptors) == RecordStatus.Valid && ControlRecords.Matches(name, bytes.Bytes.Span) == RecordStatus.Valid
                && Describes(header!, group.Started!) && group.Backed(descriptors);
            if (valid)
            {
                group.Header = header;
            }
            return valid;
        }

        // The boundary, table, anchor and totals the started records
        private static bool Describes(HeaderRecord header, StartedRecord started) =>
            header.C == started.Association.C && string.Equals(header.TableId, started.TableId, StringComparison.Ordinal) && header.O == started.O && header.A == started.A
            && header.Head == started.Head && header.FirstJson == started.FirstJson && header.Totals == started.Totals && Same(header.Anchor, started.Anchor);

        // The table, boundary versions, first commit, anchor and totals the first started records
        private static bool Describes(StartedRecord copy, StartedRecord started) =>
            string.Equals(copy.TableId, started.TableId, StringComparison.Ordinal) && copy.O == started.O && copy.A == started.A && copy.Head == started.Head
            && copy.FirstJson == started.FirstJson && copy.Totals == started.Totals && Same(copy.Anchor, started.Anchor);

        // The table, boundary versions, anchor and totals the started records
        private static bool Describes(DoneRecord done, StartedRecord started) =>
            string.Equals(done.TableId, started.TableId, StringComparison.Ordinal) && done.O == started.O && done.A == started.A && done.Head == started.Head
            && done.Totals == started.Totals && Same(done.Anchor, started.Anchor);

        // Equal anchor summaries, digest included
        private static bool Same(AnchorSummary left, AnchorSummary right) =>
            left.Version == right.Version && string.Equals(left.StageId, right.StageId, StringComparison.Ordinal) && left.Length == right.Length
            && left.Sha256.AsSpan().SequenceEqual(right.Sha256) && left.ListedSize == right.ListedSize;

        // Phase by phase, one record at a time; a record not removed ends the job
        private static async Task<int> TearDownAsync(CleanupConfiguration configuration, IFileStorage storage, IReadOnlyList<List<IOEntry>> phases, CleanupContext context, RetentionCounters? counters)
        {
            if (phases.All(x => x.Count == 0))
            {
                return 0;
            }
            if (MaintenanceSwitches.DeletesDisabled())
            {
                throw AdvanceJob.Declined(MaintenanceReason.DeletesDisabled, $"{MaintenanceSwitches.DisableMaintenanceDeletes} is on, released cleanup records are kept");
            }
            var executor = new DeletionExecutor(configuration, storage) { Settled = counters is { } counted ? counted.Add : null };
            var deleted = 0;
            foreach (var phase in phases)
            {
                foreach (var entry in phase)
                {
                    var target = executor.TargetOf(entry.Path.Full, TargetKind.Control);
                    Task<DeleteOutcome>? delete = null;
                    if (context.TryIssue(stop => delete = executor.DeleteAsync(target, false, stop)) == null)
                    {
                        AdvanceJob.StopIfIssuanceStopped(context);
                    }
                    var outcome = await delete!;
                    if (!outcome.Done)
                    {
                        throw AdvanceJob.Declined(MaintenanceReason.Transient, $"Cleanup record {entry.Name} was not removed: {outcome.Status} {outcome.Reason} {outcome.Error?.Message}");
                    }
                    deleted++;
                }
            }
            return deleted;
        }

        // One advance's listed records and what their bodies validated
        private sealed class Records
        {
            // Valid progress bodies, which every other of the same sequence must equal
            private readonly List<ProgressRecord> _progress = new List<ProgressRecord>();

            public Records(long c, string advanceId)
            {
                C = c;
                AdvanceId = advanceId;
            }

            public long C { get; }

            public string AdvanceId { get; }

            public List<IOEntry> Parts { get; } = new List<IOEntry>();

            public List<IOEntry> Headers { get; } = new List<IOEntry>();

            public List<IOEntry> StartedEntries { get; } = new List<IOEntry>();

            public List<IOEntry> LogsCompleteEntries { get; } = new List<IOEntry>();

            public List<IOEntry> ProgressEntries { get; } = new List<IOEntry>();

            public List<IOEntry> DoneEntries { get; } = new List<IOEntry>();

            public List<(IOEntry Entry, DoneRecord Record)> ValidDones { get; } = new List<(IOEntry, DoneRecord)>();

            public StartedRecord? Started { get; private set; }

            public ProgressRecord? NewestProgress { get; private set; }

            public string? NewestProgressName { get; private set; }

            // Once the header validated
            public HeaderRecord? Header { get; set; }

            public bool LogsComplete { get; private set; }

            // Whole-object bytes of the parts from the newest progress's on, once the header backs it
            public long? PartBytesLeft { get; private set; }

            // The header every validated body names
            public Association? Association { get; private set; }

            // Validated bodies name different headers
            public bool Conflicted { get; private set; }

            // A body torn, over its cap, gone, not the record its name says, a completion, logs-complete or started copy its start contradicts, or progress its start or header contradicts
            public bool Unknown { get; private set; }

            // Evidence the advance started
            public bool Authority => StartedEntries.Count + LogsCompleteEntries.Count + ProgressEntries.Count > 0;

            // Parts and headers only
            public bool Leftover => !Authority && DoneEntries.Count == 0;

            public List<IOEntry> Of(ControlKind kind) => kind switch
            {
                ControlKind.Part => Parts,
                ControlKind.Header => Headers,
                ControlKind.Started => StartedEntries,
                ControlKind.LogsComplete => LogsCompleteEntries,
                ControlKind.Progress => ProgressEntries,
                _ => DoneEntries
            };

            public ControlGroup Unresolved(GroupState state) => new ControlGroup(C, AdvanceId, state, Started, NewestProgress, LogsComplete, PartBytesLeft)
            {
                Header = Header,
                ProgressName = NewestProgressName
            };

            // A body read whole, or empty
            public void Add(IOEntry entry, ControlKind kind, ReadOnlySpan<byte> bytes)
            {
                RecordStatus status;
                Association? association;
                switch (kind)
                {
                    case ControlKind.Started:
                        status = ControlRecords.Decode(bytes, out StartedRecord? started);
                        association = started?.Association;
                        if (Agrees(ref status, entry, bytes) && Started != null && Started.Association.Equals(association) && !Describes(started!, Started))
                        {
                            // A copy of the same header's start that records another advance
                            status = RecordStatus.Torn;
                        }
                        if (status == RecordStatus.Valid)
                        {
                            Started ??= started;
                        }
                        break;
                    case ControlKind.LogsComplete:
                        status = ControlRecords.Decode(bytes, out LogsCompleteRecord? logsComplete);
                        association = logsComplete?.Association;
                        if (Agrees(ref status, entry, bytes) && Started != null && Started.Association.Equals(association)
                            && (!string.Equals(logsComplete!.TableId, Started.TableId, StringComparison.Ordinal) || logsComplete.O != Started.O))
                        {
                            // Not the logs-complete its start declares
                            status = RecordStatus.Torn;
                        }
                        LogsComplete |= status == RecordStatus.Valid;
                        break;
                    case ControlKind.Progress:
                        status = ControlRecords.Decode(bytes, out ProgressRecord? progress);
                        association = progress?.Association;
                        if (Agrees(ref status, entry, bytes) && (!Within(progress!, Started)
                            || _progress.Any(x => x.Sequence == progress!.Sequence && x.Association.Equals(progress.Association) && x != progress)))
                        {
                            // Past what its start declares, or another claim of its header at its sequence
                            status = RecordStatus.Torn;
                        }
                        if (status == RecordStatus.Valid)
                        {
                            _progress.Add(progress!);
                            if (NewestProgress == null || progress!.Sequence > NewestProgress.Sequence)
                            {
                                NewestProgress = progress;
                                NewestProgressName = entry.Name;
                            }
                        }
                        break;
                    default:
                        status = ControlRecords.Decode(bytes, out DoneRecord? done);
                        association = done?.Association;
                        if (Agrees(ref status, entry, bytes) && Started != null && !Describes(done!, Started))
                        {
                            // Not the completion its start declares
                            status = RecordStatus.Torn;
                        }
                        if (status == RecordStatus.Valid)
                        {
                            ValidDones.Add((entry, done!));
                        }
                        break;
                }
                if (status != RecordStatus.Valid)
                {
                    Unknown = true;
                }
                else if (Association == null)
                {
                    Association = association;
                }
                else if (!Association.Equals(association))
                {
                    Conflicted = true;
                }
            }

            // A decoded body whose name agrees with it
            private static bool Agrees(ref RecordStatus status, IOEntry entry, ReadOnlySpan<byte> bytes)
            {
                if (status == RecordStatus.Valid)
                {
                    status = ControlName.TryParse(entry.Name, out var name) ? ControlRecords.Matches(name, bytes) : RecordStatus.Mismatch;
                }
                return status == RecordStatus.Valid;
            }

            // The newest progress, torn when its cursor is past its part or it claims more than the header's parts hold before its cursor
            public bool Backed(IReadOnlyList<PartDescriptor> descriptors)
            {
                var progress = NewestProgress;
                if (progress == null || progress.Next.Part >= descriptors.Count || progress.Next.Record <= descriptors[progress.Next.Part].Paths
                    && progress.Next.Byte <= descriptors[progress.Next.Part].Bytes && progress.Processed <= descriptors.Take(progress.Next.Part).Sum(x => (long)x.Paths) + progress.Next.Record)
                {
                    PartBytesLeft = descriptors.Skip(progress?.Next.Part ?? 0).Sum(x => (long)x.Bytes);
                    return true;
                }
                Unknown = true;
                NewestProgress = null;
                NewestProgressName = null;
                return false;
            }

            // A next position inside a declared part with no more paths processed than emitted or than full parts hold before its record, or the terminal one with every emitted path processed
            private static bool Within(ProgressRecord progress, StartedRecord? started) => started == null
                || progress.Next.Record <= ControlRecords.MaxPartPaths && progress.Next.Byte <= ControlRecords.PartCap && (progress.Next.Part < started.Totals.Parts
                    ? progress.Processed <= Math.Min(started.Totals.Emitted, (long)progress.Next.Part * ControlRecords.MaxPartPaths + progress.Next.Record)
                    : progress.Next == new RecordCursor(started.Totals.Parts, 0, 0) && progress.Processed == started.Totals.Emitted);
        }
    }
}
