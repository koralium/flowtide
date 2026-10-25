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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.References;
using Stowage;
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance
{
    /// <summary>
    /// The exclusions a foreign change leaves an advance, or with no set the reason it suspends, retries or promotes.
    /// </summary>
    internal sealed record ExclusionResult(ExclusionSet? Set, MaintenanceReason Reason = MaintenanceReason.None, string? Detail = null)
    {
        // Proceed with a set, otherwise a suspension, a retry or a promotion
        public GateAction Action { get; init; } = Set == null ? GateAction.Suspend : GateAction.Proceed;

        // The planned head a computed set was rehashed against
        public ContentProof? PlannedHead { get; init; }
    }

    /// <summary>
    /// The planned paths that commits after the advance's planned head referenced again.
    /// </summary>
    internal static class AdvanceExclusions
    {
        // More excluded paths than this suspend the advance
        public const int MaxExclusions = 100_000;

        // Excluded paths taking more managed bytes than this suspend the advance
        public const long MaxExclusionBytes = 32L << 20;

        /// <summary>
        /// None when the epoch since the planned head was uninterrupted, otherwise those of every commit after it among the advance's paths from the cursor on; a missing or replaced planned head suspends, and the evidence keeps it for every later gate.
        /// </summary>
        public static async Task<ExclusionResult> ForResumeAsync(CleanupConfiguration configuration, IFileStorage storage, LogEvidence log, CleanupEvidence evidence, StartedRecord started,
            RecordCursor from, CleanupContext context)
        {
            var (planned, stopped) = await PlannedHeadAsync(configuration, storage, log, started, context);
            if (stopped != null)
            {
                return stopped;
            }
            // Rehashed at every later gate wherever the evidence is anchored
            evidence.KeepPlannedHead(planned!);
            if (evidence.Anchor.AdoptedAt == started.A)
            {
                return new ExclusionResult(ExclusionSet.Empty);
            }
            return await ComputeAsync(configuration, storage, log, started, planned, from, MaxExclusions, context);
        }

        /// <summary>
        /// Those of every commit and stage after the planned head, among the advance's paths from its cursor on; a missing or replaced planned head suspends.
        /// </summary>
        public static Task<ExclusionResult> ComputeAsync(CleanupConfiguration configuration, IFileStorage storage, LogEvidence log, ActiveAdvance active, CleanupContext context,
            int cap = MaxExclusions)
        {
            return ComputeAsync(configuration, storage, log, active.Started, null, active.Cursor, cap, context);
        }

        // The planned head listed and rehashed whole to the started record's anchor, or the outcome a gate gives that anchor
        private static async Task<(ContentProof? Proof, ExclusionResult? Stopped)> PlannedHeadAsync(CleanupConfiguration configuration, IFileStorage storage, LogEvidence log, StartedRecord started,
            CleanupContext context)
        {
            var anchor = started.Anchor;
            var read = await CommitAnchor.ProveAsync(configuration.Store.Table, storage, log, started.Head, context);
            var verdict = read.Check switch
            {
                AnchorCheck.Proven when read.Proof!.SameContent(anchor.Length, anchor.Sha256) => GateVerdict.Passed,
                AnchorCheck.Missing or AnchorCheck.NotPublished => GateVerdict.AnchorMissing,
                AnchorCheck.Preempted => GateVerdict.Preempted,
                // A fresh short admission spends its reads the same way
                AnchorCheck.ReadsSpent => GateVerdict.NeedsLongAdmission,
                _ => GateVerdict.AnchorReplaced
            };
            if (verdict == GateVerdict.Passed)
            {
                return (read.Proof, null);
            }
            if (verdict == GateVerdict.Preempted)
            {
                context.Stop.ThrowIfCancellationRequested();
            }
            var gate = new GateResult(verdict, verdict switch
            {
                GateVerdict.AnchorMissing => $"The planned head {started.Head} is missing",
                GateVerdict.AnchorReplaced => $"The planned head {started.Head} has other content than the advance started from",
                _ => $"Reading the planned head {started.Head} ended {read.Check}"
            }) { Stop = read.Stop, Durable = true };
            return (null, new ExclusionResult(null, gate.Reason, gate.Detail) { Action = gate.Action });
        }

        private static async Task<ExclusionResult> ComputeAsync(CleanupConfiguration configuration, IFileStorage storage, LogEvidence log, StartedRecord started, ContentProof? planned,
            RecordCursor from, int cap, CleanupContext context)
        {
            var plannedHead = started.Head;
            var head = log.Head;
            if (log.CheckRange(plannedHead + 1, head) != ListingCheck.Passed)
            {
                return Suspend(MaintenanceReason.HistoryChanged, $"Commits from {plannedHead + 1} to {head} are not all listed");
            }
            var classes = LogClassifier.Classify(log);
            if (classes.V2OrSidecar)
            {
                return Suspend(MaintenanceReason.SidecarRoot, "The log lists a V2 checkpoint or a sidecar folder");
            }
            if (classes.Names.Any(x => x.Kind == LogNameKind.UnknownVersioned && x.Version > plannedHead))
            {
                return Suspend(MaintenanceReason.UnclassifiedLogFile, $"The log lists an unknown name above the planned head {plannedHead}");
            }
            PathCanonicalizer? identity = null;
            // Promoted for its resolutions
            var promoted = false;
            try
            {
                var commits = new List<IOEntry>();
                for (var version = plannedHead + 1; version <= head; version++)
                {
                    commits.Add(log.Listing.Commits[version].IOEntry);
                }
                var stages = classes.Stages.Select(x => x.Entry).ToList();
                var reads = commits.Count + stages.Count + (planned == null ? 1 : 0);
                if (!context.Long && reads > context.ReadsRemaining)
                {
                    return new ExclusionResult(null, MaintenanceReason.None, $"{reads} counted reads need long admission") { Action = GateAction.Promote };
                }
                if (planned == null)
                {
                    var (proof, stopped) = await PlannedHeadAsync(configuration, storage, log, started, context);
                    if (stopped != null)
                    {
                        return stopped;
                    }
                    planned = proof;
                }
                identity = PathCanonicalizer.Create(configuration.Store, storage);
                using var protection = new PathSet(context.Scratch, context.Memory, identity.Domain, PathRole.Protection);
                var protect = new CommitReferenceExtractor(identity, protection, PathRole.Protection);
                using (var declined = await AdvancePlanner.ReadAllAsync(storage, commits, protect, context, MaintenanceReason.HistoryChanged))
                {
                    if (declined != null)
                    {
                        return OfDeclined(declined);
                    }
                }
                // A stage may be published or abandoned after the listing
                using (var declined = await AdvancePlanner.ReadAllAsync(storage, stages, protect, context, MaintenanceReason.Transient))
                {
                    if (declined != null)
                    {
                        return OfDeclined(declined);
                    }
                }
                protection.Seal(context.Stop);
                using var candidates = new PathSet(context.Scratch, context.Memory, identity.Domain, PathRole.Candidate);
                if (await RemainingAsync(configuration, storage, started, from, identity, candidates, context) is { } unusable)
                {
                    return unusable;
                }
                candidates.Seal(context.Stop);
                // Every resolution counts as a read before any is charged
                if (MeteredFileStorage.Of(storage) != null && !context.TryCountReads(identity.Owed))
                {
                    promoted = true;
                    return new ExclusionResult(null, MaintenanceReason.None, $"{reads} counted reads and {identity.Owed} native path resolutions need long admission") { Action = GateAction.Promote };
                }
                await identity.ChargeOwedAsync(context.Stop, context);
                var excluded = new HashSet<string>(StringComparer.Ordinal);
                // Charged while computing and by each turn that holds the set
                long charged = 0;
                try
                {
                    using var both = PathSet.Intersection(candidates, protection);
                    while (both.MoveNext())
                    {
                        var path = both.DeletionPathText;
                        var bytes = ExclusionSet.BytesOf(path);
                        if (excluded.Count >= cap)
                        {
                            return Suspend(MaintenanceReason.EntriesOverCap, $"More than {cap} planned paths are referenced again");
                        }
                        if (charged + bytes > MaxExclusionBytes)
                        {
                            return Suspend(MaintenanceReason.ManagedMemoryCap, $"The planned paths referenced again take more than {MaxExclusionBytes} bytes");
                        }
                        context.Memory.Reserve(bytes);
                        charged += bytes;
                        excluded.Add(path);
                    }
                }
                finally
                {
                    context.Memory.Release(charged);
                }
                return new ExclusionResult(ExclusionSet.Of(excluded, charged)) { PlannedHead = planned };
            }
            catch (OperationCanceledException) when (context.Stop.IsCancellationRequested)
            {
                throw;
            }
            catch (CleanupPreemptedException e) when (context.Stop.IsCancellationRequested)
            {
                throw new OperationCanceledException(e.Message, e, context.Stop);
            }
            catch (CleanupPreemptedException e)
            {
                return Retry(e.Stop == IssuanceStop.Deadline ? MaintenanceReason.JobDeadline : MaintenanceReason.None, e.Message);
            }
            // Memory, disk and deadline limits
            catch (CheckpointDeclinedException e)
            {
                return Retry(e.Reason, e.Message);
            }
            catch (Exception e)
            {
                return Retry(MaintenanceReason.Transient, $"{e.GetType().Name}: {e.Message}");
            }
            finally
            {
                // A suspension or retry repays its resolutions in turns
                if (identity != null && !promoted)
                {
                    await identity.RepayOwedAsync(context);
                }
            }
        }

        // Each path from the cursor on in the header's parts, keyed in the references' domain; null when every part matched its digest and every path keyed
        private static async Task<ExclusionResult?> RemainingAsync(CleanupConfiguration configuration, IFileStorage storage, StartedRecord started, RecordCursor from, PathCanonicalizer identity,
            PathSet candidates, CleanupContext context)
        {
            var association = started.Association;
            var folder = configuration.Store.Table.Combine(DeltaTransactionReader.DeltaLogDirName);
            var descriptors = new List<PartDescriptor>();
            using (var header = await ControlRecords.ReadAsync(storage, folder.Combine(association.HeaderName), ControlKind.Header, context.Memory, context.Stop))
            {
                if (header.Status != RecordStatus.Valid || !header.Sha256.AsSpan().SequenceEqual(association.HeaderSha256)
                    || ControlRecords.DecodeHeader(header.Bytes.Span, out _, descriptors) != RecordStatus.Valid)
                {
                    return Suspend(MaintenanceReason.ManifestSuspended, $"The header of the advance to checkpoint {association.C} is not the one its records name");
                }
            }
            var prefix = configuration.Store.Table.Full + IOPath.PathSeparatorString;
            for (var part = from.Part; part < descriptors.Count; part++)
            {
                AdvanceJob.StopIfIssuanceStopped(context);
                var descriptor = descriptors[part];
                using var bytes = await ControlRecords.ReadAsync(storage, folder.Combine(ControlName.Part(association.C, association.AdvanceId, part, descriptor.AttemptId)), ControlKind.Part,
                    context.Memory, context.Stop);
                if (bytes.Status != RecordStatus.Valid || bytes.Length != descriptor.Bytes || !bytes.Sha256.AsSpan().SequenceEqual(descriptor.Sha256)
                    || ControlRecords.ValidatePart(bytes.Bytes.Span, out _) != RecordStatus.Valid)
                {
                    return Suspend(MaintenanceReason.ManifestSuspended, $"Part {part} of the advance to checkpoint {association.C} is not the one its header names");
                }
                var rejection = Add(bytes.Bytes.Span, part == from.Part ? from.Record : 0, prefix, identity, candidates);
                if (rejection != PathRejection.None)
                {
                    return Suspend(MaintenanceReason.PathRejected, $"A path of part {part} has no key: {rejection}");
                }
            }
            return null;
        }

        // A validated part's paths after the first skip, each under its key with its exact text
        private static PathRejection Add(ReadOnlySpan<byte> part, int skip, string prefix, PathCanonicalizer identity, PathSet candidates)
        {
            var record = 0;
            foreach (var path in ControlRecords.Paths(part))
            {
                if (record++ < skip)
                {
                    continue;
                }
                var text = Encoding.UTF8.GetString(path);
                var key = text.StartsWith(prefix, StringComparison.Ordinal) ? identity.Name(text.Substring(prefix.Length), PathRole.Candidate) : PathKey.Reject(PathRejection.Outside);
                if (!key.Accepted)
                {
                    return key.Rejection;
                }
                candidates.Add(key.Key!, text);
            }
            return PathRejection.None;
        }

        // A commit gone or changed, or a reference protection cannot use, suspends; a stage gone or spent reads retry
        private static ExclusionResult OfDeclined(AdvancePlan declined) => declined.Reason == MaintenanceReason.Transient
            ? Retry(declined.Reason, declined.Detail)
            : Suspend(declined.Reason, declined.Detail);

        private static ExclusionResult Suspend(MaintenanceReason reason, string? detail) => new ExclusionResult(null, reason, detail);

        private static ExclusionResult Retry(MaintenanceReason reason, string? detail) => new ExclusionResult(null, reason, detail) { Action = GateAction.Retry };
    }
}
