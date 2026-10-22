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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance
{
    /// <summary>
    /// Ends an advance job early with its result.
    /// </summary>
    internal sealed class AdvanceEndedException : Exception
    {
        public AdvanceEndedException(CleanupResult result)
            : base(result.Detail)
        {
            Result = result;
        }

        public CleanupResult Result { get; }
    }

    /// <summary>
    /// One advance job: preparation with recovery, capacity admission, boundary selection, planning, the hint raised to C, the manifest, recheck, reservation and started record, then the hand-off to the delete turns; or the resume of a started advance.
    /// </summary>
    internal sealed class AdvanceJob
    {
        private readonly CleanupConfiguration _configuration;
        private readonly IFileStorage _storage;
        private readonly TimeProvider _clock;
        private readonly string? _formatted;

        // A short admission's read limit, which the short cap is taken from under either admission
        private long _shortReads = MaintenanceWorkerSettings.DefaultShortReads;

        // The native path resolutions of the latest plan read whole or promoted for them
        private long _resolutions;

        // Storage is the run's meter
        public AdvanceJob(CleanupConfiguration configuration, IFileStorage storage, TimeProvider clock)
        {
            _configuration = configuration;
            _storage = storage;
            _clock = clock;
            if (configuration.OptIns.Retention is { } retention)
            {
                _formatted = RetentionProperties.Format(retention);
            }
        }

        internal Func<CleanupConfiguration, IFileStorage, IOPath, AdvanceBoundary, LogClassification, CleanupContext, Task<AdvancePlan>> PlanForTests { get; set; } = AdvancePlanner.PlanAsync;

        // Sees each selected boundary
        internal Action<AdvanceBoundary>? SelectedHookForTests { get; set; }

        // Sees each plan before the job disposes it
        internal Action<AdvancePlan>? PlannedHookForTests { get; set; }

        // Takes each advance the job starts or resumes into the table's delete turns
        internal Action<ActiveAdvance>? HandOff { get; set; }

        // The per-delete latency the advance's demand is priced at
        internal Func<TimeSpan> Latency { get; set; } = static () => CapacityEstimator.DefaultLatency;

        // Awaited at the job's steps and passed to its publications
        internal AdvanceHooks Hooks { get; set; } = new AdvanceHooks();

        // The table's warn-once log
        internal Action<MaintenanceReason, string>? Warn { get; set; }

        // Sees each preparation's O and A
        internal Action<PreparedAdvance>? Prepared { get; set; }

        // Sees each preparation's recovery
        internal Action<RecoveryResult>? Recovered { get; set; }

        // The sink's counts, which recovery's teardown adds to
        internal RetentionCounters? Counters { get; set; }

        internal ResumeExclusions ResumeExclusionsForTests { get; set; } = AdvanceExclusions.ForResumeAsync;

        /// <summary>
        /// One admitted advance; never throws except an OperationCanceledException from the context's stop.
        /// </summary>
        public async Task<CleanupResult> RunAsync(CleanupContext context)
        {
            if (_configuration.OptIns.Retention is not { } retention)
            {
                return new CleanupResult(OutcomeKind.NoWork);
            }
            if (!context.Long)
            {
                Volatile.Write(ref _shortReads, context.ReadLimit);
            }
            try
            {
                return await RunCoreAsync(retention, context);
            }
            catch (AdvanceEndedException e)
            {
                return e.Result;
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
                return new CleanupResult(OutcomeKind.Preempted, e.Stop == IssuanceStop.Deadline ? MaintenanceReason.JobDeadline : MaintenanceReason.None, e.Message);
            }
            catch (CheckpointDeclinedException e)
            {
                return new CleanupResult(OutcomeKind.Declined, e.Reason, e.Message);
            }
            catch (Exception e)
            {
                return new CleanupResult(OutcomeKind.Failed, MaintenanceReason.Transient, $"{e.GetType().Name}: {e.Message}");
            }
        }

        private async Task<CleanupResult> RunCoreAsync(TimeSpan retention, CleanupContext context)
        {
            var prepared = await AdvancePreparation.PrepareAsync(_configuration, _storage, _formatted!, context, Counters, Recovered);
            Prepared?.Invoke(prepared);
            if (prepared.Resumable != null)
            {
                // Exempt from admission
                var resumed = await AdvancePreparation.ResumeAsync(_configuration, _storage, _formatted!, retention, _clock.GetUtcNow(), Latency(), prepared, ResumeExclusionsForTests, context);
                await Hooks.StepAsync(AdvanceSteps.Resumed);
                return await HandOffAsync(resumed, $"Resumed the advance to checkpoint {resumed.C}");
            }
            var classes = LogClassifier.Classify(prepared.Log);
            if (classes.V2OrSidecar)
            {
                return new CleanupResult(OutcomeKind.Declined, MaintenanceReason.SidecarRoot, "The log lists a V2 checkpoint or a sidecar folder, which the advance does not handle");
            }
            // After recovery, before any selection or planning
            if (context.Capacity is { } capacity && !capacity.Admits())
            {
                return Backlog(capacity).Result;
            }
            var boundary = await BoundarySelector.SelectAsync(_configuration, _storage, prepared, classes, retention, Volatile.Read(ref _shortReads), _clock.GetUtcNow(), context);
            SelectedHookForTests?.Invoke(boundary);
            if (classes.LowestUnknownVersioned < boundary.C)
            {
                return new CleanupResult(OutcomeKind.Declined, MaintenanceReason.UnclassifiedLogFile, $"The log lists an unknown name at version {classes.LowestUnknownVersioned}, below checkpoint {boundary.C}");
            }
            if (boundary.Step == AdvanceStep.Long && !context.Long)
            {
                return CleanupResult.Promote;
            }
            using var plan = await PlanForTests(_configuration, _storage, _configuration.Store.Table, boundary with { Resolutions = Volatile.Read(ref _resolutions) }, classes, context);
            PlannedHookForTests?.Invoke(plan);
            if (plan.Resolutions is { } resolutions)
            {
                Volatile.Write(ref _resolutions, resolutions);
            }
            switch (plan.Status)
            {
                case AdvancePlanStatus.Planned:
                    break;
                case AdvancePlanStatus.NeedsLongAdmission:
                    return context.Long ? new CleanupResult(OutcomeKind.Declined, MaintenanceReason.Unexpected, "The advance planner asked for long admission again") : CleanupResult.Promote;
                case AdvancePlanStatus.Declined:
                    return new CleanupResult(OutcomeKind.Declined, plan.Reason == MaintenanceReason.None ? MaintenanceReason.Unexpected : plan.Reason, plan.Detail);
                default:
                    return new CleanupResult(OutcomeKind.NoWork, MaintenanceReason.None, plan.Detail);
            }
            // Only a usable, marked, gap-free C read back from the destination is relied on
            if (plan.Destination is not { GapFree: true } destination || destination.Proof!.Version != boundary.C || destination.Proof.Path.Full != boundary.Checkpoint.IOEntry.Path.Full)
            {
                return new CleanupResult(OutcomeKind.Declined, MaintenanceReason.ValidationFailed, $"The plan's destination read of checkpoint {boundary.C} is not a usable, marked, gap-free checkpoint {boundary.C}: {plan.Destination?.Status} {plan.Destination?.Detail}");
            }
            // A plan no ledger could admit writes nothing
            if (context.Capacity is { } ledger)
            {
                var alone = CapacityEstimator.Advance(AdvanceDemand.Of(_configuration, prepared.Log, prepared.Evidence!, boundary.C, LogTargets.Count(classes, boundary.C), plan.Emitted, plan.Parts.Count,
                    plan.Parts.Sum(x => x.Bytes), Latency()), ledger.RequestsPerSecond);
                var drain = ledger.DrainSeconds(alone);
                if (drain > CapacityLedger.CloseSeconds)
                {
                    var detail = $"The advance to checkpoint {boundary.C} alone would take {drain / 3600:0.#} hours to drain, more than the {CapacityLedger.CloseSeconds / 3600:0} hours admission allows";
                    Warn?.Invoke(MaintenanceReason.DeleteBacklog, detail);
                    return new CleanupResult(OutcomeKind.Declined, MaintenanceReason.DeleteBacklog, detail) { Oversized = true };
                }
            }
            // The newest checkpoint at or above C the plan read whole
            var newest = plan.Newest is { Status: DestinationStatus.Read, Proof: { } proof } read && proof.Version > boundary.C ? read : destination;
            await RaiseHintAsync(_storage, _configuration.Store.Table, boundary.Log, boundary.C, newest, context);
            await Hooks.StepAsync(AdvanceSteps.Hint);
            return await StartAsync(prepared, boundary, plan, destination, context);
        }

        // The manifest, recheck, reservation and started record, then the hand-off
        private async Task<CleanupResult> StartAsync(PreparedAdvance prepared, AdvanceBoundary boundary, AdvancePlan plan, DestinationRead destination, CleanupContext context)
        {
            var table = _configuration.Store.Table;
            var publisher = new ControlPublisher(_storage, table, context, beforePublish: Hooks.BeforePublish);
            var header = await publisher.PublishManifestAsync(boundary, plan);
            await Hooks.StepAsync(AdvanceSteps.Header);
            var (log, classes) = await RecheckAsync(prepared, boundary, destination, context);
            await Hooks.StepAsync(AdvanceSteps.Recheck);
            var association = new Association(boundary.C, boundary.AdvanceId, header.Name, header.Sha256);
            var started = new StartedRecord(association, boundary.TableId, boundary.O, boundary.A, boundary.Head, boundary.FirstJson, AnchorSummary.Of(boundary.Anchor!),
                new RecordTotals(plan.Emitted, plan.Parts.Count, plan.Parts.Sum(x => x.Bytes)));
            var totals = started.Totals;
            var logTargets = LogTargets.Count(classes, boundary.C);
            CapacityLease? lease = null;
            if (context.Capacity is { } capacity)
            {
                var demand = CapacityEstimator.Advance(AdvanceDemand.Of(_configuration, log, prepared.Evidence!, boundary.C, logTargets, totals.Emitted, totals.Parts, totals.PartBytes, Latency()), capacity.RequestsPerSecond);
                // A decline publishes no started record
                lease = capacity.TryReserve(context.CapacityOwner, TableLeases.KeyOf(RawStorage.Unwrap(_storage), table), boundary.AdvanceId, demand) ?? throw Backlog(capacity);
            }
            try
            {
                await Hooks.StepAsync(AdvanceSteps.Reserved);
                await publisher.PublishAsync(ControlKind.Started, association, -1, () => ControlRecords.Encode(started));
                await Hooks.StepAsync(AdvanceSteps.Started);
                var active = new ActiveAdvance()
                {
                    Association = association,
                    Started = started,
                    Destination = CheckpointSummary.Of(destination.Proof!),
                    CutoffTicks = boundary.Cutoff.UtcTicks,
                    Evidence = prepared.Evidence!,
                    Lease = lease,
                    Cursor = ActiveAdvance.Start(totals),
                    PartBytesLeft = totals.PartBytes,
                    LogTargetsLeft = logTargets,
                    LogEntries = log.Entries.Count
                };
                lease = null;
                return await HandOffAsync(active, $"Started the advance to checkpoint {boundary.C} with {plan.Emitted} deletions in {plan.Parts.Count} parts and {logTargets} log targets");
            }
            catch
            {
                lease?.Dispose();
                throw;
            }
        }

        // A fresh listing before started: the evidence from the first JSON, C unchanged, the sources at or above C, the names, and only this manifest and completions
        private async Task<(LogEvidence Log, LogClassification Classes)> RecheckAsync(PreparedAdvance prepared, AdvanceBoundary boundary, DestinationRead destination, CleanupContext context)
        {
            var c = boundary.C;
            var evidence = prepared.Evidence!;
            var log = await AdvancePreparation.ListAsync(_configuration, _storage, context.Head, context);
            var validated = await evidence.ValidateAsync(_configuration, _storage, log, context, prepared.FirstJson);
            if (!validated.Passed)
            {
                // A foreign change waits for the next preparation
                throw validated.Action == GateAction.Rebase ? Declined(MaintenanceReason.Transient, $"{validated.Verdict}: {validated.Detail}") : AdvancePreparation.Ended(validated, context);
            }
            if (!destination.Proof!.ListedUnchanged(ContentProof.Find(log.Listing, c, true)))
            {
                throw Declined(MaintenanceReason.Transient, $"Checkpoint {c} is not listed as the plan read it");
            }
            if (evidence.Capability.Sources.FirstOrDefault(x => x.Version < c) is { } source)
            {
                throw Declined(MaintenanceReason.CleanupGate, $"The table's protocol or metadata is read from version {source.Version}, below checkpoint {c}");
            }
            var classes = LogClassifier.Classify(log);
            if (classes.V2OrSidecar)
            {
                throw Declined(MaintenanceReason.SidecarRoot, "The log lists a V2 checkpoint or a sidecar folder, which the advance does not handle");
            }
            if (classes.LowestUnknownVersioned < c)
            {
                throw Declined(MaintenanceReason.UnclassifiedLogFile, $"The log lists an unknown name at version {classes.LowestUnknownVersioned}, below checkpoint {c}");
            }
            foreach (var entry in log.Listing.Controls)
            {
                if (!ControlName.TryParse(entry.Name, out var name) || (name.Kind != ControlKind.Done
                    && (name.Kind is not (ControlKind.Part or ControlKind.Header) || name.Boundary != c || !string.Equals(name.AdvanceId, boundary.AdvanceId, StringComparison.Ordinal))))
                {
                    throw Declined(MaintenanceReason.Transient, $"Another advance's record {entry.Name} is listed");
                }
            }
            return (log, classes);
        }

        // Into the table's delete turns
        private async Task<CleanupResult> HandOffAsync(ActiveAdvance active, string detail)
        {
            HandOff?.Invoke(active);
            await Hooks.StepAsync(AdvanceSteps.HandedOff);
            return new CleanupResult(OutcomeKind.Completed, MaintenanceReason.None, detail);
        }

        /// <summary>
        /// The hint on a listed checkpoint at or above C, through the metered transport.
        /// </summary>
        internal static async Task RaiseHintAsync(IFileStorage storage, IOPath table, LogEvidence log, long c, DestinationRead newest, CleanupContext context)
        {
            StopIfIssuanceStopped(context);
            if (!log.Fence.AllowsHint(c))
            {
                throw Declined(MaintenanceReason.CleanupGate, $"The hint may not move to {c}: {log.Fence.Detail}");
            }
            var transport = new StowageCheckpointTransport(storage, table);
            var hint = await transport.ReadHintAsync(context.Stop);
            var proof = newest.Proof!;
            // A single-part hint above the newest checkpoint read stays while a listed non-empty classic checkpoint at or after it backs it
            if (hint != null && hint.Version > proof.Version && hint.Parts is (null or 1) && log.Listing.Checkpoints.Any(x => x.Version >= hint.Version && x.IOEntry.Size is not 0))
            {
                return;
            }
            // Any other hint names the newest checkpoint read, which is classic
            if (hint == null || hint.Parts is not (null or 1) || hint.Version != proof.Version || hint.SizeInBytes != proof.Length || hint.Size != newest.Rows)
            {
                await transport.WriteHintAsync(new LastCheckpointInfo() { Version = proof.Version, Size = newest.Rows, SizeInBytes = proof.Length, NumOfAddFiles = newest.Adds }, context.Stop);
            }
        }

        internal static AdvanceEndedException Declined(MaintenanceReason reason, string detail) => new AdvanceEndedException(new CleanupResult(OutcomeKind.Declined, reason, detail));

        // New advances wait while runnable demand is too high
        internal static AdvanceEndedException Backlog(CapacityLedger capacity) => Declined(MaintenanceReason.DeleteBacklog,
            $"Runnable maintenance demand would take {capacity.DrainSeconds(capacity.Current) / 3600:0.#} hours to drain; new advances wait until it is below {CapacityLedger.ReopenSeconds / 3600:0} hours");

        internal static AdvanceEndedException NoWork(MaintenanceReason reason, string detail, BuildDemand? demand = null) =>
            new AdvanceEndedException(new CleanupResult(OutcomeKind.NoWork, reason, detail) { Demand = demand });

        // A short admission promotes once; a long one declines
        internal static AdvanceEndedException Promote(CleanupContext context, string detail) =>
            context.Long ? Declined(MaintenanceReason.Transient, detail) : new AdvanceEndedException(CleanupResult.Promote);

        internal static void StopIfIssuanceStopped(CleanupContext context)
        {
            if (context.CheckStop() is var stop && stop != IssuanceStop.None)
            {
                throw new CleanupPreemptedException(stop);
            }
        }

        /// <summary>
        /// Counts one object read after the issuance check; a short admission whose reads ran out promotes.
        /// </summary>
        internal static void CountRead(CleanupContext context)
        {
            StopIfIssuanceStopped(context);
            if (!context.TryCountRead())
            {
                throw Promote(context, $"The advance needs more than the {context.ReadLimit} reads of this admission");
            }
        }

        /// <summary>
        /// A counted probe of a listed checkpoint without the root cache.
        /// </summary>
        internal static async Task<ProbedCheckpoint> ProbeAsync(ICheckpointTransport transport, LogTransactionFile file, CleanupContext context)
        {
            CountRead(context);
            var probed = await CheckpointProbe.ProbeAsync(transport, context.Scratch, context.Memory, context.Transfer, file, null, context.Stop);
            if (probed.Class == ProbeClass.Unknown)
            {
                throw Declined(MaintenanceReason.Transient, probed.Detail ?? $"Checkpoint {file.Version} changed after the listing");
            }
            return probed;
        }
    }
}
