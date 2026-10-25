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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.References;
using Microsoft.Extensions.Logging;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// The points after the freeze where a test can act on a running sweep.
    /// </summary>
    internal enum OrphanSweepStep : byte
    {
        Drained,
        SecondListing,
        ThirdListing,
        Protected,
        Emitted
    }

    /// <summary>
    /// One sink run's orphan sweep, promoted to long admission before it makes any request.
    /// </summary>
    internal sealed class OrphanSweep
    {
        // Protection reads one sweep may plan
        internal const long DefaultMaxProtectionReads = 2_000_000;

        // Deletions one sweep may register
        internal const int DefaultMaxDeletions = 100_000;

        // The producer barrier's wait
        internal static readonly TimeSpan BarrierTimeout = TimeSpan.FromSeconds(60);

        // The file whose modification time is T0
        internal const string ReferenceName = ".flowtide-orphan-sweep";
        private const string ReferenceBody = "Flowtide orphan sweep reference";
        private const string LogSegment = "_delta_log";
        private const string ChangeData = "_change_data";
        // Hadoop's empty-folder marker suffix
        private const string PlaceholderSuffix = "$folder$";
        // Estimated managed bytes per remembered nested root, its string and its two slots
        private const long RootStateBytes = 64;
        // Entries between stop checks and charges of the owed resolutions
        private const int CheckEvery = 4096;
        // The root listing's held bytes per entry, low and high, as measured at 10k and 1M entries
        private const long LowEntryBytes = 1000;
        private const long HighEntryBytes = 1700;

        private readonly CleanupConfiguration _configuration;
        private readonly IFileStorage _storage;
        private readonly TimeProvider _clock;
        private readonly ILogger _logger;
        private readonly string _tableName;
        // Set once the run's first root listing was reported
        private int _sizingReported;

        // Storage is the run's meter and clock the options clock
        public OrphanSweep(CleanupConfiguration configuration, IFileStorage storage, TimeProvider clock, ILogger logger, string tableName, RetentionCounters counters)
        {
            _configuration = configuration;
            _storage = storage;
            _clock = clock;
            _logger = logger;
            _tableName = tableName;
            Counters = counters;
        }

        // The table's warn-once log
        internal Action<MaintenanceReason, string>? Warn { get; set; }

        // Sees each preparation's O and A
        internal Action<PreparedAdvance>? Prepared { get; set; }

        // Sees each preparation's recovery
        internal Action<RecoveryResult>? Recovered { get; set; }

        // The sink's counts, which the sweep and recovery's teardown add to
        internal RetentionCounters Counters { get; }

        internal long MaxProtectionReads { get; set; } = DefaultMaxProtectionReads;

        internal int MaxDeletions { get; set; } = DefaultMaxDeletions;

        // The candidate set's arena before it spills
        internal long ArenaCap { get; set; } = PathSet.DefaultArenaCap;

        // Takes each registered run; without it the sweep disposes the run
        internal Action<OrphanRun>? Register { get; set; }

        // The delete settings a registered run starts from
        internal Func<DeletionRunSettings>? RunSettings { get; set; }

        // The per-delete latency a run's demand is priced at
        internal Func<TimeSpan>? Latency { get; set; }

        // Sees each frozen inventory before the sweep disposes it
        internal Action<OrphanInventory>? FrozenHookForTests { get; set; }

        // Awaited at each step after the freeze
        internal Func<OrphanSweepStep, Task>? StepForTests { get; set; }

        // Replaces everything after the promotion
        internal Func<CleanupContext, Task<CleanupResult>>? CoreForTests { get; set; }

        // What preparation established, without its listing
        private readonly record struct Eligibility(long O, long A, string TableId, long ProtectionReads, PathCanonicalizer Identity);

        // The first rule that keeps an entry, in the fixed filter order
        private enum Rule : byte
        {
            Reference,
            Log,
            Nested,
            Hidden,
            UnknownSize,
            Folder,
            NoTime,
            Young,
            Candidate
        }

        /// <summary>
        /// One admitted sweep; never throws except an OperationCanceledException from the context's stop.
        /// </summary>
        public async Task<CleanupResult> RunAsync(CleanupContext context)
        {
            if (_configuration.OptIns.Retention is not { } retention)
            {
                return new CleanupResult(OutcomeKind.NoWork);
            }
            // The sink's current setting, not the meter
            if (_configuration.StorageReplaced())
            {
                Counters.AddDecline(ListingCheck.StorageChanged);
                return new CleanupResult(OutcomeKind.Declined, MaintenanceReason.ListingIneligible, "The sink's storage was replaced, so the orphan sweep does not list it");
            }
            if (MaintenanceSwitches.DeletesDisabled())
            {
                return new CleanupResult(OutcomeKind.Declined, MaintenanceReason.DeletesDisabled, "Maintenance deletes are switched off");
            }
            if (!context.Long)
            {
                return CleanupResult.Promote;
            }
            if (CoreForTests is { } core)
            {
                return await core(context);
            }
            try
            {
                return await SweepAsync(retention, context);
            }
            catch (AdvanceEndedException e)
            {
                // A foreign head concludes for the orphan kind
                return e.Result.ForeignHead ? e.Result with { ForeignHead = false } : e.Result;
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

        // Eligibility, T0 and the inventory, then the barrier, protection and the registered run; the frozen set is disposed on every ending
        private async Task<CleanupResult> SweepAsync(TimeSpan retention, CleanupContext context)
        {
            var formatted = RetentionProperties.Format(retention);
            var eligible = await EligibleAsync(formatted, context);
            AdvanceJob.StopIfIssuanceStopped(context);
            var t0 = await ReferenceAsync(context);
            var cutoff = Cutoff(t0, retention);
            AdvanceJob.StopIfIssuanceStopped(context);
            // Immediately before the root listing
            var vInventory = context.PublishedNow?.Invoke() ?? context.Head;
            using var inventory = await InventoryAsync(eligible, t0, cutoff, vInventory, context);
            FrozenHookForTests?.Invoke(inventory);
            try
            {
                return await ProtectAsync(inventory, formatted, context);
            }
            catch (Exception e)
            {
                await inventory.Identity.RepayOwedAsync(context);
                if (e is CheckpointDeclinedException cap && CleanupCapability.IsCap(cap.Reason))
                {
                    // Concludes, as after the root listing
                    throw new AdvanceEndedException(new CleanupResult(OutcomeKind.Declined, cap.Reason, cap.Message) { Oversized = true });
                }
                throw;
            }
        }

        // The barrier, L2 and L3 each checked with their stages read, the anchor, protection, the subtraction, then the reservation and the registered run
        private async Task<CleanupResult> ProtectAsync(OrphanInventory inventory, string formatted, CleanupContext context)
        {
            var tableKey = TableLeases.KeyOf(RawStorage.Unwrap(_storage), _configuration.Store.Table);
            AdvanceJob.StopIfIssuanceStopped(context);
            await BarrierAsync(tableKey, context);
            // The published floor of both listings
            var vPost = context.PublishedNow?.Invoke() ?? context.Head;
            using var protection = new OrphanProtection(_configuration, _storage, inventory.Identity, context, ArenaCap);
            var l2 = await OrphanProtection.ListAsync(_configuration, _storage, vPost, inventory.O, "second log listing", Counters, context);
            await StepAsync(OrphanSweepStep.SecondListing, context);
            var l2Stages = OrphanProtection.StagesOf(l2);
            CheckReads(l2.Head, inventory.O, l2Stages.Count);
            await protection.ReadStagesAsync(l2Stages, context);
            AdvanceJob.StopIfIssuanceStopped(context);
            var l3 = await OrphanProtection.ListAsync(_configuration, _storage, vPost, inventory.O, "third log listing", Counters, context);
            if (l3.Head < l2.Head)
            {
                Counters.AddDecline(ListingCheck.RangeGap);
                throw AdvanceJob.Declined(MaintenanceReason.ListingIneligible, $"The orphan sweep's third log listing has head {l3.Head}, below the second's {l2.Head}");
            }
            await StepAsync(OrphanSweepStep.ThirdListing, context);
            var l3Stages = OrphanProtection.Unseen(OrphanProtection.StagesOf(l3), l2Stages);
            CheckReads(l3.Head, inventory.O, l2Stages.Count + l3Stages.Count);
            await protection.ReadStagesAsync(l3Stages, context);
            await protection.ResolveAsync(l3, inventory.TableId, context);
            AdvanceJob.StopIfIssuanceStopped(context);
            var evidence = await AnchorAsync(l3, inventory, formatted, context);
            AdvanceJob.StopIfIssuanceStopped(context);
            await protection.BuildAsync(l3, inventory.O, context);
            await StepAsync(OrphanSweepStep.Protected, context);

            using var writer = new OrphanTargetsWriter(context.Scratch, context.Memory);
            var emitted = protection.Emit(inventory.Candidates, new DeletionExecutor(_configuration, _storage), writer, MaxDeletions, Counters, context);
            await StepAsync(OrphanSweepStep.Emitted, context);
            var detail = $"{emitted.Protected} protected, {emitted.Kept} kept, {emitted.OverCap} over the {MaxDeletions} deletion cap";
            if (emitted.Targets == 0)
            {
                return new CleanupResult(OutcomeKind.Completed, MaintenanceReason.None, $"No orphan to delete of {inventory.Counts.Frozen} frozen: {detail}");
            }
            // Read before the reservation and the move out of the job
            var settings = RunSettings?.Invoke() ?? new DeletionRunSettings();
            var lease = Reserve(tableKey, evidence, l3, emitted.Targets, writer.PathBytes, writer.Md5Bytes, context);
            OrphanTargets targets;
            try
            {
                targets = writer.Finish();
            }
            catch
            {
                lease?.Dispose();
                throw;
            }
            OrphanRun? run = new OrphanRun(_configuration, _storage, _clock, settings, evidence, targets, lease, Counters) { Latency = Latency };
            try
            {
                if (Register is { } register)
                {
                    register(run);
                    run = null;
                }
            }
            finally
            {
                // Nothing took it, or taking it failed
                run?.Dispose();
            }
            return new CleanupResult(OutcomeKind.Completed, MaintenanceReason.None, $"{emitted.Targets} orphans registered for deletion, {detail}");
        }

        // Flips the table's producers to a new generation and waits for the old one to drain
        private async Task BarrierAsync(string tableKey, CleanupContext context)
        {
            if (context.Producers is not { } producers)
            {
                throw AdvanceJob.Declined(MaintenanceReason.OrphanNotEligible, "The orphan sweep has no producer generations to drain");
            }
            BarrierOutcome outcome;
            using (var handle = producers.Acquire(tableKey))
            {
                outcome = await handle.Entry.FlipAndDrainAsync(BarrierTimeout, _clock, context.Stop);
            }
            Counters.AddBarrier(outcome);
            switch (outcome)
            {
                case BarrierOutcome.Drained:
                    break;
                case BarrierOutcome.Timeout:
                    throw AdvanceJob.Declined(MaintenanceReason.BarrierTimeout, $"The table's writers of the old generation did not finish within {BarrierTimeout.TotalSeconds:0} seconds");
                case BarrierOutcome.StuckProducer:
                    throw AdvanceJob.Declined(MaintenanceReason.StuckProducer, "A writer of the table from two generations ago has not finished");
                case BarrierOutcome.Busy:
                    throw AdvanceJob.Declined(MaintenanceReason.Transient, "Another barrier on the table's writers is running");
                case BarrierOutcome.Cancelled:
                    throw new OperationCanceledException(context.Stop);
                default:
                    throw AdvanceJob.Declined(MaintenanceReason.OrphanNotEligible, "The table's writer generations are exhausted");
            }
            await StepAsync(OrphanSweepStep.Drained, context);
        }

        // The test's step, then a stop check
        private async Task StepAsync(OrphanSweepStep step, CleanupContext context)
        {
            if (StepForTests is { } hook)
            {
                await hook(step);
            }
            AdvanceJob.StopIfIssuanceStopped(context);
        }

        // The commits from O to the head and the stages, before any of them is read
        private void CheckReads(long head, long oldest, long stages)
        {
            var reads = head - oldest + 1 + stages;
            if (reads > MaxProtectionReads)
            {
                throw AdvanceJob.Declined(MaintenanceReason.OrphanNotEligible, $"Protection would read {reads} commits and stages, more than the {MaxProtectionReads} allowed");
            }
        }

        // The volatile anchor at L3's head, of the inventory's table and epoch, with the policy published
        private async Task<CleanupEvidence> AnchorAsync(LogEvidence l3, OrphanInventory inventory, string formatted, CleanupContext context)
        {
            var established = await CleanupEvidence.EstablishAsync(_configuration, _storage, l3, l3.Head, null, durable: false, context);
            if (established.Cap != MaintenanceReason.None)
            {
                // Concludes as every cap after the root listing does
                throw new CheckpointDeclinedException(established.Cap, DeclineDependency.None, $"The anchor at {l3.Head}: {established.Detail}");
            }
            if (!established.Passed)
            {
                throw AdvancePreparation.Ended(established, context);
            }
            var evidence = established.Evidence!;
            if (evidence.TableId != inventory.TableId || evidence.Anchor.AdoptedAt != inventory.A)
            {
                throw AdvanceJob.Declined(MaintenanceReason.HistoryChanged, $"Head {l3.Head} anchors another table or the adoption at {evidence.Anchor.AdoptedAt}, not the inventory's at {inventory.A}");
            }
            if (!RetentionProperties.Matches(evidence.Capability.Configuration, formatted))
            {
                throw AdvanceJob.Declined(MaintenanceReason.RetentionNotPublished, $"The table does not publish {RetentionProperties.LogKey} and {RetentionProperties.DeletedFileKey} as '{formatted}'");
            }
            return evidence;
        }

        // The run's demand reserved as runnable under its own id when the context has a ledger; a refusal declines without registering, and a run over admission alone concludes
        private CapacityLease? Reserve(string tableKey, CleanupEvidence evidence, LogEvidence l3, long targets, long pathBytes, long md5Bytes, CleanupContext context)
        {
            if (context.Capacity is not { } ledger)
            {
                return null;
            }
            var demand = OrphanRun.Demand(_configuration, evidence, l3.Entries.Count, targets, pathBytes, Latency?.Invoke() ?? CapacityEstimator.DefaultLatency, ledger.RequestsPerSecond, md5Bytes);
            var alone = ledger.DrainSeconds(demand);
            if (alone > CapacityLedger.CloseSeconds)
            {
                var oversized = $"The orphan run with {targets} deletions alone would take {alone / 3600:0.#} hours to drain, more than the {CapacityLedger.CloseSeconds / 3600:0} hours admission allows";
                Warn?.Invoke(MaintenanceReason.DeleteBacklog, oversized);
                throw new AdvanceEndedException(new CleanupResult(OutcomeKind.Declined, MaintenanceReason.DeleteBacklog, oversized) { Oversized = true });
            }
            if (ledger.TryReserve(context.CapacityOwner, OrphanRun.LeaseKeyOf(tableKey), OrphanRun.NextLeaseId(), demand) is { } lease)
            {
                return lease;
            }
            var detail = $"Runnable maintenance demand with {targets} orphan deletions would take {ledger.DrainSeconds(ledger.Current + demand) / 3600:0.#} hours to drain; the orphan run waits until it is below {CapacityLedger.ReopenSeconds / 3600:0} hours";
            Warn?.Invoke(MaintenanceReason.DeleteBacklog, detail);
            throw AdvanceJob.Declined(MaintenanceReason.DeleteBacklog, detail);
        }

        // Preparation reduced to scalars, or the decline that ends the job
        private async Task<Eligibility> EligibleAsync(string formatted, CleanupContext context)
        {
            var prepared = await AdvancePreparation.PrepareAsync(_configuration, _storage, formatted, context, Counters, Recovered);
            Prepared?.Invoke(prepared);
            if (prepared.Resumable is { } resumable)
            {
                throw new AdvanceEndedException(new CleanupResult(OutcomeKind.Declined, MaintenanceReason.OrphanNotEligible,
                    $"The advance to checkpoint {resumable.C} is unresolved, so orphan planning waits for it") { Defers = true });
            }
            if (prepared.O < prepared.A)
            {
                throw AdvanceJob.Declined(MaintenanceReason.OrphanNotEligible, $"The oldest retained version {prepared.O} is below the adoption at {prepared.A}");
            }
            // The root listing needs the commit at O
            if (prepared.FirstJson != prepared.O)
            {
                throw AdvanceJob.Declined(MaintenanceReason.OrphanNotEligible, $"The oldest retained version {prepared.O} is a checkpoint without its commit");
            }
            // Commits below O stay readable through roots the sweep cannot use
            if (prepared.Log.Listing.Commits.Keys.Any(x => x < prepared.O))
            {
                throw AdvanceJob.Declined(MaintenanceReason.OrphanNotEligible, $"Commits are listed below the oldest usable root {prepared.O}");
            }
            var reads = ProtectionReads(prepared);
            if (reads > MaxProtectionReads)
            {
                throw AdvanceJob.Declined(MaintenanceReason.OrphanNotEligible, $"Protection would read {reads} commits and stages, more than the {MaxProtectionReads} allowed");
            }
            var identity = PathCanonicalizer.Create(_configuration.Store, _storage);
            if (identity.Identity == PathIdentity.Unsupported)
            {
                throw AdvanceJob.Declined(MaintenanceReason.OrphanNotEligible, "The table's storage has no path identity to key candidates with");
            }
            if (context.Capacity is { } capacity && !capacity.Admits())
            {
                throw AdvanceJob.Declined(MaintenanceReason.DeleteBacklog,
                    $"Runnable maintenance demand would take {capacity.DrainSeconds(capacity.Current) / 3600:0.#} hours to drain; the orphan sweep waits until it is below {CapacityLedger.ReopenSeconds / 3600:0} hours");
            }
            return new Eligibility(prepared.O, prepared.A, prepared.TableId, reads, identity);
        }

        // The commits from the first JSON to the head and the listed stages
        private static long ProtectionReads(PreparedAdvance prepared)
        {
            var entries = prepared.Log.Entries;
            long stages = 0;
            for (int i = 0; i < entries.Count; i++)
            {
                if (!entries[i].Path.IsFolder && ListingEvidence.TryGetStageVersion(entries[i].Name, out _))
                {
                    stages++;
                }
            }
            return prepared.Log.Head - prepared.FirstJson + 1 + stages;
        }

        // The reference file, overwritten in place, and the modification time its Stat reports
        private async Task<DateTimeOffset> ReferenceAsync(CleanupContext context)
        {
            var reference = _configuration.Store.Table.Combine(ReferenceName);
            IOEntry? entry;
            try
            {
                await _storage.WriteText(reference, ReferenceBody, null, context.Stop);
                AdvanceJob.StopIfIssuanceStopped(context);
                entry = await _storage.Stat(reference, context.Stop);
            }
            catch (OperationCanceledException) when (context.Stop.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception e) when (e is not CleanupPreemptedException)
            {
                throw AdvanceJob.Declined(MaintenanceReason.Transient, $"The orphan sweep's reference file failed: {e.GetType().Name}: {e.Message}");
            }
            return entry?.LastModificationTime ?? throw AdvanceJob.Declined(MaintenanceReason.OrphanNotEligible, $"The orphan sweep's reference file {reference.Full} reports no modification time");
        }

        /// <summary>
        /// T0 less the retention and one hour, saturated at the earliest time.
        /// </summary>
        internal static DateTimeOffset Cutoff(DateTimeOffset t0, TimeSpan retention)
        {
            var room = t0.UtcTicks - DateTimeOffset.MinValue.UtcTicks;
            return retention.Ticks >= room - TimeSpan.TicksPerHour ? DateTimeOffset.MinValue : new DateTimeOffset(t0.UtcTicks - retention.Ticks - TimeSpan.TicksPerHour, TimeSpan.Zero);
        }

        // One uncapped root listing, its checks and the frozen candidates; nothing else of the listing outlives this method
        private async Task<OrphanInventory> InventoryAsync(Eligibility eligible, DateTimeOffset t0, DateTimeOffset cutoff, long vInventory, CleanupContext context)
        {
            var store = _configuration.Store;
            var folder = new IOPath(store.Table.WTS);
            IReadOnlyCollection<IOEntry> delivered;
            try
            {
                delivered = await _storage.Ls(folder, recurse: true, context.Stop);
            }
            catch (OperationCanceledException) when (context.Stop.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception e)
            {
                throw AdvanceJob.Declined(MaintenanceReason.Transient, $"The orphan sweep's root listing failed: {e.GetType().Name}: {e.Message}");
            }
            long listed = delivered.Count;
            Counters.SetOrphanListed(listed);
            ReportSizing(listed);
            AdvanceJob.StopIfIssuanceStopped(context);
            try
            {
                var check = ListingEvidence.Check(store, folder, true, delivered, out var listing, context.Memory);
                if (check != ListingCheck.Passed)
                {
                    Counters.AddDecline(check);
                    throw AdvanceJob.Declined(MaintenanceReason.ListingIneligible, $"The orphan sweep's root listing failed its {check} check");
                }
                var h1 = await CheckLogAsync(listing!, eligible.O, vInventory, context);
                return await FreezeAsync(listing!, eligible, t0, cutoff, vInventory, h1, listed, context);
            }
            catch (CheckpointDeclinedException e) when (CleanupCapability.IsCap(e.Reason))
            {
                // Concludes after the uncapped listing
                throw new AdvanceEndedException(new CleanupResult(OutcomeKind.Declined, e.Reason, e.Message) { Oversized = true });
            }
        }

        // The main table's log projected from the root listing: checked as its own folder, contiguous from O, at or above V_inventory and without its head's successor; returns H1
        private async Task<long> CheckLogAsync(EvidenceListing listing, long oldest, long vInventory, CleanupContext context)
        {
            var store = _configuration.Store;
            var logFolder = store.Table.Combine(DeltaTransactionReader.DeltaLogDirName);
            var prefix = logFolder.WTS;
            // Direct children only on a native snapshot, as preparation listed the log
            var direct = store.Store.Delivery == ListingDelivery.UnpagedSnapshot;
            var projected = new List<IOEntry>();
            for (int i = 0; i < listing.Count; i++)
            {
                var full = listing[i].Path.Full;
                if (full.Length > prefix.Length && full.StartsWith(prefix, StringComparison.Ordinal) && !(direct && ListingEvidence.IsDescendant(full, prefix.Length)))
                {
                    projected.Add(listing[i]);
                }
            }
            var check = ListingEvidence.Check(store, logFolder, false, projected, out var logListing, context.Memory);
            if (check != ListingCheck.Passed)
            {
                Counters.AddDecline(check);
                throw AdvanceJob.Declined(MaintenanceReason.ListingIneligible, $"The orphan sweep's log projection failed its {check} check");
            }
            var log = LogEvidence.From(logListing!);
            // A head below O holds no commit from O
            check = log.Head < oldest ? ListingCheck.RangeGap : log.CheckRange(oldest, log.Head);
            if (check != ListingCheck.Passed)
            {
                Counters.AddDecline(check);
                throw AdvanceJob.Declined(MaintenanceReason.ListingIneligible, $"The orphan sweep's root listing does not hold every commit from {oldest} to {log.Head}");
            }
            check = await log.CheckCurrentAsync(_storage, store.Table, vInventory, context.Stop);
            Counters.AddListing(check);
            if (check != ListingCheck.Passed)
            {
                throw AdvanceJob.Declined(MaintenanceReason.ListingIneligible, $"The orphan sweep's root listing with head {log.Head} failed its {check} check against version {vInventory}");
            }
            return log.Head;
        }

        // Nested roots from every entry, then the filter into the sealed candidate set; a failure disposes the set and repays what is owed
        private async Task<OrphanInventory> FreezeAsync(EvidenceListing listing, Eligibility eligible, DateTimeOffset t0, DateTimeOffset cutoff, long vInventory, long h1, long listed,
            CleanupContext context)
        {
            var identity = eligible.Identity;
            var start = listing.Folder.Length;
            var kept = new long[(int)Rule.Candidate];
            long unqualified = 0;
            long frozen = 0;
            var roots = new HashSet<string>(StringComparer.Ordinal);
            long rootBytes = 0;
            PathSet? set = null;
            try
            {
                string? previous = null;
                for (int i = 0; i < listing.Count; i++)
                {
                    var full = listing[i].Path.Full;
                    var length = NestedRootLength(full, start);
                    // Consecutive entries of one nested table
                    if (length == 0 || (previous != null && string.CompareOrdinal(full, start, previous, 0, length) == 0 && previous.Length == length))
                    {
                        continue;
                    }
                    var root = full.Substring(start, length);
                    previous = root;
                    if (!roots.Contains(root))
                    {
                        var bytes = RootStateBytes + 2L * length;
                        context.Memory.Reserve(bytes);
                        rootBytes += bytes;
                        roots.Add(root);
                    }
                }
                // Ordinal order for lookups by span
                string[] sorted = [.. roots];
                Array.Sort(sorted, StringComparer.Ordinal);
                set = new PathSet(context.Scratch, context.Memory, identity.Domain, PathRole.Candidate, ArenaCap);
                var azure = _configuration.Store.Store.Kind == StoreKind.Azure;
                var payload = new byte[OrphanCandidatePayload.MaxLength];
                for (int i = 0; i < listing.Count; i++)
                {
                    if (i % CheckEvery == CheckEvery - 1)
                    {
                        AdvanceJob.StopIfIssuanceStopped(context);
                        await identity.ChargeOwedAsync(context.Stop, context);
                    }
                    var entry = listing[i];
                    var rule = Classify(entry, start, sorted, azure, cutoff);
                    if (rule != Rule.Candidate)
                    {
                        kept[(int)rule]++;
                        if (rule == Rule.UnknownSize)
                        {
                            Counters.AddOrphanKept(KeepReason.UnknownSize);
                        }
                        continue;
                    }
                    var key = identity.Name(entry.Path.Full.Substring(start), PathRole.Candidate);
                    if (!key.Accepted)
                    {
                        unqualified++;
                        Counters.AddOrphanKept(KeepReason.Unqualified);
                        continue;
                    }
                    var written = OrphanCandidatePayload.Encode(payload, entry.Size, entry.LastModificationTime!.Value, entry.MD5);
                    set.Add(key.Key!, key.DeletionPath, payload.AsSpan(0, written));
                    frozen++;
                }
                AdvanceJob.StopIfIssuanceStopped(context);
                // Nothing owed at hand-over
                await identity.ChargeOwedAsync(context.Stop, context);
                set.Seal(context.Stop);
                var inventory = new OrphanInventory()
                {
                    Candidates = set,
                    Identity = identity,
                    T0 = t0,
                    Cutoff = cutoff,
                    VInventory = vInventory,
                    H1 = h1,
                    O = eligible.O,
                    A = eligible.A,
                    TableId = eligible.TableId,
                    ProtectionReads = eligible.ProtectionReads,
                    Counts = new OrphanCounts(listed, listing.Count, kept[(int)Rule.Reference], kept[(int)Rule.Log], kept[(int)Rule.Nested], roots.Count, kept[(int)Rule.Hidden],
                        kept[(int)Rule.Folder], kept[(int)Rule.NoTime], kept[(int)Rule.Young], kept[(int)Rule.UnknownSize], unqualified, frozen)
                };
                set = null;
                return inventory;
            }
            catch
            {
                set?.Dispose();
                await identity.RepayOwedAsync(context);
                throw;
            }
            finally
            {
                // After the roots' last use
                context.Memory.Release(rootBytes);
            }
        }

        // The fixed order: reference, log, nested table, hidden, pinned Azure size, folder, time, age
        private static Rule Classify(IOEntry entry, int start, string[] roots, bool azure, DateTimeOffset cutoff)
        {
            var full = entry.Path.Full;
            var relative = full.AsSpan(start);
            if (relative.SequenceEqual(ReferenceName))
            {
                return Rule.Reference;
            }
            if (HasLogSegment(relative))
            {
                return Rule.Log;
            }
            if (roots.Length > 0 && InRoot(relative, roots))
            {
                return Rule.Nested;
            }
            if (IsHidden(relative))
            {
                return Rule.Hidden;
            }
            // The pinned client reports hierarchical-namespace directories as files of size zero
            if (azure && entry.Size is not > 0)
            {
                return Rule.UnknownSize;
            }
            if (entry.Path.IsFolder || relative.Slice(relative.LastIndexOf('/') + 1).EndsWith(PlaceholderSuffix, StringComparison.OrdinalIgnoreCase))
            {
                return Rule.Folder;
            }
            if (entry.LastModificationTime is not { } modified)
            {
                return Rule.NoTime;
            }
            return modified < cutoff ? Rule.Candidate : Rule.Young;
        }

        /// <summary>
        /// The length of the nested table root before the first non-first _delta_log segment, without its slash; 0 without one.
        /// </summary>
        internal static int NestedRootLength(string full, int start)
        {
            var segment = start;
            for (int i = start; i <= full.Length; i++)
            {
                if (i < full.Length && full[i] != '/')
                {
                    continue;
                }
                if (segment > start && full.AsSpan(segment, i - segment).Equals(LogSegment, StringComparison.OrdinalIgnoreCase))
                {
                    return segment - 1 - start;
                }
                segment = i + 1;
            }
            return 0;
        }

        // Whether any segment is _delta_log
        private static bool HasLogSegment(ReadOnlySpan<char> relative)
        {
            while (true)
            {
                var slash = relative.IndexOf('/');
                var segment = slash < 0 ? relative : relative.Slice(0, slash);
                if (segment.Equals(LogSegment, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
                if (slash < 0)
                {
                    return false;
                }
                relative = relative.Slice(slash + 1);
            }
        }

        // Inside a discovered nested root, or the root's own entry with or without its slash
        private static bool InRoot(ReadOnlySpan<char> relative, string[] roots)
        {
            var end = 0;
            while (true)
            {
                var slash = relative.Slice(end).IndexOf('/');
                end = slash < 0 ? relative.Length : end + slash;
                if (Contains(roots, relative.Slice(0, end)))
                {
                    return true;
                }
                if (slash < 0)
                {
                    return false;
                }
                end++;
            }
        }

        // Binary search of names in ordinal order
        private static bool Contains(string[] sorted, ReadOnlySpan<char> name)
        {
            int low = 0;
            int high = sorted.Length - 1;
            while (low <= high)
            {
                var middle = low + ((high - low) >> 1);
                var order = sorted[middle].AsSpan().SequenceCompareTo(name);
                if (order == 0)
                {
                    return true;
                }
                if (order < 0)
                {
                    low = middle + 1;
                }
                else
                {
                    high = middle - 1;
                }
            }
            return false;
        }

        /// <summary>
        /// Whether a segment starts with '.' or '_', other than a first segment that is exactly _change_data.
        /// </summary>
        internal static bool IsHidden(ReadOnlySpan<char> relative)
        {
            var first = true;
            while (!relative.IsEmpty)
            {
                var slash = relative.IndexOf('/');
                var segment = slash < 0 ? relative : relative.Slice(0, slash);
                if (!segment.IsEmpty && segment[0] is '.' or '_' && !(first && segment.SequenceEqual(ChangeData)))
                {
                    return true;
                }
                if (slash < 0)
                {
                    return false;
                }
                relative = relative.Slice(slash + 1);
                first = false;
            }
            return false;
        }

        // Warning on the run's first root listing, Debug after
        private void ReportSizing(long listed)
        {
            var first = Interlocked.Exchange(ref _sizingReported, 1) == 0;
            _logger.Log(first ? LogLevel.Warning : LogLevel.Debug, "Delta table {table} orphan sweep: the root listing returned {count} entries, held as about {low} to {high} MiB while it is checked",
                _tableName, listed, listed * LowEntryBytes >> 20, listed * HighEntryBytes >> 20);
        }
    }
}
