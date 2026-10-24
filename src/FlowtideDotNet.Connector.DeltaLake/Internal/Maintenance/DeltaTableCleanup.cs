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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Stowage;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// One sink run's non-build work: the discard drain over the run's route, the staged sweep, the daily advance and the orphan sweep, all through the maintenance meter.
    /// </summary>
    internal sealed class DeltaTableCleanup : IMaintenanceCleanup, IDisposable
    {
        // Leak kinds, each warned once per run
        private const int DiscardLeak = 1;
        private const int SweepLeak = 2;

        private readonly CleanupConfiguration _configuration;
        private readonly IFileStorage _storage;
        private readonly TimeProvider _clock;
        private readonly DiscardHandOff? _handOff;
        private readonly DiscardRoute _route;
        private readonly Func<IFileStorage?> _current;
        private readonly DiscardCounters _counters;
        private readonly SweepCounters _sweepCounters;
        private readonly ILogger _logger;
        private readonly string _tableName;
        private readonly StagedSweep _sweep;
        private readonly AdvanceJob _advance;
        private readonly OrphanSweep _orphan;
        private long _warned;
        private int _leaksWarned;
        private Decline? _declined;
        // The daily slot of the latest concluded advance
        private long _advancedSlot = long.MinValue;
        // The daily slot of the latest foreign-head decline
        private long _foreignSlot = long.MinValue;
        // The started advance the delete turns execute, null without one
        private ActiveAdvance? _active;
        // A moving average of the advance's per-delete latency
        private long _latencyTicks = CapacityEstimator.DefaultLatency.Ticks;
        private readonly RetentionCounters _retention;
        // O, A and the newest started or done advance's cutoff in UTC ticks, unknown until this run learns them
        private long _oldest = -1;
        private long _adoption = -1;
        private long _cutoffTicks = long.MinValue;
        // Paths and consecutive failed slices the latest retired advance left, none after a done
        private long _retiredRemaining;
        private int _retiredFailures;
        // The advance state outside the delete turns, as the latest job left it
        private volatile RetentionState _held = RetentionState.Idle;
        // Set by a ManifestSuspended decline until a recovery leaves nothing unresolved
        private volatile bool _manifestSuspended;
        // On the options clock; the first scheduled orphan sweep falls on the grid point after it
        private readonly DateTimeOffset _started;
        // The orphan grid slot of the latest concluded sweep
        private long _orphanSlot = long.MinValue;
        // 1 while a requested orphan sweep has not concluded
        private int _orphanRequested;
        // The registered orphan run the delete turns execute, null without one
        private OrphanRun? _orphanRun;
        // 1 once disposed
        private int _disposed;

        // Storage is the run's meter, handOff is null for an inline run, and current reads the sink's storage now
        public DeltaTableCleanup(CleanupConfiguration configuration, IFileStorage storage, TimeProvider clock, IOPath table, DiscardHandOff? handOff, Func<IFileStorage?> current,
            DiscardCounters counters, ILogger? logger, string tableName, SweepCounters? sweepCounters = null, RetentionCounters? retentionCounters = null)
        {
            _configuration = configuration;
            _storage = storage;
            _clock = clock;
            _handOff = handOff;
            _current = current;
            _counters = counters;
            _sweepCounters = sweepCounters ?? new SweepCounters();
            _retention = retentionCounters ?? new RetentionCounters();
            _logger = logger ?? NullLogger.Instance;
            _tableName = tableName;
            if (handOff != null)
            {
                // Formed below the meter, where the sink formed its own
                _route = DiscardRoute.Of(storage, table);
            }
            Settings = new DeletionRunSettings()
            {
                // The pinned S3 signer is not safe for concurrent requests
                MaxOutstanding = configuration.Store.Store is { Kind: StoreKind.S3, Pinned: true } ? 1 : DeletionRun.MaxOutstanding,
                DeletesDisabled = IssuanceDisabled
            };
            _sweep = new StagedSweep(configuration, storage, clock, Settings) { Settled = _retention.Add };
            _advance = new AdvanceJob(configuration, storage, clock) { HandOff = Adopt, Latency = () => Latency, Hooks = AdvanceHooksForTests, Warn = Warn, Prepared = Prepared, Recovered = Recovered, Counters = _retention };
            _orphan = new OrphanSweep(configuration, storage, clock, _logger, tableName, _retention)
            {
                Warn = Warn,
                Prepared = Prepared,
                Recovered = Recovered,
                Register = RegisterOrphan,
                RunSettings = () => Settings,
                Latency = () => Latency
            };
            _started = clock.GetUtcNow();
            AdvanceOffset = OffsetOf(storage, table);
        }

        // Where in each UTC day the table's advance falls
        internal TimeSpan AdvanceOffset { get; init; }

        internal AdvanceJob AdvanceForTests => _advance;

        internal OrphanSweep OrphanForTests => _orphan;

        /// <summary>
        /// Asks this run for one orphan sweep, joining a request that has not concluded; never throws.
        /// </summary>
        internal OrphanSweepRequestResult RequestOrphanSweep()
        {
            if (_configuration.OptIns.Retention == null || !_configuration.Store.Eligible || !_configuration.Store.Store.DirectoryExclusion || StorageChanged())
            {
                return OrphanSweepRequestResult.Unavailable;
            }
            return Interlocked.CompareExchange(ref _orphanRequested, 1, 0) == 0 ? OrphanSweepRequestResult.Scheduled : OrphanSweepRequestResult.AlreadyPending;
        }

        // Every delete slice of this run uses these; tests lower the drain's limits
        public DeletionRunSettings Settings { get; internal set; }

        // The route this run drains, default for an inline run
        public DiscardRoute Route => _route;

        // Awaited by a drain after it took its handle and read the failed-closed mark, before anything else
        internal Func<CleanupContext, Task>? DrainHookForTests { get; set; }

        // Called with each group a drain drops by a verdict, and that verdict
        internal Action<DiscardGroup, GateVerdict>? DroppedHookForTests { get; set; }

        // Runs before each drain delete is submitted, after its checks passed
        internal Func<DeletionTarget, Task>? BeforeDeleteForTests { get; set; }

        internal StagedSweepReport? LastSweepForTests => _sweep.LastReport;

        internal StagedSweep SweepForTests => _sweep;

        internal ActiveAdvance? ActiveForTests => Volatile.Read(ref _active);

        // Every turn of the started advance passes these
        internal AdvanceHooks AdvanceHooksForTests { get; } = new AdvanceHooks();

        internal AdvancePhaseSlice LogPhaseForTests { get; set; } = AdvanceLogPhase.RunSliceAsync;

        internal AdvancePhaseSlice PayloadPhaseForTests { get; set; } = AdvancePayloadPhase.RunSliceAsync;

        // The advance's per-delete latency, which its demand is priced at
        internal TimeSpan Latency => TimeSpan.FromTicks(Interlocked.Read(ref _latencyTicks));

        // The sink's counts this run adds to
        internal RetentionCounters Counters => _retention;

        // The oldest retained version, -1 while unknown or with retention off
        internal long OldestVersion => _configuration.OptIns.Retention == null ? -1 : Interlocked.Read(ref _oldest);

        // The adoption version, -1 while unknown or with retention off
        internal long AdoptionVersion => _configuration.OptIns.Retention == null ? -1 : Interlocked.Read(ref _adoption);

        // Seconds from the newest started or done advance's cutoff to the cutoff now, -1 while unknown or with retention off
        internal double BacklogSeconds
        {
            get
            {
                var cutoff = Interlocked.Read(ref _cutoffTicks);
                if (_configuration.OptIns.Retention is not { } retention || cutoff == long.MinValue)
                {
                    return -1;
                }
                var now = BoundarySelector.Midnight(_clock.GetUtcNow(), retention) + DateTimeOffset.UnixEpoch.UtcTicks;
                return now > cutoff ? (double)(now - cutoff) / TimeSpan.TicksPerSecond : 0;
            }
        }

        // Paths the started advance has left until its done, else those the latest retired one left
        internal long PayloadRemaining => Volatile.Read(ref _active) switch
        {
            null => Interlocked.Read(ref _retiredRemaining),
            { Phase: AdvancePhase.Done } => 0,
            var active => Remaining(active)
        };

        // Consecutive failed slices at the started advance's position, else those the latest retired one left
        internal int FailedSlices => Volatile.Read(ref _active) switch
        {
            null => Volatile.Read(ref _retiredFailures),
            { Phase: AdvancePhase.Done } => 0,
            var active => active.Failures
        };

        // The started advance's part of the state gauge, else the state the latest job left
        internal RetentionState AdvanceGaugeState => Volatile.Read(ref _active) is { } active ? active.State switch
        {
            AdvanceState.Runnable => RetentionState.Runnable,
            AdvanceState.Paused => RetentionState.Paused,
            AdvanceState.Rebasing => RetentionState.Rebasing,
            _ => RetentionState.Suspended
        } : _held;

        // O and A as each preparation found them
        private void Prepared(PreparedAdvance prepared)
        {
            Interlocked.Exchange(ref _oldest, prepared.O);
            Interlocked.Exchange(ref _adoption, prepared.A);
        }

        // A recovery that leaves nothing unresolved ends a manifest suspension
        private void Recovered(RecoveryResult recovered)
        {
            if (recovered.Unresolved.Count == 0)
            {
                _manifestSuspended = false;
            }
        }

        /// <summary>
        /// Takes a started or resumed advance into the delete turns; the advance job's hand-off.
        /// </summary>
        internal void Adopt(ActiveAdvance active)
        {
            Interlocked.Exchange(ref _oldest, active.Started.O);
            Interlocked.Exchange(ref _adoption, active.Started.A);
            Interlocked.Exchange(ref _cutoffTicks, active.CutoffTicks);
            Volatile.Write(ref _active, active);
        }

        /// <summary>
        /// Takes an orphan sweep's run into the delete turns; publishes before checking for a dispose, and disposes a run it cannot keep.
        /// </summary>
        internal void RegisterOrphan(OrphanRun run)
        {
            run.Warn = Warn;
            run.BeforeDeleteForTests = BeforeDeleteForTests;
            run.Latency = () => Latency;
            run.ObserveLatency = ObserveLatency;
            if (Interlocked.CompareExchange(ref _orphanRun, run, null) != null)
            {
                // Another run is registered
                run.Dispose();
                return;
            }
            // A dispose that ran before the exchange found nothing to release
            if (Volatile.Read(ref _disposed) != 0 && Interlocked.CompareExchange(ref _orphanRun, null, run) == run)
            {
                run.Dispose();
            }
        }

        /// <summary>
        /// Releases a registered orphan run's targets file, charge and lease; a later registration is released at once.
        /// </summary>
        public void Dispose()
        {
            Interlocked.Exchange(ref _disposed, 1);
            Interlocked.Exchange(ref _orphanRun, null)?.Dispose();
        }

        /// <summary>
        /// Moves the latency average toward a slice's per-delete latency, a quarter of the way for a full slice and in proportion to the deletes counted for a smaller one.
        /// </summary>
        internal void ObserveLatency(TimeSpan perDelete, int deletes)
        {
            var latency = Interlocked.Read(ref _latencyTicks);
            Interlocked.Exchange(ref _latencyTicks, latency + (perDelete.Ticks - latency) / (4L * Settings.MaxPaths) * deletes);
        }

        /// <summary>
        /// Only a queued run's delete turn has work of its own, while its route holds hints; the worker asks IsDue instead; lock-free.
        /// </summary>
        public bool HasWork(JobKind kind)
        {
            return kind == JobKind.Deletion && _handOff != null && _handOff.Find(_route) is { Hints: > 0 };
        }

        /// <summary>
        /// A queued run's delete turn is due once its route's drainable hints fill a turn, the oldest waited the hand-off's spacing, the allowance runs short, or a failed-closed mark waits, and any run's while a started advance runs or pauses or an orphan run is registered; the advance at the run's first chance and after each finished one, then once a day at the table's offset, and while a started advance waits for its rebase; the orphan sweep while no advance is started or held and no orphan run is registered, on request or at each interval grid point after the run's start; lock-free.
        /// </summary>
        public bool IsDue(JobKind kind, long head, DateTimeOffset now)
        {
            var active = Volatile.Read(ref _active);
            if (kind == JobKind.Advance)
            {
                return _configuration.OptIns.Retention != null && (active == null ? SlotOf(now) > Interlocked.Read(ref _advancedSlot) : active.State == AdvanceState.Rebasing) && !StorageChanged();
            }
            if (kind == JobKind.Orphan)
            {
                // Orphan planning defers to an advance held outside the delete turns and to a registered run
                return _configuration.OptIns.Retention != null && active == null && _held == RetentionState.Idle && Volatile.Read(ref _orphanRun) == null && (Volatile.Read(ref _orphanRequested) != 0
                    || (_configuration.OptIns.OrphanSweepInterval != null && OrphanSlotOf(now) > Math.Max(Interlocked.Read(ref _orphanSlot), OrphanSlotOf(_started)))) && !StorageChanged();
            }
            return kind == JobKind.Deletion && ((_handOff != null && _handOff.Find(_route) is { } entry && entry.IsDue(head, now, DeltaMaintenanceWorker.MaxHintsPerTurn))
                || active is { State: AdvanceState.Runnable or AdvanceState.Paused } || Volatile.Read(ref _orphanRun) is { Ended: false });
        }

        public async Task<CleanupResult> RunAsync(JobKind kind, CleanupContext context)
        {
            if (kind == JobKind.Advance)
            {
                if (Volatile.Read(ref _active) is { State: AdvanceState.Rebasing } rebasing)
                {
                    var rebased = await AdvanceRebase.RunAsync(rebasing, ExecutionTurn(), context);
                    Retire(rebasing, context);
                    return Counted(rebased);
                }
                var slot = SlotOf(_clock.GetUtcNow());
                var advanced = await _advance.RunAsync(context);
                // A second foreign head in the slot waits for another day
                if (Concluded(advanced, context) || (advanced.ForeignHead && Interlocked.Exchange(ref _foreignSlot, slot) == slot))
                {
                    Interlocked.Exchange(ref _advancedSlot, slot);
                }
                if (advanced.Kind == OutcomeKind.Declined && advanced.Reason == MaintenanceReason.ManifestSuspended)
                {
                    _manifestSuspended = true;
                }
                if (Volatile.Read(ref _active) == null)
                {
                    _held = Held(context);
                }
                return Counted(advanced);
            }
            if (kind == JobKind.Deletion)
            {
                var drained = new CleanupResult(OutcomeKind.NoWork);
                // A drain that serviced nothing declines its turn, so the worker backs off
                if (Volatile.Read(ref _declined) is { } declined && ReferenceEquals(declined.Context, context) && Interlocked.CompareExchange(ref _declined, null, declined) == declined)
                {
                    drained = new CleanupResult(OutcomeKind.Declined, declined.Reason, declined.Detail);
                }
                if (Volatile.Read(ref _active) is not { } active)
                {
                    if (Volatile.Read(ref _orphanRun) is not { } orphan)
                    {
                        return drained;
                    }
                    // The registered orphan run's slice after the drain
                    var sliced = await orphan.RunSliceAsync(context);
                    if (orphan.Ended)
                    {
                        Interlocked.CompareExchange(ref _orphanRun, null, orphan);
                    }
                    return sliced.Kind == OutcomeKind.NoWork && !sliced.NeedsLongAdmission ? drained : Counted(sliced);
                }
                // The started advance's slice after the drain
                var paused = active.State == AdvanceState.Paused;
                var turned = await AdvanceExecution.RunTurnAsync(active, ExecutionTurn(), LogPhaseForTests, PayloadPhaseForTests, context);
                Retire(active, context);
                // The slice that paused the advance for a changed storage, not its retries
                if (!paused && turned.Reason == MaintenanceReason.DeletesDisabled && StorageChanged())
                {
                    _retention.AddDecline(ListingCheck.StorageChanged);
                }
                return turned.Kind == OutcomeKind.NoWork && !turned.NeedsLongAdmission ? drained : Counted(turned);
            }
            if (kind == JobKind.Orphan)
            {
                // Captured before the run, as the advance's slot is
                var orphanSlot = OrphanSlotOf(_clock.GetUtcNow());
                var swept = await _orphan.RunAsync(context);
                // A deadline the sweep passed without stopping at it still leaves the attempt due
                context.CheckStop();
                if (Concluded(swept, context))
                {
                    // Never back to an earlier grid point after the clock steps back
                    if (orphanSlot > Interlocked.Read(ref _orphanSlot))
                    {
                        Interlocked.Exchange(ref _orphanSlot, orphanSlot);
                    }
                    Interlocked.Exchange(ref _orphanRequested, 0);
                }
                // The advance state the sweep's preparation left
                if (swept.Kind == OutcomeKind.Declined && swept.Reason == MaintenanceReason.ManifestSuspended)
                {
                    _manifestSuspended = true;
                }
                _held = Held(context);
                return Counted(swept);
            }
            if (kind != JobKind.StagedSweep)
            {
                return new CleanupResult(OutcomeKind.NoWork);
            }
            try
            {
                return Counted(await _sweep.RunAsync(context));
            }
            catch (OperationCanceledException) when (context.Stop.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception e)
            {
                return new CleanupResult(OutcomeKind.Failed, MaintenanceReason.Unexpected, $"{e.GetType().Name}: {e.Message}");
            }
            finally
            {
                // Every turn sets its own report, so each is counted once
                SweepLeaked(_sweep.LastReport!);
            }
        }

        /// <summary>
        /// Services at most max hints after one checked listing and one capability replay; never throws except for the worker's stop.
        /// </summary>
        public async Task<DrainResult> DrainHintsAsync(CleanupContext context, int max)
        {
            if (_handOff == null || max <= 0 || _handOff.Find(_route) is not { } entry || !_handOff.TryAcquire(entry, out var handle))
            {
                return default;
            }
            // Held until everything this drain issued settled, so a detached drain keeps the route's hints
            using (handle)
            {
                if (!entry.TryBeginDrain())
                {
                    return default;
                }
                try
                {
                    // Read before the inbox is taken
                    var failedClosed = entry.FailedClosed;
                    if (DrainHookForTests is { } hook)
                    {
                        await hook(context);
                    }
                    return await DrainAsync(entry, failedClosed, context, max);
                }
                finally
                {
                    entry.EndDrain();
                }
            }
        }

        // The decline of the turn whose drain ran with the context
        private sealed record Decline(CleanupContext Context, MaintenanceReason Reason, string Detail);

        // What a turn has left and learned
        private sealed class Turn
        {
            public Turn(int budget, CleanupConfiguration configuration, IFileStorage storage, Func<DeletionTarget, Task>? beforeDelete)
            {
                Budget = budget;
                Executor = new DeletionExecutor(configuration, storage) { BeforeDeleteForTests = beforeDelete, Settled = Record };
            }

            public int Budget;

            // Hints settled or dropped
            public int Serviced;

            // Hints settled through their targets, their rejection or their leak, not dropped
            public bool Progressed;

            // Why the turn declines, the first reason
            public MaintenanceReason Reason;

            public string? Detail;

            public DeletionExecutor Executor { get; }

            // What each delete of the turn returned, by key
            public ConcurrentDictionary<string, DeleteOutcome> Outcomes { get; } = new ConcurrentDictionary<string, DeleteOutcome>(StringComparer.Ordinal);

            // The groups' shares, each concluded once
            public List<Part> Parts { get; set; } = new List<Part>();

            // Groups kept until their own retry, which the turn's end sets
            public List<DiscardGroup> Kept { get; } = new List<DiscardGroup>();

            public void Keep(MaintenanceReason reason, string detail)
            {
                if (Reason == MaintenanceReason.None)
                {
                    Reason = reason;
                    Detail = detail;
                }
            }

            private void Record(DeletionTarget target, DeleteOutcome outcome) => Outcomes[target.Key] = outcome;
        }

        // One group's share of a turn
        private sealed class Part
        {
            public Part(DiscardGroup group, int take)
            {
                Group = group;
                Take = take;
            }

            public DiscardGroup Group { get; }

            public int Take { get; }

            public CleanupEvidence? Evidence;

            // Deletable paths in order, each with its offset among the paths taken
            public List<(int Offset, DeletionTarget Target)> Targets { get; } = new List<(int Offset, DeletionTarget Target)>();

            // Each target's outcome in this turn, null while it is open
            public DeleteOutcome?[]? Outcomes;

            // Indexes into Targets of the part's open targets that the current run holds, in run order
            public List<int> RunTargets { get; } = new List<int>();

            // The current run's position of the part's first target in it
            public long RunStart;

            public bool Dropped;

            public bool Concluded;

            // Targets still open, none once dropped
            public int Open => Dropped || Outcomes == null ? 0 : Outcomes.Count(x => x == null);

            public bool Pending => Open > 0;
        }

        private async Task<DrainResult> DrainAsync(DiscardEntry entry, FailedClosedMark? failedClosed, CleanupContext context, int max)
        {
            var started = _clock.GetTimestamp();
            // An earlier turn's decline no job reported
            Volatile.Write(ref _declined, null);
            var pending = entry.Pending();
            if (StorageChanged())
            {
                var dropped = entry.DropAll();
                // The drain that drops hints, not each later turn
                if (dropped > 0)
                {
                    _retention.AddDecline(ListingCheck.StorageChanged);
                }
                Dropped(dropped, MaintenanceReason.ListingIneligible, "The storage instance changed during the run, so its discard hints cannot drain");
                return new DrainResult(dropped);
            }
            // Drops delete nothing, also while deletes are switched off
            var serviced = DropFailedClosed(entry, pending, failedClosed);
            if (MaintenanceSwitches.DeletesDisabled())
            {
                // Kept for when deletes are switched on again, never deleted inline
                var detail = $"{MaintenanceSwitches.DisableMaintenanceDeletes} is on, discard hints are kept";
                Warn(MaintenanceReason.DeletesDisabled, detail);
                DeclineTurn(entry, context, MaintenanceReason.DeletesDisabled, detail);
                return new DrainResult(serviced);
            }
            var turn = new Turn(max, _configuration, _storage, BeforeDeleteForTests) { Serviced = serviced };
            var promote = false;
            try
            {
                promote = await DrainTurnAsync(entry, pending, context, turn);
            }
            finally
            {
                foreach (var part in turn.Parts)
                {
                    Conclude(entry, part, turn);
                }
                // Each kept group waits at least three times as long as the turn took
                var floor = _clock.GetElapsedTime(started) * 3;
                var now = _clock.GetUtcNow();
                foreach (var group in turn.Kept)
                {
                    group.Keep(now, floor);
                }
                entry.Prune();
                _counters.Add(turn.Executor.Tally);
                _retention.Add(TargetKind.Payload, turn.Executor.Tally);
                if (turn.Reason != MaintenanceReason.None && !turn.Progressed)
                {
                    DeclineTurn(entry, context, turn.Reason, turn.Detail!);
                }
            }
            return promote ? DrainResult.Promote : new DrainResult(turn.Serviced);
        }

        // Reported once to the turn's job, and the route held whole until its next turn
        private void DeclineTurn(DiscardEntry entry, CleanupContext context, MaintenanceReason reason, string detail)
        {
            Volatile.Write(ref _declined, new Decline(context, reason, detail));
            entry.Hold();
        }

        // One listing and one capability replay per turn; true promotes before a counted read
        private async Task<bool> DrainTurnAsync(DiscardEntry entry, IReadOnlyList<DiscardGroup> pending, CleanupContext context, Turn turn)
        {
            // Failed hints not yet due leave the turn without work, so the worker's floor applies
            var now = _clock.GetUtcNow();
            if (!pending.Any(x => x.Due(now, 1) > 0) || context.CheckStop() != IssuanceStop.None)
            {
                return false;
            }
            var log = await ListAsync(entry, context, turn);
            if (log == null)
            {
                return false;
            }
            entry.Listed(log.Head);

            // Kept groups, the earliest retry first
            var kept = pending.Where(x => x.KeptTurns > 0).OrderBy(x => x.ReadyAt).ToList();
            // A quarter of the budget held for the first of them whose retry passed
            var retried = kept.FirstOrDefault(x => x.Version <= log.Head && x.Due(now, 1) > 0);
            var reserved = retried?.Due(now, Math.Max(1, turn.Budget / 4)) ?? 0;
            turn.Budget -= reserved;

            // Due hints of the groups the head reached, in queue order within the budget, kept groups last
            var parts = new List<Part>();
            foreach (var group in pending.Where(x => x.KeptTurns == 0).Concat(kept))
            {
                if (ReferenceEquals(group, retried))
                {
                    turn.Budget += reserved;
                    reserved = 0;
                }
                if (turn.Budget <= 0)
                {
                    if (reserved == 0)
                    {
                        break;
                    }
                    continue;
                }
                if (group.Version > log.Head)
                {
                    continue;
                }
                var take = group.Due(now, turn.Budget);
                if (take == 0)
                {
                    continue;
                }
                turn.Budget -= take;
                parts.Add(new Part(group, take));
            }
            if (parts.Count == 0)
            {
                return false;
            }
            turn.Parts = parts;

            var capability = await CleanupCapability.CheckAsync(_configuration, _storage, log, context, parts.Select(x => x.Group.Version).ToHashSet());
            switch (capability.Check)
            {
                case CapabilityCheck.Capable:
                    break;
                case CapabilityCheck.NeedsLongAdmission:
                case CapabilityCheck.OverReadLimit:
                    if (capability.Check == CapabilityCheck.OverReadLimit || !Rooted(log))
                    {
                        // No usable root, also one over a fixed memory or disk cap
                        DropUnrooted(entry, parts, turn, capability);
                        return false;
                    }
                    // Only before the turn counted a read
                    if (context.ReadsUsed == 0)
                    {
                        return true;
                    }
                    Keep(turn, capability.Reason == MaintenanceReason.None ? MaintenanceReason.Transient : capability.Reason, $"Discard hints are kept: {capability.Check}: {capability.Detail}");
                    return false;
                case CapabilityCheck.Preempted:
                    return false;
                case CapabilityCheck.Unavailable when capability.RootOverCap:
                    DropUnrooted(entry, parts, turn, capability);
                    return false;
                case CapabilityCheck.ReadsSpent:
                case CapabilityCheck.Changed:
                case CapabilityCheck.Unavailable:
                    Keep(turn, capability.Reason == MaintenanceReason.None ? MaintenanceReason.Transient : capability.Reason, $"Discard hints are kept: {capability.Check}: {capability.Detail}");
                    return false;
                default:
                    // The table has no cleanup capability
                    foreach (var part in parts)
                    {
                        DropGroup(entry, part, turn, GateVerdict.Incapable, capability.Reason, $"Discard hints of version {part.Group.Version} are dropped: {capability.Check}: {capability.Detail}");
                    }
                    return false;
            }

            if (!await EstablishAsync(entry, parts, log, capability, context, turn))
            {
                return false;
            }
            var ready = new List<Part>();
            foreach (var part in parts)
            {
                if (part.Evidence == null || part.Dropped)
                {
                    continue;
                }
                for (int i = 0; i < part.Take; i++)
                {
                    var keep = turn.Executor.TryTarget(part.Group.Paths[i], TargetKind.Payload, out var target);
                    if (keep != KeepReason.None)
                    {
                        turn.Executor.Tally.Add(DeleteOutcome.Keep(keep));
                        _retention.AddPathRejection();
                        Warn(MaintenanceReason.PathRejected, $"A discarded output is kept, its name is not deletable here: {keep}");
                        continue;
                    }
                    part.Targets.Add((i, target));
                }
                // Rejected paths settle when the part concludes
                part.Outcomes = new DeleteOutcome?[part.Targets.Count];
                if (part.Pending)
                {
                    ready.Add(part);
                }
            }
            await DeleteAsync(entry, ready, context, turn);
            return false;
        }

        // The marked commit's hints, dropped without a listing, then the mark unless it was replaced; returns the hints dropped
        private int DropFailedClosed(DiscardEntry entry, IReadOnlyList<DiscardGroup> pending, FailedClosedMark? mark)
        {
            if (mark == null)
            {
                return 0;
            }
            var dropped = 0;
            foreach (var group in pending)
            {
                if (group.Version == mark.Version && string.Equals(group.StageId, mark.StageId, StringComparison.Ordinal))
                {
                    dropped += entry.Drop(group);
                }
            }
            entry.ClearFailedClosed(mark);
            if (dropped > 0)
            {
                entry.Prune();
            }
            Dropped(dropped, MaintenanceReason.CleanupGate, $"Discard hints of version {mark.Version} are dropped: its publish failed closed");
            return dropped;
        }

        // A classic checkpoint the listed commits after it connect to the head
        private bool Rooted(LogEvidence log) => new DeltaLogSegment(_storage, _configuration.Store.Table, log.Listing).RootCandidates(log.Head, log.Head).Any();

        // The turn's checked listing; null when the turn ends with it
        private async Task<LogEvidence?> ListAsync(DiscardEntry entry, CleanupContext context, Turn turn)
        {
            ListingCheck check;
            LogEvidence? log;
            try
            {
                (check, log) = await ListingEvidence.ReadLogAsync(_configuration, _storage, context.Head, context.Stop, context.Memory);
            }
            catch (OperationCanceledException) when (context.Stop.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception e)
            {
                Keep(turn, MaintenanceReason.Transient, $"Discard hints are kept, the log listing failed: {e.GetType().Name}: {e.Message}");
                return null;
            }
            _retention.AddListing(check);
            switch (check)
            {
                case ListingCheck.Passed:
                    return log;
                case ListingCheck.StorageChanged:
                    DropAll(entry, turn);
                    return null;
                case ListingCheck.ControlFenced:
                    // Dropped, never deleted: a record this version cannot interpret does not clear by itself
                    var dropped = entry.DropAll();
                    turn.Serviced += dropped;
                    Dropped(dropped, MaintenanceReason.ListingIneligible, "A cleanup control record this version cannot interpret fences deletion, so the table's discard hints are dropped");
                    turn.Keep(MaintenanceReason.ListingIneligible, $"The log listing failed its {check} check");
                    return null;
                default:
                    Keep(turn, MaintenanceReason.ListingIneligible, $"Discard hints are kept, the log listing failed its {check} check");
                    return null;
            }
        }

        // Each group's commit, stage and epoch at the turn's listing; false ends the turn
        private async Task<bool> EstablishAsync(DiscardEntry entry, List<Part> parts, LogEvidence log, CapabilityResult capability, CleanupContext context, Turn turn)
        {
            var proofs = new Dictionary<long, ContentProof>(capability.Proofs);
            foreach (var part in parts)
            {
                var group = part.Group;
                var established = await CleanupEvidence.EstablishAsync(_configuration, _storage, log, group.Version, group.StageId, false, context, capability.Capability!, proofs);
                switch (established.Verdict)
                {
                    case GateVerdict.Passed:
                        break;
                    case GateVerdict.StorageChanged:
                        _retention.AddDecline(ListingCheck.StorageChanged);
                        DropAll(entry, turn);
                        return false;
                    case GateVerdict.NotPublished:
                    case GateVerdict.ListingFailed:
                    case GateVerdict.Unavailable:
                    case GateVerdict.Changed:
                        // This group waits for its own retry; the others still establish
                        KeepGroup(turn, group, established.Reason == MaintenanceReason.None ? MaintenanceReason.Transient : established.Reason, $"Discard hints of version {group.Version} are kept: {established.Detail}");
                        continue;
                    case GateVerdict.NeedsLongAdmission:
                    case GateVerdict.Preempted:
                    case GateVerdict.StaleListing:
                    case GateVerdict.ReadsSpent:
                        // This and the later groups wait for another turn; the earlier ones still drain
                        if (established.Verdict != GateVerdict.Preempted)
                        {
                            Keep(turn, established.Reason == MaintenanceReason.None ? MaintenanceReason.Transient : established.Reason, $"Discard hints of version {group.Version} are kept: {established.Detail}");
                        }
                        return true;
                    default:
                        // Another attempt's or a foreign commit, a missing commit, or another epoch's head
                        DropGroup(entry, part, turn, established.Verdict, established.Reason, $"Discard hints of version {group.Version} are dropped: {established.Verdict}: {established.Detail}");
                        continue;
                }
                var evidence = established.Evidence!;
                var claim = evidence.CheckClaim(group.TableId, group.AdoptedAt);
                if (claim != GateVerdict.Passed)
                {
                    DropGroup(entry, part, turn, claim, MaintenanceReason.CleanupGate, $"Discard hints of version {group.Version} are dropped: their table or epoch is not the table's ({claim})");
                    continue;
                }
                part.Evidence = evidence;
                group.Passed();
                // Later groups at the same versions take these instead of reading again
                proofs.TryAdd(evidence.Anchor.Version, evidence.Anchor.Proof);
                if (established.HeadProof is { } head)
                {
                    proofs.TryAdd(head.Version, head);
                }
            }
            return true;
        }

        // The parts' targets through as few runs as the gates allow; a failed delete holds back no other target
        private async Task DeleteAsync(DiscardEntry entry, List<Part> parts, CleanupContext context, Turn turn)
        {
            var settings = Settings;
            var active = parts;
            while (active.Count > 0)
            {
                var runParts = active;
                var targets = new List<DeletionTarget>();
                foreach (var part in runParts)
                {
                    part.RunStart = targets.Count;
                    part.RunTargets.Clear();
                    for (int i = 0; i < part.Targets.Count; i++)
                    {
                        if (part.Outcomes![i] == null)
                        {
                            part.RunTargets.Add(i);
                            targets.Add(part.Targets[i].Target);
                        }
                    }
                }
                DeletionRun? run = null;
                var gate = ChunkGate.ForEvidences(_configuration, _storage, () => Relied(runParts, run!.Cursor, settings.MaxPaths));
                run = new DeletionRun(turn.Executor, gate, new ListDeletionSource(targets), _clock, settings);
                while (true)
                {
                    var slice = await run.RunSliceAsync(context);
                    Observe(runParts, turn);
                    // An evidence a gate stopped never passes again: its group is dropped without a delete
                    var stopped = false;
                    foreach (var part in runParts)
                    {
                        if (!part.Dropped && part.Evidence!.StoppedBy is { } verdict)
                        {
                            stopped = true;
                            DropGroup(entry, part, turn, verdict, slice.Reason, $"Discard hints of version {part.Group.Version} are dropped: {verdict}: {slice.Detail ?? slice.Gate?.Detail}");
                        }
                    }
                    if (slice.Status == SliceStatus.Completed)
                    {
                        return;
                    }
                    if (slice.Status == SliceStatus.Progressed)
                    {
                        if (context.CheckStop() != IssuanceStop.None)
                        {
                            return;
                        }
                        continue;
                    }
                    if (slice.Status == SliceStatus.Failed)
                    {
                        // The failed hints wait for a later turn, the open targets go on through a new run
                        Keep(turn, MaintenanceReason.DeleteFailed, $"Discard hints whose deletes failed are kept for a later turn: {FirstError(turn)}");
                        break;
                    }
                    if ((slice.Status is SliceStatus.Stopped or SliceStatus.Disabled) && StorageChanged())
                    {
                        _retention.AddDecline(ListingCheck.StorageChanged);
                        DropAll(entry, turn);
                        return;
                    }
                    if (slice.Status == SliceStatus.Disabled)
                    {
                        Keep(turn, MaintenanceReason.DeletesDisabled, $"{MaintenanceSwitches.DisableMaintenanceDeletes} is on, discard hints are kept");
                        return;
                    }
                    if (slice.Status != SliceStatus.Stopped)
                    {
                        if (slice.Stop == IssuanceStop.None && slice.Reason != MaintenanceReason.None)
                        {
                            Keep(turn, slice.Reason, $"Discard hints are kept: {slice.Status}: {slice.Detail ?? slice.Gate?.Detail}");
                        }
                        return;
                    }
                    if (!stopped)
                    {
                        // A stop no evidence explains ends the run's groups
                        foreach (var part in runParts.Where(x => !x.Dropped))
                        {
                            DropGroup(entry, part, turn, slice.Gate?.Verdict ?? GateVerdict.StorageChanged, slice.Reason, $"Discard hints of version {part.Group.Version} are dropped: {slice.Status}: {slice.Detail ?? slice.Gate?.Detail}");
                        }
                    }
                    break;
                }
                // The other targets go on through a new run
                active = runParts.Where(x => x.Pending).ToList();
            }
        }

        // The evidences of the parts whose remaining targets a slice from the cursor can reach
        private static IReadOnlyCollection<CleanupEvidence> Relied(List<Part> runParts, long cursor, int maxPaths)
        {
            var relied = new List<CleanupEvidence>();
            foreach (var part in runParts)
            {
                var end = part.RunStart + part.RunTargets.Count;
                if (!part.Dropped && end > cursor && part.RunStart < cursor + maxPaths)
                {
                    relied.Add(part.Evidence!);
                }
            }
            return relied;
        }

        // What each run target's delete returned, whether or not the run's cursor passed it
        private static void Observe(List<Part> runParts, Turn turn)
        {
            foreach (var part in runParts)
            {
                if (part.Dropped)
                {
                    continue;
                }
                foreach (var index in part.RunTargets)
                {
                    if (part.Outcomes![index] == null && turn.Outcomes.TryGetValue(part.Targets[index].Target.Key, out var outcome))
                    {
                        part.Outcomes[index] = outcome;
                    }
                }
            }
        }

        // Applies the part's outcomes to its group, once
        private void Conclude(DiscardEntry entry, Part part, Turn turn)
        {
            if (part.Concluded || part.Outcomes == null)
            {
                return;
            }
            part.Concluded = true;
            var group = part.Group;
            // Rejected paths have no target and settle
            var states = new HintState[part.Take];
            Array.Fill(states, HintState.Settled);
            var leaked = 0;
            for (int i = 0; i < part.Targets.Count; i++)
            {
                var offset = part.Targets[i].Offset;
                if (part.Outcomes[i] is not { } outcome)
                {
                    states[offset] = HintState.Open;
                }
                else if (outcome.Status == DeleteStatus.Failed && group.FailedTurns(offset) + 1 < DeletionRun.MaxFailedSlices)
                {
                    states[offset] = HintState.Failed;
                }
                else if (outcome.Status == DeleteStatus.Failed)
                {
                    // Failed in the limit's turns: left behind and counted
                    leaked++;
                    turn.Executor.Tally.AddLeaked();
                }
            }
            if (leaked > 0)
            {
                entry.Owner.AddLeaked(leaked);
                WarnLeak(DiscardLeak, MaintenanceReason.DeleteFailed, $"{leaked} discarded outputs of version {group.Version} are left behind, their deletes failed in {DeletionRun.MaxFailedSlices} turns");
            }
            var settled = entry.Conclude(group, states, _clock.GetUtcNow());
            if (settled > 0)
            {
                turn.Serviced += settled;
                turn.Progressed = true;
            }
        }

        // The error of a failed delete of the turn, for the log
        private static string FirstError(Turn turn)
        {
            foreach (var outcome in turn.Outcomes.Values)
            {
                if (outcome.Status == DeleteStatus.Failed)
                {
                    return outcome.Error is { } error ? $"{error.GetType().Name}: {error.Message}" : "the delete failed";
                }
            }
            return "the delete failed";
        }

        // Hints kept for a later turn, which backs off
        private void Keep(Turn turn, MaintenanceReason reason, string detail)
        {
            Warn(reason, detail);
            turn.Keep(reason, detail);
        }

        // One group kept until its own retry, without a decline
        private void KeepGroup(Turn turn, DiscardGroup group, MaintenanceReason reason, string detail)
        {
            Warn(reason, detail);
            turn.Kept.Add(group);
        }

        // Settles what the part concluded before the rest of its group is dropped
        private void DropGroup(DiscardEntry entry, Part part, Turn turn, GateVerdict verdict, MaintenanceReason reason, string detail)
        {
            Conclude(entry, part, turn);
            part.Dropped = true;
            var dropped = entry.Drop(part.Group);
            turn.Serviced += dropped;
            DroppedHookForTests?.Invoke(part.Group, verdict);
            Dropped(dropped, reason == MaintenanceReason.None ? MaintenanceReason.CleanupGate : reason, detail);
        }

        // Drops the taken groups of a table no turn can root
        private void DropUnrooted(DiscardEntry entry, List<Part> parts, Turn turn, CapabilityResult capability)
        {
            foreach (var part in parts)
            {
                DropGroup(entry, part, turn, GateVerdict.NeedsLongAdmission, MaintenanceReason.NoAdmissibleRoot,
                    $"Discard hints of version {part.Group.Version} are dropped: no checkpoint roots a replay within short admission: {capability.Check}: {capability.Detail}");
            }
        }

        private void DropAll(DiscardEntry entry, Turn turn)
        {
            foreach (var part in turn.Parts)
            {
                Conclude(entry, part, turn);
                part.Dropped = true;
            }
            var dropped = entry.DropAll();
            turn.Serviced += dropped;
            Dropped(dropped, MaintenanceReason.ListingIneligible, "The storage instance changed during the run, so its discard hints cannot drain");
        }

        private void Dropped(int hints, MaintenanceReason reason, string detail)
        {
            if (hints > 0)
            {
                _counters.AddDropped(hints);
                Warn(reason, $"{detail} ({hints} hints)");
            }
        }

        // The index of the latest daily offset at or before the time
        private long SlotOf(DateTimeOffset now)
        {
            var since = now.UtcTicks - AdvanceOffset.Ticks;
            return since >= 0 ? since / TimeSpan.TicksPerDay : (since + 1) / TimeSpan.TicksPerDay - 1;
        }

        // The index of the latest orphan interval grid point at or before the time, anchored at the advance's offset; MinValue without an interval
        private long OrphanSlotOf(DateTimeOffset now)
        {
            if (_configuration.OptIns.OrphanSweepInterval is not { } interval)
            {
                return long.MinValue;
            }
            var since = now.UtcTicks - AdvanceOffset.Ticks;
            return since >= 0 ? since / interval.Ticks : (since + 1) / interval.Ticks - 1;
        }

        private AdvanceTurn ExecutionTurn() => new AdvanceTurn(_configuration, _storage, _clock, Settings, AdvanceHooksForTests, Warn) { Latency = () => Latency, ObserveLatency = ObserveLatency, Counters = _retention };

        // A finished or suspended advance leaves the delete turns; a suspension concludes the day, a finished advance asks for the next
        private void Retire(ActiveAdvance active, CleanupContext context)
        {
            if (active.Phase != AdvancePhase.Done && active.State != AdvanceState.Suspended)
            {
                return;
            }
            // A done leaves C the oldest retained version; a suspension keeps its authority
            var done = active.Phase == AdvancePhase.Done;
            if (done)
            {
                Interlocked.Exchange(ref _oldest, active.C);
            }
            // Only the gauges' scalars outlive the advance's turns
            Interlocked.Exchange(ref _retiredRemaining, done ? 0 : Remaining(active));
            Interlocked.Exchange(ref _retiredFailures, done ? 0 : active.Failures);
            _held = Held(context);
            Interlocked.CompareExchange(ref _active, null, active);
            Interlocked.Exchange(ref _advancedSlot, done ? long.MinValue : SlotOf(_clock.GetUtcNow()));
        }

        // Planned paths not yet processed
        private static long Remaining(ActiveAdvance active) => Math.Max(0, active.Started.Totals.Emitted - active.Processed);

        // Outside the delete turns: a manifest suspension, else the most severe of this owner's leases of the table, else idle
        private RetentionState Held(CleanupContext context)
        {
            if (_manifestSuspended)
            {
                return RetentionState.Suspended;
            }
            return context.Capacity?.StateOf(context.CapacityOwner, TableLeases.KeyOf(RawStorage.Unwrap(_storage), _configuration.Store.Table)) switch
            {
                CapacityState.Suspended => RetentionState.Suspended,
                CapacityState.Paused => RetentionState.Paused,
                CapacityState.Runnable => RetentionState.Runnable,
                _ => RetentionState.Idle
            };
        }

        // A result that rejected a path, counted once
        private CleanupResult Counted(CleanupResult result)
        {
            if (result.Reason == MaintenanceReason.PathRejected)
            {
                _retention.AddPathRejection();
            }
            return result;
        }

        // A stable hash of the table's lease key, spread over the day
        private static TimeSpan OffsetOf(IFileStorage storage, IOPath table)
        {
            string key;
            try
            {
                key = TableLeases.KeyOf(RawStorage.Unwrap(storage), table);
            }
            catch
            {
                key = table.Full;
            }
            var hash = SHA256.HashData(Encoding.UTF8.GetBytes(key));
            return TimeSpan.FromTicks((long)(BinaryPrimitives.ReadUInt64LittleEndian(hash) % (ulong)TimeSpan.TicksPerDay));
        }

        // Settled for the day: work done or found absent, or a decline that waits for another day; a foreign head waits for the anchor that restores coverage, and a passed deadline or a deferring decline stays due
        private static bool Concluded(CleanupResult result, CleanupContext context)
        {
            if (result.NeedsLongAdmission || result.ForeignHead || result.Defers || context.Stopped != IssuanceStop.None)
            {
                return false;
            }
            return result.Kind switch
            {
                OutcomeKind.Completed or OutcomeKind.AlreadyValid or OutcomeKind.NoWork => true,
                OutcomeKind.Declined => result.Oversized || result.Reason is not (MaintenanceReason.None or MaintenanceReason.Transient or MaintenanceReason.ListingIneligible or MaintenanceReason.JobDeadline
                    or MaintenanceReason.ManagedMemoryCap or MaintenanceReason.NativeMemoryCap or MaintenanceReason.DiskCap or MaintenanceReason.RetentionNotPublished
                    or MaintenanceReason.DeleteBacklog),
                _ => false
            };
        }

        // The kill switch, or a storage this run must not delete through
        private bool IssuanceDisabled() => MaintenanceSwitches.DeletesDisabled() || StorageChanged();

        // The sink's storage is no longer the instance classified at construction, which this run's route and meter are over
        private bool StorageChanged()
        {
            try
            {
                var current = _current();
                return current == null || _configuration.StorageChanged(current) || (_handOff != null && !ReferenceEquals(RawStorage.Unwrap(current), _route.Raw));
            }
            catch
            {
                return true;
            }
        }

        // The first occurrence of a reason logs at Warning, repeats at Debug
        private void Warn(MaintenanceReason reason, string detail)
        {
            var bit = 1L << ((int)reason & 63);
            var first = (Interlocked.Or(ref _warned, bit) & bit) == 0;
            _logger.Log(first ? LogLevel.Warning : LogLevel.Debug, "Delta table {table} cleanup: {reason}. {detail}", _tableName, reason, detail);
        }

        // The run's first leak of each kind logs at Warning whatever its reason already logged, repeats at Debug
        private void WarnLeak(int kind, MaintenanceReason reason, string detail)
        {
            var first = (Interlocked.Or(ref _leaksWarned, kind) & kind) == 0;
            _logger.Log(first ? LogLevel.Warning : LogLevel.Debug, "Delta table {table} cleanup: {reason}. {detail}", _tableName, reason, detail);
        }

        // What the staged sweep left behind, counted for the sink
        private void SweepLeaked(StagedSweepReport report)
        {
            if (report.PayloadLeaked + report.LeakedStages == 0)
            {
                return;
            }
            _sweepCounters.Add(report);
            var reason = report.LeakReason == MaintenanceReason.None ? MaintenanceReason.DeleteFailed : report.LeakReason;
            WarnLeak(SweepLeak, reason, $"The staged sweep left output behind: {report.PayloadLeaked} owned files, and the output of {report.LeakedStages} dead stages");
        }
    }
}
