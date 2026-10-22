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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Stowage;
using System.Collections.Frozen;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    internal sealed record MaintenanceWorkerSettings(int Interval, TimeSpan? MaxAge, TimeProvider OptionsClock)
    {
        public DeltaCompressionCodec? Codec { get; init; }

        public bool RebuildLegacyCheckpoints { get; init; }

        // Interval and age triggered builds
        public bool PeriodicEnabled { get; init; } = true;

        // Inspection, repair and builds of any kind; off, the worker builds nothing
        public bool InspectionEnabled { get; init; } = true;

        // How the table's store pages listings, for metering
        public ListingPaging Paging { get; init; }

        // The classified store is eligible for cleanup and still the configured one; off, a torn rebuild also needs every commit from zero
        public bool CleanupEligible { get; init; }

        // The history retention at sink construction; null turns retention off
        public TimeSpan? Retention { get; init; }

        public const long DefaultShortReads = 20_000;

        // Counted reads a short admission allows; a job needing more is promoted to the long gate
        public long ShortReads { get; init; } = DefaultShortReads;

        // On the options clock from admission; cooperative, for cleanup
        public TimeSpan ShortDeadline { get; init; } = TimeSpan.FromMinutes(15);

        // On the options clock from admission; cooperative, for every long job
        public TimeSpan LongDeadline { get; init; } = TimeSpan.FromHours(6);

        // Commit opens a long job keeps ahead of its reader; null derives them from the store and the budget's request rate
        public int? ReadAhead { get; init; }

        // The staged sweep runs at the first hand-off of each sink run, then this long after it completes
        public TimeSpan SweepPeriod { get; init; } = TimeSpan.FromDays(1);
    }

    internal enum OutcomeKind : byte
    {
        Completed,
        AlreadyValid,
        NoWork,
        Declined,
        HeldDecline,
        Failed,
        Cancelled,
        // A due build cancelled or stopped non-build work, which stays due without backoff
        Preempted
    }

    /// <summary>
    /// What a job does; builds come first, then the other kinds in this order.
    /// </summary>
    internal enum JobKind : byte
    {
        // Inspection, latest-head and demand checkpoint builds
        Build,
        // Dead and published stage cleanup, between delete slices
        StagedSweep,
        // A delete turn: discard hints first, then the active deletion slice
        Deletion,
        // Advance planning or resume
        Advance,
        // Orphan sweep planning
        Orphan
    }

    internal enum JobState : byte
    {
        // At the table lease or a gate
        Waiting,
        Executing,
        // Releasing its admission and applying its outcome
        Settling
    }

    /// <summary>
    /// A checkpoint wanted at exactly this version with this root policy; admission, promotion and retry never change it.
    /// </summary>
    internal sealed record BuildDemand(long Target, RootPolicy Root);

    /// <summary>
    /// The sink's acknowledgement that it staged a commit for an anchor generation at a version.
    /// </summary>
    internal sealed record StagedAnchor(long Generation, long Version);

    /// <summary>
    /// What a worker keeps of its last completed build: fixed-size figures, never the history the build listed.
    /// </summary>
    internal sealed record BuildMetrics(double ValidationMs, long SpillBytes, long TransferBytes, BuildMode Mode, bool InheritedGap, long? RootVersion, byte[]? Sha256)
    {
        public static BuildMetrics Of(BuildResult result) =>
            new BuildMetrics(result.ValidationMs, result.SpillBytes, result.TransferBytes, result.Mode, result.Checkpoint?.Marker.Inherited != null, result.RootVersion, result.Checkpoint?.Sha256);
    }

    internal sealed record JobOutcome(OutcomeKind Kind, MaintenanceReason Reason = MaintenanceReason.None, string? Detail = null)
    {
        public BuildResult? Build { get; init; }

        // A cutoff build a cleanup job asks for
        public BuildDemand? Demand { get; init; }

        // The memo that held, for a held decline
        public DeclineMemo? Memo { get; init; }

        public Exception? Error { get; init; }

        // As the cleanup result reported it
        public bool ForeignHead { get; init; }
    }

    /// <summary>
    /// One dispatched job, from dispatch until its final lock.
    /// </summary>
    internal sealed class JobRun : JobAdmission
    {
        private volatile bool _yieldRequested;

        public JobRun(long dispatchTarget, JobKind kind = JobKind.Build, BuildDemand? demand = null)
        {
            DispatchTarget = dispatchTarget;
            Kind = kind;
            Demand = demand;
        }

        public long DispatchTarget { get; }

        public JobKind Kind { get; }

        // Null for a latest-head build
        public BuildDemand? Demand { get; }

        // A build-kind job that only discovers retention coverage
        public bool Discovery { get; init; }

        // Linked to the worker's stop; a due build cancels it while non-build work waits
        public required CancellationTokenSource Cancellation { get; init; }

        public Task? Task { get; set; }

        // The build target: the demand's own, or the head at the first admission
        public long T { get; set; } = -1;

        // The published version at the first admission
        public long ObservedHead { get; set; } = -1;

        public DateTimeOffset AdmittedAt { get; set; }

        public bool Admitted { get; set; }

        // Admitted through the long gate
        public bool Long { get; set; }

        // On the options clock; null for a short build, which its reads bound instead
        public DateTimeOffset? Deadline { get; set; }

        // Changed under the worker's lock
        public JobState State { get; set; }

        // A due build cancelled this non-build job while it waited
        public bool Preempted { get; set; }

        // A due build asks this executing delete-class job to yield
        public bool YieldRequested
        {
            get => _yieldRequested;
            set => _yieldRequested = value;
        }

        public long? Covered { get; set; }

        public JobOutcome? Outcome { get; set; }

        public bool Settled { get; set; }

        public bool Detached { get; set; }

        public JobScratch? Scratch { get; set; }

        // The table id at admission, which the whole job uses
        public string? TableId { get; set; }

        public JobMemory Memory { get; } = new JobMemory();
    }

    /// <summary>
    /// Test points of a job: holds awaited at granted, admitted, inspect, build, cleanup, promoted, release and settle, and faults thrown at dispatch, release, slot and outcome.
    /// </summary>
    internal sealed class WorkerHooks
    {
        public Func<string, Task>? Hold { get; init; }

        public Action<string>? Fault { get; init; }

        public Action<string>? PublishFault { get; init; }
    }

    /// <summary>
    /// One sink run's checkpoint maintenance: arms at the first hand-off, inspects once, then builds when the interval or the age trigger is due.
    /// </summary>
    internal sealed class DeltaMaintenanceWorker : IMaintenanceWorker
    {
        public const int MaxUnsupportedRoots = 64;

        private static readonly TimeSpan s_backoffBase = TimeSpan.FromMinutes(1);
        private static readonly TimeSpan s_backoffCap = TimeSpan.FromMinutes(30);

        // How long a kind that found nothing to act on waits while it still has work
        private static readonly TimeSpan s_noWorkFloor = TimeSpan.FromMinutes(1);

        // How long a retention repair that cannot succeed by retrying stays blocked
        internal static readonly TimeSpan BlockedFor = TimeSpan.FromHours(24);

        private readonly object _sync = new object();
        private readonly DeltaMaintenanceBudget _budget;
        private readonly MaintenanceWorkerSettings _settings;
        private readonly IFileStorage _storage;
        private readonly IOPath _table;
        private readonly string _leaseKey;
        private readonly string _tableName;
        // Commit opens a long job keeps ahead of its reader
        private readonly int _readAhead;
        private readonly ILogger _logger;
        private readonly CancellationTokenSource _cancellation = new CancellationTokenSource();
        private readonly Dictionary<ObjectIdentity, long> _unsupportedRoots = new Dictionary<ObjectIdentity, long>();

        private long _target = -1;
        private bool _stopped;
        private JobRun? _run;
        private Task _lastJob = Task.CompletedTask;
        private bool _inspected;
        private long _lastCompleted = -1;
        private long? _repairDue;
        private DateTimeOffset? _oldestOrigin;
        private DateTimeOffset? _aboveOrigin;
        private int _failures;
        private DateTimeOffset _retryAt = DateTimeOffset.MinValue;
        private DeclineMemo? _memo;
        private long _weakFloor;
        private bool _weakFloorSeeded;
        private string? _factsTableId;
        private readonly ulong[] _warned = new ulong[4];
        private volatile string? _tableId;
        private int _detached;
        private bool _skipped;
        private BuildMetrics? _lastBuild;
        private double _lastDurationMs;
        private BuildDemand? _demand;
        // The target the demand backoff belongs to
        private long? _demandBackoffTarget;
        private int _demandFailures;
        private DateTimeOffset _demandRetryAt = DateTimeOffset.MinValue;
        private DateTimeOffset? _sweepDueAt;
        private ControlFence _fence = ControlFence.None;
        // The anchor channel, lock-free: the outstanding generation or 0, the latest generation issued, and the sink's latest acknowledgement
        private long _anchorWanted;
        private long _anchorGeneration;
        private StagedAnchor? _anchorStaged;
        // Retention coverage: confirmed, a pending repair that holds periodic builds back, and a repair blocked until a time on the options clock
        private bool _covered;
        private bool _repairPending;
        // When the pending repair began, on the options clock
        private DateTimeOffset? _repairSince;
        private DateTimeOffset? _blockedUntil;
        private MaintenanceReason _blockedReason;
        // Discovery runs again after a repair, a transient decline or a table id change
        private bool _rediscover;
        private long _discoveredAt = -1;
        private int _discoveryFailures;
        private DateTimeOffset _discoveryRetryAt = DateTimeOffset.MinValue;
        private string? _retentionTableId;
        private bool _retentionAdmitted;
        private readonly KindSchedule[] _schedules = { new KindSchedule(), new KindSchedule(), new KindSchedule(), new KindSchedule(), new KindSchedule() };

        // Discard hints a delete turn services before its slice
        public const int MaxHintsPerTurn = 128;

        private static readonly JobKind[] s_cleanupOrder = { JobKind.Deletion, JobKind.Advance, JobKind.Orphan };

        // A non-build kind's own retry state, which builds never share
        private sealed class KindSchedule
        {
            public int Failures;

            public DateTimeOffset RetryAt = DateTimeOffset.MinValue;
        }

        public DeltaMaintenanceWorker(DeltaMaintenanceBudget budget, MaintenanceWorkerSettings settings, IFileStorage storage, IOPath table, string tableName, ILogger? logger)
        {
            _budget = budget;
            _settings = settings;
            // Every maintenance call and byte is charged to the process's buckets; the lease key uses the raw storage
            _storage = MeteredFileStorage.Wrap(storage, budget.Buckets, settings.Paging);
            _table = table;
            _tableName = tableName;
            _logger = logger ?? NullLogger.Instance;
            _leaseKey = TableLeases.KeyOf(storage, table);
            _readAhead = settings.ReadAhead ?? CommitReadAhead.Depth(StoreDescriptor.Of(RawStorage.Unwrap(storage)), budget.Settings.RequestsPerSecond);
        }

        internal int ReadAheadForTests => _readAhead;

        internal WorkerHooks? HooksForTests { get; set; }

        // Counts entries into worker code, which the hand-off must never make
        internal long WorkerCodeCallsForTests;

        // The lock a tick must never block on
        internal object SyncForTests => _sync;

        // The scratch folder of the latest job
        internal string? LastJobFolderForTests { get; private set; }

        internal JobOutcome? LastOutcomeForTests { get; private set; }

        // The non-build work this worker schedules; null schedules builds only
        internal IMaintenanceCleanup? Cleanup { get; init; }

        public BuildDemand? PendingDemand
        {
            get
            {
                lock (_sync)
                {
                    return _demand;
                }
            }
        }

        // On the budget clock; null until the first hand-off of this run
        internal DateTimeOffset? SweepDueAt
        {
            get
            {
                lock (_sync)
                {
                    return _sweepDueAt;
                }
            }
        }

        internal int CleanupFailures(JobKind kind)
        {
            lock (_sync)
            {
                return _schedules[(int)kind].Failures;
            }
        }

        // On the options clock
        internal DateTimeOffset CleanupRetryAt(JobKind kind)
        {
            lock (_sync)
            {
                return _schedules[(int)kind].RetryAt;
            }
        }

        internal int DemandFailures
        {
            get
            {
                lock (_sync)
                {
                    return _demandFailures;
                }
            }
        }

        // A marked gap-free lineage reaches the adoption epoch, as the latest discovery found
        internal bool CoverageConfirmed
        {
            get
            {
                lock (_sync)
                {
                    return _covered;
                }
            }
        }

        // An outstanding anchor or repair build holds periodic builds back
        internal bool RepairPending
        {
            get
            {
                lock (_sync)
                {
                    return _repairPending;
                }
            }
        }

        // On the options clock; null while no repair is blocked
        internal (DateTimeOffset Until, MaintenanceReason Reason)? Blocked
        {
            get
            {
                lock (_sync)
                {
                    return _blockedUntil is { } until ? (until, _blockedReason) : null;
                }
            }
        }

        // Seconds the pending retention repair has waited on the options clock, 0 without one
        internal double RepairWaitSeconds
        {
            get
            {
                lock (_sync)
                {
                    return _repairSince is { } since ? Math.Max(0, (_settings.OptionsClock.GetUtcNow() - since).TotalSeconds) : 0;
                }
            }
        }

        // The coverage part of the state gauge: off, a blocked repair, coverage unconfirmed or its repair pending, else idle
        internal RetentionState CoverageState
        {
            get
            {
                if (!RetentionOn)
                {
                    return RetentionState.Off;
                }
                lock (_sync)
                {
                    return _blockedUntil != null ? RetentionState.RepairBlocked : !_covered || _repairPending ? RetentionState.Coverage : RetentionState.Idle;
                }
            }
        }

        // Discovery is gated on retention, an eligible store and inspection
        private bool RetentionOn => _settings.Retention != null && _settings.CleanupEligible && _settings.InspectionEnabled;

        public long Target => Interlocked.Read(ref _target);

        // Published versions after the latest checkpoint, 0 when this worker owes no checkpoint
        public long LagVersions
        {
            get
            {
                var target = Target;
                lock (_sync)
                {
                    return Owes && target >= 0 ? target - Math.Max(_lastCompleted, -1) : 0;
                }
            }
        }

        // A checkpoint is owed by periodic builds, a due repair or a pending demand, never by inspection alone
        private bool Owes => _settings.InspectionEnabled && (_settings.PeriodicEnabled || _repairDue != null || _demand != null);

        public long LastCompleted
        {
            get
            {
                lock (_sync)
                {
                    return _lastCompleted;
                }
            }
        }

        public long? RepairDue
        {
            get
            {
                lock (_sync)
                {
                    return _repairDue;
                }
            }
        }

        public bool Inspected
        {
            get
            {
                lock (_sync)
                {
                    return _inspected;
                }
            }
        }

        public int ConsecutiveFailures
        {
            get
            {
                lock (_sync)
                {
                    return _failures;
                }
            }
        }

        public DateTimeOffset RetryAt
        {
            get
            {
                lock (_sync)
                {
                    return _retryAt;
                }
            }
        }

        public DateTimeOffset? OldestOrigin
        {
            get
            {
                lock (_sync)
                {
                    return _oldestOrigin;
                }
            }
        }

        internal DeclineMemo? Memo
        {
            get
            {
                lock (_sync)
                {
                    return _memo;
                }
            }
        }

        internal IReadOnlyCollection<ObjectIdentity> UnsupportedRoots
        {
            get
            {
                lock (_sync)
                {
                    return _unsupportedRoots.Keys.ToList();
                }
            }
        }

        public bool Skipped
        {
            get
            {
                lock (_sync)
                {
                    return _skipped;
                }
            }
        }

        public int DetachedJobs
        {
            get
            {
                lock (_sync)
                {
                    return _detached;
                }
            }
        }

        public BuildMetrics? LastBuild
        {
            get
            {
                lock (_sync)
                {
                    return _lastBuild;
                }
            }
        }

        internal bool CleanupEligible => _settings.CleanupEligible;

        internal ListingPaging Paging => _settings.Paging;

        // What the control records of the latest inspection or build listing forbid
        public ControlFence Fence
        {
            get
            {
                lock (_sync)
                {
                    return _fence;
                }
            }
        }

        // On the budget clock
        public double OldestUncheckpointedSeconds
        {
            get
            {
                lock (_sync)
                {
                    return _oldestOrigin is { } origin ? Math.Max(0, (_budget.Clock.GetUtcNow() - origin).TotalSeconds) : 0;
                }
            }
        }

        public double LastDurationMs
        {
            get
            {
                lock (_sync)
                {
                    return _lastDurationMs;
                }
            }
        }

        // The dispatched job, or the last one, which completes once its warnings are logged
        internal Task CurrentJob
        {
            get
            {
                lock (_sync)
                {
                    return _run?.Task ?? _lastJob;
                }
            }
        }

        internal bool Idle
        {
            get
            {
                lock (_sync)
                {
                    return _run == null;
                }
            }
        }

        /// <summary>
        /// The hand-off: one InterlockedMax, so the engine thread runs no worker code and allocates nothing.
        /// </summary>
        public void Published(long version)
        {
            var current = Interlocked.Read(ref _target);
            while (version > current)
            {
                var seen = Interlocked.CompareExchange(ref _target, version, current);
                if (seen == current)
                {
                    return;
                }
                current = seen;
            }
        }

        // Advisory: a build without it reads its root remotely and learns the id from the log
        public void SetTableId(string? tableId)
        {
            _tableId = tableId;
        }

        /// <summary>
        /// Asks for a checkpoint at exactly the demand's version and root policy; false while a different demand is pending, above the hand-off, or when the worker builds nothing.
        /// </summary>
        public bool RequestBuild(BuildDemand demand)
        {
            lock (_sync)
            {
                if (_demand != null)
                {
                    return _demand == demand;
                }
                if (!_settings.InspectionEnabled || demand.Target > Interlocked.Read(ref _target))
                {
                    return false;
                }
                SetDemand(demand);
                return true;
            }
        }

        // Under the lock, with the slot empty
        private void SetDemand(BuildDemand demand)
        {
            // The same target keeps its retry time
            if (_demandBackoffTarget != demand.Target)
            {
                _demandBackoffTarget = demand.Target;
                _demandFailures = 0;
                _demandRetryAt = DateTimeOffset.MinValue;
            }
            _demand = demand;
        }

        /// <summary>
        /// The anchor generation the sink should stage a commit for, 0 for none; a volatile read that never takes the lock.
        /// </summary>
        public long AnchorRequest => Volatile.Read(ref _anchorWanted);

        /// <summary>
        /// The sink staged a commit for the generation at the version; never takes the lock, and the worker's read ignores an acknowledgement of an older generation.
        /// </summary>
        public void AnchorStaged(long generation, long version)
        {
            Volatile.Write(ref _anchorStaged, new StagedAnchor(generation, version));
            // Answered: the sink stages nothing more for this generation
            Interlocked.CompareExchange(ref _anchorWanted, 0, generation);
        }

        /// <summary>
        /// Asks the sink for an anchor commit and returns its generation, the outstanding one while it is not staged; worker code only, one caller at a time.
        /// </summary>
        internal long RequestAnchor()
        {
            var wanted = Volatile.Read(ref _anchorWanted);
            if (wanted != 0)
            {
                return wanted;
            }
            var generation = Interlocked.Increment(ref _anchorGeneration);
            Volatile.Write(ref _anchorWanted, generation);
            return generation;
        }

        // The version the latest requested anchor was staged at, null while it is not staged
        internal long? AnchorStagedVersion => Volatile.Read(ref _anchorStaged) is { } staged && staged.Generation == Volatile.Read(ref _anchorGeneration) ? staged.Version : null;

        // Under the lock: asks for an anchor unless one is outstanding or the latest one is staged above what the caller saw
        private void WantAnchor(long seen)
        {
            // The outstanding request is read before the staged version
            if (Volatile.Read(ref _anchorWanted) != 0 || (AnchorStagedVersion is { } staged && staged > seen))
            {
                return;
            }
            RequestAnchor();
        }

        public void Tick(DateTimeOffset now)
        {
            Interlocked.Increment(ref WorkerCodeCallsForTests);
            var t = Interlocked.Read(ref _target);
            if (t < 0 || !Monitor.TryEnter(_sync))
            {
                return;
            }
            Exception? failure = null;
            try
            {
                if (_stopped)
                {
                    return;
                }
                if (Cleanup != null)
                {
                    // The staged sweep is due at the first hand-off of this sink run
                    _sweepDueAt ??= now;
                }
                if (_run != null)
                {
                    if (_run.Kind == JobKind.Build && !_run.Discovery)
                    {
                        // Queued time never dates versions; only versions first seen while the job runs do
                        if (_run.Admitted && t > Math.Max(Math.Max(_run.T, _run.ObservedHead), _lastCompleted) && _aboveOrigin == null)
                        {
                            _aboveOrigin = now;
                        }
                    }
                    else if (_settings.InspectionEnabled)
                    {
                        // Cleanup and discovery date versions as an idle tick does, and a due build preempts queued or delete-class work
                        DateUncovered(t, now);
                        if (_run.Kind != JobKind.Build && (PeriodicDue(t, now) || DemandDue(t)))
                        {
                            Preempt(_run);
                        }
                    }
                    return;
                }
                var kind = JobKind.Build;
                BuildDemand? demand = null;
                var discovery = false;
                var due = false;
                if (_settings.InspectionEnabled)
                {
                    DateUncovered(t, now);
                    // A demand, then a discovery that may hold periodic builds back, then a periodic build
                    if (DemandDue(t))
                    {
                        due = true;
                        demand = _demand;
                    }
                    else if (DiscoveryDue(t))
                    {
                        due = true;
                        discovery = true;
                    }
                    else
                    {
                        due = PeriodicDue(t, now);
                    }
                }
                // Cleanup only after the mandatory inspection, and only when no build is due
                if (!due && Cleanup != null && (_inspected || !_settings.InspectionEnabled) && NextCleanup(t, now) is { } next)
                {
                    due = true;
                    kind = next;
                }
                if (!due)
                {
                    return;
                }
                var run = new JobRun(t, kind, demand) { Discovery = discovery, Cancellation = CancellationTokenSource.CreateLinkedTokenSource(_cancellation.Token) };
                _run = run;
                try
                {
                    HooksForTests?.Fault?.Invoke("dispatch");
                    // The job captures no caller context; inside a suppressed caller this is a no-op
                    using (ExecutionContext.SuppressFlow())
                    {
                        run.Task = Task.Run(() => RunJobAsync(run));
                    }
                }
                catch (Exception e)
                {
                    _run = null;
                    run.Cancellation.Dispose();
                    BackoffFor(run, default);
                    failure = e;
                }
            }
            finally
            {
                Monitor.Exit(_sync);
            }
            if (failure != null)
            {
                Warn(MaintenanceReason.DispatchFailed, failure.Message, failure);
            }
        }

        // The latest-head build: inspection, a dangling hint, the interval or the age trigger, outside its backoff and never past a pending retention repair
        private bool PeriodicDue(long t, DateTimeOffset now)
        {
            if (_repairPending || (_inspected && t <= _lastCompleted && _repairDue == null))
            {
                return false;
            }
            if (_settings.OptionsClock.GetUtcNow() < _retryAt)
            {
                return false;
            }
            return !_inspected
                || _repairDue != null
                || (_settings.PeriodicEnabled && t - Math.Max(_lastCompleted, 0) >= _settings.Interval)
                || (_settings.PeriodicEnabled && _settings.MaxAge is { } maxAge && _oldestOrigin is { } origin && now - origin >= maxAge);
        }

        // After the inspection, at a handed-off target, outside the demand's own backoff
        private bool DemandDue(long t) => _demand != null && _inspected && _demand.Target <= t && _settings.OptionsClock.GetUtcNow() >= _demandRetryAt;

        // After the inspection with no demand pending: a blocked repair expired, a rediscovery is wanted, or a staged anchor was handed off
        private bool DiscoveryDue(long t)
        {
            if (!RetentionOn || !_inspected || _demand != null)
            {
                return false;
            }
            var options = _settings.OptionsClock.GetUtcNow();
            if (options < _discoveryRetryAt)
            {
                return false;
            }
            if (_blockedUntil is { } until)
            {
                return options >= until;
            }
            return _rediscover || (AnchorStagedVersion is { } staged && staged <= t && staged > _discoveredAt);
        }

        // The oldest uncovered version's origin, cleared once nothing is uncovered or nothing is owed
        private void DateUncovered(long t, DateTimeOffset now)
        {
            if (_inspected && ((t <= _lastCompleted && _repairDue == null) || !Owes))
            {
                _oldestOrigin = null;
            }
            else
            {
                _oldestOrigin ??= now;
            }
        }

        // Due cleanup in priority order at the published head: the staged sweep, a delete turn, advance work, orphan planning
        private JobKind? NextCleanup(long t, DateTimeOffset now)
        {
            var options = _settings.OptionsClock.GetUtcNow();
            if (_sweepDueAt is { } sweepDue && sweepDue <= now && options >= _schedules[(int)JobKind.StagedSweep].RetryAt)
            {
                return JobKind.StagedSweep;
            }
            foreach (var kind in s_cleanupOrder)
            {
                // With retention on, an advance needs confirmed coverage and no blocked repair
                if (kind == JobKind.Advance && RetentionOn && (!_covered || _blockedUntil != null))
                {
                    continue;
                }
                if (options >= _schedules[(int)kind].RetryAt && Cleanup!.IsDue(kind, t, options))
                {
                    return kind;
                }
            }
            return null;
        }

        // Cancels waiting cleanup, which rejoins later without backoff, and asks executing delete-class work to yield
        private static void Preempt(JobRun run)
        {
            if (run.State == JobState.Waiting)
            {
                if (!run.Preempted)
                {
                    run.Preempted = true;
                    run.Cancellation.Cancel();
                }
            }
            else if (run.State == JobState.Executing && IsDeleteClass(run.Kind))
            {
                run.YieldRequested = true;
            }
        }

        private static bool IsDeleteClass(JobKind kind) => kind is JobKind.StagedSweep or JobKind.Deletion;

        private static AdmissionClass ClassOf(JobRun run)
        {
            if (IsDeleteClass(run.Kind))
            {
                return run.Long ? AdmissionClass.LongDelete : AdmissionClass.Delete;
            }
            return run.Long ? AdmissionClass.Long : AdmissionClass.Ordinary;
        }

        // None for a short build, which its reads bound
        private DateTimeOffset? DeadlineOf(JobRun run)
        {
            if (run.Kind == JobKind.Build && !run.Long)
            {
                return null;
            }
            return _settings.OptionsClock.GetUtcNow() + (run.Long ? _settings.LongDeadline : _settings.ShortDeadline);
        }

        private async Task RunJobAsync(JobRun run)
        {
            var token = run.Cancellation.Token;
            var clockStarted = _settings.OptionsClock.GetTimestamp();
            try
            {
                while (true)
                {
                    await _budget.AdmitAsync(_leaseKey, run, ClassOf(run), token);
                    await HoldAsync("granted");
                    // The duration a job is judged by starts at admission, not in the queue
                    clockStarted = _settings.OptionsClock.GetTimestamp();
                    lock (_sync)
                    {
                        // Preempted after the grant: the admission goes back in the release below
                        if (_stopped || run.Preempted)
                        {
                            throw new OperationCanceledException(token);
                        }
                        run.State = JobState.Executing;
                        run.Deadline = DeadlineOf(run);
                        if (!run.Admitted)
                        {
                            AdmitFirst(run);
                        }
                    }
                    await HoldAsync("admitted");
                    // A new job retries the pending deletions before it reserves anything
                    run.Scratch = _budget.Scratch.CreateJob();
                    LastJobFolderForTests = run.Scratch.DirectoryPath;
                    foreach (var (reason, detail) in _budget.Scratch.TakeNotes())
                    {
                        Warn(reason, detail, null);
                    }
                    try
                    {
                        if (run.Kind == JobKind.Build)
                        {
                            await BuildJobAsync(run, token);
                        }
                        else
                        {
                            await CleanupJobAsync(run, token);
                        }
                        break;
                    }
                    catch (LongAdmissionNeededException)
                    {
                    }
                    // Promoted: the admission goes back and the same target and policy wait at the long gate
                    run.Scratch?.Dispose();
                    run.Scratch = null;
                    run.Slot?.Dispose();
                    run.Slot = null;
                    run.Lease?.Dispose();
                    run.Lease = null;
                    lock (_sync)
                    {
                        run.Long = true;
                        run.State = JobState.Waiting;
                        // Cleanup asked to yield, or facing a due build, gives way before it waits again
                        var head = Interlocked.Read(ref _target);
                        if (run.Kind != JobKind.Build && (run.YieldRequested || (_settings.InspectionEnabled && (PeriodicDue(head, _budget.Clock.GetUtcNow()) || DemandDue(head)))))
                        {
                            Preempt(run);
                        }
                    }
                    await HoldAsync("promoted");
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested || run.Preempted)
            {
                run.Outcome = new JobOutcome(run.Preempted && !_cancellation.IsCancellationRequested ? OutcomeKind.Preempted : OutcomeKind.Cancelled);
            }
            catch (Exception e)
            {
                run.Outcome = new JobOutcome(OutcomeKind.Failed, ReasonOf(e), e.Message) { Error = e, Build = (e as MaintenanceJobException)?.Build };
            }
            finally
            {
                lock (_sync)
                {
                    run.State = JobState.Settling;
                }
                // Each release step contains its own failure, so settlement and the lease release always happen
                Exception? releaseFailure = null;
                try
                {
                    await HoldAsync("release");
                    HooksForTests?.Fault?.Invoke("release");
                    ReleaseLocal(run);
                }
                catch (Exception e)
                {
                    releaseFailure = e;
                }
                try
                {
                    HooksForTests?.Fault?.Invoke("slot");
                    run.Slot?.Dispose();
                }
                catch (Exception e)
                {
                    releaseFailure ??= e;
                }
                try
                {
                    run.LongSlot?.Dispose();
                }
                catch (Exception e)
                {
                    releaseFailure ??= e;
                }
                try
                {
                    run.Lease?.Dispose();
                }
                catch (Exception e)
                {
                    releaseFailure ??= e;
                }
                try
                {
                    await HoldAsync("settle");
                }
                catch (Exception e)
                {
                    releaseFailure ??= e;
                }
                Settle(run, releaseFailure, _settings.OptionsClock.GetElapsedTime(clockStarted));
            }
        }

        // The first admission fixes the target: the demand's own, or the head now
        private void AdmitFirst(JobRun run)
        {
            run.ObservedHead = Interlocked.Read(ref _target);
            run.T = run.Demand?.Target ?? run.ObservedHead;
            run.AdmittedAt = _budget.Clock.GetUtcNow();
            run.Admitted = true;
            run.TableId = _tableId;
            // A table id change drops the coverage state
            if (RetentionOn && (!_retentionAdmitted || run.TableId != _retentionTableId))
            {
                if (_retentionAdmitted)
                {
                    ForgetCoverage();
                }
                _retentionAdmitted = true;
                _retentionTableId = run.TableId;
            }
            if (run.Kind != JobKind.Build)
            {
                return;
            }
            _aboveOrigin = null;
            // Under the table lease, so every root an earlier worker cached is below the floor and a same-version repair is read fresh
            if (!_weakFloorSeeded)
            {
                _weakFloorSeeded = true;
                _weakFloor = _budget.NextFactSequence();
            }
            // Facts recorded under another table id no longer apply
            if (run.TableId != _factsTableId)
            {
                _memo = null;
                _unsupportedRoots.Clear();
                _factsTableId = run.TableId;
            }
            // Versions first seen while a demand was queued date from its admission
            if (run.Demand != null && run.ObservedHead > _lastCompleted)
            {
                _oldestOrigin ??= run.AdmittedAt;
            }
        }

        private async Task BuildJobAsync(JobRun run, CancellationToken token)
        {
            if (run.Discovery)
            {
                await DiscoveryJobAsync(run, token);
                return;
            }
            bool inspected;
            lock (_sync)
            {
                inspected = _inspected;
            }
            if (!inspected)
            {
                await HoldAsync("inspect");
                var inspection = await InspectAsync(run, token);
                if (inspection.HintKept is { } kept)
                {
                    Warn(MaintenanceReason.CleanupGate, kept, null);
                }
                // Before periodic dispatch, from the inspection's listing unless its rebuild changed the log
                var discovery = RetentionOn ? await DiscoverAsync(run, inspection.Segment, release: false, token) : null;
                lock (_sync)
                {
                    _lastCompleted = inspection.LastCompleted;
                    _inspected = true;
                    _repairDue = inspection.RepairDue;
                    run.Covered = inspection.LastCompleted;
                    DateCoverage(run, inspection.LastCompleted);
                    if (discovery != null)
                    {
                        ApplyDiscovery(discovery);
                    }
                    if (inspection.RepairDeclined is { } declined)
                    {
                        run.Outcome = new JobOutcome(OutcomeKind.Declined, MaintenanceReason.TornRepairDeclined, $"{declined.Reason}: {declined.Detail}");
                    }
                    else if (run.Demand == null && !DueAfterInspection(run))
                    {
                        run.Outcome = new JobOutcome(OutcomeKind.NoWork);
                    }
                }
                if (discovery != null)
                {
                    WarnDiscovery(discovery);
                }
            }

            // A demand is decided by its own build, never by the latest-head memo
            DeclineMemo? memo;
            lock (_sync)
            {
                memo = run.Outcome == null && run.Demand == null ? _memo : null;
            }
            if (memo != null)
            {
                var check = await memo.CheckAsync(_storage, _table, run.T, _settings.OptionsClock.GetUtcNow(), run.Memory, _budget.Transfer, token);
                if (check == MemoCheck.Holds)
                {
                    run.Outcome = new JobOutcome(OutcomeKind.HeldDecline, memo.Reason, memo.Detail) { Memo = memo };
                }
                else
                {
                    lock (_sync)
                    {
                        _memo = null;
                        if (check == MemoCheck.ExpiredWeak)
                        {
                            // Weak facts recorded before now no longer apply, so the attempt reads fresh bytes
                            _weakFloor = _budget.NextFactSequence();
                            foreach (var weak in _unsupportedRoots.Keys.Where(x => !x.Strong).ToList())
                            {
                                _unsupportedRoots.Remove(weak);
                            }
                        }
                    }
                }
            }

            if (run.Outcome == null)
            {
                await HoldAsync("build");
                var result = await BuildAsync(run, run.T, run.Demand?.Root ?? RootPolicy.Auto, token);
                if (result.Status == BuildStatus.NeedsLongAdmission)
                {
                    if (run.Long)
                    {
                        throw new MaintenanceJobException(MaintenanceReason.Unexpected, "A build asked for long admission again");
                    }
                    throw new LongAdmissionNeededException();
                }
                run.Outcome = OutcomeOf(result);
            }
        }

        private async Task CleanupJobAsync(JobRun run, CancellationToken token)
        {
            var cleanup = Cleanup!;
            // Only an advance reads them
            IReadOnlySet<ObjectIdentity> known = FrozenSet<ObjectIdentity>.Empty;
            if (run.Kind == JobKind.Advance)
            {
                lock (_sync)
                {
                    known = _unsupportedRoots.Keys.ToHashSet();
                }
            }
            var context = new CleanupContext(run, _settings.OptionsClock)
            {
                Kind = run.Kind,
                Long = run.Long,
                ReadLimit = run.Long ? long.MaxValue : _settings.ShortReads,
                Deadline = run.Deadline!.Value,
                Head = run.ObservedHead,
                TableId = run.TableId,
                Scratch = run.Scratch!,
                Memory = run.Memory,
                Transfer = _budget.Transfer,
                Stop = token,
                CheckpointInterval = _settings.Interval,
                RebuildLegacyCheckpoints = _settings.RebuildLegacyCheckpoints,
                ReadAhead = run.Long ? _readAhead : 1,
                KnownUnsupported = known,
                Capacity = _budget.Capacity,
                CapacityOwner = _cancellation.Token
            };
            CleanupResult result;
            var drained = 0;
            try
            {
                await HoldAsync("cleanup");
                var drain = run.Kind == JobKind.Deletion ? await cleanup.DrainHintsAsync(context, MaxHintsPerTurn) : default;
                drained = drain.Drained;
                // A drain too large for short admission promotes the whole turn before its slice
                result = drain.NeedsLongAdmission ? CleanupResult.Promote : await cleanup.RunAsync(run.Kind, context);
            }
            finally
            {
                // Submitted operations settle before the admission is released
                await context.SettleAsync();
            }
            if (result.NeedsLongAdmission)
            {
                // A second promotion is a defect
                if (run.Long)
                {
                    throw new MaintenanceJobException(MaintenanceReason.Unexpected, $"{run.Kind} work asked for long admission again");
                }
                throw new LongAdmissionNeededException();
            }
            run.Outcome = (context.Stopped, result.Kind) switch
            {
                (_, OutcomeKind.Failed) => new JobOutcome(result.Kind, result.Reason, result.Detail),
                (IssuanceStop.Deadline, _) => new JobOutcome(OutcomeKind.Declined, MaintenanceReason.JobDeadline, $"{run.Kind} work stopped issuing at its {(run.Long ? "long" : "short")} deadline"),
                (IssuanceStop.BuildDue, _) => new JobOutcome(OutcomeKind.Preempted),
                // Hints the drain serviced are work even without a slice
                (_, OutcomeKind.NoWork) when drained > 0 => new JobOutcome(OutcomeKind.Completed),
                _ => new JobOutcome(result.Kind, result.Reason, result.Detail)
            };
            if (result.Demand != null)
            {
                run.Outcome = run.Outcome with { Demand = result.Demand };
            }
            if (result.ForeignHead)
            {
                run.Outcome = run.Outcome with { ForeignHead = true };
            }
        }

        private async Task HoldAsync(string point)
        {
            if (HooksForTests?.Hold is { } hold)
            {
                await hold(point);
            }
        }

        // Transfer charges, native objects and ledgers are released by the builder; the job's scratch is deleted before its charges go
        private static void ReleaseLocal(JobRun run)
        {
            run.Scratch?.Dispose();
        }

        private void Settle(JobRun run, Exception? releaseFailure, TimeSpan duration)
        {
            Exception? settleFailure = null;
            JobOutcome? outcome = run.Outcome;
            lock (_sync)
            {
                try
                {
                    HooksForTests?.Fault?.Invoke("outcome");
                    if (run.Admitted)
                    {
                        if (run.Kind != JobKind.Build)
                        {
                            ApplyCleanupOutcome(run, duration);
                        }
                        else if (run.Discovery)
                        {
                            ApplyDiscoveryOutcome(run, duration);
                        }
                        else
                        {
                            if (run.Demand != null)
                            {
                                ApplyDemandOutcome(run, Interlocked.Read(ref _target), duration);
                            }
                            else
                            {
                                ApplyOutcome(run, Interlocked.Read(ref _target), duration);
                            }
                            LoseCoverageOnGap(run.Outcome);
                            _lastDurationMs = duration.TotalMilliseconds;
                        }
                    }
                }
                catch (Exception e)
                {
                    // Settlement is never skipped
                    settleFailure = e;
                    BackoffFor(run, default);
                }
                finally
                {
                    // Without the result or the error, which hold the listed history, and with the diagnostic cut as in the memo
                    LastOutcomeForTests = outcome == null ? null : outcome with { Build = null, Error = null, Detail = DeclineMemo.Cut(outcome.Detail) };
                    _aboveOrigin = null;
                    run.Settled = true;
                    // Under the lock a tick cancels it with
                    run.Cancellation.Dispose();
                    if (run.Detached)
                    {
                        _detached--;
                        _budget.AddDetached(-1);
                    }
                    // Awaiting the current job after settlement still waits for the warnings below
                    _lastJob = run.Task ?? Task.CompletedTask;
                    _run = null;
                }
            }
            if (outcome != null && run.Admitted)
            {
                switch (outcome.Kind)
                {
                    case OutcomeKind.Declined:
                    case OutcomeKind.HeldDecline:
                    case OutcomeKind.Failed:
                    // A valid checkpoint whose hint a control record kept
                    case OutcomeKind.AlreadyValid when outcome.Reason == MaintenanceReason.CleanupGate:
                        Warn(outcome.Reason, outcome.Detail, outcome.Error);
                        break;
                }
                if (_settings.MaxAge is { } maxAge && duration > maxAge && outcome.Kind is OutcomeKind.Completed && run.Kind == JobKind.Build)
                {
                    Warn(MaintenanceReason.SlowBuild, $"A checkpoint build took {duration}, longer than the {maxAge} age limit", null);
                }
            }
            if (outcome?.Build?.LegacyGapKept is { } gap)
            {
                Warn(MaintenanceReason.LegacyGapKept, gap, null);
            }
            if (releaseFailure != null)
            {
                Warn(MaintenanceReason.ReleaseFailed, releaseFailure.Message, releaseFailure);
            }
            if (settleFailure != null)
            {
                Warn(MaintenanceReason.Unexpected, settleFailure.Message, settleFailure);
            }
        }

        private void DateCoverage(JobRun run, long covered)
        {
            if (covered >= run.T)
            {
                // Only work above T remains
                _oldestOrigin = null;
            }
            else if (covered >= run.DispatchTarget)
            {
                // (covered, T] was first seen at admission
                _oldestOrigin = run.AdmittedAt;
            }
        }

        private bool DueAfterInspection(JobRun run)
        {
            if (_repairPending)
            {
                return false;
            }
            if (_repairDue != null)
            {
                return true;
            }
            if (run.T <= _lastCompleted || !_settings.PeriodicEnabled)
            {
                return false;
            }
            return run.T - Math.Max(_lastCompleted, 0) >= _settings.Interval
                || (_settings.MaxAge is { } maxAge && _oldestOrigin is { } origin && _budget.Clock.GetUtcNow() - origin >= maxAge);
        }

        private void ApplyOutcome(JobRun run, long t2, TimeSpan duration)
        {
            var outcome = run.Outcome!;
            if (t2 > run.T && _aboveOrigin == null)
            {
                // A publication after the last tick is dated at completion
                _aboveOrigin = _budget.Clock.GetUtcNow();
            }
            if (outcome.Build != null)
            {
                foreach (var stale in outcome.Build.Stale)
                {
                    _unsupportedRoots.Remove(stale);
                }
            }
            var covered = outcome.Kind is OutcomeKind.Completed or OutcomeKind.AlreadyValid ? run.T : (run.Covered ?? -1);
            if (covered >= run.T)
            {
                _oldestOrigin = t2 > run.T ? _aboveOrigin : null;
            }
            else if (covered >= run.DispatchTarget)
            {
                _oldestOrigin = run.AdmittedAt;
            }
            switch (outcome.Kind)
            {
                case OutcomeKind.Completed:
                case OutcomeKind.AlreadyValid:
                    _lastCompleted = Math.Max(_lastCompleted, run.T);
                    _failures = 0;
                    _retryAt = DateTimeOffset.MinValue;
                    _memo = null;
                    _skipped = false;
                    if (_repairDue != null && run.T >= _repairDue)
                    {
                        _repairDue = null;
                    }
                    if (outcome.Build != null)
                    {
                        _lastBuild = BuildMetrics.Of(outcome.Build);
                    }
                    break;
                case OutcomeKind.NoWork:
                    _failures = 0;
                    _retryAt = DateTimeOffset.MinValue;
                    _skipped = false;
                    break;
                case OutcomeKind.Declined:
                    var declineMemo = outcome.Build != null ? DeclineMemo.For(outcome.Build, run.T, _settings.OptionsClock.GetUtcNow()) : null;
                    // A held decline is rechecked without a build, so only one nothing holds is spaced by its duration
                    Backoff(declineMemo == null ? duration : default);
                    _skipped = true;
                    if (declineMemo != null)
                    {
                        _memo = declineMemo;
                    }
                    break;
                case OutcomeKind.HeldDecline:
                    Backoff();
                    _skipped = true;
                    // Keeps the set time, so the weak-identity expiry still fires
                    outcome.Memo!.CheckedThrough = run.T;
                    break;
                case OutcomeKind.Failed:
                    Backoff(duration);
                    _skipped = false;
                    if (outcome.Build?.RootIdentity is { } root && outcome.Reason is MaintenanceReason.RootDecodeFailure or MaintenanceReason.SidecarRoot)
                    {
                        AddUnsupported(root, _budget.NextFactSequence());
                    }
                    break;
                case OutcomeKind.Cancelled:
                    break;
            }
            if (_inspected && !Owes)
            {
                _oldestOrigin = null;
            }
        }

        // Completion at the actual target; the frontier only rises and uncovered versions keep their origin
        private void ApplyDemandOutcome(JobRun run, long t2, TimeSpan duration)
        {
            var outcome = run.Outcome!;
            var coveredAll = outcome.Kind is OutcomeKind.Completed or OutcomeKind.AlreadyValid && run.T >= run.ObservedHead;
            // Versions first seen while it ran stay dated from then unless it covered them
            if (!coveredAll && _aboveOrigin is { } seen)
            {
                _oldestOrigin ??= seen;
            }
            if (outcome.Build != null)
            {
                foreach (var stale in outcome.Build.Stale)
                {
                    _unsupportedRoots.Remove(stale);
                }
            }
            switch (outcome.Kind)
            {
                case OutcomeKind.Completed:
                case OutcomeKind.AlreadyValid:
                    // Covering all it observed, it dates the rest as a latest-head build does
                    if (run.T >= run.ObservedHead)
                    {
                        if (t2 > run.T && _aboveOrigin == null)
                        {
                            // A publication after the last tick is dated at completion
                            _aboveOrigin = _budget.Clock.GetUtcNow();
                        }
                        _oldestOrigin = t2 > run.T ? _aboveOrigin : null;
                    }
                    _lastCompleted = Math.Max(_lastCompleted, run.T);
                    if (_repairDue != null && run.T >= _repairDue)
                    {
                        _repairDue = null;
                    }
                    if (outcome.Build != null)
                    {
                        _lastBuild = BuildMetrics.Of(outcome.Build);
                    }
                    if (_demand == run.Demand)
                    {
                        _demand = null;
                        // A repair keeps its failures until a destination read confirms it
                        if (!_repairPending)
                        {
                            _demandFailures = 0;
                        }
                        _demandRetryAt = DateTimeOffset.MinValue;
                    }
                    // A repair releases periodic builds only once a destination read proves its coverage
                    if (_repairPending)
                    {
                        _rediscover = true;
                        _discoveryRetryAt = DateTimeOffset.MinValue;
                    }
                    break;
                case OutcomeKind.Declined:
                case OutcomeKind.HeldDecline:
                    if (_demand == run.Demand)
                    {
                        _demand = null;
                    }
                    // Only a repair blocks; a cutoff build backs off
                    if (RetentionOn && _repairPending && Lasting(outcome.Reason))
                    {
                        Block(outcome.Reason);
                        break;
                    }
                    // Final for this request; the target keeps its retry time
                    Backoff(ref _demandFailures, ref _demandRetryAt, duration);
                    if (_repairPending)
                    {
                        // A repair that keeps declining blocks once its backoff saturates
                        if (Saturated(_demandFailures))
                        {
                            Block(outcome.Reason);
                            break;
                        }
                        // Otherwise it is discovered again after the backoff
                        _rediscover = true;
                        _discoveryRetryAt = _demandRetryAt;
                    }
                    break;
                case OutcomeKind.Failed:
                    var unsupported = false;
                    if (outcome.Build?.RootIdentity is { } root && outcome.Reason is MaintenanceReason.RootDecodeFailure or MaintenanceReason.SidecarRoot)
                    {
                        AddUnsupported(root, _budget.NextFactSequence());
                        unsupported = true;
                    }
                    Backoff(ref _demandFailures, ref _demandRetryAt, duration);
                    if (RetentionOn && Saturated(_demandFailures))
                    {
                        // A demand that keeps failing is dropped, and a repair blocks
                        _demand = null;
                        if (_repairPending)
                        {
                            Block(outcome.Reason);
                        }
                    }
                    else if (_repairPending && (unsupported || outcome.Reason == MaintenanceReason.TargetNotInLog))
                    {
                        // A repair whose root is unsupported or whose target left the log is discovered again after the backoff
                        _demand = null;
                        _rediscover = true;
                        _discoveryRetryAt = _demandRetryAt;
                    }
                    break;
            }
        }

        // Only the kind's own due and retry state; never build dating, backoff or memo
        private void ApplyCleanupOutcome(JobRun run, TimeSpan duration)
        {
            var schedule = _schedules[(int)run.Kind];
            var kind = run.Outcome!.Kind;
            if (RetentionOn)
            {
                // Republished by the anchor commit's metaData
                if (run.Outcome.Reason == MaintenanceReason.RetentionNotPublished)
                {
                    WantAnchor(run.ObservedHead);
                }
                // A cutoff build at a handed-off version, only over confirmed coverage and into an empty slot
                if (run.Outcome.Demand is { } cutoff && cutoff.Target <= run.ObservedHead && _covered && _blockedUntil == null && _demand == null)
                {
                    SetDemand(cutoff);
                }
                // An advance that found the cutoff's root gapped or a head it cannot establish ends coverage
                if (run.Kind == JobKind.Advance && _covered && (run.Outcome is { Kind: OutcomeKind.NoWork, Reason: MaintenanceReason.NoAdmissibleRoot } || run.Outcome.ForeignHead))
                {
                    _covered = false;
                    _rediscover = true;
                    Backoff(ref _discoveryFailures, ref _discoveryRetryAt, duration);
                }
            }
            if (run.Kind == JobKind.StagedSweep)
            {
                // Due a period after any attempt that ran; a failure backs off and preempted work stays due
                switch (kind)
                {
                    case OutcomeKind.Failed:
                        Backoff(ref schedule.Failures, ref schedule.RetryAt, duration);
                        break;
                    case OutcomeKind.Preempted:
                    case OutcomeKind.Cancelled:
                        break;
                    default:
                        schedule.Failures = 0;
                        schedule.RetryAt = DateTimeOffset.MinValue;
                        _sweepDueAt = _budget.Clock.GetUtcNow() + _settings.SweepPeriod;
                        break;
                }
                return;
            }
            switch (kind)
            {
                case OutcomeKind.Completed:
                case OutcomeKind.AlreadyValid:
                    schedule.Failures = 0;
                    schedule.RetryAt = DateTimeOffset.MinValue;
                    break;
                case OutcomeKind.NoWork:
                    schedule.Failures = 0;
                    // Due work it could not act on waits the floor
                    var options = _settings.OptionsClock.GetUtcNow();
                    schedule.RetryAt = Cleanup!.IsDue(run.Kind, Interlocked.Read(ref _target), options) ? options + s_noWorkFloor : DateTimeOffset.MinValue;
                    break;
                case OutcomeKind.Declined:
                case OutcomeKind.HeldDecline:
                case OutcomeKind.Failed:
                    Backoff(ref schedule.Failures, ref schedule.RetryAt, duration);
                    break;
            }
        }

        // Latest-head builds, the demand and each cleanup kind back off separately
        private void BackoffFor(JobRun run, TimeSpan spent)
        {
            if (run.Kind != JobKind.Build)
            {
                var schedule = _schedules[(int)run.Kind];
                Backoff(ref schedule.Failures, ref schedule.RetryAt, spent);
            }
            else if (run.Discovery)
            {
                Backoff(ref _discoveryFailures, ref _discoveryRetryAt, spent);
            }
            else if (run.Demand != null)
            {
                Backoff(ref _demandFailures, ref _demandRetryAt, spent);
            }
            else
            {
                Backoff(spent);
            }
        }

        private void AddUnsupported(ObjectIdentity identity, long sequence)
        {
            if (!_unsupportedRoots.ContainsKey(identity) && _unsupportedRoots.Count >= MaxUnsupportedRoots)
            {
                _unsupportedRoots.Remove(_unsupportedRoots.MinBy(x => x.Value).Key);
            }
            _unsupportedRoots[identity] = sequence;
        }

        // On the options clock: 1 minute doubling to 30, the exponent capped
        // After a failure or a decline nothing holds, the retry also waits three times as long as the attempt took, so such a table cannot keep a job slot busy
        private void Backoff(TimeSpan spent = default) => Backoff(ref _failures, ref _retryAt, spent);

        private void Backoff(ref int failures, ref DateTimeOffset retryAt, TimeSpan spent)
        {
            failures++;
            var delay = TimeSpan.FromTicks(Math.Max(Math.Min(s_backoffBase.Ticks << Math.Min(failures - 1, 5), s_backoffCap.Ticks), spent.Ticks * 3));
            retryAt = _settings.OptionsClock.GetUtcNow() + delay;
        }

        // The backoff of this many failures has reached its cap
        private static bool Saturated(int failures) => failures > 0 && (s_backoffBase.Ticks << Math.Min(failures - 1, 5)) >= s_backoffCap.Ticks;

        private static JobOutcome OutcomeOf(BuildResult result)
        {
            var kind = result.Status switch
            {
                BuildStatus.Completed => OutcomeKind.Completed,
                BuildStatus.AlreadyValid => OutcomeKind.AlreadyValid,
                BuildStatus.Declined => OutcomeKind.Declined,
                _ => OutcomeKind.Failed
            };
            return new JobOutcome(kind, result.Reason, result.Detail) { Build = result };
        }

        private static MaintenanceReason ReasonOf(Exception e)
        {
            return e switch
            {
                CheckpointDeclinedException declined => declined.Reason,
                MaintenanceJobException job => job.Reason,
                IOException or DeltaVersionNotAvailableException or TimeoutException => MaintenanceReason.Transient,
                _ => MaintenanceReason.Unexpected
            };
        }

        // HintKept: why a control record kept the hint below a valid checkpoint; Segment: the listing it made, null once a rebuild changed the log
        private sealed record Inspection(long LastCompleted, long? RepairDue, BuildResult? RepairDeclined, string? HintKept, DeltaLogSegment? Segment);

        /// <summary>
        /// Classifies the newest checkpoint at or below T once per run; nothing above T is considered.
        /// </summary>
        private async Task<Inspection> InspectAsync(JobRun run, CancellationToken cancellationToken)
        {
            var transport = new StowageCheckpointTransport(_storage, _table);
            var segment = await transport.ListAsync(cancellationToken);
            // Control records come from the same listing, before the hint is trusted or written
            var fence = ControlFence.Of(segment.Listing);
            lock (_sync)
            {
                _fence = fence;
            }
            var hint = await transport.ReadHintAsync(cancellationToken);
            var target = run.T;
            long lastCompleted = -1;
            BuildResult? repairDeclined = null;
            string? hintKept = null;
            var changed = false;
            var checkpoint = segment.Listing.Checkpoints.FirstOrDefault(x => x.Version <= target);
            if (checkpoint != null)
            {
                if (hint != null && hint.Version == checkpoint.Version && hint.SizeInBytes != null && hint.SizeInBytes == checkpoint.IOEntry.Size)
                {
                    // Trusted without a download: the inspection made one listing and one hint read
                    lastCompleted = checkpoint.Version;
                }
                else
                {
                    var inspected = await CheckpointInspector.InspectAsync(transport, run.Scratch!, run.Memory, _budget.Transfer, checkpoint, cancellationToken, CacheScope(run));
                    switch (inspected.Class)
                    {
                        case CheckpointClass.Valid:
                            lastCompleted = checkpoint.Version;
                            // The hint is never lowered, and is repaired at the same version
                            if (CheckpointInspector.HintNeedsWrite(hint, checkpoint.Version, inspected.Rows, inspected.Length))
                            {
                                if (fence.AllowsHint(checkpoint.Version))
                                {
                                    await transport.WriteHintAsync(new LastCheckpointInfo() { Version = checkpoint.Version, Size = inspected.Rows, SizeInBytes = inspected.Length }, cancellationToken);
                                }
                                else
                                {
                                    hintKept = $"The hint stays below checkpoint {checkpoint.Version}: {fence.Detail}";
                                }
                            }
                            break;
                        case CheckpointClass.Unsupported:
                            // Never touched; kernel lists forward to it, so an older hint is not dangling
                            lastCompleted = checkpoint.Version;
                            break;
                        case CheckpointClass.Torn:
                            var rebuilt = await BuildAsync(run, checkpoint.Version, RootPolicy.Auto, cancellationToken);
                            switch (rebuilt.Status)
                            {
                                case BuildStatus.NeedsLongAdmission:
                                    // The whole inspection runs again under long admission
                                    throw new LongAdmissionNeededException();
                                case BuildStatus.Completed:
                                case BuildStatus.AlreadyValid:
                                    lastCompleted = checkpoint.Version;
                                    changed = true;
                                    break;
                                case BuildStatus.Declined:
                                    // A later build above the torn checkpoint can still supersede it
                                    lastCompleted = rebuilt.NewestValidRoot ?? -1;
                                    repairDeclined = rebuilt;
                                    break;
                                default:
                                    // A failure fails the inspection, so the next attempt inspects again; a root it found unsupported is recorded
                                    throw new MaintenanceJobException(rebuilt.Reason, rebuilt.Detail) { Build = rebuilt };
                            }
                            break;
                        default:
                            throw new MaintenanceJobException(MaintenanceReason.UnknownCheckpoint, inspected.Detail);
                    }
                }
            }
            // Kernel fails on a hint with no complete checkpoint at or after it, so that hint is due for repair
            long? repairDue = CheckpointInspector.IsDangling(hint, segment, target) ? hint!.Version : null;
            return new Inspection(lastCompleted, repairDue, repairDeclined, hintKept, changed ? null : segment);
        }

        // What a fetch may read from the cache; nothing without a table id
        private RootCacheScope? CacheScope(JobRun run)
        {
            lock (_sync)
            {
                return run.TableId != null ? new RootCacheScope(_budget.Scratch.Cache, _storage, run.TableId, _weakFloor) : null;
            }
        }

        // One listing and a discovery over it; a pending repair is released only by a destination read
        private async Task DiscoveryJobAsync(JobRun run, CancellationToken token)
        {
            bool release;
            lock (_sync)
            {
                release = _repairPending;
            }
            var discovery = await DiscoverAsync(run, null, release, token);
            bool blocked;
            lock (_sync)
            {
                blocked = ApplyDiscovery(discovery);
            }
            if (blocked)
            {
                Warn(MaintenanceReason.HistoryChanged, discovery.Kind == DiscoveryKind.Anchor
                    ? $"Another writer kept committing above the anchor commits; the retention repair waits {BlockedFor}"
                    : $"Repair builds kept going unconfirmed as the table changed; the retention repair waits {BlockedFor}", null);
            }
            run.Outcome = discovery.Kind is DiscoveryKind.Declined or DiscoveryKind.Undecided
                ? new JobOutcome(OutcomeKind.Declined, discovery.Reason, discovery.Detail)
                : new JobOutcome(OutcomeKind.NoWork);
        }

        // Throws only when the worker stops or the reads need long admission; a failed read leaves the discovery undecided
        private async Task<Discovery> DiscoverAsync(JobRun run, DeltaLogSegment? segment, bool release, CancellationToken token)
        {
            try
            {
                segment ??= await new StowageCheckpointTransport(_storage, _table).ListAsync(token);
                return await CoverageDiscovery.DiscoverAsync(segment, run.ObservedHead, release, _settings.RebuildLegacyCheckpoints, new CoverageReads(this, run, token));
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                throw;
            }
            catch (LongAdmissionNeededException)
            {
                throw;
            }
            catch (Exception e)
            {
                return new Discovery(DiscoveryKind.Undecided, run.ObservedHead, Reason: ReasonOf(e), Detail: e.Message);
            }
        }

        // Under the lock: a decided discovery replaces the coverage state, an undecided one runs again after a backoff; true when a repair that stayed unconfirmed blocked
        private bool ApplyDiscovery(Discovery discovery)
        {
            if (discovery.Kind == DiscoveryKind.Undecided)
            {
                _rediscover = true;
                Backoff(ref _discoveryFailures, ref _discoveryRetryAt, default);
                // A pending repair whose discovery stays undecided blocks once that backoff saturates
                if (_repairPending && Saturated(_discoveryFailures))
                {
                    Block(discovery.Reason);
                }
                return false;
            }
            // Asked for again, as after a repair build, not brought by a handed-off anchor
            var again = _rediscover;
            _rediscover = false;
            _discoveredAt = Math.Max(_discoveredAt, discovery.V);
            _discoveryFailures = 0;
            _discoveryRetryAt = DateTimeOffset.MinValue;
            _blockedUntil = null;
            // A pending repair starts without the failures or backoff of an earlier demand
            if (!_repairPending && discovery.Kind is DiscoveryKind.Anchor or DiscoveryKind.Repair)
            {
                _demandFailures = 0;
                _demandRetryAt = DateTimeOffset.MinValue;
            }
            switch (discovery.Kind)
            {
                case DiscoveryKind.Covered:
                    // A confirmed repair's failures end with it
                    if (_repairPending)
                    {
                        _demandFailures = 0;
                    }
                    _covered = true;
                    _repairPending = false;
                    _repairSince = null;
                    break;
                case DiscoveryKind.Anchor:
                    _covered = false;
                    // A repair that keeps needing anchors past their backoff blocks as a failing one does
                    if (Unconfirmed(discovery.V))
                    {
                        return true;
                    }
                    _repairPending = true;
                    _repairSince ??= _settings.OptionsClock.GetUtcNow();
                    WantAnchor(discovery.V);
                    break;
                case DiscoveryKind.Repair:
                    // A completed repair build its confirmation found unconfirmed counts as the anchor arm does; a failed one counted already
                    if (again && _demandRetryAt == DateTimeOffset.MinValue && Unconfirmed(discovery.V))
                    {
                        return true;
                    }
                    // Discovered again while pending, the repair keeps the failures and backoff of its earlier targets
                    var failures = _repairPending ? _demandFailures : 0;
                    var retryAt = _repairPending ? _demandRetryAt : DateTimeOffset.MinValue;
                    _covered = false;
                    _repairPending = true;
                    _repairSince ??= _settings.OptionsClock.GetUtcNow();
                    if (_demand == null)
                    {
                        SetDemand(discovery.Demand!);
                        _demandFailures = Math.Max(_demandFailures, failures);
                        _demandRetryAt = retryAt > _demandRetryAt ? retryAt : _demandRetryAt;
                    }
                    else if (_demand != discovery.Demand)
                    {
                        // With the slot taken, discovered again once it settles
                        _rediscover = true;
                    }
                    break;
                case DiscoveryKind.Declined:
                    _covered = false;
                    Block(discovery.Reason);
                    break;
            }
            return false;
        }

        // Under the lock: a pending repair found unconfirmed past its backoff counts once; true when that blocks it
        private bool Unconfirmed(long v)
        {
            // A race lost at this target already counted
            var counted = _demandBackoffTarget == v && _demandRetryAt != DateTimeOffset.MinValue;
            if (!_repairPending || _settings.OptionsClock.GetUtcNow() < _demandRetryAt || counted)
            {
                return false;
            }
            Backoff(ref _demandFailures, ref _demandRetryAt, default);
            if (!Saturated(_demandFailures))
            {
                return false;
            }
            Block(MaintenanceReason.HistoryChanged);
            return true;
        }

        // A discovery job's findings were applied in the job; only a failure that escaped it backs off
        private void ApplyDiscoveryOutcome(JobRun run, TimeSpan duration)
        {
            if (run.Outcome!.Kind == OutcomeKind.Failed)
            {
                _rediscover = true;
                Backoff(ref _discoveryFailures, ref _discoveryRetryAt, duration);
            }
        }

        // Under the lock: a built checkpoint that inherited a gap ends confirmed coverage, which is discovered again
        private void LoseCoverageOnGap(JobOutcome? outcome)
        {
            if (_covered && outcome is { Kind: OutcomeKind.Completed, Build.Checkpoint.Marker.Inherited: not null })
            {
                _covered = false;
                _rediscover = true;
            }
        }

        // Under the lock: a repair a retry cannot fix waits a day, and periodic builds resume meanwhile
        private void Block(MaintenanceReason reason)
        {
            _blockedUntil = _settings.OptionsClock.GetUtcNow() + BlockedFor;
            _blockedReason = reason;
            _repairPending = false;
            _repairSince = null;
        }

        // Under the lock: the demand, anchor, block and coverage of another table id are dropped and discovered again
        private void ForgetCoverage()
        {
            _demand = null;
            _demandBackoffTarget = null;
            _demandFailures = 0;
            _demandRetryAt = DateTimeOffset.MinValue;
            _covered = false;
            _repairPending = false;
            _repairSince = null;
            _blockedUntil = null;
            _discoveryFailures = 0;
            _discoveryRetryAt = DateTimeOffset.MinValue;
            _rediscover = _inspected;
            Interlocked.Exchange(ref _anchorWanted, 0);
            Volatile.Write(ref _anchorStaged, null);
        }

        // Declines a retry can resolve: races, transient errors and resource caps; a long build's deadline is not one
        private static bool Lasting(MaintenanceReason reason) => reason is not (MaintenanceReason.CheckpointExists or MaintenanceReason.TargetNotInLog
            or MaintenanceReason.Transient or MaintenanceReason.Unexpected
            or MaintenanceReason.ManagedMemoryCap or MaintenanceReason.NativeMemoryCap or MaintenanceReason.DiskCap);

        private void WarnDiscovery(Discovery discovery)
        {
            if (discovery.Kind is DiscoveryKind.Declined or DiscoveryKind.Undecided)
            {
                Warn(discovery.Reason, discovery.Detail, null);
            }
        }

        /// <summary>
        /// A discovery's reads, each one counted: strict commit headers, the Auto walk with the worker's unsupported roots, and uncached destination reads.
        /// </summary>
        private sealed class CoverageReads : ICoverageReads
        {
            private readonly DeltaMaintenanceWorker _worker;
            private readonly JobRun _run;
            private readonly CancellationToken _token;
            private readonly IReadOnlySet<ObjectIdentity> _known;
            private readonly RootCacheScope? _cache;
            private long _reads;

            public CoverageReads(DeltaMaintenanceWorker worker, JobRun run, CancellationToken token)
            {
                _worker = worker;
                _run = run;
                _token = token;
                lock (worker._sync)
                {
                    _known = worker._unsupportedRoots.Keys.ToHashSet();
                }
                _cache = worker.CacheScope(run);
            }

            public async Task<CommitProvenance?> ProvenanceAsync(LogTransactionFile commit)
            {
                Count();
                var header = await CommitHeaderReader.ReadAsync(_worker._storage, commit.IOEntry.Path, HeaderMode.Strict, null, _token);
                return header == null ? null : CommitProvenance.FromStrict(commit.Version, header);
            }

            public Task<RootPrediction> PredictAsync(IReadOnlyList<LogTransactionFile> candidates)
            {
                return CheckpointProbe.PredictAutoRootAsync(new StowageCheckpointTransport(_worker._storage, _worker._table), _run.Scratch!, _run.Memory, _worker._budget.Transfer,
                    candidates, _known, _cache, Count, _token);
            }

            public async Task<bool?> ConfirmAsync(LogTransactionFile checkpoint)
            {
                Count();
                var read = await DestinationCheckpoint.ReadAsync(_worker._table, _worker._storage, checkpoint, _run.Scratch!, _run.Memory, _worker._budget.Transfer, null, null, _token);
                return read.Status switch
                {
                    DestinationStatus.Read => read.GapFree,
                    DestinationStatus.Invalid => false,
                    _ => null
                };
            }

            // Before each read: a short admission whose reads are spent is promoted, a long one stops at its deadline
            private void Count()
            {
                if (_run.Deadline is { } deadline && _worker._settings.OptionsClock.GetUtcNow() >= deadline)
                {
                    throw new CheckpointDeclinedException(MaintenanceReason.JobDeadline, DeclineDependency.None, $"The job deadline {deadline:O} passed");
                }
                if (!_run.Long && ++_reads > _worker._settings.ShortReads)
                {
                    throw new LongAdmissionNeededException();
                }
            }
        }

        private async Task<BuildResult> BuildAsync(JobRun run, long version, RootPolicy root, CancellationToken cancellationToken)
        {
            HashSet<ObjectIdentity> known;
            long weakFloor;
            lock (_sync)
            {
                // Weak facts recorded before the weak floor were purged when it rose
                known = _unsupportedRoots.Keys.ToHashSet();
                weakFloor = _weakFloor;
            }
            var context = new CheckpointBuildContext()
            {
                Storage = _storage,
                Table = _table,
                Scratch = run.Scratch!,
                Memory = run.Memory,
                Cache = _budget.Scratch.Cache,
                Transfer = _budget.Transfer,
                Clock = _settings.OptionsClock,
                Codec = _settings.Codec,
                RebuildLegacyCheckpoints = _settings.RebuildLegacyCheckpoints,
                CleanupEligible = _settings.CleanupEligible,
                ReadAhead = run.Long ? _readAhead : 1,
                PublishFaultForTests = HooksForTests?.PublishFault
            };
            var request = new BuildRequest()
            {
                Version = version,
                Root = root,
                TableId = run.TableId,
                KnownUnsupported = known,
                WeakFloor = weakFloor,
                ReadLimit = run.Long ? null : _settings.ShortReads,
                Deadline = run.Deadline
            };
            var result = await CheckpointBuilder.BuildAsync(context, request, cancellationToken);
            lock (_sync)
            {
                _fence = result.Fence;
            }
            return result;
        }

        // The first occurrence of a reason code logs at Warning, repeats at Debug
        private void Warn(MaintenanceReason reason, string? detail, Exception? exception)
        {
            var code = (int)reason;
            var bit = 1UL << (code & 63);
            var first = (Interlocked.Or(ref _warned[(code >> 6) & 3], bit) & bit) == 0;
            _logger.Log(first ? LogLevel.Warning : LogLevel.Debug, exception, "Delta table {table} checkpoint maintenance: {reason}. {detail}", _tableName, reason, detail);
        }

        /// <summary>
        /// Stops dispatch, cancels the job and joins it for at most the given time; a job still running is counted as detached once.
        /// </summary>
        public async Task StopAsync(TimeSpan join)
        {
            JobRun? run;
            try
            {
                lock (_sync)
                {
                    _stopped = true;
                    run = _run;
                }
                _budget.Unregister(this);
                _cancellation.Cancel();
                var task = run?.Task ?? Task.CompletedTask;
                await Task.WhenAny(task, Task.Delay(join, TimeProvider.System));
                lock (_sync)
                {
                    if (run != null && !run.Settled && !run.Detached)
                    {
                        run.Detached = true;
                        _detached++;
                        _budget.AddDetached(1);
                    }
                }
                // This worker's demand no longer counts as runnable
                _budget.Capacity.Pause(_cancellation.Token);
            }
            catch (Exception e)
            {
                Warn(MaintenanceReason.Unexpected, e.Message, e);
            }
        }
    }

    /// <summary>
    /// A job failure with its reason code, such as an unknown checkpoint or a failed torn rebuild.
    /// </summary>
    internal sealed class MaintenanceJobException : Exception
    {
        public MaintenanceJobException(MaintenanceReason reason, string? detail)
            : base($"{reason}: {detail}")
        {
            Reason = reason;
        }

        public MaintenanceReason Reason { get; }

        // The failed build behind it, such as a torn rebuild's, whose root identity the worker records
        public BuildResult? Build { get; init; }
    }

    /// <summary>
    /// A job too large for short admission, raised before it opened anything counted; the worker re-admits it through the long gate.
    /// </summary>
    internal sealed class LongAdmissionNeededException : Exception
    {
    }
}
