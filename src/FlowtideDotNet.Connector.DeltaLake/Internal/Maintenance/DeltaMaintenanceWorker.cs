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
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    internal sealed record MaintenanceWorkerSettings(int Interval, TimeSpan? MaxAge, TimeProvider OptionsClock)
    {
        public DeltaCompressionCodec? Codec { get; init; }

        public bool RebuildLegacyCheckpoints { get; init; }
    }

    internal enum OutcomeKind : byte
    {
        Completed,
        AlreadyValid,
        NoWork,
        Declined,
        HeldDecline,
        Failed,
        Cancelled
    }

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

        // The memo that held, for a held decline
        public DeclineMemo? Memo { get; init; }

        public Exception? Error { get; init; }
    }

    /// <summary>
    /// One dispatched job, from dispatch until its final lock.
    /// </summary>
    internal sealed class JobRun : JobAdmission
    {
        public JobRun(long dispatchTarget)
        {
            DispatchTarget = dispatchTarget;
        }

        public long DispatchTarget { get; }

        public Task? Task { get; set; }

        public long T { get; set; } = -1;

        public DateTimeOffset AdmittedAt { get; set; }

        public bool Admitted { get; set; }

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
    /// Test points of a job: holds awaited at admitted, inspect, build and release, and faults thrown at dispatch, release, slot and outcome.
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

        private readonly object _sync = new object();
        private readonly DeltaMaintenanceBudget _budget;
        private readonly MaintenanceWorkerSettings _settings;
        private readonly IFileStorage _storage;
        private readonly IOPath _table;
        private readonly string _leaseKey;
        private readonly string _tableName;
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
        private ulong _warned;
        private volatile string? _tableId;
        private int _detached;
        private bool _skipped;
        private BuildMetrics? _lastBuild;
        private double _lastDurationMs;

        public DeltaMaintenanceWorker(DeltaMaintenanceBudget budget, MaintenanceWorkerSettings settings, IFileStorage storage, IOPath table, string tableName, ILogger? logger)
        {
            _budget = budget;
            _settings = settings;
            _storage = storage;
            _table = table;
            _tableName = tableName;
            _logger = logger ?? NullLogger.Instance;
            _leaseKey = TableLeases.KeyOf(storage, table);
        }

        internal WorkerHooks? HooksForTests { get; set; }

        // Counts entries into worker code, which the hand-off must never make
        internal long WorkerCodeCallsForTests;

        // The lock a tick must never block on
        internal object SyncForTests => _sync;

        // The scratch folder of the latest job
        internal string? LastJobFolderForTests { get; private set; }

        internal JobOutcome? LastOutcomeForTests { get; private set; }

        public long Target => Interlocked.Read(ref _target);

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
                if (_run != null)
                {
                    // Queued time never dates versions; only versions seen while the job runs do
                    if (_run.Admitted && t > _run.T && _aboveOrigin == null)
                    {
                        _aboveOrigin = now;
                    }
                    return;
                }
                if (_inspected && t <= _lastCompleted && _repairDue == null)
                {
                    _oldestOrigin = null;
                    return;
                }
                _oldestOrigin ??= now;
                if (_settings.OptionsClock.GetUtcNow() < _retryAt)
                {
                    return;
                }
                var due = !_inspected
                    || _repairDue != null
                    || t - Math.Max(_lastCompleted, 0) >= _settings.Interval
                    || (_settings.MaxAge is { } maxAge && now - _oldestOrigin.Value >= maxAge);
                if (!due)
                {
                    return;
                }
                var run = new JobRun(t);
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
                    Backoff();
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

        private async Task RunJobAsync(JobRun run)
        {
            var token = _cancellation.Token;
            var clockStarted = _settings.OptionsClock.GetTimestamp();
            try
            {
                await _budget.AdmitAsync(_leaseKey, run, token);
                // The duration a build is judged by starts at admission, not in the queue
                clockStarted = _settings.OptionsClock.GetTimestamp();
                lock (_sync)
                {
                    if (_stopped)
                    {
                        throw new OperationCanceledException(token);
                    }
                    run.T = Interlocked.Read(ref _target);
                    run.AdmittedAt = _budget.Clock.GetUtcNow();
                    run.Admitted = true;
                    _aboveOrigin = null;
                    run.TableId = _tableId;
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
                }
                await HoldAsync("admitted");
                // A new job retries the pending deletions before it reserves anything
                run.Scratch = _budget.Scratch.CreateJob();
                LastJobFolderForTests = run.Scratch.DirectoryPath;
                foreach (var (reason, detail) in _budget.Scratch.TakeNotes())
                {
                    Warn(reason, detail, null);
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
                    lock (_sync)
                    {
                        _lastCompleted = inspection.LastCompleted;
                        _inspected = true;
                        _repairDue = inspection.RepairDue;
                        run.Covered = inspection.LastCompleted;
                        DateCoverage(run, inspection.LastCompleted);
                        if (inspection.RepairDeclined is { } declined)
                        {
                            run.Outcome = new JobOutcome(OutcomeKind.Declined, MaintenanceReason.TornRepairDeclined, $"{declined.Reason}: {declined.Detail}");
                        }
                        else if (!DueAfterInspection(run))
                        {
                            run.Outcome = new JobOutcome(OutcomeKind.NoWork);
                        }
                    }
                }

                DeclineMemo? memo;
                lock (_sync)
                {
                    memo = run.Outcome == null ? _memo : null;
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
                    var result = await BuildAsync(run, run.T, token);
                    run.Outcome = OutcomeOf(result);
                }
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                run.Outcome = new JobOutcome(OutcomeKind.Cancelled);
            }
            catch (Exception e)
            {
                run.Outcome = new JobOutcome(OutcomeKind.Failed, ReasonOf(e), e.Message) { Error = e, Build = (e as MaintenanceJobException)?.Build };
            }
            finally
            {
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
                        ApplyOutcome(run, Interlocked.Read(ref _target), duration);
                        _lastDurationMs = duration.TotalMilliseconds;
                    }
                }
                catch (Exception e)
                {
                    // Settlement is never skipped
                    settleFailure = e;
                    Backoff();
                }
                finally
                {
                    // Without the result or the error, which hold the listed history, and with the diagnostic cut as in the memo
                    LastOutcomeForTests = outcome == null ? null : outcome with { Build = null, Error = null, Detail = DeclineMemo.Cut(outcome.Detail) };
                    _aboveOrigin = null;
                    run.Settled = true;
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
                        Warn(outcome.Reason, outcome.Detail, outcome.Error);
                        break;
                }
                if (_settings.MaxAge is { } maxAge && duration > maxAge && outcome.Kind is OutcomeKind.Completed)
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
            if (_repairDue != null)
            {
                return true;
            }
            if (run.T <= _lastCompleted)
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
        private void Backoff(TimeSpan spent = default)
        {
            _failures++;
            var delay = TimeSpan.FromTicks(Math.Max(Math.Min(s_backoffBase.Ticks << Math.Min(_failures - 1, 5), s_backoffCap.Ticks), spent.Ticks * 3));
            _retryAt = _settings.OptionsClock.GetUtcNow() + delay;
        }

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

        private sealed record Inspection(long LastCompleted, long? RepairDue, BuildResult? RepairDeclined);

        /// <summary>
        /// Classifies the newest checkpoint at or below T once per run; nothing above T is considered.
        /// </summary>
        private async Task<Inspection> InspectAsync(JobRun run, CancellationToken cancellationToken)
        {
            var transport = new StowageCheckpointTransport(_storage, _table);
            var segment = await transport.ListAsync(cancellationToken);
            var hint = await transport.ReadHintAsync(cancellationToken);
            var target = run.T;
            long lastCompleted = -1;
            BuildResult? repairDeclined = null;
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
                                await transport.WriteHintAsync(new LastCheckpointInfo() { Version = checkpoint.Version, Size = inspected.Rows, SizeInBytes = inspected.Length }, cancellationToken);
                            }
                            break;
                        case CheckpointClass.Unsupported:
                            // Never touched; kernel lists forward to it, so an older hint is not dangling
                            lastCompleted = checkpoint.Version;
                            break;
                        case CheckpointClass.Torn:
                            var rebuilt = await BuildAsync(run, checkpoint.Version, cancellationToken);
                            switch (rebuilt.Status)
                            {
                                case BuildStatus.Completed:
                                case BuildStatus.AlreadyValid:
                                    lastCompleted = checkpoint.Version;
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
            return new Inspection(lastCompleted, repairDue, repairDeclined);
        }

        // What a fetch may read from the cache; nothing without a table id
        private RootCacheScope? CacheScope(JobRun run)
        {
            lock (_sync)
            {
                return run.TableId != null ? new RootCacheScope(_budget.Scratch.Cache, _storage, run.TableId, _weakFloor) : null;
            }
        }

        private Task<BuildResult> BuildAsync(JobRun run, long version, CancellationToken cancellationToken)
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
                PublishFaultForTests = HooksForTests?.PublishFault
            };
            return CheckpointBuilder.BuildAsync(context, new BuildRequest() { Version = version, TableId = run.TableId, KnownUnsupported = known, WeakFloor = weakFloor }, cancellationToken);
        }

        // The first occurrence of a reason code logs at Warning, repeats at Debug
        private void Warn(MaintenanceReason reason, string? detail, Exception? exception)
        {
            var bit = 1UL << ((int)reason & 63);
            var first = (Interlocked.Or(ref _warned, bit) & bit) == 0;
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
}
