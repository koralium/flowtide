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

using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion
{
    /// <summary>
    /// A run's targets in a fixed order; each slice reads from the run's cursor and stops reading once its buffer is full.
    /// </summary>
    internal interface IDeletionSource
    {
        IAsyncEnumerable<DeletionTarget> ReadFrom(long position, CancellationToken cancellationToken);
    }

    /// <summary>
    /// Targets held in memory, for small sets such as one attempt's discarded files.
    /// </summary>
    internal sealed class ListDeletionSource : IDeletionSource
    {
        private readonly IReadOnlyList<DeletionTarget> _targets;

        public ListDeletionSource(IReadOnlyList<DeletionTarget> targets)
        {
            _targets = targets;
        }

        public async IAsyncEnumerable<DeletionTarget> ReadFrom(long position, [EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask;
            for (var i = position; i < _targets.Count; i++)
            {
                yield return _targets[(int)i];
            }
        }
    }

    /// <summary>
    /// The limits and hooks of one deletion run; tests lower the limits.
    /// </summary>
    internal sealed record DeletionRunSettings
    {
        public const int DefaultMaxPaths = 1000;
        public const long DefaultMaxBufferBytes = 1L << 20;

        // Paths one slice reads and holds
        public int MaxPaths { get; init; } = DefaultMaxPaths;

        // Estimated managed bytes of the paths one slice holds, charged to the job's memory
        public long MaxBufferBytes { get; init; } = DefaultMaxBufferBytes;

        // From the first issuance, excluding the gate and the final settlement
        public TimeSpan IssuanceTime { get; init; } = TimeSpan.FromSeconds(30);

        // Outstanding payload, stage, temporary and control deletes, never above the store's limit
        public int MaxOutstanding { get; init; } = DeletionRun.MaxOutstanding;

        // The wait before a slice retries a failed delete, doubled for each further failed slice
        public TimeSpan RetryDelay { get; init; } = DeletionRun.DefaultRetryDelay;

        // Pinned Azure orphan targets need a positive size at inventory and submission
        public bool Orphan { get; init; }

        // A caller's own switch, read with the process-wide delete switch before the gate and before every issuance
        public Func<bool>? DeletesDisabled { get; init; }

        // Records the new cursor after the slice's deletes settled, under the slice's admission; false keeps the previous cursor
        public Func<long, CleanupContext, Task<bool>>? SettleProgress { get; init; }
    }

    internal enum SliceStatus : byte
    {
        // Every target settled and nothing remains
        Completed,
        // What was issued settled and more remains
        Progressed,
        // Nothing concluded: the run keeps its cursor and targets for a later slice
        Retry,
        // The gate needs long admission
        Promote,
        // A volatile run stopped and drops what remains
        Stopped,
        // A durable run stopped issuing and keeps its authority and cursor
        Suspended,
        // A durable advance applies its own foreign-change handling
        Rebase,
        // A delete failed and the cursor stays on it: a durable or log target, or a volatile one before its leak limit
        Failed,
        // Maintenance deletes are switched off
        Disabled,
        // Not run under a delete admission with the maintenance meter
        NotAdmitted
    }

    /// <summary>
    /// What one slice did; the cursor is the run's after the slice.
    /// </summary>
    internal sealed record SliceResult(SliceStatus Status, MaintenanceReason Reason, long Cursor)
    {
        public GateResult? Gate { get; init; }

        // Why issuance closed early, None when it did not
        public IssuanceStop Stop { get; init; }

        // Deletes submitted by this slice
        public int Issued { get; init; }

        public string? Detail { get; init; }

        // From the first issuance until every submitted delete settled
        public TimeSpan Elapsed { get; init; }

        // From the first issuance to the last, excluding the final settlement
        public TimeSpan IssuanceWindow { get; init; }

        // Spent passing the gate
        public TimeSpan GateElapsed { get; init; }
    }

    /// <summary>
    /// Executes a run's targets in slices: each slice passes its gate, issues within its limits through the job's context, settles every submitted delete and then the progress record, all under one delete admission.
    /// </summary>
    internal sealed class DeletionRun
    {
        public const int MaxOutstanding = 4;

        // Each dependent log delete settles successfully before the next is issued
        public const int LogOutstanding = 1;

        // Failed slices on one volatile payload, stage, temporary or control target before it is counted as a leak and passed
        public const int MaxFailedSlices = 3;

        // The first wait before a retry slice
        public static readonly TimeSpan DefaultRetryDelay = TimeSpan.FromSeconds(1);

        // The longest wait before one retry slice
        private static readonly TimeSpan s_maxRetryDelay = TimeSpan.FromSeconds(30);

        // How often a retry wait looks for a stop, a due build or the deadline
        private static readonly TimeSpan s_retryStep = TimeSpan.FromMilliseconds(250);

        // Estimated managed bytes per buffered target beside its key's characters
        public const long TargetOverheadBytes = 48;

        // Estimated managed bytes of a buffered target's frozen MD5 beside its characters
        public const long Md5OverheadBytes = 32;

        // What a slice buffer charges for a target's frozen MD5
        public static long Md5Bytes(string? md5) => md5 == null ? 0 : Md5OverheadBytes + 2L * md5.Length;

        private readonly DeletionExecutor _executor;
        private readonly ChunkGate _gate;
        private readonly IDeletionSource _source;
        private readonly TimeProvider _clock;
        private readonly DeletionRunSettings _settings;
        private SliceResult? _ended;
        // The position of the volatile target the latest slices failed on, and in how many of them
        private long _failedAt = -1;
        private int _failedSlices;
        // Positions at or after the cursor whose targets settled without failing, with their outcomes, within one slice of it
        private readonly Dictionary<long, DeleteOutcome> _doneAhead = new Dictionary<long, DeleteOutcome>();

        public DeletionRun(DeletionExecutor executor, ChunkGate gate, IDeletionSource source, TimeProvider clock, DeletionRunSettings? settings = null, long cursor = 0)
        {
            Cursor = cursor;
            _executor = executor;
            _gate = gate;
            _source = source;
            _clock = clock;
            _settings = settings ?? new DeletionRunSettings();
        }

        public bool Durable => _gate.Durable;

        // The position of the first target the run has not settled; never moved past a durable or log target that could not be deleted
        public long Cursor { get; private set; }

        // One past the furthest target a slice submitted, so a gate accepts that a target before it is already gone
        public long Reached { get; private set; }

        public DeletionTally Tally => _executor.Tally;

        // A slice deleted, found absent or kept the target at the position, and the cursor has not passed it
        public bool DoneAhead(long position) => _doneAhead.ContainsKey(position);

        internal int DoneAheadCountForTests => _doneAhead.Count;

        /// <summary>
        /// The most deletes a run on this store may have outstanding: one on pinned S3, whose signer is not safe for concurrent requests.
        /// </summary>
        public static int OutstandingLimit(StoreDescriptor store) => store.Kind == StoreKind.S3 ? 1 : MaxOutstanding;

        /// <summary>
        /// Waits before the slice that retries a failed delete, the retry delay doubled for each earlier failed slice; false, waiting no longer, once issuance stops.
        /// </summary>
        public async Task<bool> BackOffAsync(int failedSlices, CleanupContext context)
        {
            var first = Math.Clamp(_settings.RetryDelay.Ticks, 0, s_maxRetryDelay.Ticks);
            var delay = TimeSpan.FromTicks(Math.Min(first << Math.Clamp(failedSlices - 1, 0, 4), s_maxRetryDelay.Ticks));
            var started = _clock.GetTimestamp();
            // The deadline, a due build and the worker's stop are each seen within one step
            while (context.CheckStop() == IssuanceStop.None)
            {
                var left = delay - _clock.GetElapsedTime(started);
                if (left <= TimeSpan.Zero)
                {
                    return true;
                }
                try
                {
                    await Task.Delay(left < s_retryStep ? left : s_retryStep, _clock, context.Stop);
                }
                catch (OperationCanceledException) when (context.Stop.IsCancellationRequested)
                {
                    // The loop's check sees the stop
                }
            }
            return false;
        }

        /// <summary>
        /// Runs one slice inside a delete-class job; never throws for runtime failures, and returns only after everything it submitted settled.
        /// </summary>
        public async Task<SliceResult> RunSliceAsync(CleanupContext context)
        {
            if (_ended != null)
            {
                return _ended;
            }
            // The delete permit is the job's admission, and every delete is charged to the maintenance meter
            if (context.Kind is not (JobKind.Deletion or JobKind.StagedSweep) || MeteredFileStorage.Of(_executor.Storage) == null)
            {
                return new SliceResult(SliceStatus.NotAdmitted, MaintenanceReason.Unexpected, Cursor) { Detail = $"A {context.Kind} job or an unmetered storage cannot run delete slices" };
            }
            if (DeletesDisabled())
            {
                return new SliceResult(SliceStatus.Disabled, MaintenanceReason.DeletesDisabled, Cursor);
            }
            // Before any listing through a storage the sink no longer holds
            if (_executor.StorageChanged)
            {
                return End(new SliceResult(Durable ? SliceStatus.Retry : SliceStatus.Stopped, MaintenanceReason.ListingIneligible, Cursor) { Detail = "The storage instance changed during the run" });
            }
            var gateStarted = _clock.GetTimestamp();
            var gate = await _gate.PassAsync(context);
            var gateElapsed = _clock.GetElapsedTime(gateStarted);
            if (!gate.Passed)
            {
                return Timed(OfGate(gate), gateElapsed);
            }

            var buffer = new SliceBuffer();
            try
            {
                try
                {
                    await FillAsync(buffer, context);
                }
                catch (CheckpointDeclinedException e) when (buffer.Targets.Count == 0)
                {
                    return new SliceResult(SliceStatus.Retry, e.Reason, Cursor) { Gate = gate, Detail = e.Message, GateElapsed = gateElapsed };
                }
                catch (CheckpointDeclinedException)
                {
                    // The ledger holds what was read, the rest waits for a later slice
                }
                catch (OperationCanceledException) when (context.Stop.IsCancellationRequested)
                {
                    return new SliceResult(SliceStatus.Retry, MaintenanceReason.None, Cursor) { Gate = gate, Stop = IssuanceStop.Stopped, GateElapsed = gateElapsed };
                }
                catch (Exception e)
                {
                    return new SliceResult(SliceStatus.Retry, MaintenanceReason.Transient, Cursor) { Gate = gate, Detail = $"{e.GetType().Name}: {e.Message}", GateElapsed = gateElapsed };
                }
                return Timed(await IssueAsync(context, gate, buffer), gateElapsed, buffer.Elapsed, buffer.IssuanceWindow);
            }
            finally
            {
                // Released only after every delete over the buffer settled
                context.Memory.Release(buffer.Charged);
            }
        }

        // The targets one slice holds and the memory charged for them
        private sealed class SliceBuffer
        {
            public List<DeletionTarget> Targets { get; } = new List<DeletionTarget>();

            public long Charged;

            // The source has nothing after the buffered targets
            public bool Exhausted;

            // From the first issuance until every submitted delete settled
            public TimeSpan Elapsed;

            // From the first issuance to the last
            public TimeSpan IssuanceWindow;
        }

        // Reads from the cursor until the path or byte limit
        private async Task FillAsync(SliceBuffer buffer, CleanupContext context)
        {
            await using var reader = _source.ReadFrom(Cursor, context.Stop).GetAsyncEnumerator(context.Stop);
            var more = await reader.MoveNextAsync();
            while (more && buffer.Targets.Count < _settings.MaxPaths)
            {
                var target = reader.Current;
                var size = TargetOverheadBytes + 2L * (target.Key?.Length ?? 0) + Md5Bytes(target.Md5);
                if (buffer.Targets.Count > 0 && buffer.Charged + size > _settings.MaxBufferBytes)
                {
                    break;
                }
                context.Memory.Reserve(size);
                buffer.Charged += size;
                buffer.Targets.Add(target);
                more = await reader.MoveNextAsync();
            }
            buffer.Exhausted = !more;
        }

        private async Task<SliceResult> IssueAsync(CleanupContext context, GateResult gate, SliceBuffer buffer)
        {
            var targets = buffer.Targets;
            var durable = Durable;
            var cap = Math.Clamp(_settings.MaxOutstanding, 1, OutstandingLimit(_executor.Configuration.Store.Store));
            var outcomes = new DeleteOutcome?[targets.Count];
            var tasks = new Task<DeleteOutcome>?[targets.Count];
            var pending = new List<Task<DeleteOutcome>>();
            var issued = 0;
            var reached = 0;
            var scan = new SliceScan();
            var disabled = false;
            var aboveHead = false;
            var stop = IssuanceStop.None;
            var started = _clock.GetTimestamp();
            try
            {
                for (int i = 0; i < targets.Count; i++)
                {
                    // An earlier slice settled it ahead of the cursor; its outcome stands and is not counted again
                    if (_doneAhead.TryGetValue(Cursor + i, out var settled))
                    {
                        outcomes[i] = settled;
                        continue;
                    }
                    var target = targets[i];
                    var log = target.Kind == TargetKind.Log;
                    while (pending.Count >= (log ? LogOutstanding : cap) || (log && pending.Count > 0))
                    {
                        pending.Remove(await Task.WhenAny(pending));
                    }
                    if (Blocked(outcomes, tasks, scan, i, durable || log))
                    {
                        break;
                    }
                    if (DeletesDisabled())
                    {
                        disabled = true;
                        break;
                    }
                    var issuing = _clock.GetElapsedTime(started);
                    if (issuing >= _settings.IssuanceTime)
                    {
                        break;
                    }
                    // A versioned file or stage above the admission's published version waits for a later admission
                    if (DeletionExecutor.BoundVersion(target) > context.Head)
                    {
                        aboveHead = true;
                        break;
                    }
                    var reason = _executor.Check(target);
                    if (reason != KeepReason.None)
                    {
                        outcomes[i] = _executor.Withhold(target, reason);
                        // A changed storage invalidates every later target too
                        if (reason == KeepReason.StorageChanged)
                        {
                            break;
                        }
                        continue;
                    }
                    Task<DeleteOutcome>? task = null;
                    var orphan = _settings.Orphan;
                    // A submitted delete runs to completion; a stop only closes issuance
                    if (context.TryIssue(_ => task = Task.Run(() => _executor.DeleteAsync(target, orphan, CancellationToken.None))) == null)
                    {
                        stop = context.Stopped;
                        break;
                    }
                    tasks[i] = task;
                    pending.Add(task!);
                    issued++;
                    reached = i + 1;
                    buffer.IssuanceWindow = issuing;
                }
            }
            finally
            {
                // Submitted deletes always settle before the slice moves on; they never throw
                foreach (var task in pending)
                {
                    await task;
                }
            }
            buffer.Elapsed = _clock.GetElapsedTime(started);
            Reached = Math.Max(Reached, Cursor + reached);

            // The cursor passes settled targets in order; it stops at a durable or log target not done, and at a volatile failure before its leak limit
            long advanced = 0;
            long leaked = 0;
            var failedAt = _failedAt;
            var failedSlices = _failedSlices;
            SliceStatus? blocked = null;
            var blockedReason = MaintenanceReason.None;
            for (int i = 0; i < targets.Count; i++)
            {
                if ((tasks[i]?.Result ?? outcomes[i]) is not { } outcome)
                {
                    break;
                }
                if (!outcome.Passes)
                {
                    var holds = durable || targets[i].Kind == TargetKind.Log;
                    if (outcome.Reason == KeepReason.StorageChanged)
                    {
                        (blocked, blockedReason) = (durable ? SliceStatus.Retry : SliceStatus.Stopped, MaintenanceReason.ListingIneligible);
                        break;
                    }
                    if (outcome.Status == DeleteStatus.Failed)
                    {
                        if (!holds)
                        {
                            var position = Cursor + advanced;
                            failedSlices = failedAt == position ? failedSlices + 1 : 1;
                            failedAt = position;
                        }
                        if (holds || failedSlices < MaxFailedSlices)
                        {
                            (blocked, blockedReason) = (SliceStatus.Failed, MaintenanceReason.DeleteFailed);
                            break;
                        }
                        // Retried in the limit's slices without success: left behind, counted and passed
                        leaked++;
                        failedAt = -1;
                        failedSlices = 0;
                    }
                    else if (holds)
                    {
                        (blocked, blockedReason) = (durable ? SliceStatus.Suspended : SliceStatus.Stopped, MaintenanceReason.PathRejected);
                        break;
                    }
                }
                advanced++;
            }
            var start = Cursor;
            var cursor = start + advanced;
            if (advanced > 0 && _settings.SettleProgress is { } settle)
            {
                bool recorded;
                try
                {
                    recorded = await settle(cursor, context);
                }
                catch
                {
                    recorded = false;
                }
                if (!recorded)
                {
                    return new SliceResult(SliceStatus.Retry, MaintenanceReason.Transient, Cursor) { Gate = gate, Issued = issued, Detail = "The progress record was not written" };
                }
            }
            Cursor = cursor;
            NoteDone(cursor);
            _failedAt = failedAt;
            _failedSlices = failedSlices;
            for (long i = 0; i < leaked; i++)
            {
                _executor.Tally.AddLeaked();
            }

            if (blocked is { } status)
            {
                return End(new SliceResult(status, blockedReason, Cursor) { Gate = gate, Issued = issued });
            }
            if (disabled)
            {
                return new SliceResult(SliceStatus.Disabled, MaintenanceReason.DeletesDisabled, Cursor) { Gate = gate, Issued = issued };
            }
            if (aboveHead && advanced == 0)
            {
                return new SliceResult(SliceStatus.Retry, MaintenanceReason.CleanupGate, Cursor) { Gate = gate, Detail = "The next log target is above the admission's published version" };
            }
            if (buffer.Exhausted && advanced == targets.Count)
            {
                var result = new SliceResult(SliceStatus.Completed, MaintenanceReason.None, Cursor) { Gate = gate, Issued = issued };
                _ended = result;
                return result;
            }
            return new SliceResult(SliceStatus.Progressed, stop == IssuanceStop.Deadline ? MaintenanceReason.JobDeadline : MaintenanceReason.None, Cursor)
            {
                Gate = gate,
                Issued = issued,
                Stop = stop
            };

            // The slice's settled targets from the cursor on, except keeps for a changed storage
            void NoteDone(long from)
            {
                foreach (var passed in _doneAhead.Keys.Where(x => x < from).ToList())
                {
                    _doneAhead.Remove(passed);
                }
                for (int i = 0; i < targets.Count; i++)
                {
                    if (start + i >= from && (tasks[i]?.Result ?? outcomes[i]) is { Status: not DeleteStatus.Failed, Reason: not KeepReason.StorageChanged } outcome)
                    {
                        _doneAhead[start + i] = outcome;
                    }
                }
            }
        }

        // The process-wide switch, then the caller's; a switch that cannot be read counts as on
        private bool DeletesDisabled()
        {
            if (MaintenanceSwitches.DeletesDisabled())
            {
                return true;
            }
            try
            {
                return _settings.DeletesDisabled?.Invoke() == true;
            }
            catch
            {
                return true;
            }
        }

        // How far a slice's earlier targets were judged for blocking issuance
        private sealed class SliceScan
        {
            // Every target before it settled without stopping issuance
            public int Settled;

            // One of those is not done, which only a strict target cannot pass
            public bool NotDone;
        }

        // A settled earlier delete of the slice that is not done stops issuance when it must not be passed, and a failure does unless its leak limit passes it here
        private bool Blocked(DeleteOutcome?[] outcomes, Task<DeleteOutcome>?[] tasks, SliceScan scan, int next, bool strict)
        {
            var prefix = true;
            for (int i = scan.Settled; i < next; i++)
            {
                DeleteOutcome? outcome = tasks[i] is { } task ? (task.IsCompleted ? task.Result : null) : outcomes[i];
                if (outcome is not { } settled)
                {
                    prefix = false;
                    continue;
                }
                if (!settled.Passes)
                {
                    if (strict || (settled.Status == DeleteStatus.Failed && !LeaksAt(Cursor + i)))
                    {
                        return true;
                    }
                    if (prefix)
                    {
                        scan.NotDone = true;
                    }
                }
                if (prefix)
                {
                    scan.Settled = i + 1;
                }
            }
            return strict && scan.NotDone;
        }

        // A failure at this position reaches the leak limit, so the slice counts it and passes it
        private bool LeaksAt(long position) => _failedAt == position && _failedSlices + 1 >= MaxFailedSlices;

        // The slice's times, also on the end it latched
        private SliceResult Timed(SliceResult result, TimeSpan gate, TimeSpan issuance = default, TimeSpan window = default)
        {
            var timed = result with { GateElapsed = gate, Elapsed = issuance, IssuanceWindow = window };
            if (ReferenceEquals(_ended, result))
            {
                _ended = timed;
            }
            return timed;
        }

        // A stopped or suspended run keeps its end for every later slice
        private SliceResult End(SliceResult result)
        {
            if (result.Status is SliceStatus.Stopped or SliceStatus.Suspended)
            {
                _ended = result;
            }
            return result;
        }

        private SliceResult OfGate(GateResult gate)
        {
            var status = gate.Action switch
            {
                GateAction.Promote => SliceStatus.Promote,
                GateAction.Stop => SliceStatus.Stopped,
                GateAction.Suspend => SliceStatus.Suspended,
                GateAction.Rebase => SliceStatus.Rebase,
                _ => SliceStatus.Retry
            };
            return End(new SliceResult(status, gate.Reason, Cursor) { Gate = gate, Stop = gate.Stop, Detail = gate.Detail });
        }
    }
}
