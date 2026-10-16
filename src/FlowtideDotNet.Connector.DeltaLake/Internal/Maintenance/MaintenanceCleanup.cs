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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// The non-build work a worker schedules; the worker admits, preempts, settles and backs off each job.
    /// </summary>
    internal interface IMaintenanceCleanup
    {
        // The default IsDue's answer, which never blocks; the worker schedules the staged sweep itself
        bool HasWork(JobKind kind);

        // Whether the kind's work is due at the published head and the options clock's time; called under the worker's lock, so it never blocks
        bool IsDue(JobKind kind, long head, DateTimeOffset now) => HasWork(kind);

        // Services at most max eligible discard hints; called at the start of every delete turn
        Task<DrainResult> DrainHintsAsync(CleanupContext context, int max);

        // One admitted job of the kind; remote operations go through the context
        Task<CleanupResult> RunAsync(JobKind kind, CleanupContext context);
    }

    /// <summary>
    /// What a non-build job reports; a promotion is returned before the job opens anything it counts.
    /// </summary>
    internal sealed record CleanupResult(OutcomeKind Kind, MaintenanceReason Reason = MaintenanceReason.None, string? Detail = null)
    {
        // Too many reads for short admission: the job runs again through the long gate
        public bool NeedsLongAdmission { get; init; }

        public static CleanupResult Promote { get; } = new CleanupResult(OutcomeKind.NoWork) { NeedsLongAdmission = true };
    }

    /// <summary>
    /// What a delete turn's hint drain did: the hints it deleted or dropped, or a promotion returned before it opened anything it counts.
    /// </summary>
    internal readonly record struct DrainResult(int Drained)
    {
        // Too many reads for short admission: the whole turn runs again through the long gate
        public bool NeedsLongAdmission { get; init; }

        public static DrainResult Promote => new DrainResult(0) { NeedsLongAdmission = true };
    }

    /// <summary>
    /// Why a job's issuance stopped.
    /// </summary>
    internal enum IssuanceStop : byte
    {
        None,
        Deadline,
        // A build of the same table became due
        BuildDue,
        Stopped,
        // The job returned
        Closed
    }

    /// <summary>
    /// An admitted non-build job's limits and issuance gate; issuance never resumes once it stops.
    /// </summary>
    internal sealed class CleanupContext
    {
        // Completed operations are pruned once this many are tracked
        private const int PruneAt = 64;

        private readonly object _lock = new object();
        private readonly List<Task> _issued = new List<Task>();
        private readonly JobRun _run;
        private readonly TimeProvider _clock;
        private IssuanceStop _stopped;

        public CleanupContext(JobRun run, TimeProvider clock)
        {
            _run = run;
            _clock = clock;
        }

        public required JobKind Kind { get; init; }

        // Admitted through the long gate
        public required bool Long { get; init; }

        // Counted reads this admission allows; a job needing more returns a promotion before its first counted read
        public required long ReadLimit { get; init; }

        // Guarded by _lock
        private long _readsUsed;

        // Counted reads this admission made
        public long ReadsUsed
        {
            get
            {
                lock (_lock)
                {
                    return _readsUsed;
                }
            }
        }

        // Counted reads this admission has left
        public long ReadsRemaining
        {
            get
            {
                lock (_lock)
                {
                    return ReadLimit - _readsUsed;
                }
            }
        }

        /// <summary>
        /// Counts one object read against this admission; false, counting nothing, once its reads are spent.
        /// </summary>
        public bool TryCountRead()
        {
            lock (_lock)
            {
                if (_readsUsed >= ReadLimit)
                {
                    return false;
                }
                _readsUsed++;
                return true;
            }
        }

        // On the options clock
        public required DateTimeOffset Deadline { get; init; }

        // The published version at admission
        public required long Head { get; init; }

        // Advisory, as for builds
        public required string? TableId { get; init; }

        public required JobScratch Scratch { get; init; }

        public required JobMemory Memory { get; init; }

        public required TransferAllowance Transfer { get; init; }

        // Cancelled when the worker stops; the deadline and a due build never cancel it
        public required CancellationToken Stop { get; init; }

        // A build of the same table is due, so the job yields at its next cooperative point
        public bool BuildDue => _run.YieldRequested;

        public IssuanceStop Stopped
        {
            get
            {
                lock (_lock)
                {
                    return _stopped;
                }
            }
        }

        // Issued operations not yet pruned
        public int Tracked
        {
            get
            {
                lock (_lock)
                {
                    return _issued.Count;
                }
            }
        }

        /// <summary>
        /// Why issuance stopped, None while it is open; cooperative reads check it between chunks, and a stop it finds is latched as TryIssue latches it.
        /// </summary>
        public IssuanceStop CheckStop()
        {
            lock (_lock)
            {
                return LatchLocked();
            }
        }

        private IssuanceStop LatchLocked()
        {
            if (_stopped == IssuanceStop.None)
            {
                _stopped = Stop.IsCancellationRequested ? IssuanceStop.Stopped
                    : _run.YieldRequested ? IssuanceStop.BuildDue
                    : _clock.GetUtcNow() >= Deadline ? IssuanceStop.Deadline
                    : IssuanceStop.None;
            }
            return _stopped;
        }

        /// <summary>
        /// Starts a remote operation with the stop token, or returns null once the deadline passed, a build is due, the worker stopped or the job returned.
        /// </summary>
        public Task? TryIssue(Func<CancellationToken, Task> operation)
        {
            lock (_lock)
            {
                if (LatchLocked() != IssuanceStop.None)
                {
                    return null;
                }
                Task task;
                try
                {
                    task = operation(Stop);
                }
                catch (Exception e)
                {
                    task = Task.FromException(e);
                }
                if (_issued.Count >= PruneAt)
                {
                    _issued.RemoveAll(x => x.IsCompleted);
                }
                _issued.Add(task);
                return task;
            }
        }

        /// <summary>
        /// Closes issuance and waits for every issued operation; their results are the job's to observe.
        /// </summary>
        internal async Task SettleAsync()
        {
            Task[] pending;
            lock (_lock)
            {
                if (_stopped == IssuanceStop.None)
                {
                    _stopped = IssuanceStop.Closed;
                }
                pending = _issued.ToArray();
                _issued.Clear();
            }
            foreach (var task in pending)
            {
                try
                {
                    await task;
                }
                catch
                {
                    // Observed by the job through the task it was handed
                }
            }
        }

        /// <summary>
        /// Waits for every operation issued so far and leaves issuance open, so a job can confirm one step before the next.
        /// </summary>
        public async Task AwaitIssuedAsync()
        {
            Task[] pending;
            lock (_lock)
            {
                // Kept, so the worker's settle also waits for them
                pending = _issued.ToArray();
            }
            foreach (var task in pending)
            {
                try
                {
                    await task;
                }
                catch
                {
                    // Observed by the job through the task it was handed
                }
            }
            lock (_lock)
            {
                _issued.RemoveAll(x => x.IsCompleted);
            }
        }
    }
}
