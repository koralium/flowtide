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

using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// A registered worker; the budget's tick visits it.
    /// </summary>
    internal interface IMaintenanceWorker
    {
        // Decides whether a job is due and dispatches it; never blocks and never awaits
        void Tick(DateTimeOffset now);
    }

    /// <summary>
    /// The process-wide settings; the first sink's values win.
    /// </summary>
    internal sealed record MaintenanceBudgetSettings(int MaxConcurrentJobs, string TempDirectory)
    {
        public const int DefaultConcurrentJobs = 2;

        public const int DefaultLongJobs = 1;

        public const int DefaultDeleteJobs = 2;

        public const string DefaultTempDirectory = "./data/tempFiles/deltalake";

        // Long jobs take one of these before a main slot
        public int LongJobs { get; init; } = DefaultLongJobs;

        // Deletion slices take one of these instead of a main slot
        public int DeleteJobs { get; init; } = DefaultDeleteJobs;

        // Raised only: a short admission's reads must still fit its deadline
        public long RequestsPerSecond { get; init; } = MaintenanceBuckets.DefaultRequestsPerSecond;

        public long BytesPerSecond { get; init; } = MaintenanceBuckets.DefaultBytesPerSecond;

        /// <summary>
        /// The rate options, which throw below their defaults.
        /// </summary>
        public static (long Requests, long Bytes) Rates(DeltaLakeOptions options)
        {
            var requests = options.MaintenanceRequestsPerSecond;
            if (requests < MaintenanceBuckets.DefaultRequestsPerSecond)
            {
                throw new ArgumentException($"MaintenanceRequestsPerSecond is {requests}; use at least {MaintenanceBuckets.DefaultRequestsPerSecond}, the default.", nameof(DeltaLakeOptions.MaintenanceRequestsPerSecond));
            }
            var bytes = options.MaintenanceBytesPerSecond;
            if (bytes < MaintenanceBuckets.DefaultBytesPerSecond)
            {
                throw new ArgumentException($"MaintenanceBytesPerSecond is {bytes}; use at least {MaintenanceBuckets.DefaultBytesPerSecond} (64 MiB), the default.", nameof(DeltaLakeOptions.MaintenanceBytesPerSecond));
            }
            return (requests, bytes);
        }

        public static MaintenanceBudgetSettings From(DeltaLakeOptions options, (long Requests, long Bytes) rates)
        {
            return new MaintenanceBudgetSettings(Math.Max(1, options.MaxConcurrentMaintenanceJobs), Path.GetFullPath(options.MaintenanceTempDirectory ?? DefaultTempDirectory))
            {
                RequestsPerSecond = rates.Requests,
                BytesPerSecond = rates.Bytes
            };
        }
    }

    /// <summary>
    /// Which gates a job waits on after its table lease.
    /// </summary>
    internal enum AdmissionClass : byte
    {
        // The main gate
        Ordinary,
        // The long gate, then the main gate
        Long,
        // The delete gate
        Delete,
        // The long gate, then the delete gate
        LongDelete
    }

    /// <summary>
    /// A job's admission: the table lease first, then its gate slots; each is kept as soon as it is held.
    /// </summary>
    internal class JobAdmission
    {
        public IDisposable? Lease { get; set; }

        // The main or the delete slot
        public IDisposable? Slot { get; set; }

        public IDisposable? LongSlot { get; set; }
    }

    /// <summary>
    /// The process-wide maintenance budget: the tick, the FIFO gates, the table leases, the transfer allowance, the rate buckets and the scratch folder.
    /// </summary>
    internal sealed class DeltaMaintenanceBudget
    {
        public static readonly TimeSpan TickPeriod = TimeSpan.FromSeconds(1);

        private static readonly SharedBudget s_shared = new SharedBudget(registerGauges: true);

        private readonly object _lock = new object();
        private readonly object _scratchLock = new object();
        private readonly bool _manualTick;
        private readonly TimeSpan _period;
        private IMaintenanceWorker[] _workers = Array.Empty<IMaintenanceWorker>();
        private ITimer? _timer;
        private ScratchDirectory? _scratch;
        private int _ticking;
        private long _facts;
        private long _tickFailures;
        private long _lastTickTicks;
        private long _detached;

        public DeltaMaintenanceBudget(MaintenanceBudgetSettings settings, TimeProvider clock, bool manualTick, TimeSpan? tickPeriod = null)
        {
            Settings = settings;
            Clock = clock;
            _manualTick = manualTick;
            _period = tickPeriod ?? TickPeriod;
            Gate = new FifoGate(settings.MaxConcurrentJobs, clock);
            LongGate = new FifoGate(settings.LongJobs, clock);
            DeleteGate = new FifoGate(settings.DeleteJobs, clock);
            Buckets = new MaintenanceBuckets(TimeProvider.System, settings.RequestsPerSecond, settings.RequestsPerSecond, settings.BytesPerSecond, settings.BytesPerSecond);
            // Reads the buckets and hand-off a test supplied; drains at the configured rates, never the buckets'
            Capacity = new CapacityLedger(() => CapacityLedger.Ambient(Buckets, Discards), settings.RequestsPerSecond, settings.BytesPerSecond);
        }

        /// <summary>
        /// The process's budget; the first caller fixes the settings, and a caller with other settings gets false.
        /// </summary>
        public static DeltaMaintenanceBudget Shared(MaintenanceBudgetSettings settings, out bool matches) => s_shared.Get(settings, out matches);

        // Where sinks find and create the process's budget
        internal static SharedBudget SharedSource => s_shared;

        public MaintenanceBudgetSettings Settings { get; }

        // Tick time and age origins
        public TimeProvider Clock { get; }

        // The main gate: builds and planning
        public FifoGate Gate { get; }

        // Taken before the main gate by jobs too large for short admission
        public FifoGate LongGate { get; }

        public FifoGate DeleteGate { get; }

        public TableLeases Leases { get; } = new TableLeases();

        // Producers of table outputs, by lease key
        public ProducerGenerations Producers { get; } = new ProducerGenerations();

        // Tests set a smaller cap
        public TransferAllowance Transfer { get; init; } = new TransferAllowance();

        // Discarded outputs handed from sinks to their workers; tests set a smaller allowance
        public DiscardHandOff Discards { get; init; } = new DiscardHandOff();

        // Request and byte rates of all maintenance traffic at the configured rates; on the system clock unless a test supplies its own
        public MaintenanceBuckets Buckets { get; init; }

        // Runnable demand that admits new expensive cleanup
        public CapacityLedger Capacity { get; }

        public int Registered => Volatile.Read(ref _workers).Length;

        internal IReadOnlyList<IMaintenanceWorker> WorkersForTests => Volatile.Read(ref _workers);

        /// <summary>
        /// The first registered worker that maintains the table on the route, null when none does.
        /// </summary>
        internal DeltaMaintenanceWorker? WorkerFor(DiscardRoute route)
        {
            foreach (var worker in Volatile.Read(ref _workers))
            {
                if (worker is DeltaMaintenanceWorker candidate && candidate.Serves(route))
                {
                    return candidate;
                }
            }
            return null;
        }

        internal bool TimerActive
        {
            get
            {
                lock (_lock)
                {
                    return _timer != null;
                }
            }
        }

        public long TickFailures => Interlocked.Read(ref _tickFailures);

        public double LastTickMilliseconds => TimeSpan.FromTicks(Interlocked.Read(ref _lastTickTicks)).TotalMilliseconds;

        // Jobs whose worker stopped before they settled; workers change it only under their own lock
        public long DetachedJobs => Interlocked.Read(ref _detached);

        public bool InitFailed { get; set; }

        public void AddDetached(int delta) => Interlocked.Add(ref _detached, delta);

        /// <summary>
        /// The scratch folder, created by the first job that needs it; a failure reaches that job, and the next one tries again.
        /// </summary>
        public ScratchDirectory Scratch
        {
            get
            {
                lock (_scratchLock)
                {
                    return _scratch ??= new ScratchDirectory(Settings.TempDirectory, nextFactSequence: NextFactSequence);
                }
            }
        }

        // The scratch folder if a job has created it
        // Without the lock, which sibling cleanup holds while the folder is created
        public ScratchDirectory? ScratchIfCreated => Volatile.Read(ref _scratch);

        // Fact sequence numbers compare across workers whose clocks differ
        public long NextFactSequence() => Interlocked.Increment(ref _facts);

        public void Register(IMaintenanceWorker worker)
        {
            lock (_lock)
            {
                var workers = new IMaintenanceWorker[_workers.Length + 1];
                _workers.CopyTo(workers, 0);
                workers[^1] = worker;
                Volatile.Write(ref _workers, workers);
                if (_timer == null && !_manualTick)
                {
                    // The timer captures no caller context, so ticks and the jobs they start see none of the registering stream's state
                    using (ExecutionContext.SuppressFlow())
                    {
                        _timer = Clock.CreateTimer(_ => TickNow(), null, _period, _period);
                    }
                }
            }
        }

        public void Unregister(IMaintenanceWorker worker)
        {
            lock (_lock)
            {
                var index = Array.IndexOf(_workers, worker);
                if (index < 0)
                {
                    return;
                }
                var workers = new IMaintenanceWorker[_workers.Length - 1];
                Array.Copy(_workers, 0, workers, 0, index);
                Array.Copy(_workers, index + 1, workers, index, _workers.Length - index - 1);
                Volatile.Write(ref _workers, workers);
                if (workers.Length == 0 && _timer != null)
                {
                    _timer.Dispose();
                    _timer = null;
                }
            }
        }

        /// <summary>
        /// Visits every registered worker once; an overlapping tick is skipped and a worker's failure never stops the others.
        /// </summary>
        internal void TickNow()
        {
            if (Interlocked.Exchange(ref _ticking, 1) != 0)
            {
                return;
            }
            try
            {
                var started = Stopwatch.GetTimestamp();
                var now = Clock.GetUtcNow();
                foreach (var worker in Volatile.Read(ref _workers))
                {
                    try
                    {
                        worker.Tick(now);
                    }
                    catch
                    {
                        Interlocked.Increment(ref _tickFailures);
                    }
                }
                Interlocked.Exchange(ref _lastTickTicks, Stopwatch.GetElapsedTime(started).Ticks);
            }
            finally
            {
                Volatile.Write(ref _ticking, 0);
            }
        }

        /// <summary>
        /// The table lease first, so a job that waits on its own table holds no gate slot.
        /// </summary>
        public Task AdmitAsync(string tableKey, JobAdmission admission, CancellationToken cancellationToken) => AdmitAsync(tableKey, admission, AdmissionClass.Ordinary, cancellationToken);

        /// <summary>
        /// The table lease, then the class's gates: the long gate before the main or delete gate, so nothing holds either slot while it waits for the long one.
        /// </summary>
        public async Task AdmitAsync(string tableKey, JobAdmission admission, AdmissionClass admissionClass, CancellationToken cancellationToken)
        {
            admission.Lease = await Leases.AcquireAsync(tableKey, cancellationToken);
            if (admissionClass is AdmissionClass.Long or AdmissionClass.LongDelete)
            {
                admission.LongSlot = await LongGate.AcquireAsync(cancellationToken);
            }
            var gate = admissionClass is AdmissionClass.Delete or AdmissionClass.LongDelete ? DeleteGate : Gate;
            admission.Slot = await gate.AcquireAsync(cancellationToken);
        }

        // How long the head of the longest-waiting gate queue has waited
        public TimeSpan OldestWait
        {
            get
            {
                var main = Gate.OldestWait;
                var longest = LongGate.OldestWait;
                var delete = DeleteGate.OldestWait;
                var oldest = main > longest ? main : longest;
                return oldest > delete ? oldest : delete;
            }
        }

        // How long the head of the longer-waiting bucket queue has waited
        public TimeSpan BucketOldestWait
        {
            get
            {
                var requests = Buckets.Requests.OldestWait;
                var bytes = Buckets.Bytes.OldestWait;
                return requests > bytes ? requests : bytes;
            }
        }

        /// <summary>
        /// Holds the process's budget and registers its gauges once.
        /// </summary>
        internal sealed class SharedBudget
        {
            private static readonly Meter s_meter = new Meter("FlowtideDotNet.Connector.DeltaLake");

            private readonly object _lock = new object();
            private readonly bool _registerGauges;
            private DeltaMaintenanceBudget? _budget;

            // A test source can start out holding a budget, as the process's does once a sink created it
            public SharedBudget(bool registerGauges = false, DeltaMaintenanceBudget? existing = null)
            {
                _registerGauges = registerGauges;
                _budget = existing;
            }

            public DeltaMaintenanceBudget? Existing
            {
                get
                {
                    lock (_lock)
                    {
                        return _budget;
                    }
                }
            }

            public DeltaMaintenanceBudget Get(MaintenanceBudgetSettings settings, out bool matches)
            {
                lock (_lock)
                {
                    if (_budget == null)
                    {
                        _budget = new DeltaMaintenanceBudget(settings, TimeProvider.System, manualTick: false);
                        if (_registerGauges)
                        {
                            RegisterGauges(s_meter, _budget);
                        }
                    }
                    matches = _budget.Settings == settings;
                    return _budget;
                }
            }

            // Tests register on a meter of their own
            internal static void RegisterGauges(Meter meter, DeltaMaintenanceBudget budget)
            {
                meter.CreateObservableGauge("flowtide_delta_maintenance_running_jobs", () => budget.Gate.Running + budget.DeleteGate.Running);
                meter.CreateObservableGauge("flowtide_delta_maintenance_queued_jobs", () => budget.Gate.QueueLength + budget.LongGate.QueueLength + budget.DeleteGate.QueueLength + budget.Leases.Waiting);
                meter.CreateObservableGauge("flowtide_delta_maintenance_oldest_wait_seconds", () => budget.OldestWait.TotalSeconds);
                meter.CreateObservableGauge("flowtide_delta_maintenance_transfer_bytes", () => budget.Transfer.Charged);
                meter.CreateObservableGauge("flowtide_delta_maintenance_disk_bytes", () => budget.ScratchIfCreated?.Ledger.Reserved ?? 0);
                meter.CreateObservableGauge("flowtide_delta_maintenance_pending_deletion_bytes", () => budget.ScratchIfCreated?.PendingBytes ?? 0);
                meter.CreateObservableGauge("flowtide_delta_maintenance_detached_jobs", () => budget.DetachedJobs);
                meter.CreateObservableGauge("flowtide_delta_maintenance_tick_ms", () => budget.LastTickMilliseconds);
                meter.CreateObservableGauge("flowtide_delta_maintenance_init_failed", () => budget.InitFailed ? 1 : 0);
                meter.CreateObservableGauge("flowtide_delta_maintenance_request_debt", () => budget.Buckets.Requests.Debt);
                meter.CreateObservableGauge("flowtide_delta_maintenance_byte_debt", () => budget.Buckets.Bytes.Debt);
                meter.CreateObservableGauge("flowtide_delta_maintenance_bucket_waiters", () => budget.Buckets.Requests.QueueLength + budget.Buckets.Bytes.QueueLength);
                meter.CreateObservableGauge("flowtide_delta_maintenance_bucket_oldest_wait_seconds", () => budget.BucketOldestWait.TotalSeconds);
                meter.CreateObservableGauge("flowtide_delta_maintenance_discard_hints", () => budget.Discards.Queued);
                meter.CreateObservableGauge("flowtide_delta_maintenance_discard_bytes", () => budget.Discards.Charged);
                meter.CreateObservableGauge("flowtide_delta_maintenance_discard_dropped", () => budget.Discards.Dropped);
                meter.CreateObservableGauge("flowtide_delta_maintenance_discard_leaked", () => budget.Discards.Leaked);
                meter.CreateObservableGauge("flowtide_delta_maintenance_runnable_request_equivalents", () => budget.Capacity.Current.Requests);
                meter.CreateObservableGauge("flowtide_delta_maintenance_runnable_bytes", () => budget.Capacity.Current.Bytes);
                meter.CreateObservableGauge("flowtide_delta_maintenance_estimated_drain_seconds", () => new[]
                {
                    new Measurement<double>(budget.Capacity.DrainSeconds(budget.Capacity.Current), new KeyValuePair<string, object?>("state", "runnable")),
                    new Measurement<double>(budget.Capacity.DrainSeconds(budget.Capacity.Total(CapacityState.Paused)), new KeyValuePair<string, object?>("state", "paused")),
                    new Measurement<double>(budget.Capacity.DrainSeconds(budget.Capacity.Total(CapacityState.Suspended)), new KeyValuePair<string, object?>("state", "suspended"))
                });
            }
        }
    }
}
