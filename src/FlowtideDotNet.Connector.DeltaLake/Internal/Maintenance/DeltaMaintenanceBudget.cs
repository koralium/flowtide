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

        public const string DefaultTempDirectory = "./data/tempFiles/deltalake";

        public static MaintenanceBudgetSettings From(DeltaLakeOptions options)
        {
            return new MaintenanceBudgetSettings(Math.Max(1, options.MaxConcurrentMaintenanceJobs), Path.GetFullPath(options.MaintenanceTempDirectory ?? DefaultTempDirectory));
        }
    }

    /// <summary>
    /// A job's admission: the table lease first, then a gate slot; each is kept as soon as it is held.
    /// </summary>
    internal class JobAdmission
    {
        public IDisposable? Lease { get; set; }

        public IDisposable? Slot { get; set; }
    }

    /// <summary>
    /// The process-wide maintenance budget: the tick, the FIFO gate, the table leases, the transfer allowance and the scratch folder.
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
        }

        /// <summary>
        /// The process's budget; the first caller fixes the settings, and a caller with other settings gets false.
        /// </summary>
        public static DeltaMaintenanceBudget Shared(MaintenanceBudgetSettings settings, out bool matches) => s_shared.Get(settings, out matches);

        public MaintenanceBudgetSettings Settings { get; }

        // Tick time and age origins
        public TimeProvider Clock { get; }

        public FifoGate Gate { get; }

        public TableLeases Leases { get; } = new TableLeases();

        // Tests set a smaller cap
        public TransferAllowance Transfer { get; init; } = new TransferAllowance();

        public int Registered => Volatile.Read(ref _workers).Length;

        internal IReadOnlyList<IMaintenanceWorker> WorkersForTests => Volatile.Read(ref _workers);

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
        public async Task AdmitAsync(string tableKey, JobAdmission admission, CancellationToken cancellationToken)
        {
            admission.Lease = await Leases.AcquireAsync(tableKey, cancellationToken);
            admission.Slot = await Gate.AcquireAsync(cancellationToken);
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

            public SharedBudget(bool registerGauges = false)
            {
                _registerGauges = registerGauges;
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
                            RegisterGauges(_budget);
                        }
                    }
                    matches = _budget.Settings == settings;
                    return _budget;
                }
            }

            private static void RegisterGauges(DeltaMaintenanceBudget budget)
            {
                s_meter.CreateObservableGauge("flowtide_delta_maintenance_running_jobs", () => budget.Gate.Running);
                s_meter.CreateObservableGauge("flowtide_delta_maintenance_queued_jobs", () => budget.Gate.QueueLength + budget.Leases.Waiting);
                s_meter.CreateObservableGauge("flowtide_delta_maintenance_oldest_wait_seconds", () => budget.Gate.OldestWait.TotalSeconds);
                s_meter.CreateObservableGauge("flowtide_delta_maintenance_transfer_bytes", () => budget.Transfer.Charged);
                s_meter.CreateObservableGauge("flowtide_delta_maintenance_disk_bytes", () => budget.ScratchIfCreated?.Ledger.Reserved ?? 0);
                s_meter.CreateObservableGauge("flowtide_delta_maintenance_pending_deletion_bytes", () => budget.ScratchIfCreated?.PendingBytes ?? 0);
                s_meter.CreateObservableGauge("flowtide_delta_maintenance_detached_jobs", () => budget.DetachedJobs);
                s_meter.CreateObservableGauge("flowtide_delta_maintenance_tick_ms", () => budget.LastTickMilliseconds);
                s_meter.CreateObservableGauge("flowtide_delta_maintenance_init_failed", () => budget.InitFailed ? 1 : 0);
            }
        }
    }
}
