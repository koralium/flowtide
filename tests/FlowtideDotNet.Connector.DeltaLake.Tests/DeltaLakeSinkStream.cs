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

using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Core;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    /// <summary>
    /// A stream's manual maintenance budget: background checkpoints run only when a test ticks it.
    /// </summary>
    internal sealed class MaintenanceKit
    {
        public MaintenanceKit(string testName)
        {
            Budget = new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(MaintenanceBudgetSettings.DefaultConcurrentJobs, Path.Combine(AppContext.BaseDirectory, "maintenance", testName)),
                TimeProvider.System, manualTick: true);
        }

        public DeltaMaintenanceBudget Budget { get; }

        public IReadOnlyList<DeltaMaintenanceWorker> Workers => Budget.WorkersForTests.OfType<DeltaMaintenanceWorker>().ToList();

        /// <summary>
        /// Waits until a worker's target reaches the version, since the commit file is visible before the hand-off, then ticks until a tick starts no job.
        /// </summary>
        public async Task TickAndSettleAsync(long minTarget, TimeSpan? timeout = null)
        {
            var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromMinutes(1));
            while (!Workers.Any(x => x.Target >= minTarget))
            {
                if (DateTime.UtcNow > deadline)
                {
                    throw new TimeoutException($"No maintenance worker reached version {minTarget}");
                }
                await Task.Delay(10);
            }
            while (await TickOnceAsync(deadline - DateTime.UtcNow))
            {
                // Until a tick starts no job
            }
        }

        // One tick and the jobs it started; false when it started none
        public async Task<bool> TickOnceAsync(TimeSpan? timeout = null)
        {
            Budget.TickNow();
            var jobs = Workers.Select(x => x.CurrentJob).Where(x => !x.IsCompleted).ToList();
            if (jobs.Count == 0)
            {
                return false;
            }
            await Task.WhenAll(jobs).WaitAsync(timeout is { } t && t > TimeSpan.Zero ? t : TimeSpan.FromMinutes(1));
            return true;
        }
    }

    internal class DeltaLakeSinkStream : FlowtideTestStream
    {
        private readonly IFileStorage storage;
        private readonly Action<DeltaLakeOptions>? configureOptions;
        private readonly KeepAliveMemoryFileProvider? stateFiles;
        private readonly Func<IPersistentStorage, IPersistentStorage>? wrapState;
        private readonly bool legacySink;

        /// <param name="stateFiles">Shared state files, a new stream over the same files restores the last checkpoint.</param>
        /// <param name="wrapState">Wraps the persistent storage, for example to observe page writes.</param>
        /// <param name="legacySink">Writes with the sink as it was before Phase 2.</param>
        public DeltaLakeSinkStream(
            string testName,
            IFileStorage storage,
            Action<DeltaLakeOptions>? configureOptions = null,
            KeepAliveMemoryFileProvider? stateFiles = null,
            Func<IPersistentStorage, IPersistentStorage>? wrapState = null,
            bool legacySink = false) : base(testName)
        {
            this.storage = storage;
            this.configureOptions = configureOptions;
            this.stateFiles = stateFiles;
            this.wrapState = wrapState;
            this.legacySink = legacySink;
            Maintenance = new MaintenanceKit(testName);
        }

        public MaintenanceKit Maintenance { get; }

        protected override void AddWriteResolvers(IConnectorManager connectorManger)
        {
            var options = new DeltaLakeOptions()
            {
                StorageLocation = storage,
                MaintenanceBudgetOverride = Maintenance.Budget
            };
            configureOptions?.Invoke(options);
            if (legacySink)
            {
                connectorManger.AddSink(new LegacyDeltaLakeSinkFactory(options));
            }
            else
            {
                connectorManger.AddDeltaLakeSink(options);
            }
        }

        protected override IPersistentStorage CreatePersistentStorage(string testName, bool ignoreSameDataCheck)
        {
            var state = stateFiles != null
                ? new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = stateFiles })
                : base.CreatePersistentStorage(testName, ignoreSameDataCheck);
            return wrapState != null ? wrapState(state) : state;
        }
    }
}
