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
using FlowtideDotNet.Connector.DeltaLake.Internal;
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
        public MaintenanceKit()
        {
            // A short scratch root for Windows path limits, and discard hints that drain at every tick unless a test spaces them
            Budget = new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(MaintenanceBudgetSettings.DefaultConcurrentJobs, Path.Combine(Path.GetTempPath(), "mk-" + ScratchOwners.ShortId())),
                TimeProvider.System, manualTick: true)
            {
                Discards = new DiscardHandOff() { DrainSpacing = TimeSpan.Zero }
            };
        }

        public DeltaMaintenanceBudget Budget { get; }

        public IReadOnlyList<DeltaMaintenanceWorker> Workers => Budget.WorkersForTests.OfType<DeltaMaintenanceWorker>().ToList();

        /// <summary>
        /// Waits until a worker's target reaches the version, since the commit file is visible before the hand-off.
        /// </summary>
        public async Task WaitForTargetAsync(long minTarget, TimeSpan? timeout = null)
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
        }

        /// <summary>
        /// Waits until a worker's target reaches the version, then ticks until a tick starts no job; throws once the timeout passed.
        /// </summary>
        public async Task TickAndSettleAsync(long minTarget, TimeSpan? timeout = null)
        {
            var limit = timeout ?? TimeSpan.FromMinutes(1);
            var deadline = DateTime.UtcNow + limit;
            await WaitForTargetAsync(minTarget, deadline - DateTime.UtcNow);
            while (await TickOnceAsync(limit))
            {
                if (DateTime.UtcNow > deadline)
                {
                    throw new TimeoutException($"Maintenance ticks still started jobs {limit} after the settle began");
                }
            }
        }

        // One tick and the jobs it started; false when it started none
        public async Task<bool> TickOnceAsync(TimeSpan? timeout = null)
        {
            var workers = Workers;
            var before = workers.Select(x => x.CurrentJob).ToList();
            Budget.TickNow();
            // Every job this tick started, also one that already finished, and any still running
            var jobs = workers.Select(x => x.CurrentJob).Where((job, i) => !ReferenceEquals(job, before[i]) || !job.IsCompleted).ToList();
            if (jobs.Count == 0)
            {
                return false;
            }
            await Task.WhenAll(jobs).WaitAsync(timeout is { } t && t > TimeSpan.Zero ? t : TimeSpan.FromMinutes(1));
            return true;
        }

        // Best effort, once the stream stopped its workers; kept while a detached job may still use it
        public void DeleteScratch()
        {
            if (Budget.DetachedJobs > 0)
            {
                return;
            }
            Budget.ScratchIfCreated?.ReleaseLockForTests();
            try
            {
                Directory.Delete(Budget.Settings.TempDirectory, true);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }
    }

    internal class DeltaLakeSinkStream : FlowtideTestStream, IAsyncDisposable
    {
        private readonly IFileStorage storage;
        private readonly Action<DeltaLakeOptions>? configureOptions;
        private readonly KeepAliveMemoryFileProvider? stateFiles;
        private readonly Func<IPersistentStorage, IPersistentStorage>? wrapState;
        private readonly bool legacySink;
        private readonly List<DeltaLakeSink> sinks = new List<DeltaLakeSink>();

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
            Maintenance = new MaintenanceKit();
        }

        public MaintenanceKit Maintenance { get; }

        // The sinks this stream constructed
        public IReadOnlyList<DeltaLakeSink> Sinks
        {
            get
            {
                lock (sinks)
                {
                    return sinks.ToList();
                }
            }
        }

        protected override void AddWriteResolvers(IConnectorManager connectorManger)
        {
            var options = new DeltaLakeOptions()
            {
                StorageLocation = storage,
                MaintenanceBudgetOverride = Maintenance.Budget
            };
            configureOptions?.Invoke(options);
            var created = options.SinkCreatedForTests;
            options.SinkCreatedForTests = sink =>
            {
                lock (sinks)
                {
                    sinks.Add(sink);
                }
                created?.Invoke(sink);
            };
            if (legacySink)
            {
                connectorManger.AddSink(new LegacyDeltaLakeSinkFactory(options));
            }
            else
            {
                connectorManger.AddDeltaLakeSink(options);
            }
        }

        // The stream, then the maintenance kit's scratch folder
        public new async ValueTask DisposeAsync()
        {
            await base.DisposeAsync();
            Maintenance.DeleteScratch();
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
