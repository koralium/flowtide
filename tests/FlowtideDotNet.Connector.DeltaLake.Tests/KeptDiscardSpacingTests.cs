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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class KeptDiscardSpacingTests
    {
        [Fact]
        public async Task AKeptAnchorCannotMakeFreshHintsReplayAtEveryTick()
        {
            var directory = Path.Combine(Path.GetTempPath(), "kd-" + ScratchOwners.ShortId());
            Directory.CreateDirectory(directory);
            var clock = new FakeTimeProvider(new DateTimeOffset(2026, 10, 15, 12, 0, 0, TimeSpan.Zero));
            var owners = new ScratchOwners();
            DeltaMaintenanceWorker? worker = null;
            DeltaMaintenanceBudget? budget = null;
            try
            {
                using var local = Files.Of.LocalDisk(directory);
                var storage = OwnedHookFileStorage.Over(local);
                var configuration = CleanupConfiguration.Validate(storage, "t", false, CleanupOptIns.Off);
                var metadata = Metadata(UserSchema);
                metadata.MetaData!.Id = "table-a";
                await WriteCommit(storage, "t", 0, Own(0), Protocol(), metadata);
                for (long version = 1; version <= 10; version++)
                {
                    await WriteCommit(storage, "t", version, Own(version));
                }
                var scratchOwner = owners.Add(new ScratchDirectory(Path.Combine(directory, "build")));
                using (var scratch = scratchOwner.CreateJob())
                {
                    var build = await CheckpointBuilder.BuildAsync(new CheckpointBuildContext()
                    {
                        Storage = local,
                        Table = "t",
                        Scratch = scratch,
                        Memory = new JobMemory(),
                        Clock = clock
                    }, new BuildRequest() { Version = 5 });
                    Assert.Equal(BuildStatus.Completed, build.Status);
                }

                var handOff = new DiscardHandOff();
                using var producer = handOff.Acquire(DiscardRoute.Of(storage, "t"));
                var counters = new DiscardCounters();
                var metered = MeteredFileStorage.Wrap(storage, new MaintenanceBuckets(TimeProvider.System), ListingPaging.LocalDisk);
                var cleanup = new DeltaTableCleanup(configuration, metered, clock, "t", handOff, () => storage, counters, null, "t");
                budget = new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(1, Path.Combine(directory, "worker")), clock, manualTick: true) { Discards = handOff };
                worker = new DeltaMaintenanceWorker(budget, new MaintenanceWorkerSettings(0, null, clock) { PeriodicEnabled = false, InspectionEnabled = false },
                    storage, "t", "t", null) { Cleanup = cleanup };
                budget.Register(worker);
                worker.Published(10);
                Assert.True(await TickOnce(budget, worker));

                await storage.WriteText("/t/_change_data/old.parquet", "old");
                Assert.Equal(0, producer.Entry.Enqueue(new DiscardGroup("table-a", 0, 3, "stage-3", new[] { "_change_data/old.parquet" }) { QueuedAt = clock.GetUtcNow() }));
                clock.Advance(handOff.DrainSpacing);
                // A pre-checkpoint anchor has a transient per-object read failure, while newer commits remain available
                storage.Before = (verb, path) => verb == "OpenRead" && path.Full == CommitPath("t", 3)
                    ? throw new IOException("injected anchor read failure") : Task.CompletedTask;
                storage.ClearRequests();
                var turns = new List<bool>();
                for (long version = 11; version <= 14; version++)
                {
                    clock.Advance(TimeSpan.FromSeconds(10));
                    await WriteCommit(storage, "t", version, Own(version));
                    var relative = $"_change_data/fresh-{version}.parquet";
                    await storage.WriteText("/t/" + relative, "fresh");
                    Assert.Equal(0, producer.Entry.Enqueue(new DiscardGroup("table-a", 0, version, $"stage-{version}", new[] { relative }) { QueuedAt = clock.GetUtcNow() }));
                    worker.Published(version);
                    turns.Add(await TickOnce(budget, worker));
                }

                var checkpointReads = storage.Requests.Count(x => x == "OpenRead /t/_delta_log/00000000000000000005.checkpoint.parquet");
                Assert.True(turns[0], "The aged hint must make the first drain due");
                Assert.Equal(new[] { false, false, false }, turns.Skip(1));
                Assert.True(counters.Deleted > 0, "The healthy newer group must make progress past the unavailable anchor");
                Assert.True(checkpointReads is >= 1 and <= 2, $"One kept anchor caused {checkpointReads} full checkpoint downloads in forty seconds; drain turns: {string.Join(",", turns)}");
            }
            finally
            {
                if (worker != null)
                {
                    await worker.StopAsync(TimeSpan.FromSeconds(30));
                }
                budget?.ScratchIfCreated?.ReleaseLockForTests();
                owners.Release();
                Directory.Delete(directory, true);
            }
        }

        private static DeltaAction Own(long version) => new DeltaAction()
        {
            CommitInfo = new DeltaCommitInfoAction() { StageId = $"stage-{version}", AdoptedAt = 0, Timestamp = 1000 + version, Operation = "WRITE" }
        };

        private static async Task<bool> TickOnce(DeltaMaintenanceBudget budget, DeltaMaintenanceWorker worker)
        {
            var before = worker.CurrentJob;
            budget.TickNow();
            var job = worker.CurrentJob;
            if (ReferenceEquals(before, job))
            {
                return false;
            }
            await job.WaitAsync(TimeSpan.FromSeconds(30));
            return true;
        }
    }
}
