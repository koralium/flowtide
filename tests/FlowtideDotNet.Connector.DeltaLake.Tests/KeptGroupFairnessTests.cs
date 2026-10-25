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
    public class KeptGroupFairnessTests
    {
        [Fact]
        public async Task RecoveredLaterGroupsCannotStarveBehindCappedGateRetries()
        {
            var root = Path.Combine(Path.GetTempPath(), "kf-" + ScratchOwners.ShortId());
            Directory.CreateDirectory(root);
            var clock = new FakeTimeProvider(new DateTimeOffset(2026, 10, 16, 12, 0, 0, TimeSpan.Zero));
            var owners = new ScratchOwners();
            try
            {
                using var local = Files.Of.LocalDisk(root);
                var storage = OwnedHookFileStorage.Over(local);
                var metadata = Metadata(UserSchema);
                metadata.MetaData!.Id = "x";
                await WriteCommit(local, "t", 0, Own(0), Protocol(), metadata);
                for (var version = 1; version <= 200; version++)
                {
                    await WriteCommit(local, "t", version, Own(version));
                }

                var scratchOwner = owners.Add(new ScratchDirectory(Path.Combine(root, "scratch")));
                using (var buildScratch = scratchOwner.CreateJob())
                {
                    var build = await CheckpointBuilder.BuildAsync(new CheckpointBuildContext()
                    {
                        Storage = local,
                        Table = "t",
                        Scratch = buildScratch,
                        Memory = new JobMemory(),
                        Clock = clock
                    }, new BuildRequest() { Version = 200 });
                    Assert.Equal(BuildStatus.Completed, build.Status);
                }

                var handOff = new DiscardHandOff();
                using var producer = handOff.Acquire(DiscardRoute.Of(storage, "t"));
                var counters = new DiscardCounters();
                var metered = MeteredFileStorage.Wrap(storage, new MaintenanceBuckets(TimeProvider.System), ListingPaging.LocalDisk);
                var cleanup = new DeltaTableCleanup(CleanupConfiguration.Validate(storage, "t", false, CleanupOptIns.Off),
                    metered, clock, "t", handOff, () => storage, counters, null, "t");
                var groups = new List<DiscardGroup>();
                for (var version = 1; version <= 125; version++)
                {
                    var path = $"_change_data/p{version}.parquet";
                    await local.WriteText("/t/" + path, "discarded");
                    var group = new DiscardGroup("x", 0, version, $"stage-{version}", new[] { path }) { QueuedAt = clock.GetUtcNow() - handOff.DrainSpacing };
                    Assert.Equal(0, producer.Entry.Enqueue(group));
                    groups.Add(group);
                }

                var recoverTail = false;
                var attempts = new int[126];
                storage.Before = (verb, path) =>
                {
                    if (verb == "OpenRead")
                    {
                        for (var version = 1; version <= 125; version++)
                        {
                            if (path.Full == CommitPath("t", version))
                            {
                                attempts[version]++;
                                if (version <= 120 || !recoverTail)
                                {
                                    throw new IOException("injected anchor failure");
                                }
                                break;
                            }
                        }
                    }
                    return Task.CompletedTask;
                };

                // Four hints per minute and thirty-minute capped retries let the first 120 groups saturate every turn
                for (var minute = 0; minute <= 140; minute++)
                {
                    if (minute == 100)
                    {
                        Assert.All(groups, group => Assert.True(group.KeptTurns > 0));
                        recoverTail = true;
                    }
                    using var turnScratch = scratchOwner.CreateJob();
                    using var cancellation = new CancellationTokenSource();
                    var context = new CleanupContext(new JobRun(0, JobKind.Deletion) { Cancellation = cancellation }, clock)
                    {
                        Kind = JobKind.Deletion,
                        Long = false,
                        ReadLimit = MaintenanceWorkerSettings.DefaultShortReads,
                        Deadline = clock.GetUtcNow() + TimeSpan.FromMinutes(15),
                        Head = 200,
                        TableId = "x",
                        Scratch = turnScratch,
                        Memory = new JobMemory(),
                        Transfer = new TransferAllowance(),
                        Stop = default
                    };
                    await cleanup.DrainHintsAsync(context, 4);
                    await cleanup.RunAsync(JobKind.Deletion, context);
                    await context.SettleAsync();
                    clock.Advance(TimeSpan.FromMinutes(1));
                }

                Assert.Equal(0L, counters.Dropped);
                var tail = Path.Combine(root, "t", "_change_data", "p125.parquet");
                Assert.True(!File.Exists(tail), $"Recovered anchor 125 was still kept after forty more turns; it had {attempts[125]} reads while anchor 1 had {attempts[1]}");
            }
            finally
            {
                owners.Release();
                Directory.Delete(root, true);
            }
        }

        private static DeltaAction Own(long version) => new DeltaAction()
        {
            CommitInfo = new DeltaCommitInfoAction() { StageId = $"stage-{version}", AdoptedAt = 0, Timestamp = 1000 + version, Operation = "WRITE" }
        };
    }
}
