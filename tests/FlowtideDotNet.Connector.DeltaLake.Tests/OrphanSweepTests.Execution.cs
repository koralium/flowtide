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

using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using Stowage.Impl;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class OrphanSweepTests
    {
        // Old files no commit names
        private static readonly string[] s_unreferenced = { "_change_data/cdc-old.parquet", "old-a.parquet", "sub/old-b.parquet" };

        // Old files the retained commits name
        private static readonly string[] s_referenced = { "part-0000.parquet", "part-0001.parquet", "part-0002.parquet" };

        // Old files the inventory never freezes
        private static readonly string[] s_excluded = { ".hidden.parquet", "_hidden/x.parquet", "nested/_delta_log/00000000000000000000.json", "nested/part.parquet" };

        // A short delete admission, as a worker's delete turn takes it
        internal CleanupContext DeleteTurnContext(long head)
        {
            var clock = new FakeTimeProvider(Now);
            return new CleanupContext(new JobRun(head, JobKind.Deletion) { Cancellation = new CancellationTokenSource() }, clock)
            {
                Kind = JobKind.Deletion,
                Long = false,
                ReadLimit = MaintenanceWorkerSettings.DefaultShortReads,
                Deadline = clock.GetUtcNow() + TimeSpan.FromMinutes(15),
                Head = head,
                TableId = null,
                Scratch = Scratch(DiskLedger.DefaultCap),
                Memory = new JobMemory(),
                Transfer = new TransferAllowance(),
                Stop = default
            };
        }

        // One settled delete turn of the run, which holds nothing afterwards
        internal async Task<CleanupResult> DeleteTurnAsync(OrphanRun run, long head)
        {
            var context = DeleteTurnContext(head);
            CleanupResult result;
            try
            {
                result = await run.RunSliceAsync(context);
            }
            finally
            {
                await context.SettleAsync();
            }
            Assert.Equal(0, context.Memory.Managed);
            Assert.Empty(context.Scratch.Files);
            return result;
        }

        // Delete turns until the run retires
        internal async Task<List<CleanupResult>> DeleteTurnsAsync(OrphanRun run, long head)
        {
            var results = new List<CleanupResult>();
            while (!run.Ended)
            {
                Assert.True(results.Count < 10, "The run never retired: " + string.Join(", ", results.Select(x => $"{x.Kind} {x.Reason}: {x.Detail}")));
                results.Add(await DeleteTurnAsync(run, head));
            }
            return results;
        }

        // Sweeps the pinned table with its run registered and returns that run
        internal async Task<OrphanRun> RegisterPinnedAsync(PinnedOrphanRun pinned)
        {
            var sweep = Sweep(pinned.Table.Configuration, pinned.Table.Owned, counters: pinned.Counters);
            var runs = Registered(sweep);
            CleanupResult result;
            try
            {
                result = await sweep.RunAsync(pinned.Context);
            }
            finally
            {
                await pinned.Context.SettleAsync();
            }
            AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
            return Assert.Single(runs);
        }

        // Own commits 0 to 2 written long ago: on local disk, or below c/tables/t on the loopback store over listing pages of three
        private async Task<OrphanTable> ReclaimedTableAsync(LoopbackObjectStore? server)
        {
            if (server == null)
            {
                return await LocalWithOldAsync(PinnedOrphanRun.Head);
            }
            server.Now = PinnedOrphanRun.Old;
            var table = LoopbackTable(server);
            await RetainedTableAsync(table.Raw, table.Name, PinnedOrphanRun.Head);
            server.Now = Now;
            server.PageSize = _ => 3;
            return table;
        }

        // Three bytes below the table, written long before the cutoff or at the sweep's time
        private static void PlantBelow(OrphanTable table, LoopbackObjectStore? server, string name, bool old)
        {
            if (server != null)
            {
                server.Put("tables/t/" + name, new byte[3], old ? PinnedOrphanRun.Old : Now);
            }
            else if (old)
            {
                Plant(table, name);
            }
            else
            {
                File.WriteAllBytes(Path.Combine(table.Folder!, name), new byte[3]);
            }
        }

        private static bool Present(OrphanTable table, LoopbackObjectStore? server, string name)
        {
            return server != null ? server.Contains("tables/t/" + name) : File.Exists(Path.Combine(table.Folder!, name));
        }

        [Theory]
        [InlineData(StoreFlavor.S3)]
        [InlineData(StoreFlavor.Azure)]
        [InlineData(StoreFlavor.AzureHns)]
        // Local disk
        [InlineData(null)]
        public async Task SweepThenDeleteTurnsReclaimOnlyUnprotected(StoreFlavor? flavor)
        {
            await using var server = flavor is { } cloud ? new LoopbackObjectStore(cloud) : null;
            var table = await ReclaimedTableAsync(server);
            foreach (var name in s_unreferenced.Concat(s_referenced).Concat(s_excluded))
            {
                PlantBelow(table, server, name, true);
            }
            PlantBelow(table, server, "young.parquet", false);
            var ledger = new CapacityLedger(() => default);
            var key = OrphanRun.LeaseKeyOf(ProducerKey(table));
            var counters = new RetentionCounters();
            var sweep = Sweep(table.Configuration, table.Owned, counters: counters);
            var runs = Registered(sweep);
            var context = OrphanContext(PinnedOrphanRun.Head, capacity: ledger);

            var (result, seen) = await RunAsync(sweep, table, context);

            AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.Equal(s_unreferenced.Concat(s_referenced).Order(StringComparer.Ordinal), seen!.Frozen.Select(x => x.Relative).Order(StringComparer.Ordinal));
            var run = Assert.Single(runs);
            Assert.Equal(s_unreferenced, TargetsOf(run, table).Order(StringComparer.Ordinal));
            // Held until the run retires
            Assert.Equal(CapacityState.Runnable, ledger.StateOf(default, key));
            Assert.Equal(new FileInfo(run.Targets.Path).Length, context.Scratch.Ledger.Reserved);
            server?.Requests.Clear();

            var turns = await DeleteTurnsAsync(run, PinnedOrphanRun.Head);

            Assert.All(turns, x => AssertResult(x, OutcomeKind.Completed, MaintenanceReason.None));
            Assert.All(s_unreferenced, x => Assert.False(Present(table, server, x), x));
            Assert.All(s_referenced.Concat(s_excluded).Append("young.parquet"), x => Assert.True(Present(table, server, x), x));
            Assert.Equal(s_unreferenced.Length, counters.Deletes(TargetKind.Payload, DeleteStatus.Deleted));
            Assert.Equal(0, counters.Deletes(TargetKind.Payload, DeleteStatus.Kept));
            if (server != null)
            {
                var deletes = server.Requests.Where(x => x.StartsWith("DELETE ", StringComparison.Ordinal)).Order(StringComparer.Ordinal);
                Assert.Equal(s_unreferenced.Select(x => $"DELETE /c/tables/t/{x} HTTP/1.1"), deletes);
            }
            Assert.Null(ledger.StateOf(default, key));
            Assert.Equal(default, ledger.Total(CapacityState.Runnable));
            Assert.False(File.Exists(run.Targets.Path));
            Assert.Equal(0, context.Scratch.Ledger.Reserved);
        }

        [Fact]
        public async Task SuccessiveSweepsReclaimWhatTheCapLeft()
        {
            var orphans = new[] { "o1.parquet", "o2.parquet", "o3.parquet", "o4.parquet", "o5.parquet" };
            var table = await LocalWithOldAsync(PinnedOrphanRun.Head, orphans.Concat(s_referenced).ToArray());
            var sweep = Sweep(table.Configuration, table.Owned);
            sweep.MaxDeletions = 2;
            var runs = Registered(sweep);

            var (first, _) = await RunAsync(sweep, table, OrphanContext(2));
            AssertResult(first, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.Contains("3 over the 2 deletion cap", first.Detail);
            Assert.Single(runs);
            Assert.Equal(new[] { "o1.parquet", "o2.parquet" }, TargetsOf(runs[^1], table));
            await DeleteTurnsAsync(runs[^1], 2);

            // A commit between the sweeps names a file written long ago
            Plant(table, "late.parquet");
            await DeltaTestKit.WriteOwnCommit(table.Raw, table.Name, 3, 0, BoundarySelectorTests.Old(3), DeltaLakeCheckpointBuildTests.Add("late.parquet"));
            var (second, seen) = await RunAsync(sweep, table, OrphanContext(3));
            AssertResult(second, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.Contains("late.parquet", seen!.Frozen.Select(x => x.Relative));
            Assert.Contains("1 over the 2 deletion cap", second.Detail);
            Assert.Equal(2, runs.Count);
            Assert.Equal(new[] { "o3.parquet", "o4.parquet" }, TargetsOf(runs[^1], table));
            await DeleteTurnsAsync(runs[^1], 3);

            var (third, _) = await RunAsync(sweep, table, OrphanContext(3));
            AssertResult(third, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.Equal(3, runs.Count);
            Assert.Equal(new[] { "o5.parquet" }, TargetsOf(runs[^1], table));
            await DeleteTurnsAsync(runs[^1], 3);

            // Nothing is left to register
            var (fourth, _) = await RunAsync(sweep, table, OrphanContext(3));
            AssertResult(fourth, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.Equal(3, runs.Count);
            Assert.All(orphans, x => Assert.False(File.Exists(Path.Combine(table.Folder!, x)), x));
            Assert.All(s_referenced.Append("late.parquet"), x => Assert.True(File.Exists(Path.Combine(table.Folder!, x)), x));
        }

        [Fact]
        public async Task SweepRegistersAtMostOneHundredThousandDeletions()
        {
            await using var server = new LoopbackObjectStore(StoreFlavor.S3);
            var table = await ReclaimedTableAsync(server);
            PlantBelow(table, server, "a-old.parquet", true);
            PlantBelow(table, server, "part-0002.parquet", true);
            // Two more than the cap, none of them stored, listed after every stored name
            const int synthesized = 100_001;
            table.Owned.AfterRecursiveLs = (folder, listed) =>
            {
                var entries = listed.ToList();
                for (int i = 0; i < synthesized; i++)
                {
                    entries.Add(new IOEntry($"{table.Prefix}z{i:D6}.parquet") { Size = 1, LastModificationTime = PinnedOrphanRun.Old });
                }
                return entries;
            };
            var sweep = Sweep(table.Configuration, table.Owned);
            var runs = Registered(sweep);
            Assert.Equal(100_000, sweep.MaxDeletions);
            // Two targets a slice
            sweep.RunSettings = static () => new DeletionRunSettings() { MaxPaths = 2, MaxOutstanding = 1, RetryDelay = TimeSpan.Zero };

            var (result, seen) = await RunAsync(sweep, table, OrphanContext(PinnedOrphanRun.Head));

            AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.Equal(synthesized + 2, seen!.Frozen.Count);
            Assert.Contains(", 1 protected, 0 kept, 2 over the 100000 deletion cap", result.Detail);
            var run = Assert.Single(runs);
            Assert.Equal(100_000, run.Targets.Count);
            var targets = TargetsOf(run, table);
            // The first in key order
            Assert.Equal("a-old.parquet", targets[0]);
            Assert.Equal("z000000.parquet", targets[1]);
            Assert.Equal("z099998.parquet", targets[^1]);
            server.Requests.Clear();

            var turn = await DeleteTurnAsync(run, PinnedOrphanRun.Head);

            AssertResult(turn, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.False(run.Ended);
            Assert.Equal(2, run.Cursor);
            Assert.Equal(new[] { "DELETE /c/tables/t/a-old.parquet HTTP/1.1", "DELETE /c/tables/t/z000000.parquet HTTP/1.1" }, server.Requests.Where(x => x.StartsWith("DELETE ", StringComparison.Ordinal)));
            Assert.False(server.Contains("tables/t/a-old.parquet"));
            Assert.True(server.Contains("tables/t/part-0002.parquet"));
        }

        [Fact]
        public async Task ManualSweepDeletesThroughTheWorker()
        {
            using var kit = new AdvanceExecutionKit();
            var rig = await RequestRigAsync(kit, null);
            string Native(string name) => ((ILocalDiskFileStorage)rig.Raw).ToNativeLocalPath(new IOPath("/t/" + name));
            var submitted = new List<string>();
            var registered = default(CapacityDemand);
            try
            {
                await QuiesceRequestRigAsync(rig);
                foreach (var name in new[] { "old.parquet", ".hidden.parquet", "young.parquet" })
                {
                    await File.WriteAllBytesAsync(Native(name), new byte[3]);
                    if (name != "young.parquet")
                    {
                        File.SetLastWriteTimeUtc(Native(name), s_old.UtcDateTime);
                    }
                }
                rig.Cleanup.BeforeDeleteForTests = target =>
                {
                    lock (submitted)
                    {
                        submitted.Add(Path.GetFileName(target.Key));
                    }
                    return Task.CompletedTask;
                };
                // The run's lease as the orphan job settles
                rig.Hold = point =>
                {
                    if (point == "release" && RunningJob(rig.Worker).Kind == JobKind.Orphan)
                    {
                        registered = rig.Budget.Capacity.Total(CapacityState.Runnable);
                    }
                    return Task.CompletedTask;
                };
                var before = rig.Kinds.Count;

                Assert.Equal(OrphanSweepRequestResult.Scheduled, rig.Request());
                await QuiesceRequestRigAsync(rig);

                Assert.Equal(new[] { JobKind.Orphan, JobKind.Deletion }, rig.Kinds.Skip(before).Where(x => x is JobKind.Orphan or JobKind.Deletion));
                Assert.Equal(OutcomeKind.Completed, rig.Worker.LastOutcomeForTests!.Kind);
                Assert.Equal(new[] { "old.parquet" }, submitted);
                Assert.False(File.Exists(Native("old.parquet")));
                Assert.True(File.Exists(Native(".hidden.parquet")));
                Assert.True(File.Exists(Native("young.parquet")));
                Assert.Equal(1, rig.Cleanup.Counters.Deletes(TargetKind.Payload, DeleteStatus.Deleted));
                Assert.NotEqual(default, registered);
                Assert.Equal(default, rig.Budget.Capacity.Total(CapacityState.Runnable));
                var runs = Path.Combine(rig.Budget.ScratchIfCreated!.ProcessPath, "runs");
                Assert.True(Directory.Exists(runs));
                Assert.Empty(Directory.GetFiles(runs));
                Assert.False(rig.Cleanup.IsDue(JobKind.Deletion, 0, rig.Clock.GetUtcNow()));
                // The completed attempt took the request
                Assert.False(rig.OrphanDue());
            }
            finally
            {
                await StopRequestRigAsync(rig);
            }
        }
    }
}
