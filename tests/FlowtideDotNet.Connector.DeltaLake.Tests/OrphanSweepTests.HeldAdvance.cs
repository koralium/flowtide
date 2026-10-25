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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance;
using Microsoft.Extensions.Time.Testing;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class OrphanSweepTests
    {
        // Runnable demand past the ledger's close, so no new advance plans
        private static CapacityDemand ClosedLedger => new CapacityDemand((long)(CapacityLedger.CloseSeconds + 1) * MaintenanceBuckets.DefaultRequestsPerSecond, 0);

        // Every cleanup record of the table, as an operator removes them
        private static void RemoveCleanupRecords(ExecutionTable table)
        {
            foreach (var file in Directory.GetFiles(Path.Combine(table.Root!, "t", "_delta_log"), ".flowtide-cleanup.*"))
            {
                File.Delete(file);
            }
        }

        [Theory]
        [InlineData(nameof(RetentionState.Paused))]
        [InlineData(nameof(RetentionState.Suspended))]
        public async Task UnresolvedAdvanceHoldsTheOrphanSweepUntilItResolves(string held)
        {
            using var kit = new AdvanceExecutionKit();
            var table = await kit.ChurnedAsync();
            DeltaTableCleanup cleanup;
            if (held == nameof(RetentionState.Paused))
            {
                // A started advance whose retention this sink does not publish
                await kit.PublishAsync(AdvanceExecutionKit.Cleanup(table), table);
                var eightDays = CleanupConfiguration.Validate(table.Raw, "t", false, new CleanupOptIns(TimeSpan.FromDays(8), null));
                cleanup = new DeltaTableCleanup(eightDays, table.Metered, new FakeTimeProvider(Now), "t", null, () => table.Raw, new DiscardCounters(), null, "t");
            }
            else
            {
                // A started advance whose boundary is not the checkpoint its header names
                var c = (await DeltaTransactionReader.ListLog(table.Raw, "t")).Checkpoints.Single(x => x.Version == 12).IOEntry;
                var fixture = CleanupRecordKit.Fixture("table-id", 12, 0, 0, table.Head, 0) with
                {
                    Destination = new CheckpointSummary(c.Size!.Value, CleanupRecordKit.Hash("another checkpoint"), c.Size.Value, c.LastModificationTime!.Value.UtcTicks)
                };
                var part = CleanupRecordKit.Part(fixture, 0, "/t/part-0003.parquet");
                var header = CleanupRecordKit.Header(fixture, part);
                foreach (var record in new[] { part, header, CleanupRecordKit.Started(fixture, header) })
                {
                    await CleanupRecordKit.PlantAsync(table.Raw, "t", record);
                }
                cleanup = AdvanceExecutionKit.Cleanup(table);
            }
            AdvanceExecutionTests.WithoutExclusions(cleanup);
            var ambient = default(CapacityDemand);
            var ledger = new CapacityLedger(() => ambient);
            var key = TableLeases.KeyOf(RawStorage.Unwrap(table.Raw), table.Configuration.Store.Table);
            await AdvanceExecutionKit.JobAsync(cleanup, kit.AdvanceContext(table.Head, ledger));
            Assert.Equal(Enum.Parse<RetentionState>(held), cleanup.AdvanceGaugeState);
            Assert.Equal(Enum.Parse<CapacityState>(held), ledger.StateOf(default, key));

            // Held whenever the worker looks, so no attempt restores the lease as runnable
            Assert.Equal(OrphanSweepRequestResult.Scheduled, cleanup.RequestOrphanSweep());
            foreach (var later in new[] { TimeSpan.Zero, TimeSpan.FromMinutes(30), TimeSpan.FromHours(12) })
            {
                Assert.False(cleanup.IsDue(JobKind.Orphan, table.Head, Now + later), $"Due {later} later while the run holds the advance {held}");
            }
            Assert.Equal(Enum.Parse<CapacityState>(held), ledger.StateOf(default, key));

            // The kept request runs once a preparation finds nothing to hold
            RemoveCleanupRecords(table);
            ambient = ClosedLedger;
            Assert.Equal(MaintenanceReason.DeleteBacklog, (await AdvanceExecutionKit.JobAsync(cleanup, kit.AdvanceContext(table.Head, ledger))).Reason);
            Assert.Equal(RetentionState.Idle, cleanup.AdvanceGaugeState);
            Assert.True(cleanup.IsDue(JobKind.Orphan, table.Head, Now));
            Assert.Equal(OrphanSweepRequestResult.AlreadyPending, cleanup.RequestOrphanSweep());
        }

        [Fact]
        public async Task OrphanPreparationLeavesTheStateAnAdvancePreparationWould()
        {
            using var kit = new AdvanceExecutionKit();
            var table = await kit.ChurnedAsync();
            // Another advance's started record that no preparation can interpret
            await File.WriteAllTextAsync(Path.Combine(table.Root!, "t", "_delta_log", $".flowtide-cleanup.{12:D20}.advance.0123456789abcdef.attempt.started"), "{}");
            var cleanup = AdvanceExecutionKit.Cleanup(table);
            Assert.Equal(OrphanSweepRequestResult.Scheduled, cleanup.RequestOrphanSweep());

            // Without a ledger, only the suspension itself can show
            var result = await OrphanJobAsync(cleanup, kit.AdvanceContext(table.Head));

            Assert.Equal((OutcomeKind.Declined, MaintenanceReason.ManifestSuspended), (result.Kind, result.Reason));
            Assert.Equal(RetentionState.Suspended, cleanup.AdvanceGaugeState);
            // A later request waits while the suspension holds
            Assert.Equal(OrphanSweepRequestResult.Scheduled, cleanup.RequestOrphanSweep());
            Assert.False(cleanup.IsDue(JobKind.Orphan, table.Head, Now));
        }
    }
}
