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

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class OrphanSweepSwitchTests
    {
        [Fact]
        public async Task KillSwitchDropsOrphanRun()
        {
            using var kit = new OrphanRunKit();
            var table = await kit.TableAsync();
            OrphanRunKit.Plant(table, "old-a.parquet", "old-b.parquet");
            var cleanup = OrphanRunKit.Cleanup(table);
            var (evidence, _) = await kit.EvidenceAsync(table.Configuration, table.Metered, table.Head);
            var targets = kit.Targets(table.Configuration, "old-a.parquet", "old-b.parquet");
            var run = kit.Run(table.Configuration, table.Metered, evidence, targets, cleanup.Settings, cleanup.Counters);
            cleanup.RegisterOrphan(run);
            Assert.True(cleanup.IsDue(JobKind.Deletion, table.Head, AdvanceExecutionKit.Now));

            Switch(true);
            var result = await kit.TurnAsync(cleanup, table.Head);

            Assert.True(result.Kind == OutcomeKind.Declined && result.Reason == MaintenanceReason.DeletesDisabled, $"{result.Kind} {result.Reason}: {result.Detail}");
            Assert.True(run.Ended);
            Assert.True(OrphanRunKit.Exists(table, "old-a.parquet"));
            Assert.True(OrphanRunKit.Exists(table, "old-b.parquet"));
            Assert.False(File.Exists(targets.Path));
            Assert.Equal(0, kit.Owner.Ledger.Reserved);
            Assert.False(cleanup.IsDue(JobKind.Deletion, table.Head, AdvanceExecutionKit.Now));
        }
    }
}
