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
using Microsoft.Extensions.Time.Testing;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class OrphanSweepSwitchTests
    {
        [Fact]
        public void KillSwitchStillSchedulesRequests()
        {
            var root = Path.Combine(_directory, ScratchOwners.ShortId());
            Directory.CreateDirectory(root);
            using var raw = Files.Of.LocalDisk(root);
            var clock = new FakeTimeProvider(AdvanceExecutionKit.Now);
            var budget = new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(1, Path.Combine(_directory, ScratchOwners.ShortId())), clock, manualTick: true);
            var configuration = CleanupConfiguration.Validate(raw, "t", false, new CleanupOptIns(AdvanceExecutionKit.Retention, TimeSpan.FromDays(7)));
            var cleanup = new DeltaTableCleanup(configuration, MeteredFileStorage.Wrap(raw, budget.Buckets, configuration.Store.Store.Paging), clock, "t", null, () => raw, new DiscardCounters(), null, "t");
            budget.Register(new DeltaMaintenanceWorker(budget, new MaintenanceWorkerSettings(0, null, clock) { PeriodicEnabled = false, InspectionEnabled = false }, raw, "t", "t", null) { Cleanup = cleanup });
            var source = new DeltaMaintenanceBudget.SharedBudget(existing: budget);

            // Taken while the switch is on
            Switch(true);
            Assert.Equal(OrphanSweepRequestResult.Scheduled, DeltaLakeMaintenance.RequestOrphanSweep(raw, "t", source));
            Assert.Equal(OrphanSweepRequestResult.AlreadyPending, DeltaLakeMaintenance.RequestOrphanSweep(raw, "t", source));
            Assert.True(cleanup.IsDue(JobKind.Orphan, 0, clock.GetUtcNow()));
        }
    }
}
