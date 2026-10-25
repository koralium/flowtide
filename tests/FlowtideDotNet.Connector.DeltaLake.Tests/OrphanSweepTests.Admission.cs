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

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class OrphanSweepTests
    {
        [Fact]
        public async Task OrphanRunAloneOverAdmissionConcludesAsOversized()
        {
            var table = await LocalWithOldAsync(2, "old.parquet");
            var ledger = new CapacityLedger(() => default);
            var sweep = Sweep(table.Configuration, table.Owned);
            var runs = Registered(sweep);
            var warned = new List<MaintenanceReason>();
            sweep.Warn = (reason, _) => warned.Add(reason);
            // No ledger admits one delete at this latency, however empty it is
            sweep.Latency = () => TimeSpan.FromDays(30);

            var (result, seen) = await RunAsync(sweep, table, OrphanContext(2, capacity: ledger));

            Assert.Equal("old.parquet", Assert.Single(seen!.Frozen).Relative);
            Assert.True(result.Kind == OutcomeKind.Declined && result.Reason == MaintenanceReason.DeleteBacklog && result.Oversized,
                $"{result.Kind} {result.Reason} oversized {result.Oversized}: {result.Detail}");
            Assert.Equal(new[] { MaintenanceReason.DeleteBacklog }, warned);
            Assert.Empty(runs);
            Assert.Equal(default, ledger.Total(CapacityState.Runnable));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task IndependentCloudOrphanRunsKeepBothCapacityReservations(bool disposeSecond)
        {
            await using var firstServer = new LoopbackObjectStore(StoreFlavor.Azure);
            await using var secondServer = new LoopbackObjectStore(StoreFlavor.Azure);
            var firstTable = LoopbackTable(firstServer);
            var secondTable = LoopbackTable(secondServer);
            foreach (var (server, table) in new[] { (firstServer, firstTable), (secondServer, secondTable) })
            {
                server.Now = s_old;
                await RetainedTableAsync(table.Raw, table.Name, 2);
                server.Put("tables/t/old.parquet", new byte[3], s_old);
                server.Now = Now;
            }
            Assert.Equal(ProducerKey(firstTable), ProducerKey(secondTable));
            var ledger = new CapacityLedger(() => default);
            using var firstOwner = new CancellationTokenSource();
            using var secondOwner = new CancellationTokenSource();
            CleanupContext Context(CancellationTokenSource owner)
            {
                var clock = new FakeTimeProvider(Now);
                return new CleanupContext(new JobRun(2, JobKind.Orphan) { Cancellation = owner }, clock)
                {
                    Kind = JobKind.Orphan,
                    Long = true,
                    ReadLimit = long.MaxValue,
                    Deadline = Now + TimeSpan.FromHours(6),
                    Head = 2,
                    TableId = null,
                    Scratch = Scratch(DiskLedger.DefaultCap),
                    Memory = new JobMemory(),
                    Transfer = new TransferAllowance(),
                    Stop = owner.Token,
                    Producers = new ProducerGenerations(),
                    Capacity = ledger,
                    CapacityOwner = owner.Token
                };
            }
            var firstSweep = Sweep(firstTable.Configuration, firstTable.Owned);
            var firstRuns = Registered(firstSweep);
            var (firstResult, _) = await RunAsync(firstSweep, firstTable, Context(firstOwner));
            AssertResult(firstResult, OutcomeKind.Completed, MaintenanceReason.None);
            var firstRun = Assert.Single(firstRuns);
            Assert.Equal(new[] { "old.parquet" }, TargetsOf(firstRun, firstTable));
            var firstDemand = ledger.Total(CapacityState.Runnable);
            Assert.True(firstDemand.Requests > 0);

            var secondSweep = Sweep(secondTable.Configuration, secondTable.Owned);
            var secondRuns = Registered(secondSweep);
            var (secondResult, _) = await RunAsync(secondSweep, secondTable, Context(secondOwner));
            AssertResult(secondResult, OutcomeKind.Completed, MaintenanceReason.None);
            var secondRun = Assert.Single(secondRuns);
            Assert.Equal(new[] { "old.parquet" }, TargetsOf(secondRun, secondTable));
            Assert.False(firstRun.Ended);
            Assert.False(secondRun.Ended);
            if (disposeSecond)
            {
                secondRun.Dispose();
                Assert.False(firstRun.Ended);
                Assert.Equal(firstDemand, ledger.Total(CapacityState.Runnable));
            }
            else
            {
                Assert.Equal(firstDemand + firstDemand, ledger.Total(CapacityState.Runnable));
            }
        }
    }
}
