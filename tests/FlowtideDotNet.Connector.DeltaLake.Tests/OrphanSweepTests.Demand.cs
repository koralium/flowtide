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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using Microsoft.Extensions.Time.Testing;
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class OrphanSweepTests
    {
        // The longest key a blob name takes
        private const int MaxBlobKey = 1024;

        // Commit 0 grown past 1 MiB with valid actions
        private static async Task PadMetadataSourceAsync(LoopbackObjectStore server, OrphanTable table)
        {
            string original;
            await using (var stream = await table.Raw.OpenRead(table.Name + "/_delta_log/00000000000000000000.json"))
            using (var reader = new StreamReader(stream!))
            {
                original = await reader.ReadToEndAsync();
            }
            var padding = string.Concat(Enumerable.Repeat("{\"txn\":{\"appId\":\"padding\",\"version\":0,\"lastUpdated\":0}}\n", 20_000));
            server.Put("tables/t/_delta_log/00000000000000000000.json", Encoding.UTF8.GetBytes(original.TrimEnd() + "\n" + padding));
        }

        // A name below the table whose key is the longest a blob takes
        private static string LongName(OrphanTable table, int index) => $"{index:D4}" + new string('x', MaxBlobKey - table.Prefix.Length - 4);

        // Every buffer-limited slice of the targets left rehashes the capability sources at its gate
        private static void AssertPricesEverySlice(CapacityDemand demand, long left, long perSlice, long sourceBytes)
        {
            var slices = (left + perSlice - 1) / perSlice;
            Assert.True(demand.Bytes >= slices * sourceBytes, $"{left} targets take {slices} buffer-limited slices that rehash {slices * sourceBytes} bytes, priced at {demand.Bytes}");
        }

        [Fact]
        public async Task LongOrphanNamesPriceEveryBufferLimitedSlice()
        {
            await using var server = new LoopbackObjectStore(StoreFlavor.Azure);
            using var kit = new OrphanRunKit();
            server.Now = PinnedOrphanRun.Old;
            var table = LoopbackTable(server);
            await RetainedTableAsync(table.Raw, table.Name, PinnedOrphanRun.Head);
            await PadMetadataSourceAsync(server, table);
            const int targets = 1501;
            for (int i = 0; i < targets; i++)
            {
                server.Put("tables/t/" + LongName(table, i), new byte[3]);
            }
            server.Now = Now;
            var (evidence, _) = await kit.EvidenceAsync(table.Configuration, Metered(table), PinnedOrphanRun.Head);
            var sourceBytes = evidence.Capability.Sources.Sum(x => x.Length);
            Assert.True(sourceBytes >= 1L << 20);
            var perSlice = DeletionRunSettings.DefaultMaxBufferBytes / (DeletionRun.TargetOverheadBytes + 2L * MaxBlobKey);
            Assert.Equal(500, perSlice);
            var ledger = new CapacityLedger(() => default);
            var sweep = Sweep(table.Configuration, table.Owned);
            var runs = Registered(sweep);
            sweep.RunSettings = static () => new DeletionRunSettings() { RetryDelay = TimeSpan.Zero };

            var (result, _) = await RunAsync(sweep, table, OrphanContext(PinnedOrphanRun.Head, capacity: ledger));

            AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
            var run = Assert.Single(runs);
            Assert.Equal(targets, run.Targets.Count);
            AssertPricesEverySlice(ledger.Total(CapacityState.Runnable), targets, perSlice, sourceBytes);

            var turn = await DeleteTurnAsync(run, PinnedOrphanRun.Head);

            AssertResult(turn, OutcomeKind.Completed, MaintenanceReason.None);
            // The buffer, not the path limit, ended the slice
            Assert.Equal(perSlice, run.Cursor);
            Assert.False(server.Contains("tables/t/" + LongName(table, 0)));
            Assert.True(server.Contains("tables/t/" + LongName(table, (int)perSlice)));
            AssertPricesEverySlice(ledger.Total(CapacityState.Runnable), targets - perSlice, perSlice, sourceBytes);
        }

        [Fact]
        public async Task LongOrphanKeysPriceEveryRequiredCapabilityRehash()
        {
            await using var server = new LoopbackObjectStore(StoreFlavor.Azure);
            using var kit = new OrphanRunKit();
            var table = LoopbackTable(server);
            await RetainedTableAsync(table.Raw, table.Name, OrphanRunKit.Head);
            await PadMetadataSourceAsync(server, table);
            var metered = Metered(table);
            var (evidence, entries) = await kit.EvidenceAsync(table.Configuration, metered, OrphanRunKit.Head);
            var executor = new DeletionExecutor(table.Configuration, metered);
            Assert.Equal(KeepReason.None, executor.TryTarget(LongName(table, 0), TargetKind.Payload, out var target));
            Assert.Equal(MaxBlobKey, target.Key!.Length);
            // The deletion cap's worth of the longest keys
            const long targets = 100_000;
            var perSlice = DeletionRunSettings.DefaultMaxBufferBytes / (DeletionRun.TargetOverheadBytes + 2L * MaxBlobKey);
            var sourceBytes = evidence.Capability.Sources.Sum(x => x.Length);
            Assert.True(sourceBytes >= 1L << 20);

            var demand = OrphanRun.Demand(table.Configuration, evidence, entries, targets, targets * MaxBlobKey, CapacityEstimator.DefaultLatency, MaintenanceBuckets.DefaultRequestsPerSecond);

            Assert.Equal(200, (targets + perSlice - 1) / perSlice);
            AssertPricesEverySlice(demand, targets, perSlice, sourceBytes);
        }

        [Fact]
        public async Task FrozenMd5sPriceTheBuffersTheyFill()
        {
            await using var server = new LoopbackObjectStore(StoreFlavor.Azure);
            using var kit = new OrphanRunKit();
            var table = LoopbackTable(server);
            await RetainedTableAsync(table.Raw, table.Name, OrphanRunKit.Head);
            var (evidence, entries) = await kit.EvidenceAsync(table.Configuration, Metered(table), OrphanRunKit.Head);
            var sourceBytes = evidence.Capability.Sources.Sum(x => x.Length);
            Assert.True(sourceBytes > 0);
            // The deletion cap's worth of 1 KiB keys, each with a listed base64 MD5
            const long targets = 100_000;
            var md5Bytes = targets * (DeletionRun.Md5OverheadBytes + 2L * 24);
            var buffers = md5Bytes / DeletionRunSettings.DefaultMaxBufferBytes;
            Assert.True(buffers >= 7);

            var plain = OrphanRun.Demand(table.Configuration, evidence, entries, targets, targets * MaxBlobKey, CapacityEstimator.DefaultLatency, MaintenanceBuckets.DefaultRequestsPerSecond);
            var frozen = OrphanRun.Demand(table.Configuration, evidence, entries, targets, targets * MaxBlobKey, CapacityEstimator.DefaultLatency, MaintenanceBuckets.DefaultRequestsPerSecond, md5Bytes);

            Assert.True(frozen.Bytes - plain.Bytes >= buffers * sourceBytes,
                $"{md5Bytes} MD5 bytes fill {buffers} more slice buffers that rehash {buffers * sourceBytes} bytes, priced at {frozen.Bytes - plain.Bytes}");
        }

        [Fact]
        public async Task SlowOrphanSliceUpdatesMeasuredLatency()
        {
            using var kit = new OrphanRunKit();
            var table = await kit.TableAsync();
            OrphanRunKit.Plant(table, "slow-a.parquet", "slow-b.parquet");
            using var cleanup = OrphanRunKit.Cleanup(table);
            var (evidence, _) = await kit.EvidenceAsync(table.Configuration, table.Metered, table.Head);
            var clock = new FakeTimeProvider(AdvanceExecutionKit.Now);
            var elapsed = TimeSpan.FromSeconds(1);
            cleanup.BeforeDeleteForTests = _ =>
            {
                clock.Advance(elapsed);
                return Task.CompletedTask;
            };
            using var run = new OrphanRun(table.Configuration, table.Metered, clock, cleanup.Settings, evidence, kit.Targets(table.Configuration, "slow-a.parquet", "slow-b.parquet"), null, cleanup.Counters);
            cleanup.RegisterOrphan(run);

            var result = await kit.TurnAsync(cleanup, table.Head);

            Assert.Equal(OutcomeKind.Completed, result.Kind);
            Assert.Equal(1, run.Cursor);
            Assert.False(OrphanRunKit.Exists(table, "slow-a.parquet"));
            Assert.True(OrphanRunKit.Exists(table, "slow-b.parquet"));
            Assert.True(cleanup.Latency > CapacityEstimator.DefaultLatency,
                $"An orphan slice completed one delete in {elapsed}, but its remaining demand is still priced at {cleanup.Latency} per delete");
        }
    }
}
