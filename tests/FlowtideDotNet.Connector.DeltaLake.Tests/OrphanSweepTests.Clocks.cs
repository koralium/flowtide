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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class OrphanSweepTests
    {
        [Theory]
        [InlineData(StoreFlavor.S3)]
        [InlineData(StoreFlavor.Azure)]
        [InlineData(StoreFlavor.AzureHns)]
        // Local disk
        [InlineData(null)]
        public async Task UnixEpochAndFutureTimesDecideOnlyEligibility(StoreFlavor? flavor)
        {
            await using var server = flavor is { } cloud ? new LoopbackObjectStore(cloud) : null;
            var table = await ReclaimedTableAsync(server);
            var epoch = DateTimeOffset.UnixEpoch;
            // A year after T0: the store's time, or local disk's own clock
            var now = DateTimeOffset.UtcNow;
            var future = (server != null ? Now : new DateTimeOffset(now.Ticks - now.Ticks % TimeSpan.TicksPerSecond, TimeSpan.Zero)) + TimeSpan.FromDays(365);
            await PublishAsync(table, 3, "epoch-live.parquet");
            // The next commit's stage
            await DeltaTransactionWriter.StageCommit(table.Raw, table.Name, 4, new List<DeltaAction>() { CommitInfo("stage-4", 4), DeltaLakeCheckpointBuildTests.Add("epoch-staged.parquet") });
            void Stamp(string name, DateTimeOffset modified)
            {
                if (server != null)
                {
                    server.Put("tables/t/" + name, new byte[3], modified);
                    return;
                }
                var path = Path.Combine(table.Folder!, name);
                File.WriteAllBytes(path, new byte[3]);
                File.SetLastWriteTimeUtc(path, modified.UtcDateTime);
            }
            var stamped = new Dictionary<string, DateTimeOffset>()
            {
                ["epoch-orphan.parquet"] = epoch,
                ["epoch-live.parquet"] = epoch,
                ["epoch-staged.parquet"] = epoch,
                ["future.parquet"] = future
            };
            foreach (var (name, modified) in stamped)
            {
                Stamp(name, modified);
            }
            var delivered = new Dictionary<string, DateTimeOffset?>(StringComparer.Ordinal);
            table.Owned.AfterRecursiveLs = (_, listed) =>
            {
                foreach (var entry in listed)
                {
                    delivered[entry.Path.Full] = entry.LastModificationTime;
                }
                return listed;
            };
            var kit = await OrphanInvariants.CaptureAsync(table, server);
            var sweep = Sweep(table.Configuration, table.Owned);
            var runs = Registered(sweep);
            var context = OrphanContext(3);

            var (result, seen) = await RunAsync(sweep, table, context);

            // The client delivers each time as stamped, none as missing
            foreach (var (name, modified) in stamped)
            {
                Assert.True(delivered.TryGetValue(table.Prefix + name, out var time) && time == modified, $"{name} delivered as {time?.ToString("o") ?? "no time"}, stamped {modified:o}");
            }
            AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.Equal(new[] { "epoch-live.parquet", "epoch-orphan.parquet", "epoch-staged.parquet" }, seen!.Frozen.Select(x => x.Relative).Order(StringComparer.Ordinal));
            Assert.All(seen.Frozen, x => Assert.Equal(epoch, x.Modified));
            Assert.Equal(1, seen.Counts.Young);
            Assert.Equal(0, seen.Counts.NoTime);
            var run = Assert.Single(runs);
            Assert.Equal(new[] { "epoch-orphan.parquet" }, kit.Record(run));

            await DeleteTurnsAsync(run, 3);

            Assert.False(Present(table, server, "epoch-orphan.parquet"));
            Assert.All(new[] { "epoch-live.parquet", "epoch-staged.parquet", "future.parquet" }, x => Assert.True(Present(table, server, x), x));
            await kit.AssertAsync(runs, 1, contexts: context);
        }

        [Theory]
        [InlineData(StoreFlavor.S3)]
        [InlineData(StoreFlavor.Azure)]
        [InlineData(StoreFlavor.AzureHns)]
        public async Task StoreClockGoingBackFreezesNothingNew(StoreFlavor flavor)
        {
            await using var server = new LoopbackObjectStore(flavor);
            var table = await ReclaimedTableAsync(server);
            foreach (var name in s_referenced.Append("o.parquet"))
            {
                PlantBelow(table, server, name, true);
            }
            var ahead = Now + TimeSpan.FromDays(1);
            server.Put("tables/t/f.parquet", new byte[3], ahead);
            var kit = await OrphanInvariants.CaptureAsync(table, server);
            var sweep = Sweep(table.Configuration, table.Owned);
            var runs = Registered(sweep);
            // The store's time at each sweep, its frozen candidates and its one target
            var sweeps = new (DateTimeOffset Store, string[] Frozen, string? Target)[]
            {
                (Now, s_referenced.Append("o.parquet").ToArray(), "o.parquet"),
                // Two days back
                (Now - TimeSpan.FromDays(2), s_referenced, null),
                // One second past f's cutoff
                (ahead + Retention + TimeSpan.FromHours(1) + TimeSpan.FromSeconds(1), s_referenced.Append("f.parquet").ToArray(), "f.parquet")
            };

            var registered = 0;
            for (int i = 0; i < sweeps.Length; i++)
            {
                var (store, frozen, target) = sweeps[i];
                server.Now = store;
                var context = OrphanContext(PinnedOrphanRun.Head);

                var (result, seen) = await RunAsync(sweep, table, context);

                AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
                Assert.Equal(frozen.Order(StringComparer.Ordinal), seen!.Frozen.Select(x => x.Relative).Order(StringComparer.Ordinal));
                // T0 is the store's time, not the host's
                Assert.Equal(store - Retention - TimeSpan.FromHours(1), seen.Cutoff);
                registered += target == null ? 0 : 1;
                Assert.Equal(registered, runs.Count);
                if (target != null)
                {
                    Assert.Equal(new[] { target }, kit.Record(runs[^1]));
                    await DeleteTurnsAsync(runs[^1], PinnedOrphanRun.Head);
                    Assert.False(server.Contains("tables/t/" + target), target);
                    // The next sweep's own calls hold no delete
                    table.Owned.Calls.Clear();
                }
                // The head's live file stays
                Assert.True(server.Contains("tables/t/part-0002.parquet"));
                Assert.True(server.Contains("tables/t/f.parquet") == (i < 2), $"f.parquet after sweep {i + 1}");
                await kit.AssertAsync(runs, i + 1, contexts: context);
            }
        }
    }
}
