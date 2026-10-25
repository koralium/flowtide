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
    public partial class OrphanSweepTests
    {
        [Theory]
        [InlineData(StoreFlavor.S3)]
        [InlineData(StoreFlavor.Azure)]
        [InlineData(StoreFlavor.AzureHns)]
        public async Task CloudCaseVariantsAreDistinctAndHiddenRulesStayExact(StoreFlavor flavor)
        {
            await using var server = new LoopbackObjectStore(flavor);
            var pinned = await PinnedOrphanRun.SeedAsync(this, server);
            // The head's live file and its case variant
            pinned.PutOld("part-0002.parquet", 3);
            pinned.PutOld("PART-0002.parquet", 3);
            pinned.PutOld("_DELTA_LOG/x.parquet", 3);
            pinned.PutOld("Nested/_Delta_Log/00000000000000000000.json", 2);
            pinned.PutOld("Nested/y.parquet", 3);
            pinned.PutOld("_Change_Data/c.parquet", 3);
            pinned.PutOld("_change_data/c-old.parquet", 3);
            var table = pinned.Table;
            var kit = await OrphanInvariants.CaptureAsync(table, server);
            var sweep = Sweep(table.Configuration, table.Owned, counters: pinned.Counters);
            var runs = Registered(sweep);

            var (result, seen) = await RunAsync(sweep, table, pinned.Context);

            AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.Equal(new[] { "PART-0002.parquet", "_change_data/c-old.parquet", "part-0002.parquet" }, seen!.Frozen.Select(x => x.Relative).Order(StringComparer.Ordinal));
            var hns = flavor == StoreFlavor.AzureHns ? 1 : 0;
            // The commits, _DELTA_LOG/x.parquet and the nested commit, and on a hierarchical namespace their three directories
            Assert.Equal(PinnedOrphanRun.Head + 3 + 3 * hns, seen.Counts.Log);
            // Nested/y.parquet, and on a hierarchical namespace the root's own directory
            Assert.Equal(1, seen.Counts.NestedRoots);
            Assert.Equal(1 + hns, seen.Counts.Nested);
            Assert.Equal(new[] { "PART-0002.parquet", "_change_data/c-old.parquet" }, kit.Record(Assert.Single(runs)));
            server.Requests.Clear();

            var turns = await DeleteTurnsAsync(runs[0], PinnedOrphanRun.Head);

            Assert.All(turns, x => AssertResult(x, OutcomeKind.Completed, MaintenanceReason.None));
            var deletes = server.Requests.Where(x => x.StartsWith("DELETE ", StringComparison.Ordinal)).Order(StringComparer.Ordinal);
            Assert.Equal(new[] { "DELETE /c/tables/t/PART-0002.parquet HTTP/1.1", "DELETE /c/tables/t/_change_data/c-old.parquet HTTP/1.1" }, deletes);
            Assert.True(server.Contains("tables/t/part-0002.parquet"));
            Assert.False(server.Contains("tables/t/PART-0002.parquet"));
            Assert.All(new[] { "_DELTA_LOG/x.parquet", "Nested/_Delta_Log/00000000000000000000.json", "Nested/y.parquet", "_Change_Data/c.parquet" },
                x => Assert.True(server.Contains("tables/t/" + x), x));
            await kit.AssertAsync(runs, 1, contexts: pinned.Context);
        }
    }
}
