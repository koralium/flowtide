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
using Xunit.Sdk;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class OrphanSweepTests
    {
        // A swept and drained cell whose stage the cell removed afterwards
        private sealed record KitCell(OrphanTable Table, OrphanInvariants Kit, List<OrphanRun> Runs, CleanupContext Context, string Stage)
        {
            public Task AssertAsync() => Kit.AssertAsync(Runs, 1, removedByTheCell: new[] { Stage }, contexts: Context);
        }

        // The reclaim cell with a stage protecting staged.parquet, captured before its sweep
        private async Task<KitCell> KitCellAsync(LoopbackObjectStore? server)
        {
            var table = await ReclaimedTableAsync(server);
            foreach (var name in s_unreferenced.Concat(s_referenced).Concat(s_excluded).Append("staged.parquet"))
            {
                PlantBelow(table, server, name, true);
            }
            PlantBelow(table, server, "young.parquet", false);
            var staged = await DeltaTransactionWriter.StageCommit(table.Raw, table.Name, 3, new List<DeltaAction>() { CommitInfo("stage-3", 3), DeltaLakeCheckpointBuildTests.Add("staged.parquet") });
            var stage = "_delta_log/" + staged.FileName;
            var kit = await OrphanInvariants.CaptureAsync(table, server);
            var sweep = Sweep(table.Configuration, table.Owned);
            var runs = Registered(sweep);
            var context = OrphanContext(PinnedOrphanRun.Head);

            var (result, _) = await RunAsync(sweep, table, context);

            AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.Equal(s_unreferenced, kit.Record(Assert.Single(runs)).Order(StringComparer.Ordinal));
            await DeleteTurnsAsync(runs[0], PinnedOrphanRun.Head);
            // Published or abandoned after the sweep
            await table.Raw.Rm(table.Prefix + stage);
            return new KitCell(table, kit, runs, context, stage);
        }

        [Theory]
        [InlineData(StoreFlavor.S3)]
        [InlineData(StoreFlavor.AzureHns)]
        // Local disk
        [InlineData(null)]
        public async Task InvariantKitPassesACleanCell(StoreFlavor? flavor)
        {
            await using var server = flavor is { } cloud ? new LoopbackObjectStore(cloud) : null;
            var cell = await KitCellAsync(server);

            // Without reading the removed stage
            await cell.AssertAsync();
        }

        [Theory]
        // Protected by commit 2
        [InlineData(null, "part-0002.parquet", "A retained reference was deleted")]
        [InlineData(StoreFlavor.S3, "part-0002.parquet", "A retained reference was deleted")]
        [InlineData(StoreFlavor.AzureHns, "part-0002.parquet", "A retained reference was deleted")]
        // Protected only by the stage the cell removed
        [InlineData(null, "staged.parquet", "A retained reference was deleted")]
        [InlineData(StoreFlavor.S3, "staged.parquet", "A retained reference was deleted")]
        [InlineData(StoreFlavor.AzureHns, "staged.parquet", "A retained reference was deleted")]
        // Unreferenced, young and no run's target
        [InlineData(null, "young.parquet", "Deleted without a target")]
        [InlineData(StoreFlavor.S3, "young.parquet", "Deleted without a target")]
        [InlineData(StoreFlavor.AzureHns, "young.parquet", "Deleted without a target")]
        // Never stored, seen only in the request line
        [InlineData(StoreFlavor.S3, "ghost.parquet", "A DELETE named no target")]
        public async Task InvariantKitCatchesAProtectedOrUntargetedDeletion(StoreFlavor? flavor, string name, string check)
        {
            await using var server = flavor is { } cloud ? new LoopbackObjectStore(cloud) : null;
            var cell = await KitCellAsync(server);
            if (server != null)
            {
                using var http = new HttpClient();
                await http.DeleteAsync(new Uri(server.Endpoint, "c/tables/t/" + name));
            }
            else
            {
                File.Delete(Path.Combine(cell.Table.Folder!, name));
            }

            var thrown = await Assert.ThrowsAnyAsync<XunitException>(cell.AssertAsync);

            Assert.Contains($"{check}: {name}", thrown.Message);
        }

        [Theory]
        // A folder the run emptied
        [InlineData("directory", "A directory was removed: sub")]
        // Run bounds and leftovers
        [InlineData("cap", "A run of 3 targets is over the 2 deletion cap")]
        [InlineData("attempts", "1 root listings for 2 attempts")]
        [InlineData("memory", "A context holds 1 managed bytes")]
        [InlineData("scratch", "A context holds scratch files: ")]
        [InlineData("targets", "An ended run left its targets file: ")]
        // Protected only by a stage written after capture
        [InlineData("stage", "A retained reference was deleted: young.parquet")]
        public async Task InvariantKitCatchesEachBrokenInvariant(string broken, string check)
        {
            var cell = await KitCellAsync(null);
            var (attempts, maxDeletions) = (1, OrphanSweep.DefaultMaxDeletions);
            switch (broken)
            {
                case "directory":
                    Directory.Delete(Path.Combine(cell.Table.Folder!, "sub"));
                    break;
                case "cap":
                    maxDeletions = 2;
                    break;
                case "attempts":
                    attempts = 2;
                    break;
                case "memory":
                    cell.Context.Memory.Reserve(1);
                    break;
                case "scratch":
                    cell.Context.Scratch.Create(cell.Context.Scratch.NewPath(".x")).Dispose();
                    break;
                case "targets":
                    File.WriteAllBytes(cell.Runs[0].Targets.Path, new byte[1]);
                    break;
                case "stage":
                    await DeltaTransactionWriter.StageCommit(cell.Table.Raw, cell.Table.Name, 3, new List<DeltaAction>() { CommitInfo("stage-3", 3), DeltaLakeCheckpointBuildTests.Add("young.parquet") });
                    File.Delete(Path.Combine(cell.Table.Folder!, "young.parquet"));
                    break;
            }

            var thrown = await Assert.ThrowsAnyAsync<XunitException>(() => cell.Kit.AssertAsync(cell.Runs, attempts, maxDeletions, new[] { cell.Stage }, cell.Context));

            Assert.Contains(check, thrown.Message);
        }

        [Fact]
        public async Task InvariantKitCatchesADirectoryDeleteOnHns()
        {
            await using var server = new LoopbackObjectStore(StoreFlavor.AzureHns);
            var cell = await KitCellAsync(server);
            using var http = new HttpClient();
            // The nested table's folder, which the store answers as absent
            await http.DeleteAsync(new Uri(server.Endpoint, "c/tables/t/nested"));

            var thrown = await Assert.ThrowsAnyAsync<XunitException>(cell.AssertAsync);

            Assert.Contains("A DELETE named a directory: nested", thrown.Message);
        }
    }
}
