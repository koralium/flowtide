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

using FlowtideDotNet.Connector.DeltaLake.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class OrphanSweepTests
    {
        [Fact]
        public async Task StageWithAnUnusableReferenceDeclinesWithoutRegistering()
        {
            var table = await LocalWithOldAsync(2, "old-c.parquet");
            // Outside the table
            var stage = await StageAsync(table, 3, "../outside.parquet");
            var sweep = Sweep(table.Configuration, table.Owned);
            var runs = Registered(sweep);
            var context = OrphanContext(2);

            var (result, seen) = await RunAsync(sweep, table, context);

            AssertResult(result, OutcomeKind.Declined, MaintenanceReason.PathRejected);
            Assert.Equal("A listed stage holds a reference protection cannot use: Traversal path in Add", result.Detail);
            Assert.Equal("old-c.parquet", Assert.Single(seen!.Frozen).Relative);
            Assert.Empty(runs);
            Assert.Empty(RunFiles(context));
            Assert.True(File.Exists(Path.Combine(table.Folder!, "old-c.parquet")));

            // Without the stage the same table registers
            File.Delete(stage);
            var (control, _) = await RunAsync(sweep, table, OrphanContext(2));

            AssertResult(control, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.Equal(new[] { "old-c.parquet" }, TargetsOf(Assert.Single(runs), table));
        }

        [Theory]
        [InlineData("grown")]
        [InlineData("cut at a line end")]
        [InlineData("cut inside a line")]
        public async Task StageChangingUnderItsReadDeclinesTransient(string change)
        {
            var table = await LocalWithOldAsync(2, "old-c.parquet", "cut.parquet");
            // Its last line adds cut.parquet
            var stage = await StageAsync(table, 3, "cut.parquet");
            var original = await File.ReadAllBytesAsync(stage);
            var lastLine = Array.LastIndexOf(original, (byte)'\n', original.Length - 2) + 1;
            Assert.True(lastLine > 0 && original[^1] == (byte)'\n');
            var changed = change switch
            {
                "grown" => original.Concat(DeltaTransactionWriter.Serialize(new List<DeltaAction>() { DeltaLakeCheckpointBuildTests.Add("grown.parquet") }).ToArray()).ToArray(),
                "cut at a line end" => original[..lastLine],
                _ => original[..(lastLine + (original.Length - lastLine) / 2)]
            };
            var sweep = Sweep(table.Configuration, table.Owned);
            var runs = Registered(sweep);
            // After L2 listed the stage, before its read
            sweep.StepForTests = step => step == OrphanSweepStep.SecondListing ? File.WriteAllBytesAsync(stage, changed) : Task.CompletedTask;

            var (result, seen) = await RunAsync(sweep, table, OrphanContext(2));

            AssertResult(result, OutcomeKind.Declined, MaintenanceReason.Transient);
            Assert.Equal("A listed stage changed or could not be read", result.Detail);
            Assert.Equal(new[] { "cut.parquet", "old-c.parquet" }, seen!.Frozen.Select(x => x.Relative).Order(StringComparer.Ordinal));
            Assert.Empty(runs);
            Assert.True(File.Exists(Path.Combine(table.Folder!, "cut.parquet")));

            // The stage as listed lets the same table register
            sweep.StepForTests = null;
            await File.WriteAllBytesAsync(stage, original);
            var (control, _) = await RunAsync(sweep, table, OrphanContext(2));

            AssertResult(control, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.Equal(new[] { "old-c.parquet" }, TargetsOf(Assert.Single(runs), table));
        }

        [Theory]
        [InlineData(StoreFlavor.S3)]
        [InlineData(StoreFlavor.Azure)]
        // Local disk
        [InlineData(null)]
        public async Task RevivedStagePublishesWithEveryFilePresent(StoreFlavor? flavor)
        {
            await using var server = flavor is { } cloud ? new LoopbackObjectStore(cloud) : null;
            var table = await ReclaimedTableAsync(server);
            foreach (var name in s_referenced.Append("revived.parquet").Append("old-c.parquet"))
            {
                PlantBelow(table, server, name, true);
            }
            // Left by a sink that crashed before it published
            var staged = await DeltaTransactionWriter.StageCommit(table.Raw, table.Name, 3, new List<DeltaAction>() { CommitInfo("stage-3", 3), DeltaLakeCheckpointBuildTests.Add("revived.parquet") });
            var kit = await OrphanInvariants.CaptureAsync(table, server);
            var sweep = Sweep(table.Configuration, table.Owned);
            var runs = Registered(sweep);
            var context = OrphanContext(PinnedOrphanRun.Head);

            var (result, seen) = await RunAsync(sweep, table, context);

            AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.Contains("revived.parquet", seen!.Frozen.Select(x => x.Relative));
            Assert.Equal(new[] { "old-c.parquet" }, kit.Record(Assert.Single(runs)));
            await DeleteTurnsAsync(runs[0], PinnedOrphanRun.Head);

            // As the restored sink publishes its pending commit
            var pending = new DeltaLakePendingCommit() { Version = 3, StagedFile = staged.FileName, StageId = staged.StageId, Length = staged.Length, CheckpointId = 1 };
            var published = await DeltaTransactionWriter.PublishCommit(table.Raw, table.Name, pending, null, checkSuccessor: true);

            Assert.Equal(PublishOutcome.Published, published.Outcome);
            var added = new List<string>();
            for (long version = 0; version <= 3; version++)
            {
                added.AddRange((await DeltaTransactionReader.ReadVersionCommit(table.Raw, table.Name, version))!.AddedFiles.Select(x => x.Path!));
            }
            Assert.Equal(s_referenced.Append("revived.parquet"), added);
            Assert.All(added, x => Assert.True(Present(table, server, x), x));
            Assert.False(Present(table, server, "old-c.parquet"));
            await kit.AssertAsync(runs, 1, removedByTheCell: new[] { "_delta_log/" + staged.FileName }, contexts: context);
        }
    }
}
