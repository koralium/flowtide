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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class DeltaLakeStagedSweepTests
    {
        private static (string Name, DeletionVector At1, DeletionVector At50) Container()
        {
            var g = Guid.NewGuid();
            var at1 = AdvancePlannerKit.Uuid(g, offset: 1);
            return ($"deletion_vector_{g:D}.bin", at1, AdvancePlannerKit.At(at1, 50));
        }

        // A stage owning these files, adds at two offsets
        private static async Task<string> PackedStage(Table table, long version, string id, IEnumerable<string> created, DeletionVector a, DeletionVector b)
        {
            var actions = new List<DeltaAction>() { Info(id, 0, 2000 + version, created), DeltaLakeCheckpointBuildTests.Add("part-a.parquet", a), DeltaLakeCheckpointBuildTests.Add("part-b.parquet", b) };
            return (await DeltaTransactionWriter.StageCommit(table.Local, "t", version, actions)).FileName;
        }

        [Fact]
        public async Task ADeadStageDeletesItsContainer()
        {
            var table = NewTable();
            await Create(table, 2);
            var (x, a, b) = Container();
            table.Put(x);
            var stage = await PackedStage(table, 3, Attempt, new[] { x }, a, b);
            await WriteOther(table, 3);
            await Write(table, 4, Own(4));
            var sweep = Sweep(table);
            var result = await sweep.RunAsync(Context(4));
            Assert.Equal(OutcomeKind.Completed, result.Kind);
            Assert.Equal(StageVerdict.Dead, sweep.LastReport!.Verdicts[stage]);
            Assert.False(table.Exists(x));
            Assert.Equal(1, sweep.LastReport.PayloadDeleted);
        }

        [Fact]
        public async Task ARepeatedNameIsOneDelete()
        {
            var table = NewTable();
            await Create(table, 2);
            var (x, a, b) = Container();
            table.Put(x);
            var stage = await PackedStage(table, 3, Attempt, new[] { x, x, x }, a, b);
            await WriteOther(table, 3);
            await Write(table, 4, Own(4));
            var sweep = Sweep(table);
            var result = await sweep.RunAsync(Context(4));
            Assert.Equal(OutcomeKind.Completed, result.Kind);
            Assert.False(table.Exists(x));
            Assert.Equal(1, sweep.LastReport!.PayloadDeleted);
            Assert.Equal(0, sweep.LastReport.LeakedStages);
        }

        // Why a file is named once, repeats can lose ownership
        [Fact]
        public async Task RepeatedNamesCountAgainstTheOwnedLimit()
        {
            var table = NewTable();
            await Create(table, 2);
            var (x, a, b) = Container();
            table.Put(x);
            await PackedStage(table, 3, Attempt, new[] { x, x, x }, a, b);
            await WriteOther(table, 3);
            await Write(table, 4, Own(4));
            var sweep = new StagedSweep(table.Configuration, table.Metered, TimeProvider.System, new DeletionRunSettings()) { OwnedFileLimit = 2 };
            var result = await sweep.RunAsync(Context(4));
            Assert.Equal(MaintenanceReason.CreatedFilesUnreadable, result.Reason);
            Assert.True(table.Exists(x));
            Assert.Equal(1, sweep.LastReport!.LeakedStages);
        }

        [Fact]
        public async Task ACommitAtTheStageProtectsItsContainer()
        {
            var table = NewTable();
            await Create(table, 2);
            var (x, a, b) = Container();
            table.Put(x);
            await PackedStage(table, 3, Attempt, new[] { x }, a, b);
            await Write(table, 3, Info("other-attempt", 0, 3003, new[] { x }), DeltaLakeCheckpointBuildTests.Add("part-z.parquet", a));
            await Write(table, 4, Own(4));
            var sweep = Sweep(table);
            Assert.Equal(OutcomeKind.Completed, (await sweep.RunAsync(Context(4))).Kind);
            Assert.True(table.Exists(x));
            Assert.Equal(0, sweep.LastReport!.PayloadDeleted);
        }

        // Protection starts at the oldest dead stage
        [Fact]
        public async Task ACommitBelowTheStageDoesNotProtectItsContainer()
        {
            var table = NewTable();
            await Create(table, 1);
            var (x, a, b) = Container();
            table.Put(x);
            await Write(table, 2, Own(2), DeltaLakeCheckpointBuildTests.Add("part-live.parquet", a));
            await PackedStage(table, 3, Attempt, new[] { x }, a, b);
            await WriteOther(table, 3);
            await Write(table, 4, Own(4));
            var sweep = Sweep(table);
            await sweep.RunAsync(Context(4));
            Assert.False(table.Exists(x));
        }

        [Fact]
        public async Task ALaterStageProtectsAContainerADeadStageOwns()
        {
            var table = NewTable();
            await Create(table, 2);
            var (x, a, b) = Container();
            table.Put(x);
            await PackedStage(table, 3, Attempt, new[] { x }, a, b);
            await WriteOther(table, 3);
            await Write(table, 4, Own(4));
            await DeltaTransactionWriter.StageCommit(table.Local, "t", 5, new List<DeltaAction>() { Info("live", 0, 2005, new List<string>()), DeltaLakeCheckpointBuildTests.Remove("part-a.parquet", 1, a) });
            var sweep = Sweep(table);
            Assert.Equal(OutcomeKind.Completed, (await sweep.RunAsync(Context(4))).Kind);
            Assert.True(table.Exists(x));
        }

        [Fact]
        public async Task ADeadStageOwningAnAbsentContainerCompletes()
        {
            var table = NewTable();
            await Create(table, 2);
            var (x, a, _) = Container();
            var (y, _, b) = Container();
            table.Put(x);
            await PackedStage(table, 3, Attempt, new[] { x, y }, a, b);
            await WriteOther(table, 3);
            await Write(table, 4, Own(4));
            var sweep = Sweep(table);
            var result = await sweep.RunAsync(Context(4));
            Assert.Equal(OutcomeKind.Completed, result.Kind);
            Assert.False(table.Exists(x));
        }

        // A torn first line owns nothing, the file only leaks
        [Fact]
        public async Task ATornStageHeaderOnlyLeaksItsContainer()
        {
            var table = NewTable();
            await Create(table, 2);
            var (x, a, b) = Container();
            table.Put(x);
            var stage = await PackedStage(table, 3, Attempt, new[] { x }, a, b);
            var bytes = File.ReadAllBytes(table.Log(stage));
            var firstLine = Array.IndexOf(bytes, (byte)'\n');
            File.WriteAllBytes(table.Log(stage), bytes.Take(firstLine - 3).ToArray());
            await WriteOther(table, 3);
            await Write(table, 4, Own(4));
            var sweep = Sweep(table);
            await sweep.RunAsync(Context(4));
            Assert.True(table.Exists(x));
            Assert.Equal(StageVerdict.Dead, sweep.LastReport!.Verdicts[stage]);
            Assert.Equal(1, sweep.LastReport.LeakedStages);
        }
    }
}
