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

using FlowtideDotNet.AcceptanceTests.Entities;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Advance;
using Stowage;
using static FlowtideDotNet.Connector.DeltaLake.Tests.AdvancePlannerKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // The real planner, one vector file for two data files
    public class DeletionVectorContainerRetentionTests : IDisposable
    {
        private readonly AdvancePlannerKit _kit = new AdvancePlannerKit();
        private readonly List<AdvancePlan> _plans = new List<AdvancePlan>();
        public void Dispose()
        {
            foreach (var plan in _plans) plan.Dispose();
            _kit.Dispose();
        }

        private static string X(Guid g) => Upper($"deletion_vector_{g:D}.bin");

        private async Task<(AdvancePlan Plan, List<string> Emitted)> Plan(Table table, AdvanceBoundary boundary)
        {
            await RetainedReferenceOracle.CreateReferencedFilesAsync(table.Native);
            var context = _kit.Context(true, boundary.V);
            var plan = await AdvancePlanner.PlanAsync(table.Configuration, table.Local, table.Configuration.Store.Table, boundary, Classes(boundary.Log), context, AdvanceWindowLimits.Default);
            _plans.Add(plan);
            Assert.True(plan.Status == AdvancePlanStatus.Planned, $"{plan.Status} {plan.Reason}: {plan.Detail}");
            var emitted = Emitted(plan, AdvanceWindowLimits.Default);
            return (plan, emitted);
        }

        // Commit 1 as the sink writes it, two offsets
        private static async Task<(Guid G, DeletionVector Xa, DeletionVector Xb)> PackedTable(Table table)
        {
            var g = Guid.NewGuid();
            var xa = Uuid(g, offset: 1);
            var xb = At(xa, 50);
            await Commit(table, 0, 0, Create(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema), Add("part-a.parquet"), Add("part-b.parquet"));
            await Commit(table, 1, 0, Remove("part-a.parquet", 1), Add("part-a.parquet", xa), Remove("part-b.parquet", 1), Add("part-b.parquet", xb));
            return (g, xa, xb);
        }

        [Fact]
        public async Task AContainerStaysWhileItsOtherFileIsLive()
        {
            var table = _kit.Local();
            var (g, xa, _) = await PackedTable(table);
            await Commit(table, 2, 0, Remove("part-a.parquet", 2, xa), Add("part-a2.parquet"));
            await Commit(table, 3, 0, Add("part-c.parquet"));
            await _kit.Build(table, 3);
            await Commit(table, 4, 0, Add("part-d.parquet"));
            var (_, emitted) = await Plan(table, Boundary(await Log(table), 0, 3));
            Assert.Contains("PART-A.PARQUET", emitted);
            Assert.DoesNotContain(X(g), emitted);
        }

        [Fact]
        public async Task AFileMovingToANewContainerKeepsTheOldOne()
        {
            var table = _kit.Local();
            var (g, xa, _) = await PackedTable(table);
            var y = Guid.NewGuid();
            await Commit(table, 2, 0, Remove("part-a.parquet", 2, xa), Add("part-a.parquet", Uuid(y)));
            await Commit(table, 3, 0, Add("part-c.parquet"));
            await _kit.Build(table, 3);
            await Commit(table, 4, 0, Add("part-d.parquet"));
            var (_, emitted) = await Plan(table, Boundary(await Log(table), 0, 3));
            Assert.DoesNotContain(X(g), emitted);
            Assert.DoesNotContain(X(y), emitted);
            Assert.DoesNotContain("PART-A.PARQUET", emitted);
        }

        [Fact]
        public async Task AContainerGoesOnlyWhenEveryRemoveIsBelowTheBoundary()
        {
            var table = _kit.Local();
            var (g, xa, xb) = await PackedTable(table);
            await Commit(table, 2, 0, Remove("part-a.parquet", 2, xa));
            await Commit(table, 3, 0, Add("part-c.parquet"));
            await Commit(table, 4, 0, Remove("part-b.parquet", 4, xb));
            await Commit(table, 5, 0, Add("part-d.parquet"));
            await Commit(table, 6, 0, Add("part-e.parquet"));
            await _kit.Build(table, 3);
            await _kit.Build(table, 4);
            await _kit.Build(table, 5);
            // The second remove is above the boundary, still live
            Assert.DoesNotContain(X(g), (await Plan(table, Boundary(await Log(table), 0, 3))).Emitted);
            // The second remove is in the boundary's commit, still kept
            Assert.DoesNotContain(X(g), (await Plan(table, Boundary(await Log(table), 0, 4))).Emitted);
            // Both removes are below the boundary
            Assert.Contains(X(g), (await Plan(table, Boundary(await Log(table), 0, 5))).Emitted);
        }

        [Fact]
        public async Task ALaterAdvanceStillFindsTheContainer()
        {
            var table = _kit.Local();
            var (g, xa, xb) = await PackedTable(table);
            await Commit(table, 2, 0, Remove("part-a.parquet", 2, xa));
            await Commit(table, 3, 0, Remove("part-b.parquet", 3, xb));
            await Commit(table, 4, 0, Add("part-c.parquet"));
            await Commit(table, 5, 0, Add("part-d.parquet"));
            await _kit.Build(table, 3);
            await _kit.Build(table, 5);
            // The first advance keeps it through the boundary's commit
            Assert.DoesNotContain(X(g), (await Plan(table, Boundary(await Log(table), 0, 3))).Emitted);
            Truncate(table, 3);
            // The next finds it in that retained commit
            Assert.Contains(X(g), (await Plan(table, Boundary(await Log(table), 3, 5))).Emitted);
        }

        [Fact]
        public async Task ACheckpointAloneProtectsAContainer()
        {
            var table = _kit.Local();
            var (g, xa, xb) = await PackedTable(table);
            await Commit(table, 2, 0, Add("part-d.parquet"));
            await Commit(table, 3, 0, Add("part-c.parquet"));
            await _kit.Build(table, 3);
            Truncate(table, 3);
            await Commit(table, 4, 0, Remove("part-a.parquet", 4, xa));
            await Commit(table, 5, 0, Add("part-e.parquet"));
            await _kit.Build(table, 5);
            // The remove makes it a candidate, only checkpoints keep it
            Assert.DoesNotContain(X(g), (await Plan(table, Boundary(await Log(table), 3, 5))).Emitted);
            Truncate(table, 5);
            await Commit(table, 6, 0, Remove("part-b.parquet", 6, xb));
            await Commit(table, 7, 0, Add("part-f.parquet"));
            await Commit(table, 8, 0, Add("part-g.parquet"));
            await _kit.Build(table, 7);
            Assert.Contains(X(g), (await Plan(table, Boundary(await Log(table), 5, 7))).Emitted);
        }

        [Fact]
        public async Task ALiveAddProtectsAContainerAnUnmarkedRootTombstones()
        {
            var table = _kit.Local();
            var (g, xa, xb) = await PackedTable(table);
            await Commit(table, 2, 0, Remove("part-a.parquet", 2, xa));
            await Commit(table, 3, 0, Add("part-c.parquet"));
            await Commit(table, 4, 0, Add("part-d.parquet"));
            await Commit(table, 5, 0, Add("part-e.parquet"));
            await _kit.Build(table, 5);
            await _kit.UnmarkedRoot(table, 3, new[]
            {
                Create(), Metadata(),
                Add("part-b.parquet", xb), Add("part-c.parquet"),
                Remove("part-a.parquet", 2, xa)
            });
            Truncate(table, 3);
            var (plan, emitted) = await Plan(table, Boundary(await Log(table), 3, 5, oldestMarked: false));
            Assert.DoesNotContain(X(g), emitted);
            Assert.Equal(3, plan.Protected);
        }

        private static DeltaAction Metadata()
        {
            var m = DeltaTestKit.Metadata(DeltaTestKit.UserSchema);
            m.MetaData!.Id = "table-a";
            return m;
        }

        [Fact]
        public async Task ANewerCheckpointProtectsAContainer()
        {
            var table = _kit.Local();
            var (g, xa, xb) = await PackedTable(table);
            await Commit(table, 2, 0, Remove("part-a.parquet", 2, xa), Remove("part-b.parquet", 2, xb));
            await Commit(table, 3, 0, Add("part-c.parquet"));
            await _kit.Build(table, 3);
            // Only the newer checkpoint names the file
            await Commit(table, 4, 0, Add("part-b.parquet", xb));
            await _kit.Build(table, 4);
            await Commit(table, 5, 0, Add("part-e.parquet"));
            Assert.DoesNotContain(X(g), (await Plan(table, Boundary(await Log(table), 0, 3))).Emitted);
        }

        [Fact]
        public async Task AnAbsoluteSpellingProtectsTheSameFile()
        {
            var table = _kit.Local();
            var (g, xa, xb) = await PackedTable(table);
            // Another engine's absolute spelling of the same container
            var absolute = new DeletionVector() { StorageType = "p", PathOrInlineDv = (OperatingSystem.IsWindows() ? "file:///" : "file://") + table.Native.Replace('\\', '/') + $"/deletion_vector_{g:D}.bin", Offset = 50, SizeInBytes = 10, Cardinality = 2 };
            await Commit(table, 2, 0, Remove("part-a.parquet", 2, xa), Remove("part-b.parquet", 2, xb), Add("part-b.parquet", absolute));
            await Commit(table, 3, 0, Add("part-c.parquet"));
            await Commit(table, 4, 0, Add("part-d.parquet"));
            await _kit.Build(table, 3);
            var (_, emitted) = await Plan(table, Boundary(await Log(table), 0, 3));
            Assert.DoesNotContain(X(g), emitted);
        }

        [Fact]
        public async Task AVectorOutsideTheTableDeclinesTheAdvance()
        {
            var table = _kit.Local();
            var (g, xa, xb) = await PackedTable(table);
            var outside = new DeletionVector() { StorageType = "p", PathOrInlineDv = "file:///elsewhere/deletion_vector_00000000-0000-4000-8000-000000000000.bin", Offset = 50, SizeInBytes = 10, Cardinality = 2 };
            await Commit(table, 2, 0, Remove("part-a.parquet", 2, xa), Remove("part-b.parquet", 2, xb), Add("part-b.parquet", outside));
            await Commit(table, 3, 0, Add("part-c.parquet"));
            await Commit(table, 4, 0, Add("part-d.parquet"));
            await _kit.Build(table, 3);
            await RetainedReferenceOracle.CreateReferencedFilesAsync(table.Native);
            var boundary = Boundary(await Log(table), 0, 3);
            var plan = await AdvancePlanner.PlanAsync(table.Configuration, table.Local, table.Configuration.Store.Table, boundary, Classes(boundary.Log), _kit.Context(true, boundary.V), AdvanceWindowLimits.Default);
            _plans.Add(plan);
            Assert.Equal(AdvancePlanStatus.Declined, plan.Status);
            Assert.Equal(MaintenanceReason.PathRejected, plan.Reason);
        }

        // Why the writer never re-adds an old descriptor
        [Fact]
        public async Task AnOwnReAddAboveTheBoundaryIsNotSeen()
        {
            var table = _kit.Local();
            var (g, xa, xb) = await PackedTable(table);
            await Commit(table, 2, 0, Remove("part-a.parquet", 2, xa), Remove("part-b.parquet", 2, xb));
            await Commit(table, 3, 0, Add("part-c.parquet"));
            await _kit.Build(table, 3);
            // The sink never writes this
            await Commit(table, 4, 0, Add("part-b.parquet", xb));
            var (_, emitted) = await Plan(table, Boundary(await Log(table), 0, 3));
            Assert.Contains(X(g), emitted);
        }

        // Sink written bytes through real checkpoints and the planner
        [Fact]
        public async Task ASinkWrittenContainerStaysUntilItsLastFileGoes()
        {
            var root = Path.Join(Path.GetTempPath(), "dvr-" + ScratchOwners.ShortId());
            Directory.CreateDirectory(root);
            try
            {
                var storage = OwnedHookFileStorage.Over(Files.Of.LocalDisk(root));
                await using var stream = new DeltaLakeSinkStream(nameof(ASinkWrittenContainerStaysUntilItsLastFileGoes), storage, options =>
                {
                    options.CheckpointInterval = 0;
                    options.MaxFileSizeBytes = 1_500;
                });
                stream.WaitForUpdateDoesNotRequireDataChange();
                for (int i = 0; i < 600; i++)
                {
                    stream.AddOrUpdateUser(Fixed(i));
                }
                await stream.StartStream(DeltaTestKit.UserInsert);
                await DeltaTestKit.WaitForVersion(storage, "test", stream, 0);

                stream.DeleteUsers(Users(0, 100, 200, 300));
                await DeltaTestKit.WaitForVersion(storage, "test", stream, 1);
                var packed = (await DeltaTestKit.ReadCommitActions(storage, "test", 1)).Where(x => x.Add?.DeletionVector != null).Select(x => x.Add!).ToList();
                Assert.Equal(4, packed.Count);
                var container = packed[0].DeletionVector!.AbsolutePath!;
                Assert.All(packed, x => Assert.Equal(container, x.DeletionVector!.AbsolutePath));

                // The first file moves to a new container
                stream.DeleteUsers(Users(1));
                await DeltaTestKit.WaitForVersion(storage, "test", stream, 2);
                var moved = (await DeltaTestKit.ReadCommitActions(storage, "test", 2)).Single(x => x.Add?.DeletionVector != null).Add!;
                Assert.NotEqual(container, moved.DeletionVector!.AbsolutePath);
                stream.AddOrUpdateUser(Fixed(600));
                await DeltaTestKit.WaitForVersion(storage, "test", stream, 3);
                stream.AddOrUpdateUser(Fixed(601));
                await DeltaTestKit.WaitForVersion(storage, "test", stream, 4);

                // Enough rows go to rewrite the container's other files
                stream.DeleteUsers(Users(Enumerable.Range(0, 400).Where(x => x > 60 && x % 2 == 0 && x % 100 != 0).ToArray()));
                await DeltaTestKit.WaitForVersion(storage, "test", stream, 5);
                var fifth = await DeltaTestKit.ReadCommitActions(storage, "test", 5);
                Assert.Equal(3, fifth.Count(x => x.Remove?.DeletionVector?.AbsolutePath == container));
                Assert.DoesNotContain(fifth, x => x.Add?.DeletionVector?.AbsolutePath == container);
                stream.AddOrUpdateUser(Fixed(602));
                await DeltaTestKit.WaitForVersion(storage, "test", stream, 6);
                stream.AddOrUpdateUser(Fixed(603));
                await DeltaTestKit.WaitForVersion(storage, "test", stream, 7);
                await DeltaTestKit.SettlePublications(stream);

                // The planner kit lays a table out below t
                var table = _kit.Local();
                CopyDirectory(Path.Join(root, "test"), table.Native);
                await _kit.Build(table, 3);
                await _kit.Build(table, 6);
                var read = (await DeltaTransactionReader.ReadTable(table.Local, "t", 3))!;
                var fromCheckpoint = read.AddFiles.Where(x => x.DeletionVector != null).ToList();
                Assert.Equal(4, fromCheckpoint.Count);
                foreach (var add in fromCheckpoint)
                {
                    Assert.Equal(add.DeletionVector!.Cardinality, (await new DeletionVectorReader(table.Local, "t").Read(add.DeletionVector)).Count());
                }

                Assert.DoesNotContain(Upper(container), (await Plan(table, Boundary(await Log(table), 0, 3))).Emitted);
                Assert.Contains(Upper(container), (await Plan(table, Boundary(await Log(table), 0, 6))).Emitted);
            }
            finally
            {
                try
                {
                    Directory.Delete(root, true);
                }
                catch (Exception e) when (e is IOException or UnauthorizedAccessException)
                {
                    // A folder the system still holds is left in temp
                }
            }
        }

        private static User Fixed(int key)
        {
            return new User() { UserKey = key, FirstName = $"first name of {key * 7919}", LastName = "Last" };
        }

        private static List<User> Users(params int[] keys)
        {
            return keys.Select(Fixed).ToList();
        }

        private static void CopyDirectory(string from, string to)
        {
            Directory.CreateDirectory(to);
            foreach (var file in Directory.GetFiles(from))
            {
                File.Copy(file, Path.Join(to, Path.GetFileName(file)));
            }
            foreach (var folder in Directory.GetDirectories(from))
            {
                CopyDirectory(folder, Path.Join(to, Path.GetFileName(folder)));
            }
        }
    }
}
