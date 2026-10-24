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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class OrphanSweepTests
    {
        // One registered sweep of the local table at the head with exactly these targets, its delete turns and the kit
        private async Task SweepAndDeleteAsync(OrphanTable table, long head, string[] targets, RetentionCounters? counters = null, Action<Seen>? seen = null)
        {
            var kit = await OrphanInvariants.CaptureAsync(table, null);
            var sweep = Sweep(table.Configuration, table.Owned, counters: counters);
            var runs = Registered(sweep);
            var context = OrphanContext(head);

            var (result, frozen) = await RunAsync(sweep, table, context);

            AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
            seen?.Invoke(frozen!);
            Assert.Equal(targets, kit.Record(Assert.Single(runs)));
            var turns = await DeleteTurnsAsync(runs[0], head);
            Assert.All(turns, x => AssertResult(x, OutcomeKind.Completed, MaintenanceReason.None));
            await kit.AssertAsync(runs, 1, contexts: context);
        }

        [Fact]
        public async Task CaseAliasesProtectThroughTheSweep()
        {
            var table = await LocalWithOldAsync(2, "old-a.parquet", "dir/old-b.parquet", "old-c.parquet");
            await PublishAsync(table, 3, "OLD-A.PARQUET");
            await StageAsync(table, 4, "Dir/Old-B.Parquet");

            await SweepAndDeleteAsync(table, 3, new[] { "old-c.parquet" });

            Assert.True(File.Exists(Path.Combine(table.Folder!, "old-a.parquet")));
            Assert.True(File.Exists(Path.Combine(table.Folder!, "dir", "old-b.parquet")));
            Assert.False(File.Exists(Path.Combine(table.Folder!, "old-c.parquet")));
        }

        [WindowsFact]
        public async Task WindowsShortAliasProtectsThroughTheSweep()
        {
            var leaf = "part-00000-" + Guid.NewGuid().ToString("N") + ".snappy.parquet";
            var table = await LocalWithOldAsync(2, leaf, "old-c.parquet");
            var alias = PathIdentityTests.ShortName(Path.Combine(table.Folder!, leaf));
            // 8.3 names are disabled on this volume
            if (alias == null || alias.Equals(leaf, StringComparison.OrdinalIgnoreCase))
            {
                return;
            }
            await PublishAsync(table, 3, alias);

            await SweepAndDeleteAsync(table, 3, new[] { "old-c.parquet" });

            Assert.True(File.Exists(Path.Combine(table.Folder!, leaf)), $"The alias {alias} did not protect {leaf}");
            Assert.False(File.Exists(Path.Combine(table.Folder!, "old-c.parquet")));
        }

        [Fact]
        public async Task DirectoryLinkInTheTableNeverDeletesItsTarget()
        {
            var table = await LocalWithOldAsync(2, "old-c.parquet");
            var outside = Path.Combine(_directory, "outside-" + ScratchOwners.ShortId());
            Directory.CreateDirectory(outside);
            var linked = Path.Combine(outside, "x.parquet");
            File.WriteAllBytes(linked, new byte[3]);
            File.SetLastWriteTimeUtc(linked, s_old.UtcDateTime);
            var link = Path.Combine(table.Folder!, "linked");
            // No file system here creates a directory link
            if (!await DirectoryLinks.TryCreateAsync(link, outside))
            {
                return;
            }
            try
            {
                var below = 0;
                table.Owned.AfterRecursiveLs = (_, listed) =>
                {
                    below = listed.Count(x => !x.Path.IsFolder && x.Path.Full.StartsWith(table.Prefix + "linked/", StringComparison.Ordinal));
                    return listed;
                };
                var counters = new RetentionCounters();
                var unqualified = -1L;

                await SweepAndDeleteAsync(table, 2, new[] { "old-c.parquet" }, counters, x => unqualified = x.Counts.Unqualified);

                Assert.True(File.Exists(linked));
                Assert.False(File.Exists(Path.Combine(table.Folder!, "old-c.parquet")));
                Assert.True(below > 0, "LocalDisk's recursive listing did not descend the link");
                // Rejected at the freeze, before any delete
                Assert.Equal(below, unqualified);
                Assert.Equal(below, counters.OrphanKept(KeepReason.Unqualified));
                Assert.Equal(0, counters.Deletes(TargetKind.Payload, DeleteStatus.Kept));
                Assert.Equal(1, counters.Deletes(TargetKind.Payload, DeleteStatus.Deleted));
            }
            finally
            {
                // The link alone, never its target's files
                Directory.Delete(link);
            }
        }
    }
}
