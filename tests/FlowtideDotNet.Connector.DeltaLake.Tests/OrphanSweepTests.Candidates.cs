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
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class OrphanSweepTests
    {
        [Fact]
        public async Task EveryOldCandidateIsFrozenWithoutACountCap()
        {
            await using var server = new LoopbackObjectStore(StoreFlavor.S3);
            server.Now = Now;
            var table = LoopbackTable(server);
            await RetainedTableAsync(table.Raw, table.Name, 0);
            server.Put("tables/t/a-old.parquet", new byte[3], s_old);
            // More than one attempt's deletions, in listing order after the provider's entries
            const int synthesized = 100_001;
            table.Owned.AfterRecursiveLs = (folder, listed) =>
            {
                var entries = listed.ToList();
                for (int i = 0; i < synthesized; i++)
                {
                    entries.Add(new IOEntry($"{table.Prefix}o{i:D6}.parquet") { Size = 1, LastModificationTime = s_old });
                }
                return entries;
            };

            var (result, seen) = await RunAsync(Sweep(table.Configuration, table.Owned), table, OrphanContext(0));

            AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.Equal(synthesized + 1, seen!.Counts.Frozen);
            Assert.Equal(synthesized + 1, seen.Frozen.Count);
            Assert.Equal(new FrozenCandidate("o100000.parquet", 1, s_old), seen.Frozen[^1]);
        }

        [Fact]
        public async Task NestedMembersListedBeforeTheirLogAreExcluded()
        {
            // Both members sort before the nested log
            var table = await LocalWithOldAsync(2, "nested/0.parquet", "nested/A.parquet", $"nested/_delta_log/{0:D20}.json", "nested/z.parquet", "a-old.parquet");

            var (result, seen) = await RunAsync(Sweep(table.Configuration, table.Owned), table, OrphanContext(2));

            AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.Equal("a-old.parquet", Assert.Single(seen!.Frozen).Relative);
            Assert.Equal(1, seen.Counts.NestedRoots);
            Assert.True(seen.Counts.Nested >= 3, $"{seen.Counts.Nested} nested");
        }

        [Fact]
        public async Task UnqualifiedOldNameIsKeptAndCounted()
        {
            await using var server = new LoopbackObjectStore(StoreFlavor.S3);
            server.Now = Now;
            var table = LoopbackTable(server);
            await RetainedTableAsync(table.Raw, table.Name, 0);
            // A character outside the qualified object names
            server.Put("tables/t/x~1.parquet", new byte[3], s_old);
            server.Put("tables/t/a-old.parquet", new byte[5], s_old);
            var counters = new RetentionCounters();

            var (result, seen) = await RunAsync(Sweep(table.Configuration, table.Owned, counters: counters), table, OrphanContext(0));

            AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.Equal(new FrozenCandidate("a-old.parquet", 5, s_old), Assert.Single(seen!.Frozen));
            Assert.Equal(1, seen.Counts.Unqualified);
            Assert.Equal(1, counters.OrphanKept(KeepReason.Unqualified));
            Assert.Equal(0, counters.OrphanKept(KeepReason.UnknownSize));
        }
    }
}
