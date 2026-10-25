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
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class OrphanSweepTests
    {
        [Theory]
        // An uninterpreted cleanup control record
        [InlineData("_delta_log/.flowtide-cleanup.future", nameof(ListingCheck.ControlFenced))]
        [InlineData("_delta_log/00000000000000000003.checkpoint.parquet", nameof(ListingCheck.RangeGap))]
        [InlineData("_delta_log/00000000000000000005.crc", nameof(ListingCheck.RangeGap))]
        public async Task RootLogProjectionDeclinesAFenceOrAVersionAboveItsHead(string injected, string check)
        {
            var table = await LocalWithOldAsync(2, "old.parquet");
            // In the root listing only, after preparation's own log listing passed
            table.Owned.AfterRecursiveLs = (folder, listed) => listed.Append(new IOEntry(table.Prefix + injected) { Size = 10, LastModificationTime = s_old }).ToList();
            var counters = new RetentionCounters();

            var (result, seen) = await RunAsync(Sweep(table.Configuration, table.Owned, counters: counters), table, OrphanContext(2));

            AssertResult(result, OutcomeKind.Declined, MaintenanceReason.ListingIneligible);
            Assert.Null(seen);
            Assert.Equal(1, counters.ListingDeclines(Enum.Parse<ListingCheck>(check)));
            Assert.Single(table.Owned.Recursive);
            // Declined before its own successor probe
            Assert.Empty(table.Owned.CallsAfterRecursive());
            Assert.Equal(0, counters.Probes(ProbeResult.Absent));
        }

        [Fact]
        public async Task SiblingTableInTheRootListingDeclines()
        {
            await using var server = new LoopbackObjectStore(StoreFlavor.S3);
            server.Now = Now;
            var table = LoopbackTable(server);
            await RetainedTableAsync(table.Raw, table.Name, 0);
            server.Put("tables/t/part-old.parquet", new byte[3], s_old);
            // As a provider matching the unterminated prefix delivers a table whose name extends this one's
            var sibling = new[] { $"/c/tables/tx/_delta_log/{0:D20}.json", "/c/tables/tx/part-old.parquet" };
            table.Owned.AfterRecursiveLs = (folder, listed) => listed.Concat(sibling.Select(x => new IOEntry(x) { Size = 3, LastModificationTime = s_old })).ToList();
            var counters = new RetentionCounters();

            var (result, seen) = await RunAsync(Sweep(table.Configuration, table.Owned, counters: counters), table, OrphanContext(0));

            AssertResult(result, OutcomeKind.Declined, MaintenanceReason.ListingIneligible);
            Assert.Null(seen);
            Assert.Equal(1, counters.ListingDeclines(ListingCheck.Containment));
            Assert.Single(table.Owned.Recursive);
            Assert.Empty(table.Owned.CallsAfterRecursive());
        }
    }
}
