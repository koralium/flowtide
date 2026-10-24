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
        // Rewrites the first two log listings after the root listing, L2 and L3; returns how many it rewrote
        private static Func<int> AtBothProtectionListings(OrphanTable table, Func<List<IOEntry>, List<IOEntry>> rewrite)
        {
            var log = table.Prefix + "_delta_log/";
            var listings = 0;
            var rewritten = 0;
            var current = false;
            table.Owned.BeforeLs = folder =>
            {
                current = folder.Full == log && table.Owned.Recursive.Count == 1 && ++listings <= 2;
                return Task.CompletedTask;
            };
            table.Owned.AfterLs = listed =>
            {
                if (!current)
                {
                    return listed;
                }
                rewritten++;
                return rewrite(listed.ToList());
            };
            return () => rewritten;
        }

        [Fact]
        public async Task StageOmittedFromBothProtectionListingsIsAResidual()
        {
            var table = await LocalWithOldAsync(2, "old.parquet", "staged.parquet");
            var stage = await StageAsync(table, 3, "staged.parquet");
            var name = Path.GetFileName(stage);
            // The complete-listing premise's residual: no listing check sees a stage both protection listings omit
            var rewritten = AtBothProtectionListings(table, listed => listed.Where(x => x.Name != name).ToList());
            var counters = new RetentionCounters();
            var sweep = Sweep(table.Configuration, table.Owned, counters: counters);
            var runs = Registered(sweep);

            var (result, seen) = await RunAsync(sweep, table, OrphanContext(2));

            Assert.Equal(2, rewritten());
            AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.All(Enum.GetValues<ListingCheck>(), x => Assert.Equal(0, counters.ListingDeclines(x)));
            Assert.Equal(new[] { "old.parquet", "staged.parquet" }, seen!.Frozen.Select(x => x.Relative).Order(StringComparer.Ordinal));
            // Registered although the stage names it
            Assert.Equal(new[] { "old.parquet", "staged.parquet" }, TargetsOf(Assert.Single(runs), table));
            Assert.True(File.Exists(stage));
        }
    }
}
