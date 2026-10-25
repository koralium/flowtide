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
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class OrphanSweepTests
    {
        [Fact]
        public async Task FirstRootListingWarnsItsSizeOnce()
        {
            var table = await LocalWithOldAsync(2, "old.parquet");
            // Enough entries for whole MiB estimates
            const int synthesized = 10_000;
            table.Owned.AfterRecursiveLs = (folder, listed) =>
            {
                var entries = listed.ToList();
                for (int i = 0; i < synthesized; i++)
                {
                    entries.Add(new IOEntry($"{table.Prefix}_synthesized/f{i:D6}.parquet") { Size = 1, LastModificationTime = s_old });
                }
                return entries;
            };
            var logs = new DeltaMaintenanceWorkerTests.CollectingLogger();
            var counters = new RetentionCounters();
            var sweep = new OrphanSweep(table.Configuration, Metered(table), new FakeTimeProvider(Now), logs, table.Name, counters);

            var (first, _) = await RunAsync(sweep, table, OrphanContext(2));
            var (second, _) = await RunAsync(sweep, table, OrphanContext(2));

            AssertResult(first, OutcomeKind.Completed, MaintenanceReason.None);
            AssertResult(second, OutcomeKind.Completed, MaintenanceReason.None);
            var listed = counters.OrphanListed;
            Assert.True(listed > synthesized, $"{listed} listed");
            var sizing = logs.Entries.Where(x => x.Message.Contains("orphan sweep: the root listing returned", StringComparison.Ordinal)).ToList();
            // A warning for the run's first listing, Debug after
            Assert.Equal(new[] { LogLevel.Warning, LogLevel.Debug }, sizing.Select(x => x.Level));
            // About 1000 to 1700 bytes held per entry
            Assert.Contains($"returned {listed} entries, held as about {listed * 1000 >> 20} to {listed * 1700 >> 20} MiB", sizing[0].Message);
            Assert.Contains($"returned {listed} entries", sizing[1].Message);
        }
    }
}
