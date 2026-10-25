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
using Microsoft.Extensions.Time.Testing;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class OrphanSweepTests
    {
        [Fact]
        public async Task CandidateAgeIsStrictlyBeforeTheStoreTimeLessRetentionAndAnHour()
        {
            await using var server = new LoopbackObjectStore(StoreFlavor.S3);
            // The store's clock, ahead of the sweep's
            var t0 = Now + new TimeSpan(2, 3, 17, 29);
            server.Now = t0;
            var table = LoopbackTable(server);
            await RetainedTableAsync(table.Raw, table.Name, 0);
            var cutoff = t0 - Retention - TimeSpan.FromHours(1);
            server.Put("tables/t/before.parquet", new byte[3], cutoff.AddSeconds(-1));
            server.Put("tables/t/at.parquet", new byte[3], cutoff);
            server.Put("tables/t/after.parquet", new byte[3], cutoff.AddSeconds(1));
            // Older than the retention alone
            server.Put("tables/t/margin.parquet", new byte[3], t0 - Retention - TimeSpan.FromMinutes(30));

            var (result, seen) = await RunAsync(Sweep(table.Configuration, table.Owned, new FakeTimeProvider(Now)), table, OrphanContext(0));

            AssertResult(result, OutcomeKind.Completed, MaintenanceReason.None);
            Assert.Equal(cutoff, seen!.Cutoff);
            Assert.Equal("before.parquet", Assert.Single(seen.Frozen).Relative);
            Assert.Equal(3, seen.Counts.Young);
        }
    }
}
