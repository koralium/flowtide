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

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public partial class OrphanSweepTests
    {
        [Fact]
        public async Task CommitsListedBelowTheOldestUsableRootDeclineTheSweep()
        {
            var table = LocalTable();
            var published = new Dictionary<string, string>()
            {
                [RetentionProperties.LogKey] = RetentionProperties.Format(Retention),
                [RetentionProperties.DeletedFileKey] = RetentionProperties.Format(Retention)
            };
            // part-v is added by commit v and removed by commit v + 1
            for (long version = 0; version <= 6; version++)
            {
                var add = DeltaLakeCheckpointBuildTests.Add($"part-{version:D4}.parquet");
                var actions = version == 0
                    ? new[] { DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema, published), add }
                    : new[] { add, DeltaLakeCheckpointBuildTests.Remove($"part-{version - 1:D4}.parquet", BoundarySelectorTests.Old(version)) };
                await DeltaTestKit.WriteOwnCommit(table.Raw, table.Name, version, 0, BoundarySelectorTests.Old(version), actions);
            }
            using var kit = new AdvanceExecutionKit();
            await kit.BuildAsync(table.Raw, 2);
            await kit.BuildAsync(table.Raw, 4);
            var log = Path.Combine(table.Folder!, "_delta_log");
            // The root at 2 in the multi-part form other writers use
            File.Move(Path.Combine(log, $"{2:D20}.checkpoint.parquet"), Path.Combine(log, $"{2:D20}.checkpoint.{1:D10}.{1:D10}.parquet"));
            File.Delete(Path.Combine(log, $"{0:D20}.json"));
            File.Delete(Path.Combine(log, $"{1:D20}.json"));
            // Live at version 2 and removed by commit 3
            Plant(table, "part-0002.parquet");
            Plant(table, "old.parquet");
            var sweep = Sweep(table.Configuration, table.Owned);
            long? oldest = null;
            sweep.Prepared = prepared => oldest = prepared.O;
            var runs = Registered(sweep);

            var (result, _) = await RunAsync(sweep, table, OrphanContext(6));

            Assert.Equal(4, oldest);
            Assert.Equal(OutcomeKind.Declined, result.Kind);
            Assert.Equal(MaintenanceReason.OrphanNotEligible, result.Reason);
            Assert.False(result.Defers);
            Assert.Contains("below the oldest usable root 4", result.Detail);
            Assert.Empty(runs.SelectMany(x => TargetsOf(x, table)));
        }
    }
}
