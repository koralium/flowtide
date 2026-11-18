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
using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.LocalDisk;

namespace FlowtideDotNet.AcceptanceTests.MultiJoin
{
    [Collection("Multi join row budget")]
    public class MultiJoinSettingTests
    {
        private record Row(int OrderKey, string? FirstName, string? CompanyName, string? ProjectName);

        private const string Sql = @"
            INSERT INTO output
            SELECT o.orderkey, u.firstName, c.name, p.name
            FROM orders o
            INNER JOIN users u ON o.userkey = u.userkey
            INNER JOIN companies c ON u.companyId = c.companyId
            INNER JOIN projects p ON p.companyId = c.companyId";

        // The same rows for every stream of a test
        private static void Seed(FlowtideTestStream stream)
        {
            for (int c = 0; c < 4; c++)
            {
                stream.AddOrUpdateCompany(new Company() { CompanyId = c.ToString(), Name = "company " + c });
                stream.AddProjects(new Project() { ProjectKey = c, CompanyId = c.ToString(), ProjectNumber = "p" + c, Name = "project " + c });
            }
            for (int u = 0; u < 40; u++)
            {
                stream.AddOrUpdateUser(new User() { UserKey = u, FirstName = "user " + u, CompanyId = (u % 5).ToString() });
            }
            for (int o = 0; o < 200; o++)
            {
                stream.AddOrUpdateOrder(new Order() { OrderKey = o, UserKey = o % 45 });
            }
        }

        private static IEnumerable<Row> Expected(FlowtideTestStream stream)
        {
            return from o in stream.Orders
                   join u in stream.Users on o.UserKey equals u.UserKey
                   join c in stream.Companies on u.CompanyId equals c.CompanyId
                   join p in stream.Projects on c.CompanyId equals p.CompanyId
                   select new Row(o.OrderKey, u.FirstName, c.Name, p.Name);
        }

        private static PlanOptimizerSettings? Settings(int maxInputs, int parallelization = 1)
        {
            return new PlanOptimizerSettings() { MultiJoin = maxInputs > 0, MultiJoinMaxInputs = Math.Max(maxInputs, 3), Parallelization = parallelization };
        }

        // With a cap of five every join is in the multi join, with three the join below it is partitioned and feeds it
        [Theory]
        [InlineData(4, 1, 5)]
        [InlineData(1, 4, 5)]
        [InlineData(4, 1, 3)]
        [InlineData(1, 4, 3)]
        public async Task GivesTheRowsOfTheJoinsAtAnyPartitioning(int parallelization, int parallelism, int maxInputs)
        {
            await using var stream = new FlowtideTestStream($"MultiJoinSetting/Partitioned_{parallelization}_{parallelism}_{maxInputs}") { MultiJoinMaxInputs = maxInputs };
            Seed(stream);
            await stream.StartStream(Sql, parallelism, planOptimizerSettings: Settings(maxInputs, parallelization));
            await stream.WaitForUpdate();
            Assert.Equal(1, stream.MultiJoinGroupsMarked);
            stream.AssertCurrentDataEqual(Expected(stream));

            var user = stream.Users[3];
            user.FirstName = "changed";
            stream.AddOrUpdateUser(user);
            await stream.WaitForUpdate();
            stream.AssertCurrentDataEqual(Expected(stream));
        }

        private record CompanyCount(string? CompanyName, long Orders);

        [Theory]
        [InlineData(4, 1)]
        [InlineData(1, 4)]
        public async Task GivesTheRowsOfAnAggregateOverPartitionedJoinsAndAMultiJoin(int parallelization, int parallelism)
        {
            await using var stream = new FlowtideTestStream($"MultiJoinSetting/Aggregate_{parallelization}_{parallelism}") { MultiJoinMaxInputs = 3 };
            Seed(stream);
            await stream.StartStream(@"
                INSERT INTO output
                SELECT c.name, count(*)
                FROM orders o
                INNER JOIN users u ON o.userkey = u.userkey
                INNER JOIN companies c ON u.companyId = c.companyId
                INNER JOIN projects p ON p.companyId = c.companyId
                GROUP BY c.name", parallelism, planOptimizerSettings: Settings(3, parallelization));
            await stream.WaitForUpdate();
            Assert.Equal(1, stream.MultiJoinGroupsMarked);
            stream.AssertCurrentDataEqual(Expected(stream).GroupBy(x => x.CompanyName).Select(x => new CompanyCount(x.Key, x.LongCount())));

            // A user moves to another company, so two counts change
            var user = stream.Users[3];
            user.CompanyId = "0";
            stream.AddOrUpdateUser(user);
            await stream.WaitForUpdate();
            stream.AssertCurrentDataEqual(Expected(stream).GroupBy(x => x.CompanyName).Select(x => new CompanyCount(x.Key, x.LongCount())));
        }

        [Fact]
        public async Task TheOptimizerSettingAloneTurnsItOn()
        {
            if (Environment.GetEnvironmentVariable("FLOWTIDE_TEST_MULTIJOIN") != null)
            {
                // The suite wide setting would replace the one under test
                return;
            }
            await using var stream = new FlowtideTestStream("MultiJoinSetting/SettingAlone");
            Seed(stream);
            await stream.StartStream(Sql, planOptimizerSettings: new PlanOptimizerSettings() { MultiJoin = true });
            await stream.WaitForUpdate();
            Assert.Equal(1, stream.MultiJoinGroupsMarked);
            stream.AssertCurrentDataEqual(Expected(stream));
        }

        // Keeps its state on disk, where every version of a stream has a folder of its own
        private sealed class DiskStream : FlowtideTestStream
        {
            private readonly string _directory;

            public DiskStream(string testName, string directory) : base(testName)
            {
                _directory = directory;
            }

            protected override IPersistentStorage CreatePersistentStorage(string testName, bool ignoreSameDataCheck)
            {
                return new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new LocalDiskProvider(_directory) });
            }
        }

        // From and to are the most inputs of a multi join, zero is the setting turned off. Set on the stream so a suite wide setting cannot change them
        [Theory]
        [InlineData(0, 5, true)]
        [InlineData(5, 0, true)]
        [InlineData(5, 3, true)]
        [InlineData(0, 5, false)]
        public async Task AStreamWithoutAVersionOfItsOwnStartsOverWhenTheSettingChanges(int from, int to, bool versionFromPlanHash)
        {
            string directory = $"./data/multiJoinSetting/{Guid.NewGuid():N}";
            string name = $"MultiJoinSetting/PlanVersion_{from}_{to}_{versionFromPlanHash}";
            try
            {
                await using (var before = new DiskStream(name, directory) { VersionFromPlanHash = versionFromPlanHash, MultiJoinMaxInputs = from })
                {
                    Seed(before);
                    await before.StartStream(Sql, planOptimizerSettings: Settings(from));
                    await before.WaitForUpdate();
                    before.AssertCurrentDataEqual(Expected(before));
                    await before.StopStream();
                }

                await using (var after = new DiskStream(name, directory) { VersionFromPlanHash = versionFromPlanHash, MultiJoinMaxInputs = to })
                {
                    Seed(after);
                    await after.StartStream(Sql, planOptimizerSettings: Settings(to));
                    await after.WaitForUpdate();
                    // The storage keeps the state under the hash of the plan, so nothing of the state before is used
                    after.AssertCurrentDataEqual(Expected(after));
                    await after.StopStream();
                }
            }
            finally
            {
                if (Directory.Exists(directory))
                {
                    Directory.Delete(directory, true);
                }
            }
        }

        [Theory]
        [InlineData(0, 5)]
        [InlineData(5, 0)]
        [InlineData(5, 3)]
        public async Task AStreamWithAVersionOfItsOwnRefusesToStartWhenTheSettingChanges(int from, int to)
        {
            var fileProvider = new KeepAliveMemoryFileProvider();
            string name = $"MultiJoinSetting/FixedVersion_{from}_{to}";
            await using (var before = new SharedStorageTestStream(name, fileProvider) { MultiJoinMaxInputs = from })
            {
                Seed(before);
                await before.StartStream(Sql, planOptimizerSettings: Settings(from), version: "v1");
                await before.WaitForUpdate();
                await before.StopStream();
            }

            await using var after = new SharedStorageTestStream(name, fileProvider) { MultiJoinMaxInputs = to };
            Seed(after);
            var error = await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await after.StartStream(Sql, planOptimizerSettings: Settings(to), version: "v1");
                await after.WaitForUpdate();
            });
            Assert.Contains("plan hash", error.ToString());
        }
    }
}
