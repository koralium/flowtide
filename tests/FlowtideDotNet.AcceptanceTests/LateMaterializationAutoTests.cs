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

using FlowtideDotNet.Core.Engine;
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests
{
    public class LateMaterializationAutoTests : FlowtideAcceptanceBase
    {
        public LateMaterializationAutoTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        private const string WideJoin = @"
            INSERT INTO output
            SELECT o.orderkey, u.firstName, u.lastName, u.nullableString, u.trimmableNullableString, u.doubleValue, u.visits, c.name, m.firstName
            FROM orders o
            INNER JOIN users u ON o.userkey = u.userkey
            INNER JOIN companies c ON u.companyId = c.companyId
            INNER JOIN users m ON u.managerKey = m.userkey";

        private void AssertWideJoin()
        {
            AssertCurrentDataEqual(
                from o in Orders
                join u in Users on o.UserKey equals u.UserKey
                join c in Companies on u.CompanyId equals c.CompanyId
                join m in Users on u.ManagerKey equals m.UserKey
                select new { o.OrderKey, u.FirstName, u.LastName, u.NullableString, u.TrimmableNullableString, u.DoubleValue, u.Visits, c.Name, ManagerName = m.FirstName });
        }

        [Fact]
        public async Task WideColumnsThroughThreeJoinsGetAStore()
        {
            LateMaterialization = LateMaterializationMode.Auto;
            MultiJoinMaxInputs = 0;
            GenerateData();
            await StartStream(WideJoin);
            await WaitForUpdate();
            AssertWideJoin();

            var report = LateMaterializationReport!;
            var users = Assert.Single(report.Hosts, x => x.Admitted);
            Assert.Equal("users", users.Table);
            Assert.Equal(3, users.Copies);
            Assert.Contains(GetDiagnosticsGraph().Nodes.Values, x => x.DisplayName == "Fetch required columns");
        }

        [Fact]
        public async Task TheSameColumnsInOneMultiJoinAreDeclined()
        {
            LateMaterialization = LateMaterializationMode.Auto;
            MultiJoinMaxInputs = 5;
            GenerateData();
            await StartStream(WideJoin);
            await WaitForUpdate();
            AssertWideJoin();

            var report = LateMaterializationReport!;
            Assert.NotEmpty(report.Hosts);
            Assert.Equal(0, report.Admitted);
            Assert.All(report.Hosts, x => Assert.True(x.Copies <= 1));
            Assert.DoesNotContain(GetDiagnosticsGraph().Nodes.Values, x => x.DisplayName == "Fetch required columns");
        }

        [Fact]
        public async Task AJoinWithAFilterAndFewColumnsIsDeclined()
        {
            LateMaterialization = LateMaterializationMode.Auto;
            GenerateData();
            await StartStream(@"
                INSERT INTO output
                SELECT u.firstName, u.lastName, u.companyId, o.orderkey
                FROM orders o INNER JOIN users u ON o.userkey = u.userkey
                WHERE u.visits > 2");
            await WaitForUpdate();
            AssertCurrentDataEqual(
                from o in Orders
                join u in Users on o.UserKey equals u.UserKey
                where u.Visits > 2
                select new { u.FirstName, u.LastName, u.CompanyId, o.OrderKey });

            Assert.NotEmpty(LateMaterializationReport!.Hosts);
            Assert.Equal(0, LateMaterializationReport!.Admitted);
        }

        [Fact]
        public async Task ACountPerKeyIsDeclined()
        {
            LateMaterialization = LateMaterializationMode.Auto;
            GenerateData();
            await StartStream("INSERT INTO output SELECT userkey, count(*) FROM orders GROUP BY userkey");
            await WaitForUpdate();
            AssertCurrentDataEqual(Orders.GroupBy(x => x.UserKey).Select(x => new { UserKey = x.Key, Count = (long)x.Count() }));

            Assert.NotEmpty(LateMaterializationReport!.Hosts);
            Assert.Equal(0, LateMaterializationReport!.Admitted);
        }

        [Fact]
        public async Task AJoinOfTwoDistinctSetsIsDeclined()
        {
            LateMaterialization = LateMaterializationMode.Auto;
            GenerateData();
            await StartStream(@"
                INSERT INTO output
                SELECT u.userkey, u.firstName
                FROM users u INNER JOIN (SELECT DISTINCT userkey FROM orders) o ON u.userkey = o.userkey");
            await WaitForUpdate();
            AssertCurrentDataEqual(
                from u in Users
                join o in Orders.Select(x => x.UserKey).Distinct() on u.UserKey equals o
                select new { u.UserKey, u.FirstName });

            Assert.NotEmpty(LateMaterializationReport!.Hosts);
            Assert.Equal(0, LateMaterializationReport!.Admitted);
        }
    }
}
