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

using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests.MultiJoin
{
    [Collection("Multi join row budget")]
    public class MultiJoinInnerTests : FlowtideAcceptanceBase
    {
        public MultiJoinInnerTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
            MultiJoinMaxInputs = 5;
        }

        private record ChainRow(int OrderKey, string? FirstName, string? Name);
        private record StarRow(int ProjectMemberKey, string? FirstName, string? ProjectName, string? Name);

        private const string ChainSql = @"
            INSERT INTO output
            SELECT o.orderkey, u.firstName, c.name
            FROM orders o
            INNER JOIN users u ON o.userkey = u.userkey
            INNER JOIN companies c ON u.companyId = c.companyId";

        private IEnumerable<ChainRow> ExpectedChain()
        {
            return from o in Orders
                   join u in Users on o.UserKey equals u.UserKey
                   join c in Companies on u.CompanyId equals c.CompanyId
                   select new ChainRow(o.OrderKey, u.FirstName, c.Name);
        }

        [Fact]
        public async Task ChainOfThreeInputs()
        {
            GenerateData(1000);
            await StartStream(ChainSql);
            await WaitForUpdate();

            Assert.Equal(1, MultiJoinGroupsMarked);
            AssertCurrentDataEqual(ExpectedChain());
        }

        [Fact]
        public async Task ChangesOnEveryInput()
        {
            GenerateData(1000);
            await StartStream(ChainSql);
            await WaitForUpdate();
            Assert.Equal(1, MultiJoinGroupsMarked);

            // Payload change in the middle input
            var user = Users[10];
            user.FirstName = "changed";
            AddOrUpdateUser(user);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedChain());

            // Key change in the middle input, its secondary index entry moves
            var moved = Users[20];
            moved.CompanyId = Companies.First(x => x.CompanyId != moved.CompanyId).CompanyId;
            AddOrUpdateUser(moved);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedChain());

            // Change at the far end
            var company = Companies[0];
            company.Name = "renamed";
            AddOrUpdateCompany(company);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedChain());

            // Deletes and new rows on the first input
            DeleteOrder(Orders[0]);
            DeleteOrder(Orders[1]);
            GenerateOrders(100);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedChain());

            DeleteUser(Users[30]);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedChain());
        }

        [Fact]
        public async Task StarOfFourInputsWithACompositeKey()
        {
            GenerateData(1000);
            GenerateProjects(100);
            GenerateProjectMembers(1000);
            await StartStream(@"
                INSERT INTO output
                SELECT pm.projectMemberKey, u.firstName, p.name, c.name
                FROM projectmembers pm
                INNER JOIN users u ON pm.userkey = u.userkey
                INNER JOIN projects p ON pm.projectNumber = p.projectNumber AND pm.companyId = p.companyId
                INNER JOIN companies c ON pm.companyId = c.companyId");
            await WaitForUpdate();
            Assert.Equal(1, MultiJoinGroupsMarked);

            IEnumerable<StarRow> Expected() =>
                from pm in ProjectMembers
                join u in Users on pm.UserKey equals u.UserKey
                join p in Projects on new { pm.ProjectNumber, pm.CompanyId } equals new { p.ProjectNumber, p.CompanyId }
                join c in Companies on pm.CompanyId equals c.CompanyId
                select new StarRow(pm.ProjectMemberKey, u.FirstName, p.Name, c.Name);

            AssertCurrentDataEqual(Expected());

            var project = Projects[5];
            project.Name = "renamed project";
            AddOrUpdateProject(project);
            var user = Users[40];
            user.FirstName = "changed";
            AddOrUpdateUser(user);
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());
        }

        [Fact]
        public async Task AnInputCapOfThreeGivesTwoMultiJoinsInARow()
        {
            MultiJoinMaxInputs = 3;
            GenerateData(500);
            GenerateProjects(50);
            GenerateProjectMembers(500);
            await StartStream(@"
                INSERT INTO output
                SELECT pm.projectMemberKey, u.firstName, p.name, c.name
                FROM projectmembers pm
                INNER JOIN users u ON pm.userkey = u.userkey
                INNER JOIN projects p ON pm.projectNumber = p.projectNumber AND pm.companyId = p.companyId
                INNER JOIN companies c ON pm.companyId = c.companyId
                INNER JOIN orders o ON o.userkey = u.userkey");
            await WaitForUpdate();

            // Four joins at a cap of three inputs: two groups of two joins, the lower one is an input of the upper one
            Assert.Equal(2, MultiJoinGroupsMarked);

            AssertCurrentDataEqual(
                from pm in ProjectMembers
                join u in Users on pm.UserKey equals u.UserKey
                join p in Projects on new { pm.ProjectNumber, pm.CompanyId } equals new { p.ProjectNumber, p.CompanyId }
                join c in Companies on pm.CompanyId equals c.CompanyId
                join o in Orders on u.UserKey equals o.UserKey
                select new StarRow(pm.ProjectMemberKey, u.FirstName, p.Name, c.Name));
        }

        [Fact]
        public async Task CrashThenChanges()
        {
            GenerateData(1000);
            await StartStream(ChainSql, ignoreSameDataCheck: true);
            await WaitForUpdate();
            Assert.Equal(1, MultiJoinGroupsMarked);

            await Crash();

            var user = Users[10];
            user.FirstName = "changed";
            AddOrUpdateUser(user);
            DeleteOrder(Orders[0]);
            GenerateOrders(200);
            var company = Companies[0];
            company.Name = "renamed";
            AddOrUpdateCompany(company);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedChain());
        }

        [Fact]
        public async Task CrashWithUnprocessedChanges()
        {
            GenerateData(1000);
            await StartStream(ChainSql, ignoreSameDataCheck: true);
            await WaitForUpdate();

            // Changes made right before the crash are delivered again after the rollback
            DeleteUser(Users[5]);
            GenerateOrders(300);
            var renamed = Users[20];
            renamed.FirstName = "changed";
            AddOrUpdateUser(renamed);
            await Crash();

            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedChain());

            DeleteUser(Users[7]);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedChain());
        }

        [Fact]
        public async Task StopAndStart()
        {
            GenerateData(1000);
            await StartStream(ChainSql, ignoreSameDataCheck: true);
            await WaitForUpdate();

            await StopStream();

            // Changes on every input while the stream is down
            DeleteUser(Users[5]);
            var renamed = Users[20];
            renamed.FirstName = "changed";
            AddOrUpdateUser(renamed);
            GenerateOrders(100);
            var company = Companies[0];
            company.Name = "renamed";
            AddOrUpdateCompany(company);

            await StartStream();
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedChain());
        }

        [Fact]
        public async Task ASliceOfOneRowGivesTheSameResult()
        {
            int before = Core.Operators.Join.MultiJoin.MultiJoinOperator.RowBudget;
            Core.Operators.Join.MultiJoin.MultiJoinOperator.RowBudget = 1;
            try
            {
                GenerateData(300);
                await StartStream(ChainSql);
                await WaitForUpdate();
                AssertCurrentDataEqual(ExpectedChain());

                var company = Companies[0];
                company.Name = "renamed";
                AddOrUpdateCompany(company);
                await WaitForUpdate();
                AssertCurrentDataEqual(ExpectedChain());
            }
            finally
            {
                Core.Operators.Join.MultiJoin.MultiJoinOperator.RowBudget = before;
            }
        }
    }
}
