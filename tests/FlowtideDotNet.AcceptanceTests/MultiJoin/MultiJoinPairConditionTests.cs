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
using FlowtideDotNet.Core.Operators.Join.MultiJoin;
using FlowtideDotNet.Core.Optimizer;

namespace FlowtideDotNet.AcceptanceTests.MultiJoin
{
    [Collection("Multi join row budget")]
    public class MultiJoinPairConditionTests
    {
        private record Row(int OrderKey, int? UserKey, string? CompanyName);

        // An order is in the period of a user: the order key is the date, visits the start and the user key the end
        private const string Sql = @"
            INSERT INTO output
            SELECT o.orderkey, u.userkey, c.name
            FROM orders o
            LEFT JOIN users u ON o.userkey = u.managerKey AND o.orderkey >= u.visits AND o.orderkey < u.userkey
            LEFT JOIN companies c ON u.companyId = c.companyId";

        // Six periods of at least 30 per key with a gap between them, key 5 has none. The end is the key of the user, so it is its own for every user
        private static void Seed(FlowtideTestStream stream)
        {
            for (int c = 0; c < 3; c++)
            {
                stream.AddOrUpdateCompany(new Company() { CompanyId = c.ToString(), Name = "company " + c });
            }
            for (int u = 0; u < 30; u++)
            {
                int start = u / 5 * 40;
                stream.AddOrUpdateUser(new User() { UserKey = start + 30 + u % 5, ManagerKey = u % 5, Visits = start, CompanyId = (u % 4).ToString() });
            }
            for (int o = 0; o < 250; o++)
            {
                stream.AddOrUpdateOrder(new Order() { OrderKey = o, UserKey = o % 6 });
            }
        }

        private static IEnumerable<Row> Expected(FlowtideTestStream stream)
        {
            return from o in stream.Orders
                   from u in stream.Users.Where(u => u.ManagerKey == o.UserKey && o.OrderKey >= u.Visits && o.OrderKey < u.UserKey).DefaultIfEmpty()
                   from c in stream.Companies.Where(c => u != null && c.CompanyId == u.CompanyId).DefaultIfEmpty()
                   select new Row(o.OrderKey, u?.UserKey, c?.Name);
        }

        private static async Task<FlowtideTestStream> Start(string name)
        {
            var stream = new FlowtideTestStream("MultiJoinPairCondition/" + name) { MultiJoinMaxInputs = 5 };
            Seed(stream);
            // The setting alone: the stream is built from the marks it leaves
            await stream.StartStream(Sql, planOptimizerSettings: new PlanOptimizerSettings() { MultiJoinPairConditions = true });
            await stream.WaitForUpdate();
            stream.AssertCurrentDataEqual(Expected(stream));
            return stream;
        }

        [Fact]
        public async Task ALeftJoinWithARangeOnBothSidesRunsAsAMultiJoin()
        {
            await using var stream = await Start("Grouped");

            Assert.Equal(1, stream.MultiJoinGroupsMarked);
        }

        [Fact]
        public async Task AChangeOnTheKeptSideIsMatchedByTheRange()
        {
            await using var stream = await Start("KeptSide");

            // Past every period, and one inside a period
            stream.AddOrUpdateOrder(new Order() { OrderKey = 1000, UserKey = 0 });
            stream.AddOrUpdateOrder(new Order() { OrderKey = 1001, UserKey = 1 });
            await stream.WaitForUpdate();
            stream.AssertCurrentDataEqual(Expected(stream));

            // To another key, where the same date is in the period of another user or of none
            for (int o = 0; o < 12; o++)
            {
                var order = stream.Orders[o];
                order.UserKey = (order.UserKey + 1) % 6;
                stream.AddOrUpdateOrder(order);
            }
            await stream.WaitForUpdate();
            stream.AssertCurrentDataEqual(Expected(stream));

            stream.DeleteOrder(stream.Orders[20]);
            stream.DeleteOrder(stream.Orders[35]);
            await stream.WaitForUpdate();
            stream.AssertCurrentDataEqual(Expected(stream));

            Assert.Equal(1, stream.MultiJoinGroupsMarked);
        }

        // With a budget of one row every part of the rows read has one row
        // With no entry kept in memory every change goes through the trees of the pad table
        [Theory]
        [InlineData(1024, 1024)]
        [InlineData(1, 1024)]
        [InlineData(1024, 0)]
        [InlineData(1, 0)]
        public async Task AChangeOfAPeriodTurnsOnlyTheRowsInIt(int rowBudget, int memoryKeys)
        {
            int budgetBefore = MultiJoinOperator.RowBudget;
            int memoryBefore = MultiJoinPadTable.MemoryKeys;
            MultiJoinOperator.RowBudget = rowBudget;
            MultiJoinPadTable.MemoryKeys = memoryKeys;
            try
            {
                await ChangePeriods($"{rowBudget}_{memoryKeys}");
            }
            finally
            {
                MultiJoinOperator.RowBudget = budgetBefore;
                MultiJoinPadTable.MemoryKeys = memoryBefore;
            }
        }

        private static async Task ChangePeriods(string name)
        {
            await using var stream = await Start("Period_" + name);

            // The period starts later: the orders of the key before the new start are filled with nulls, the ones after it stay
            var later = stream.Users.First(u => u.ManagerKey == 0 && u.Visits == 0);
            later.Visits = 12;
            stream.AddOrUpdateUser(later);
            await stream.WaitForUpdate();
            stream.AssertCurrentDataEqual(Expected(stream));

            // The period starts earlier, over the gap: orders that were filled with nulls get a match
            var earlier = stream.Users.First(u => u.ManagerKey == 1 && u.Visits == 80);
            earlier.Visits = 72;
            stream.AddOrUpdateUser(earlier);
            await stream.WaitForUpdate();
            stream.AssertCurrentDataEqual(Expected(stream));

            // A period that overlaps another: orders in both match twice
            stream.AddOrUpdateUser(new User() { UserKey = 55, ManagerKey = 2, Visits = 20, CompanyId = "1" });
            // The first period of a key that had none
            stream.AddOrUpdateUser(new User() { UserKey = 100, ManagerKey = 5, Visits = 0, CompanyId = "2" });
            await stream.WaitForUpdate();
            stream.AssertCurrentDataEqual(Expected(stream));

            // A period goes, with the one that overlapped
            stream.DeleteUser(stream.Users.First(u => u.ManagerKey == 3 && u.Visits == 120));
            stream.DeleteUser(stream.Users.First(u => u.UserKey == 55));
            await stream.WaitForUpdate();
            stream.AssertCurrentDataEqual(Expected(stream));

            Assert.Equal(1, stream.MultiJoinGroupsMarked);
        }
    }
}
