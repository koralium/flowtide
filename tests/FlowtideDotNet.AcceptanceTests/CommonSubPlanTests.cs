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

using FlowtideDotNet.Core.Optimizer;
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests
{
    [Collection("Acceptance tests")]
    public class CommonSubPlanTests : FlowtideAcceptanceBase
    {
        public CommonSubPlanTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        // Nexmark query 5 shape, the count aggregate subtree is used both for
        // the per user counts and as input to the max aggregate.
        private const string HotUsersSql = @"
            INSERT INTO output
            SELECT UserOrders.userkey, UserOrders.num, UserOrders.starttime
            FROM (
              SELECT UserKey AS userkey, count(*) AS num, window_start AS starttime
              FROM orders INNER JOIN hopping_window(Orderdate, 5, 'MINUTE', 10, 'MINUTE')
              GROUP BY UserKey, window_start
            ) AS UserOrders
            JOIN (
              SELECT max(CountOrders.num) AS maxn, CountOrders.starttime
              FROM (
                SELECT count(*) AS num, window_start AS starttime
                FROM orders INNER JOIN hopping_window(Orderdate, 5, 'MINUTE', 10, 'MINUTE')
                GROUP BY UserKey, window_start
              ) AS CountOrders
              GROUP BY CountOrders.starttime
            ) AS MaxOrders
            ON UserOrders.starttime = MaxOrders.starttime AND
                UserOrders.num >= MaxOrders.maxn;";

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task UsersWithMostOrdersPerWindow(bool findCommonSubPlans)
        {
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 1, 0) });
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 2, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 2, 0) });
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 3, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 3, 0) });
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 4, UserKey = 2, Orderdate = new DateTime(2000, 1, 1, 0, 2, 0) });
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 5, UserKey = 2, Orderdate = new DateTime(2000, 1, 1, 0, 4, 0) });
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 6, UserKey = 2, Orderdate = new DateTime(2000, 1, 1, 0, 6, 0) });

            await StartStream(HotUsersSql, planOptimizerSettings: new PlanOptimizerSettings()
            {
                FindCommonSubPlans = findCommonSubPlans
            });
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                // Window 23:55 - 00:05, user 1 has 3 orders, user 2 has 2
                new { userkey = 1, num = 3, starttime = new DateTimeOffset(1999, 12, 31, 23, 55, 0, TimeSpan.Zero) },
                // Window 00:00 - 00:10, both users have 3 orders
                new { userkey = 1, num = 3, starttime = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero) },
                new { userkey = 2, num = 3, starttime = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero) },
                // Window 00:05 - 00:15, only user 2 has an order
                new { userkey = 2, num = 1, starttime = new DateTimeOffset(2000, 1, 1, 0, 5, 0, TimeSpan.Zero) },
            });
        }

        public record WindowSumResult(int userkey, long value);

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task DuplicatedWindowFunction(bool findCommonSubPlans)
        {
            GenerateData();

            await StartStream(@"
                INSERT INTO output
                SELECT a.userkey, a.value
                FROM (
                  SELECT UserKey AS userkey,
                    CAST(SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY userkey ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS INT) AS value
                  FROM users
                ) a
                JOIN (
                  SELECT UserKey AS userkey,
                    CAST(SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY userkey ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS INT) AS value
                  FROM users
                ) b ON a.userkey = b.userkey;", planOptimizerSettings: new PlanOptimizerSettings()
            {
                FindCommonSubPlans = findCommonSubPlans
            });
            await WaitForUpdate();

            var expected = Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var sum = 0.0;
                    var orderedByKey = g.OrderBy(x => x.UserKey).ToList();
                    var values = new Queue<double>();
                    var output = new List<WindowSumResult>();
                    for (int i = 0; i < orderedByKey.Count; i++)
                    {
                        while (values.Count > 4)
                        {
                            sum -= values.Dequeue();
                        }
                        values.Enqueue(orderedByKey[i].DoubleValue);
                        sum += orderedByKey[i].DoubleValue;
                        output.Add(new WindowSumResult(orderedByKey[i].UserKey, (long)sum));
                    }
                    return output;
                }).ToList();

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task SharedSubtreeUpdatesBothUsages()
        {
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 1, 0) });
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 2, UserKey = 2, Orderdate = new DateTime(2000, 1, 1, 0, 2, 0) });

            await StartStream(HotUsersSql);
            await WaitForUpdate();

            // A new order makes user 2 the single winner in every window
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 3, UserKey = 2, Orderdate = new DateTime(2000, 1, 1, 0, 3, 0) });
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new { userkey = 2, num = 2, starttime = new DateTimeOffset(1999, 12, 31, 23, 55, 0, TimeSpan.Zero) },
                new { userkey = 2, num = 2, starttime = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero) },
            });
        }
    }
}
