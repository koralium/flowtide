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

namespace FlowtideDotNet.AcceptanceTests
{
    [Collection("Acceptance tests")]
    public class ListTests : FlowtideAcceptanceBase
    {
        public ListTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        [Fact]
        public async Task ListAggWithGrouping()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    userkey, list_agg(orderkey)
                FROM orders
                GROUP BY userkey
                ");
            await WaitForUpdate();

            AssertCurrentDataEqual(Orders.GroupBy(x => x.UserKey).Select(x => new { UserKey = x.Key, MinVal = x.Select(y => y.OrderKey).ToList() }));
        }

        [Fact]
        public async Task ListAgg()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    list_agg(orderkey)
                FROM orders
                ");
            await WaitForUpdate();

            AssertCurrentDataEqual(new [] { new { list = Orders.Select(x => x.OrderKey).ToList() } });
        }

        [Fact]
        public async Task ListAggWithUpdate()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    list_agg(firstName)
                FROM users
                ");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[] { new { list = Users.Select(x => x.FirstName).OrderBy(x => x).ToList() } });

            GenerateUsers(1);

            await WaitForUpdate();

            AssertCurrentDataEqual(new[] { new { list = Users.Select(x => x.FirstName).OrderBy(x => x).ToList() } });

            var firstUser = Users[0];
            firstUser.FirstName = "Aaa";
            AddOrUpdateUser(firstUser);

            await WaitForUpdate();

            AssertCurrentDataEqual(new[] { new { list = Users.Select(x => x.FirstName).OrderBy(x => x).ToList() } });

            DeleteUser(firstUser);

            await WaitForUpdate();

            AssertCurrentDataEqual(new[] { new { list = Users.Select(x => x.FirstName).OrderBy(x => x).ToList() } });
        }

        [Fact]
        public async Task ListAggWithObject()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    list_agg(map(orderkey, orderkey))
                FROM orders
                ");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[] { new { list = Orders.Select(x => new KeyValuePair<string, int>(x.OrderKey.ToString(), x.OrderKey)).ToList() } });
        }

        [Fact]
        public async Task ListAggWithList()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    list_agg(list(orderkey, userkey))
                FROM orders
                ");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[] { new { list = Orders.Select(x => new List<int>() { x.OrderKey, x.UserKey }).ToList() } });
        }

        [Fact]
        public async Task ListAggInsertThenDelete()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    list_agg(orderkey)
                FROM orders
                group by orderkey
                ");
            await WaitForUpdate();

            var rows1 = GetActualRows();
            Assert.Equal(1000, rows1.Count);
            var order = Orders[0];
            DeleteOrder(order);
            await WaitForUpdate();

            var rows2 = GetActualRows();

            Assert.Equal(999, rows2.Count);
        }
    }
}
