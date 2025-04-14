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

            AssertCurrentDataEqual(new[] { new { list = Orders.Select(x => x.OrderKey).ToList() } });
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

            var expectedList = Orders.Select(x => new Dictionary<int, int>() { { x.OrderKey, x.OrderKey } }).ToList();
            AssertCurrentDataEqual(new[] { new { list = expectedList } });
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

        [Fact]
        public async Task ListUnionDistinctAgg()
        {
            await StartStream(@"
                CREATE VIEW testdata AS
                SELECT 
                list('a', 'b') as list
                UNION ALL
                SELECT
                list('b', 'c') as list;

                INSERT INTO output 
                SELECT 
                    list_union_distinct_agg(list)
                FROM testdata
                ");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new {list = new List<string>(){ "a", "b", "c" }}
            });
        }

        [Fact]
        public async Task ListUnionDistinctAggUnionType()
        {
            await StartStream(@"
                CREATE VIEW testdata AS
                SELECT 
                list('a', 2) as list
                UNION ALL
                SELECT
                list(2, 'c') as list;

                INSERT INTO output 
                SELECT 
                    list_union_distinct_agg(list)
                FROM testdata
                ");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new {list = new List<object>(){ 2, "a", "c" }}
            });
        }

        [Fact]
        public async Task ListSortAscendingNullLast()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    list_sort_asc_null_last(list(orderkey, userkey))
                FROM orders
                ");

            await WaitForUpdate();

            var expectedList = Orders.Select(x => new
            {
                list = (new List<int>() { x.OrderKey, x.UserKey }).OrderBy(x => x).ToList()
            } );

            AssertCurrentDataEqual(expectedList);
        }

        [Fact]
        public async Task ListFirstDifference()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    list_first_difference(list(orderkey, userkey), list(orderkey, 5)) as val
                FROM orders
                ");

            await WaitForUpdate();

            var expectedList = Orders.Select(x => new
            {
                val = x.UserKey
            });

            AssertCurrentDataEqual(expectedList);
        }

        [Fact]
        public async Task ListFilterNull()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    list_filter_null(list(orderkey, userkey, null)) as val
                FROM orders
                ");

            await WaitForUpdate();

            var expectedList = Orders.Select(x => new
            {
                val = new List<int>() { x.OrderKey, x.UserKey }
            });

            AssertCurrentDataEqual(expectedList);
        }
    }
}
