﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

using FlowtideDotNet.Substrait.Expressions;
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests
{
    [Collection("Acceptance tests")]
    public class TableFunctionTests : FlowtideAcceptanceBase
    {
        public TableFunctionTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        [Fact]
        public async Task TableFunctionNotFound()
        {
            GenerateData(100);
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await StartStream(@"
                    INSERT INTO output 
                    SELECT 
                        userkey, order_item
                    FROM orders
                    LEFT JOIN notfound(orderkey) order_item;");
            });
            Assert.Equal("Table function 'notfound' not found", ex.Message);
        }

        [Fact]
        public async Task TableFunctionRightJoinFails()
        {
            GenerateData(100);
            var ex = await Assert.ThrowsAsync<NotImplementedException>(async () =>
            {
                await StartStream(@"
                    INSERT INTO output 
                    SELECT 
                        userkey, order_item
                    FROM orders
                    RIGHT JOIN unnest(orderkey) order_item;");
            });
            Assert.Equal("Join type 'RightOuter' is not yet supported for table function with joins in SQL mode.", ex.Message);
        }

        [Fact]
        public async Task UnnestLeftJoinNoCondition()
        {
            GenerateData(100);
            await StartStream(@"
                CREATE VIEW test AS
                SELECT 
                    userkey, list_agg(orderkey) as orders
                FROM orders
                GROUP BY userkey;

                INSERT INTO output 
                SELECT 
                    userkey, order_item
                FROM test
                LEFT JOIN unnest(orders) order_item;");
            await WaitForUpdate();

            this.AssertCurrentDataEqual(Orders.Select(x => new { x.UserKey, x.OrderKey }));
        }

        [Fact]
        public async Task UnnestLeftJoinWithStaticCondition()
        {
            GenerateData(100);
            await StartStream(@"
                CREATE VIEW test AS
                SELECT 
                    userkey, list_agg(orderkey) as orders
                FROM orders
                GROUP BY userkey;

                INSERT INTO output 
                SELECT 
                    userkey, order_item
                FROM test
                LEFT JOIN unnest(orders) order_item ON order_item = 105;");
            await WaitForUpdate();

            AssertCurrentDataEqual(Orders.GroupBy(x => x.UserKey).Select(x => new { UserKey = x.Key, x.FirstOrDefault(y => y.OrderKey == 105)?.OrderKey }));
        }

        [Fact]
        public async Task UnnestLeftJoinWithDynamicCondition()
        {
            GenerateData(100);
            await StartStream(@"
                CREATE VIEW test AS
                SELECT 
                    userkey, list_agg(orderkey) as orders
                FROM orders
                GROUP BY userkey;

                INSERT INTO output 
                SELECT 
                    userkey, order_item
                FROM test
                LEFT JOIN unnest(orders) order_item ON order_item = userkey + 100;");
            await WaitForUpdate();

            // One row will have a match, the rest will be null in this result
            AssertCurrentDataEqual(Orders.GroupBy(x => x.UserKey).Select(x => new { UserKey = x.Key, x.FirstOrDefault(y => y.OrderKey == x.Key + 100)?.OrderKey }));
        }

        [Fact]
        public async Task UnnestInnerJoinNoCondition()
        {
            GenerateData(100);
            await StartStream(@"
                CREATE VIEW test AS
                SELECT 
                    userkey, list_agg(orderkey) as orders
                FROM orders
                GROUP BY userkey;

                INSERT INTO output 
                SELECT 
                    userkey, order_item
                FROM test
                INNER JOIN unnest(orders) order_item;");
            await WaitForUpdate();

            this.AssertCurrentDataEqual(Orders.Select(x => new { x.UserKey, x.OrderKey }));
        }

        [Fact]
        public async Task UnnestInnerJoinWithCondition()
        {
            GenerateData(100);
            await StartStream(@"
                CREATE VIEW test AS
                SELECT 
                    userkey, list_agg(orderkey) as orders
                FROM orders
                GROUP BY userkey;

                INSERT INTO output 
                SELECT 
                    userkey, order_item
                FROM test
                INNER JOIN unnest(orders) order_item ON order_item = 105;");
            await WaitForUpdate();

            AssertCurrentDataEqual(Orders.Where(x => x.OrderKey == 105).Select(x => new { x.UserKey, x.OrderKey }));
        }

        [Fact]
        public async Task UnnestInFrom()
        {
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    t
                FROM unnest(list(1,2,3)) t;");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[] { new { x = 1 }, new { x = 2 }, new { x = 3 } });
        }

        [Fact]
        public async Task UnnestInFromWithMap()
        {
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    t
                FROM unnest(map('key1', 1, 'key2', 2)) t;");
            await WaitForUpdate();
            var act = GetActualRows();

            var key1 = act.Columns[0].GetValueAt(0, new MapKeyReferenceSegment() { Key = "key" });
            var value1 = act.Columns[0].GetValueAt(0, new MapKeyReferenceSegment() { Key = "value" });

            Assert.Equal("key1", key1.AsString.ToString());
            Assert.Equal(1, value1.AsLong);

            var key2 = act.Columns[0].GetValueAt(1, new MapKeyReferenceSegment() { Key = "key" });
            var value2 = act.Columns[0].GetValueAt(1, new MapKeyReferenceSegment() { Key = "value" });

            Assert.Equal("key2", key2.AsString.ToString());
            Assert.Equal(2, value2.AsLong);
        }
    }
}
