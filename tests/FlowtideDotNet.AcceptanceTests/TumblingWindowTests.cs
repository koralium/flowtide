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
    public class TumblingWindowTests : FlowtideAcceptanceBase
    {
        public TumblingWindowTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        [Fact]
        public async Task TumblingWindowSingleWindow()
        {
            // Each timestamp lands in exactly one window
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 7, 0) });
            await StartStream(@"
                INSERT INTO output
                SELECT window_start, window_end
                FROM orders
                INNER JOIN tumbling_window(Orderdate, 10, 'MINUTE');");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new { window_start = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero), window_end = new DateTimeOffset(2000, 1, 1, 0, 10, 0, TimeSpan.Zero) },
            });
        }

        [Fact]
        public async Task TumblingWindowTimestampOnBoundary()
        {
            // The start is inclusive, so 00:10 starts a new window
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 10, 0) });
            await StartStream(@"
                INSERT INTO output
                SELECT window_start, window_end
                FROM orders
                INNER JOIN tumbling_window(Orderdate, 10, 'MINUTE');");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new { window_start = new DateTimeOffset(2000, 1, 1, 0, 10, 0, TimeSpan.Zero), window_end = new DateTimeOffset(2000, 1, 1, 0, 20, 0, TimeSpan.Zero) },
            });
        }

        [Fact]
        public async Task TumblingWindowSecondUnit()
        {
            // Checks that second precision is kept through the stream
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 0, 17) });
            await StartStream(@"
                INSERT INTO output
                SELECT window_start, window_end
                FROM orders
                INNER JOIN tumbling_window(Orderdate, 10, 'SECOND');");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new { window_start = new DateTimeOffset(2000, 1, 1, 0, 0, 10, TimeSpan.Zero), window_end = new DateTimeOffset(2000, 1, 1, 0, 0, 20, TimeSpan.Zero) },
            });
        }

        [Fact]
        public async Task TumblingWindowWeekUnit()
        {
            // Weeks are aligned from year one, so they start on a monday
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 6, 12, 0, 0) });
            await StartStream(@"
                INSERT INTO output
                SELECT window_start, window_end
                FROM orders
                INNER JOIN tumbling_window(Orderdate, 1, 'WEEK');");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new { window_start = new DateTimeOffset(2000, 1, 3, 0, 0, 0, TimeSpan.Zero), window_end = new DateTimeOffset(2000, 1, 10, 0, 0, 0, TimeSpan.Zero) },
            });
        }

        [Fact]
        public async Task TumblingWindowGroupByAggregation()
        {
            // The common use case, aggregate the rows per window
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 3, 0) });
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 2, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 7, 0) });
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 3, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 13, 0) });
            await StartStream(@"
                INSERT INTO output
                SELECT window_start, count(*) as cnt
                FROM orders
                INNER JOIN tumbling_window(Orderdate, 10, 'MINUTE')
                GROUP BY window_start;");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new { window_start = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero), cnt = 2 },
                new { window_start = new DateTimeOffset(2000, 1, 1, 0, 10, 0, TimeSpan.Zero), cnt = 1 },
            });
        }

        [Fact]
        public async Task TumblingWindowRetractsOnDelete()
        {
            // Deleting an order must retract it from its window
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 3, 0) });
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 2, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 7, 0) });
            await StartStream(@"
                INSERT INTO output
                SELECT window_start, count(*) as cnt
                FROM orders
                INNER JOIN tumbling_window(Orderdate, 10, 'MINUTE')
                GROUP BY window_start;");
            await WaitForUpdate();

            DeleteOrder(Orders[0]);
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new { window_start = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero), cnt = 1 },
            });
        }

        [Fact]
        public async Task TumblingWindowUpdateMovesRowToNewWindow()
        {
            // Changing the timestamp must move the row to the new window
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 3, 0) });
            await StartStream(@"
                INSERT INTO output
                SELECT window_start
                FROM orders
                INNER JOIN tumbling_window(Orderdate, 10, 'MINUTE');");
            await WaitForUpdate();

            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 3, 33, 0) });
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new { window_start = new DateTimeOffset(2000, 1, 1, 3, 30, 0, TimeSpan.Zero) },
            });
        }

        [Fact]
        public async Task TumblingWindowNullTimestampInnerJoin()
        {
            // A null timestamp belongs to no window, so the row is dropped
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 7, 0) });
            await StartStream(@"
                INSERT INTO output
                SELECT OrderKey, window_start
                FROM orders
                INNER JOIN tumbling_window(null, 10, 'MINUTE');");
            await WaitForUpdate();

            AssertCurrentDataEqual(Orders.Take(0).Select(x => new { x.OrderKey, window_start = (DateTimeOffset?)null }));
        }

        [Fact]
        public async Task TumblingWindowNullTimestampLeftJoin()
        {
            // A left join keeps the row and sets both window columns to null
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 7, 0) });
            await StartStream(@"
                INSERT INTO output
                SELECT OrderKey, window_start, window_end
                FROM orders
                LEFT JOIN tumbling_window(null, 10, 'MINUTE');");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new { OrderKey = 1, window_start = (DateTimeOffset?)null, window_end = (DateTimeOffset?)null },
            });
        }

        [Fact]
        public async Task TumblingWindowMaxPerWindowJoin()
        {
            // Nexmark q7 shape, join each row against the max value in its window
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 0, 3), Money = 10 });
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 2, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 0, 7), Money = 25 });
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 3, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 0, 13), Money = 15 });
            await StartStream(@"
                INSERT INTO output
                SELECT o.OrderKey, o.Money
                FROM orders o
                JOIN (
                    SELECT max(Money) AS maxmoney, window_start, window_end
                    FROM orders
                    INNER JOIN tumbling_window(Orderdate, 10, 'SECOND')
                    GROUP BY window_start, window_end
                ) m
                ON o.Money = m.maxmoney AND o.Orderdate >= m.window_start AND o.Orderdate < m.window_end;");
            await WaitForUpdate();

            // Order 2 wins the first window and order 3 its own window
            AssertCurrentDataEqual(new[]
            {
                new { OrderKey = 2, Money = 25m },
                new { OrderKey = 3, Money = 15m },
            });
        }

        [Fact]
        public async Task TumblingWindowRejectsCalendarUnit()
        {
            // Months vary in length and must be rejected
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 7, 0) });
            var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await StartStream(@"
                    INSERT INTO output
                    SELECT window_start
                    FROM orders
                    INNER JOIN tumbling_window(Orderdate, 1, 'MONTH');");
                await WaitForUpdate();
            });
            Assert.Contains("unit 'MONTH' is not supported", ex.ToString());
        }

        [Fact]
        public async Task TumblingWindowRejectsZeroAmount()
        {
            // A zero size window can never contain a timestamp
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 7, 0) });
            var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await StartStream(@"
                    INSERT INTO output
                    SELECT window_start
                    FROM orders
                    INNER JOIN tumbling_window(Orderdate, 0, 'MINUTE');");
                await WaitForUpdate();
            });
            Assert.Contains("must be a positive duration", ex.ToString());
        }

        [Fact]
        public async Task TumblingWindowRejectsFractionalAmount()
        {
            // A fraction should be written with a smaller unit instead
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 7, 0) });
            var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await StartStream(@"
                    INSERT INTO output
                    SELECT window_start
                    FROM orders
                    INNER JOIN tumbling_window(Orderdate, 5.7, 'MINUTE');");
                await WaitForUpdate();
            });
            Assert.Contains("must be a whole number", ex.ToString());
        }

        [Fact]
        public async Task TumblingWindowRejectsColumnAsAmount()
        {
            // The size is resolved on start, so columns can not be used
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 7, 0) });
            var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await StartStream(@"
                    INSERT INTO output
                    SELECT window_start
                    FROM orders
                    INNER JOIN tumbling_window(Orderdate, UserKey, 'MINUTE');");
                await WaitForUpdate();
            });
            Assert.Contains("must be a numeric literal", ex.ToString());
        }

        [Fact]
        public async Task TumblingWindowRejectsWrongArgumentCount()
        {
            // The function takes exactly three arguments
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 7, 0) });
            var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await StartStream(@"
                    INSERT INTO output
                    SELECT window_start
                    FROM orders
                    INNER JOIN tumbling_window(Orderdate, 10);");
                await WaitForUpdate();
            });
            Assert.Contains("requires three arguments", ex.ToString());
        }
    }
}
