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
    public class HoppingWindowTests : FlowtideAcceptanceBase
    {
        public HoppingWindowTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        [Fact]
        public async Task HoppingWindowOverlappingWindows()
        {
            // 00:07 with hop 5 min and size 10 min lands in two overlapping windows
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 7, 0) });
            await StartStream(@"
                INSERT INTO output
                SELECT window_start, window_end
                FROM orders
                INNER JOIN hopping_window(Orderdate, 5, 'MINUTE', 10, 'MINUTE');");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new { window_start = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero), window_end = new DateTimeOffset(2000, 1, 1, 0, 10, 0, TimeSpan.Zero) },
                new { window_start = new DateTimeOffset(2000, 1, 1, 0, 5, 0, TimeSpan.Zero), window_end = new DateTimeOffset(2000, 1, 1, 0, 15, 0, TimeSpan.Zero) },
            });
        }

        [Fact]
        public async Task HoppingWindowTumblingSingleWindow()
        {
            // Equal hop and size gives tumbling windows, each timestamp lands in exactly one
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 7, 0) });
            await StartStream(@"
                INSERT INTO output
                SELECT window_start, window_end
                FROM orders
                INNER JOIN hopping_window(Orderdate, 10, 'MINUTE', 10, 'MINUTE');");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new { window_start = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero), window_end = new DateTimeOffset(2000, 1, 1, 0, 10, 0, TimeSpan.Zero) },
            });
        }

        [Fact]
        public async Task HoppingWindowGapEmitsNullForLeftJoin()
        {
            // A hop larger than the size leaves gaps, 00:07 lands in a gap and gets null windows
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 7, 0) });
            await StartStream(@"
                INSERT INTO output
                SELECT OrderKey, window_start
                FROM orders
                LEFT JOIN hopping_window(Orderdate, 10, 'MINUTE', 5, 'MINUTE');");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new { OrderKey = 1, window_start = (DateTimeOffset?)null },
            });
        }

        [Fact]
        public async Task HoppingWindowTimestampOnBoundary()
        {
            // The start is inclusive and the end exclusive, so 00:05 is not in [23:55, 00:05)
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 5, 0) });
            await StartStream(@"
                INSERT INTO output
                SELECT window_start, window_end
                FROM orders
                INNER JOIN hopping_window(Orderdate, 5, 'MINUTE', 10, 'MINUTE');");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new { window_start = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero), window_end = new DateTimeOffset(2000, 1, 1, 0, 10, 0, TimeSpan.Zero) },
                new { window_start = new DateTimeOffset(2000, 1, 1, 0, 5, 0, TimeSpan.Zero), window_end = new DateTimeOffset(2000, 1, 1, 0, 15, 0, TimeSpan.Zero) },
            });
        }

        [Fact]
        public async Task HoppingWindowDayUnit()
        {
            // Daily tumbling window, checks a larger unit than minutes
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 12, 0, 0) });
            await StartStream(@"
                INSERT INTO output
                SELECT window_start, window_end
                FROM orders
                INNER JOIN hopping_window(Orderdate, 1, 'DAY', 1, 'DAY');");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new { window_start = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero), window_end = new DateTimeOffset(2000, 1, 2, 0, 0, 0, TimeSpan.Zero) },
            });
        }

        [Fact]
        public async Task HoppingWindowSecondUnit()
        {
            // Checks that second precision is kept through the stream
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 0, 7) });
            await StartStream(@"
                INSERT INTO output
                SELECT window_start, window_end
                FROM orders
                INNER JOIN hopping_window(Orderdate, 5, 'SECOND', 10, 'SECOND');");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new { window_start = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero), window_end = new DateTimeOffset(2000, 1, 1, 0, 0, 10, TimeSpan.Zero) },
                new { window_start = new DateTimeOffset(2000, 1, 1, 0, 0, 5, TimeSpan.Zero), window_end = new DateTimeOffset(2000, 1, 1, 0, 0, 15, TimeSpan.Zero) },
            });
        }

        [Fact]
        public async Task HoppingWindowMillisecondUnit()
        {
            // Checks that millisecond precision is kept through the stream
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 0, 0, 7) });
            await StartStream(@"
                INSERT INTO output
                SELECT window_start, window_end
                FROM orders
                INNER JOIN hopping_window(Orderdate, 5, 'MILLISECOND', 10, 'MILLISECOND');");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new { window_start = new DateTimeOffset(2000, 1, 1, 0, 0, 0, 0, TimeSpan.Zero), window_end = new DateTimeOffset(2000, 1, 1, 0, 0, 0, 10, TimeSpan.Zero) },
                new { window_start = new DateTimeOffset(2000, 1, 1, 0, 0, 0, 5, TimeSpan.Zero), window_end = new DateTimeOffset(2000, 1, 1, 0, 0, 0, 15, TimeSpan.Zero) },
            });
        }

        [Fact]
        public async Task HoppingWindowMicrosecondUnit()
        {
            // Checks that microsecond precision is kept through the stream
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 0, 0, 0, 7) });
            await StartStream(@"
                INSERT INTO output
                SELECT window_start, window_end
                FROM orders
                INNER JOIN hopping_window(Orderdate, 5, 'MICROSECOND', 10, 'MICROSECOND');");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new { window_start = new DateTimeOffset(2000, 1, 1, 0, 0, 0, 0, 0, TimeSpan.Zero), window_end = new DateTimeOffset(2000, 1, 1, 0, 0, 0, 0, 10, TimeSpan.Zero) },
                new { window_start = new DateTimeOffset(2000, 1, 1, 0, 0, 0, 0, 5, TimeSpan.Zero), window_end = new DateTimeOffset(2000, 1, 1, 0, 0, 0, 0, 15, TimeSpan.Zero) },
            });
        }

        [Fact]
        public async Task HoppingWindowSizeTenHopTwoGivesFiveWindows()
        {
            // A size five times the hop should give five overlapping windows
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 0, 11) });
            await StartStream(@"
                INSERT INTO output
                SELECT window_start, window_end
                FROM orders
                INNER JOIN hopping_window(Orderdate, 2, 'SECOND', 10, 'SECOND');");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new { window_start = new DateTimeOffset(2000, 1, 1, 0, 0, 2, TimeSpan.Zero), window_end = new DateTimeOffset(2000, 1, 1, 0, 0, 12, TimeSpan.Zero) },
                new { window_start = new DateTimeOffset(2000, 1, 1, 0, 0, 4, TimeSpan.Zero), window_end = new DateTimeOffset(2000, 1, 1, 0, 0, 14, TimeSpan.Zero) },
                new { window_start = new DateTimeOffset(2000, 1, 1, 0, 0, 6, TimeSpan.Zero), window_end = new DateTimeOffset(2000, 1, 1, 0, 0, 16, TimeSpan.Zero) },
                new { window_start = new DateTimeOffset(2000, 1, 1, 0, 0, 8, TimeSpan.Zero), window_end = new DateTimeOffset(2000, 1, 1, 0, 0, 18, TimeSpan.Zero) },
                new { window_start = new DateTimeOffset(2000, 1, 1, 0, 0, 10, TimeSpan.Zero), window_end = new DateTimeOffset(2000, 1, 1, 0, 0, 20, TimeSpan.Zero) },
            });
        }

        [Fact]
        public async Task HoppingWindowProjectOnlyLeftColumns()
        {
            // Using no window column should still duplicate the row for each window
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 7, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 7, 0) });
            await StartStream(@"
                INSERT INTO output
                SELECT OrderKey
                FROM orders
                INNER JOIN hopping_window(Orderdate, 5, 'MINUTE', 10, 'MINUTE');");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new { OrderKey = 7 },
                new { OrderKey = 7 },
            });
        }

        [Fact]
        public async Task HoppingWindowRejectsExcessiveFanout()
        {
            // A millisecond hop with a day long size must be rejected on start
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 0, 0) });
            var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await StartStream(@"
                    INSERT INTO output
                    SELECT window_start
                    FROM orders
                    INNER JOIN hopping_window(Orderdate, 1, 'MILLISECOND', 1, 'DAY');");
                await WaitForUpdate();
            });
            Assert.Contains("exceeding the limit", ex.ToString());
        }

        [Fact]
        public async Task HoppingWindowGroupByAggregation()
        {
            // The common use case, aggregate the rows per window
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 3, 0) });
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 2, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 7, 0) });
            await StartStream(@"
                INSERT INTO output
                SELECT window_start, count(*) as cnt
                FROM orders
                INNER JOIN hopping_window(Orderdate, 5, 'MINUTE', 10, 'MINUTE')
                GROUP BY window_start;");
            await WaitForUpdate();

            // Both orders share the window starting at 00:00
            AssertCurrentDataEqual(new[]
            {
                new { window_start = new DateTimeOffset(1999, 12, 31, 23, 55, 0, TimeSpan.Zero), cnt = 1 },
                new { window_start = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero), cnt = 2 },
                new { window_start = new DateTimeOffset(2000, 1, 1, 0, 5, 0, TimeSpan.Zero), cnt = 1 },
            });
        }

        [Fact]
        public async Task HoppingWindowRetractsOnDelete()
        {
            // Deleting an order must retract it from all its windows
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 3, 0) });
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 2, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 7, 0) });
            await StartStream(@"
                INSERT INTO output
                SELECT window_start, count(*) as cnt
                FROM orders
                INNER JOIN hopping_window(Orderdate, 5, 'MINUTE', 10, 'MINUTE')
                GROUP BY window_start;");
            await WaitForUpdate();

            DeleteOrder(Orders[0]);
            await WaitForUpdate();

            // The 23:55 window is now empty and 00:00 only has the second order left
            AssertCurrentDataEqual(new[]
            {
                new { window_start = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero), cnt = 1 },
                new { window_start = new DateTimeOffset(2000, 1, 1, 0, 5, 0, TimeSpan.Zero), cnt = 1 },
            });
        }

        [Fact]
        public async Task HoppingWindowUpdateMovesRowToNewWindows()
        {
            // Changing the timestamp must move the row to the new windows
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 3, 0) });
            await StartStream(@"
                INSERT INTO output
                SELECT window_start
                FROM orders
                INNER JOIN hopping_window(Orderdate, 5, 'MINUTE', 10, 'MINUTE');");
            await WaitForUpdate();

            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 3, 33, 0) });
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new { window_start = new DateTimeOffset(2000, 1, 1, 3, 25, 0, TimeSpan.Zero) },
                new { window_start = new DateTimeOffset(2000, 1, 1, 3, 30, 0, TimeSpan.Zero) },
            });
        }

        [Fact]
        public async Task HoppingWindowInnerJoinWithCondition()
        {
            // A condition should only keep the windows that match
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 7, 0) });
            await StartStream(@"
                INSERT INTO output
                SELECT window_start, window_end
                FROM orders
                INNER JOIN hopping_window(Orderdate, 5, 'MINUTE', 10, 'MINUTE')
                ON window_start = timestamp_parse('2000-01-01 00:05:00', 'yyyy-MM-dd HH:mm:ss');");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new { window_start = new DateTimeOffset(2000, 1, 1, 0, 5, 0, TimeSpan.Zero), window_end = new DateTimeOffset(2000, 1, 1, 0, 15, 0, TimeSpan.Zero) },
            });
        }

        [Fact]
        public async Task HoppingWindowLeftJoinWithConditionThatNeverMatches()
        {
            // The row is kept but both window columns are null
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 7, 0) });
            await StartStream(@"
                INSERT INTO output
                SELECT OrderKey, window_start, window_end
                FROM orders
                LEFT JOIN hopping_window(Orderdate, 5, 'MINUTE', 10, 'MINUTE')
                ON window_start = timestamp_parse('2010-01-01 00:00:00', 'yyyy-MM-dd HH:mm:ss');");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new { OrderKey = 1, window_start = (DateTimeOffset?)null, window_end = (DateTimeOffset?)null },
            });
        }

        [Fact]
        public async Task HoppingWindowNullTimestampInnerJoin()
        {
            // A null timestamp belongs to no window, so the row is dropped
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 7, 0) });
            await StartStream(@"
                INSERT INTO output
                SELECT OrderKey, window_start
                FROM orders
                INNER JOIN hopping_window(null, 5, 'MINUTE', 10, 'MINUTE');");
            await WaitForUpdate();

            AssertCurrentDataEqual(Orders.Take(0).Select(x => new { x.OrderKey, window_start = (DateTimeOffset?)null }));
        }

        [Fact]
        public async Task HoppingWindowNullTimestampLeftJoin()
        {
            // A left join keeps the row and sets both window columns to null
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 7, 0) });
            await StartStream(@"
                INSERT INTO output
                SELECT OrderKey, window_start, window_end
                FROM orders
                LEFT JOIN hopping_window(null, 5, 'MINUTE', 10, 'MINUTE');");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new { OrderKey = 1, window_start = (DateTimeOffset?)null, window_end = (DateTimeOffset?)null },
            });
        }

        [Fact]
        public async Task HoppingWindowSelectOnlyWindowEnd()
        {
            // Only using the second column must not emit the first one
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 7, 0) });
            await StartStream(@"
                INSERT INTO output
                SELECT window_end
                FROM orders
                INNER JOIN hopping_window(Orderdate, 5, 'MINUTE', 10, 'MINUTE');");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new { window_end = new DateTimeOffset(2000, 1, 1, 0, 10, 0, TimeSpan.Zero) },
                new { window_end = new DateTimeOffset(2000, 1, 1, 0, 15, 0, TimeSpan.Zero) },
            });
        }

        [Fact]
        public async Task HoppingWindowWeekUnit()
        {
            // Weeks are aligned from year one, so they start on a monday
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 6, 12, 0, 0) });
            await StartStream(@"
                INSERT INTO output
                SELECT window_start, window_end
                FROM orders
                INNER JOIN hopping_window(Orderdate, 1, 'WEEK', 1, 'WEEK');");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new { window_start = new DateTimeOffset(2000, 1, 3, 0, 0, 0, TimeSpan.Zero), window_end = new DateTimeOffset(2000, 1, 10, 0, 0, 0, TimeSpan.Zero) },
            });
        }

        [Fact]
        public async Task HoppingWindowHourUnit()
        {
            // Hourly windows with a half hour hop gives two windows
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 12, 45, 0) });
            await StartStream(@"
                INSERT INTO output
                SELECT window_start, window_end
                FROM orders
                INNER JOIN hopping_window(Orderdate, 30, 'MINUTE', 1, 'HOUR');");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new { window_start = new DateTimeOffset(2000, 1, 1, 12, 0, 0, TimeSpan.Zero), window_end = new DateTimeOffset(2000, 1, 1, 13, 0, 0, TimeSpan.Zero) },
                new { window_start = new DateTimeOffset(2000, 1, 1, 12, 30, 0, TimeSpan.Zero), window_end = new DateTimeOffset(2000, 1, 1, 13, 30, 0, TimeSpan.Zero) },
            });
        }

        [Fact]
        public async Task HoppingWindowLowercaseUnit()
        {
            // The unit should not be case sensitive
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 7, 0) });
            await StartStream(@"
                INSERT INTO output
                SELECT window_start
                FROM orders
                INNER JOIN hopping_window(Orderdate, 5, 'minute', 10, 'minute');");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new { window_start = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero) },
                new { window_start = new DateTimeOffset(2000, 1, 1, 0, 5, 0, TimeSpan.Zero) },
            });
        }

        [Fact]
        public async Task HoppingWindowRejectsCalendarUnit()
        {
            // Months vary in length and must be rejected
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 7, 0) });
            var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await StartStream(@"
                    INSERT INTO output
                    SELECT window_start
                    FROM orders
                    INNER JOIN hopping_window(Orderdate, 1, 'MONTH', 1, 'MONTH');");
                await WaitForUpdate();
            });
            Assert.Contains("unit 'MONTH' is not supported", ex.ToString());
        }

        [Fact]
        public async Task HoppingWindowRejectsZeroAmount()
        {
            // A zero hop would never move the window forward
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 7, 0) });
            var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await StartStream(@"
                    INSERT INTO output
                    SELECT window_start
                    FROM orders
                    INNER JOIN hopping_window(Orderdate, 0, 'MINUTE', 10, 'MINUTE');");
                await WaitForUpdate();
            });
            Assert.Contains("must be a positive duration", ex.ToString());
        }

        [Fact]
        public async Task HoppingWindowRejectsFractionalAmount()
        {
            // A fraction should be written with a smaller unit instead
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 7, 0) });
            var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await StartStream(@"
                    INSERT INTO output
                    SELECT window_start
                    FROM orders
                    INNER JOIN hopping_window(Orderdate, 5.7, 'MINUTE', 10, 'MINUTE');");
                await WaitForUpdate();
            });
            Assert.Contains("must be a whole number", ex.ToString());
        }

        [Fact]
        public async Task HoppingWindowRejectsTooLargeAmount()
        {
            // The amount must fit in ticks, otherwise the multiplication overflows
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 7, 0) });
            var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await StartStream(@"
                    INSERT INTO output
                    SELECT window_start
                    FROM orders
                    INNER JOIN hopping_window(Orderdate, 10000000, 'WEEK', 10, 'MINUTE');");
                await WaitForUpdate();
            });
            Assert.Contains("is too large", ex.ToString());
        }

        [Fact]
        public async Task HoppingWindowRejectsColumnAsAmount()
        {
            // The hop and size are resolved on start, so columns can not be used
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 7, 0) });
            var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await StartStream(@"
                    INSERT INTO output
                    SELECT window_start
                    FROM orders
                    INNER JOIN hopping_window(Orderdate, UserKey, 'MINUTE', 10, 'MINUTE');");
                await WaitForUpdate();
            });
            Assert.Contains("must be a numeric literal", ex.ToString());
        }
    }
}
