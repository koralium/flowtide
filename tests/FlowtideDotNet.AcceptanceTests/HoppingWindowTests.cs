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
            // hop 5 min, size 10 min => each timestamp belongs to two overlapping windows.
            // 00:07 falls into [00:00, 00:10) and [00:05, 00:15).
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
            // hop == size => non-overlapping (tumbling) windows, each timestamp in exactly one.
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
            // hop 10 min > size 5 min leaves gaps between windows. 00:07 falls in the gap
            // [00:05, 00:10), so it belongs to no window and the LEFT join emits a null window.
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
            // A timestamp exactly on a window start must be included in that window (start-inclusive)
            // and excluded from the window that ends on it (end-exclusive). 00:05 belongs to
            // [00:00, 00:10) and [00:05, 00:15) but NOT [23:55, 00:05).
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
            // Exercises a different (larger) fixed unit than MINUTE. Daily tumbling window.
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
            // Sub-minute unit: verifies second precision survives timestamp ingestion and the
            // SECOND multiplier. 00:00:07 with hop 5s / size 10s falls into two windows.
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
            // Millisecond-granularity windows. 7ms with hop 5ms / size 10ms falls into two windows.
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
            // Microsecond-granularity windows: verifies precision below a millisecond survives
            // timestamp ingestion. 7us with hop 5us / size 10us falls into two windows.
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
            // size 10s / hop 2s => each timestamp falls into ceil(10/2) = 5 overlapping windows.
            // 00:00:11 keeps all five windows on the same day.
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
            // Projecting none of the function's output columns exercises the empty-right-column
            // path of the join emit mapping (all generated columns produced then disposed), while
            // the fan-out still duplicates the left row once per window.
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 7, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 7, 0) });
            await StartStream(@"
                INSERT INTO output
                SELECT OrderKey
                FROM orders
                INNER JOIN hopping_window(Orderdate, 5, 'MINUTE', 10, 'MINUTE');");
            await WaitForUpdate();

            // 00:07 falls into two windows, so the row is emitted twice.
            AssertCurrentDataEqual(new[]
            {
                new { OrderKey = 7 },
                new { OrderKey = 7 },
            });
        }

        [Fact]
        public async Task HoppingWindowRejectsExcessiveFanout()
        {
            // A millisecond hop against a day-long size would fan every row into ~86M windows.
            // The compile-time guard must reject it rather than exhaust memory.
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
            // The typical usage: fan rows into windows, then aggregate per window.
            // Two orders four minutes apart; with hop 5 min / size 10 min they share the
            // window [00:00, 00:10) and each also lands in one neighbouring window.
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 3, 0) });
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 2, UserKey = 1, Orderdate = new DateTime(2000, 1, 1, 0, 7, 0) });
            await StartStream(@"
                INSERT INTO output
                SELECT window_start, count(*) as cnt
                FROM orders
                INNER JOIN hopping_window(Orderdate, 5, 'MINUTE', 10, 'MINUTE')
                GROUP BY window_start;");
            await WaitForUpdate();

            // 00:03 -> windows starting 23:55(prev), 00:00 ; 00:07 -> windows starting 00:00, 00:05
            AssertCurrentDataEqual(new[]
            {
                new { window_start = new DateTimeOffset(1999, 12, 31, 23, 55, 0, TimeSpan.Zero), cnt = 1 },
                new { window_start = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero), cnt = 2 },
                new { window_start = new DateTimeOffset(2000, 1, 1, 0, 5, 0, TimeSpan.Zero), cnt = 1 },
            });
        }
    }
}
