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
    public class DatetimeFunctionTests : FlowtideAcceptanceBase
    {
        public DatetimeFunctionTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        /// <summary>
        /// Test to convert a timestamp to a string
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task StrfTime()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT
                strftime(Orderdate, '%Y-%m-%d %H:%M:%S') as Orderdate
            FROM Orders
            ");
            await WaitForUpdate();
            AssertCurrentDataEqual(
                Orders.Select(o => new
                {
                    Orderdate = o.Orderdate.ToString("yyyy-MM-dd HH:mm:ss")
                })
                );
        }

        [Fact]
        public async Task GetTimestamp()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT
                gettimestamp() as CurrentTime
            FROM Orders
            ");
            await WaitForUpdate();

            // This test only validates that it can run for now
        }

        [Fact]
        public async Task GetTimestampInAggregate()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT
                userkey, list_agg(map('time', gettimestamp())) as CurrentTimes
            FROM Orders
            GROUP BY userkey
            ");
            await WaitForUpdate();

            // This test only validates that it can run for now
        }

        [Fact]
        public async Task GetTimestampInFilter()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT
                orderkey
            FROM Orders
            where orderdate < gettimestamp()
            ");
            await WaitForUpdate();

            AssertCurrentDataEqual(
                Orders.Where(x => x.Orderdate < DateTime.UtcNow).Select(o => new
                {
                    o.OrderKey
                })
                );
        }

        /// <summary>
        /// Special case test when gettimestamp creates buffer operator before the join and the join is followed by a filter
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task GetTimestampInFilterJoinFilterPushedInfront()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT
                orderkey, orderdate < gettimestamp() as active
            FROM Orders o
            JOIN users u2 ON o.userkey = u2.userkey
            JOIN users u ON o.userkey = u.userkey AND o.userkey = u2.userkey
            where orderdate < gettimestamp() 
            ");
            await WaitForUpdate();

            AssertCurrentDataEqual(
                Orders.Where(x => x.Orderdate < DateTime.UtcNow).Select(o => new
                {
                    o.OrderKey,
                    Active = o.Orderdate < DateTime.UtcNow
                })
                );
        }

        [Fact]
        public async Task GetTimestampInBufferedView()
        {
            GenerateData();
            await StartStream(@"

            CREATE VIEW buffered WITH (BUFFERED = true) AS
            SELECT
                CASE WHEN orderdate < gettimestamp() THEN true ELSE false END as active
            FROM orders;

            INSERT INTO output
            SELECT
                active
            FROM buffered
            ",
            // Block-nested loop join can sometimes write the same data multiple times between checkpoints
            ignoreSameDataCheck: true);
            await WaitForUpdate();

            var rows = GetActualRows();

            await WaitForUpdate();

            var rows2 = GetActualRows();
            // This test only validates that it can run for now
        }

        [Fact]
        public async Task ToStringTest()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT
                to_string(orderkey) as orderkey
            FROM Orders
            ");
            await WaitForUpdate();
            AssertCurrentDataEqual(
                Orders.Select(o => new
                {
                    OrderKey = o.OrderKey.ToString()
                })
                );
        }

        [Fact]
        public async Task FloorTimestampDay()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT
                floor_timestamp_day(Orderdate) as Orderdate
            FROM Orders
            ");
            await WaitForUpdate();
            AssertCurrentDataEqual(
                Orders.Select(o => new
                {
                    Orderdate = new DateTimeOffset(o.Orderdate.Year, o.Orderdate.Month, o.Orderdate.Day, 0, 0, 0, TimeSpan.Zero)
                })
                );
        }

        [Fact]
        public async Task TimestampParse()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT
                timestamp_parse('20200317-140301', 'yyyyMMdd-HHmmss') as Date
            FROM Orders
            ");
            await WaitForUpdate();
            AssertCurrentDataEqual(
                Orders.Select(o => new
                {
                    Date = new DateTimeOffset(2020, 03, 17, 14, 03, 01, TimeSpan.Zero)
                })
            );
        }

        [Fact]
        public async Task TimestampExtract()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT
                timestamp_extract('DAY', Orderdate) as days
            FROM Orders
            ");
            await WaitForUpdate();
            AssertCurrentDataEqual(
                Orders.Select(o => new
                {
                    days = o.Orderdate.Day
                })
                );
        }

        [Fact]
        public async Task TimestampAdd()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT
                timestamp_add('DAY', 1, Orderdate) as days
            FROM Orders
            ");
            await WaitForUpdate();
            AssertCurrentDataEqual(
                Orders.Select(o => new
                {
                    days = o.Orderdate.AddDays(1)
                })
            );
        }

        [Fact]
        public async Task Datediff()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT
                datediff('DAY', CAST('2017-01-01' AS TIMESTAMP), Orderdate) as days
            FROM Orders
            ");
            await WaitForUpdate();
            AssertCurrentDataEqual(
                Orders.Select(o => new
                {
                    days = (long)o.Orderdate.Date.Subtract(DateTime.Parse("2017-01-01")).TotalDays
                })
            );
        }

        [Fact]
        public async Task RoundCalendarToMinute()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT
                round_calendar(Orderdate, 'FLOOR', 'MINUTE', 'MINUTE', 1) as Orderdate
            FROM Orders
            ");
            await WaitForUpdate();
            AssertCurrentDataEqual(
                Orders.Select(o => new
                {
                    Orderdate = new DateTimeOffset(o.Orderdate.Year, o.Orderdate.Month, o.Orderdate.Day, o.Orderdate.Hour, o.Orderdate.Minute, 0, TimeSpan.Zero)
                })
                );
        }
    }
}
