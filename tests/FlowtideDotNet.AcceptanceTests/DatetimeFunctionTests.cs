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
                Orders.Select(o => new { 
                    Orderdate =  DateTimeOffset.FromUnixTimeMilliseconds(new DateTimeOffset(o.Orderdate).ToUnixTimeMilliseconds()).ToString("yyyy-MM-dd HH:mm:ss") 
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
                Orders.Where(x => x.Orderdate < DateTime.UtcNow).Select(o => new {
                    o.OrderKey
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
            ");
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
                Orders.Select(o => new {
                    OrderKey = o.OrderKey.ToString()
                })
                );
        }
    }
}
