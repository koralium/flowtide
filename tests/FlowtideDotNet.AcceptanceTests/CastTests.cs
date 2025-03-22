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
    public class CastTests : FlowtideAcceptanceBase
    {
        public CastTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        [Fact]
        public async Task ConvertBoolToString()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT 
                CAST(active AS string) 
            FROM users
            ");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(u => new { val = u.Active.ToString() }));
        }

        [Fact]
        public async Task ConvertIntToString()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT 
                CAST(userkey AS string) 
            FROM users
            ");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(u => new { userkey = u.UserKey.ToString() }));
        }

        [Fact]
        public async Task ConvertDoubleToString()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT 
                CAST(DoubleValue AS string) 
            FROM users
            ");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(u => new { userkey = u.DoubleValue.ToString() }));
        }

        [Fact]
        public async Task ConvertDecimalToString()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT 
                CAST(Money AS string) 
            FROM orders
            ");

            await WaitForUpdate();

            AssertCurrentDataEqual(Orders.Select(u => new { money = u.Money.ToString() }));
        }

        [Fact]
        public async Task ConvertBoolToInt()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT 
                CAST(active AS int)
            FROM users
            ");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(u => new { val = u.Active ? 1 : 0 }));
        }

        [Fact]
        public async Task ConvertStringToInt()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT 
                CAST('17' AS int)
            FROM users
            ");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(u => new { val = 17 }));
        }

        [Fact]
        public async Task ConvertDoubleToInt()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT 
                CAST(DoubleValue AS int)
            FROM users
            ");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(u => new { val = (int)u.DoubleValue }));
        }

        [Fact]
        public async Task ConvertDecimalToInt()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT 
                CAST(Money AS int)
            FROM orders
            ");

            await WaitForUpdate();

            AssertCurrentDataEqual(Orders.Select(u => new { val = (int?)u.Money }));
        }

        [Fact]
        public async Task ConvertBoolToDecimal()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT 
                CAST(active AS decimal)
            FROM users
            ");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(u => new { val = u.Active ? (decimal)1 : (decimal)0 }));
        }

        [Fact]
        public async Task ConvertStringToDecimal()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT 
                CAST('17' AS decimal)
            FROM users
            ");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(u => new { val = (decimal)17 }));
        }

        [Fact]
        public async Task ConvertDoubleToDecimal()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT 
                CAST(DoubleValue AS decimal)
            FROM users
            ");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(u => new { val = (decimal)u.DoubleValue }));
        }

        [Fact]
        public async Task ConvertIntToDecimal()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT 
                CAST(userkey AS decimal) 
            FROM users
            ");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(u => new { userkey = (decimal)u.UserKey }));
        }

        [Fact]
        public async Task ConvertIntToBool()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT 
                CAST(userkey AS boolean) 
            FROM users
            ");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(u => new { userkey = u.UserKey != 0 }));
        }

        [Fact]
        public async Task ConvertStringToBool()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT 
                CAST('true' AS boolean), CAST('false' AS boolean), CAST('asd' AS boolean)
            FROM users
            ");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(u => new { val = true, val2 = false, val3 = (bool?)null }));
        }

        [Fact]
        public async Task ConvertDoubleToBool()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT 
                CAST(DoubleValue AS boolean)
            FROM users
            ");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(u => new { val = u.DoubleValue != 0.0 }));
        }

        [Fact]
        public async Task ConvertDecimalToBool()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT 
                CAST(Money AS boolean) 
            FROM orders
            ");

            await WaitForUpdate();

            AssertCurrentDataEqual(Orders.Select(u => new { money = u.Money != (decimal)0.0 }));
        }

        [Fact]
        public async Task ConvertIntToDouble()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT 
                CAST(userkey AS double) 
            FROM users
            ");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(u => new { userkey = (double)u.UserKey }));
        }

        [Fact]
        public async Task ConvertDecimalToDouble()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT 
                CAST(Money AS double)
            FROM orders
            ");

            await WaitForUpdate();

            AssertCurrentDataEqual(Orders.Select(u => new { val = (double?)u.Money }));
        }

        [Fact]
        public async Task ConvertStringToDouble()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT 
                CAST('17' AS double)
            FROM users
            ");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(u => new { val = (double)17 }));
        }

        [Fact]
        public async Task ConvertBoolToDouble()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT 
                CAST(active AS decimal)
            FROM users
            ");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(u => new { val = u.Active ? (decimal)1 : (decimal)0 }));
        }

        [Fact]
        public async Task ConvertStringToTimestamp()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT 
                CAST('2021-01-01T00:00:00.000Z' AS TIMESTAMP)
            FROM users
            ");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(u => new { val = new DateTimeOffset(2021, 1, 1, 0, 0, 0, TimeSpan.FromMinutes(0)) }));
        }

        [Fact]
        public async Task ConvertIntegerInUnixDotnetTicksToTimestamp()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT 
                CAST(16094592000000000 AS TIMESTAMP)
            FROM users
            ");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(u => new { val = new DateTimeOffset(2021, 1, 1, 0, 0, 0, TimeSpan.FromMinutes(0)) }));
        }

        [Fact]
        public async Task CastTimestampToString()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT 
                CAST(CAST(16094592000000000 AS TIMESTAMP) AS STRING)
            FROM users
            ");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(u => new { val = "2021-01-01T00:00:00.000Z" }));
        }

        [Fact]
        public async Task CastTimestampToInteger()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT 
                CAST(CAST(16094592000000000 AS TIMESTAMP) AS INT)
            FROM users
            ");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(u => new { val = 1609459200000000 }));
        }
    }
}
