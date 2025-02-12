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
    public class ArithmaticFunctionTests : FlowtideAcceptanceBase
    {
        public ArithmaticFunctionTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        [Fact]
        public async Task SelectWithAdditionIntInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT userkey + 17 as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = x.UserKey + 17 }));
        }

        [Fact]
        public async Task SelectWithAdditionIntFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT userkey + CAST(17 as float) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = x.UserKey + (float)17 }));
        }

        [Fact]
        public async Task SelectWithAdditionIntDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT userkey + CAST(17 as decimal) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = x.UserKey + (decimal)17 }));
        }

        [Fact]
        public async Task SelectWithAdditionString()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT firstName + 17 as firstName FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { FirstName = default(string) }));
        }

        [Fact]
        public async Task SelectWithAdditionDecimalInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT Money + 3 FROM orders");
            await WaitForUpdate();
            var act = GetActualRows();
            AssertCurrentDataEqual(Orders.Select(x => new { Money = x.Money + 3 }));
        }

        [Fact]
        public async Task SelectWithAdditionDecimalFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT Money + CAST(3 as float) FROM orders");
            await WaitForUpdate();
            var act = GetActualRows();
            AssertCurrentDataEqual(Orders.Select(x => new { Money = x.Money + 3 }));
        }

        [Fact]
        public async Task SelectWithAdditionDecimalDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT Money + CAST(3 as decimal) FROM orders");
            await WaitForUpdate();
            var act = GetActualRows();
            AssertCurrentDataEqual(Orders.Select(x => new { Money = x.Money + 3 }));
        }

        [Fact]
        public async Task SelectWithAdditionDoubleInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT DoubleValue + 3 FROM users");
            await WaitForUpdate();
            var act = GetActualRows();
            AssertCurrentDataEqual(Users.Select(x => new { v = x.DoubleValue + 3 }));
        }

        [Fact]
        public async Task SelectWithAdditionDoubleFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT DoubleValue + CAST(3 as float) FROM users");
            await WaitForUpdate();
            var act = GetActualRows();
            AssertCurrentDataEqual(Users.Select(x => new { v = x.DoubleValue + 3 }));
        }

        [Fact]
        public async Task SelectWithAdditionDoubleDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT DoubleValue + CAST(3 as decimal) FROM users");
            await WaitForUpdate();
            var act = GetActualRows();
            AssertCurrentDataEqual(Users.Select(x => new { v = (decimal)x.DoubleValue + (decimal)3 }));
        }

        [Fact]
        public async Task SelectWithSubtractionIntInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT userkey - 17 as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = x.UserKey - 17 }));
        }

        [Fact]
        public async Task SelectWithSubtractionIntFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT userkey - CAST(17 as float) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = x.UserKey - (float)17 }));
        }

        [Fact]
        public async Task SelectWithSubtractionIntDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT userkey - CAST(17 as decimal) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = x.UserKey - (decimal)17 }));
        }

        [Fact]
        public async Task SelectWithSubtractionDoubleInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT DoubleValue - 17 as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = x.DoubleValue - 17 }));
        }

        [Fact]
        public async Task SelectWithSubtractionDoubleFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT DoubleValue - CAST(17 as float) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = x.DoubleValue - 17 }));
        }

        [Fact]
        public async Task SelectWithSubtractionDoubleDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT DoubleValue - CAST(17 as decimal) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = (decimal)x.DoubleValue - (decimal)17 }));
        }

        [Fact]
        public async Task SelectWithSubtractionDecimalInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT Money - 3 FROM orders");
            await WaitForUpdate();
            var act = GetActualRows();
            AssertCurrentDataEqual(Orders.Select(x => new { Money = x.Money - 3 }));
        }

        [Fact]
        public async Task SelectWithSubtractionDecimalFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT Money - CAST(3 as float) FROM orders");
            await WaitForUpdate();
            var act = GetActualRows();
            AssertCurrentDataEqual(Orders.Select(x => new { Money = x.Money - 3 }));
        }

        [Fact]
        public async Task SelectWithSubtractionDecimalDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT Money - CAST(3 as decimal) FROM orders");
            await WaitForUpdate();
            var act = GetActualRows();
            AssertCurrentDataEqual(Orders.Select(x => new { Money = x.Money - 3 }));
        }

        [Fact]
        public async Task SelectWithSubtractionString()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT firstName - 17 as firstName FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { FirstName = default(string) }));
        }

        [Fact]
        public async Task SelectWithMultiplyIntInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT userkey * 3 as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = x.UserKey * 3 }));
        }

        [Fact]
        public async Task SelectWithMultiplyIntFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT userkey * CAST(3 as float) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = x.UserKey * (float)3 }));
        }

        [Fact]
        public async Task SelectWithMultiplyIntDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT userkey * CAST(3 as decimal) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = x.UserKey * (decimal)3 }));
        }

        [Fact]
        public async Task SelectWithMultiplyDoubleInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT DoubleValue * 3 as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = x.DoubleValue * 3 }));
        }

        [Fact]
        public async Task SelectWithMultiplyDoubleFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT DoubleValue * CAST(3 as float) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = x.DoubleValue * 3 }));
        }

        [Fact]
        public async Task SelectWithMultiplyDoubleDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT DoubleValue * CAST(3 as decimal) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = (decimal)x.DoubleValue * (decimal)3 }));
        }

        [Fact]
        public async Task SelectWithMultiplyDecimalInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT Money * 3 FROM orders");
            await WaitForUpdate();
            var act = GetActualRows();
            AssertCurrentDataEqual(Orders.Select(x => new { Money = x.Money * 3 }));
        }

        [Fact]
        public async Task SelectWithMultiplyDecimalFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT Money * CAST(3 as float) FROM orders");
            await WaitForUpdate();
            var act = GetActualRows();
            AssertCurrentDataEqual(Orders.Select(x => new { Money = x.Money * 3 }));
        }

        [Fact]
        public async Task SelectWithMultiplyDecimalDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT Money * CAST(3 as decimal) FROM orders");
            await WaitForUpdate();
            var act = GetActualRows();
            AssertCurrentDataEqual(Orders.Select(x => new { Money = x.Money * 3 }));
        }

        [Fact]
        public async Task SelectWithMultiplyString()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT firstName * 3 as firstName FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { FirstName = default(string) }));
        }

        [Fact]
        public async Task SelectWithDivideIntInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT userkey / 3 as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = x.UserKey / (double)3 }));
        }

        [Fact]
        public async Task SelectWithDivideIntFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT userkey / CAST(3 as float) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = x.UserKey / (double)3 }));
        }

        [Fact]
        public async Task SelectWithDivideIntDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT userkey / CAST(3 as decimal) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = x.UserKey / (decimal)3 }));
        }

        [Fact]
        public async Task SelectWithDivideDoubleInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT DoubleValue / 3 as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = x.DoubleValue / 3 }));
        }

        [Fact]
        public async Task SelectWithDivideDoubleFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT DoubleValue / CAST(3 as float) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = x.DoubleValue / 3 }));
        }

        [Fact]
        public async Task SelectWithDivideDoubleDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT DoubleValue / CAST(3 as decimal) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = (decimal)x.DoubleValue / 3 }));
        }

        [Fact]
        public async Task SelectWithDivideDecimalInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT Money / 3 FROM orders");
            await WaitForUpdate();
            var act = GetActualRows();
            AssertCurrentDataEqual(Orders.Select(x => new { Money = x.Money / 3 }));
        }

        [Fact]
        public async Task SelectWithDivideDecimalFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT Money / CAST(3 as float) FROM orders");
            await WaitForUpdate();
            var act = GetActualRows();
            AssertCurrentDataEqual(Orders.Select(x => new { Money = x.Money / 3 }));
        }

        [Fact]
        public async Task SelectWithDivideDecimalDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT Money / CAST(3 as decimal) FROM orders");
            await WaitForUpdate();
            var act = GetActualRows();
            AssertCurrentDataEqual(Orders.Select(x => new { Money = x.Money / 3 }));
        }

        [Fact]
        public async Task SelectWithDivideNumberZero()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT userkey / 0 as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = x.UserKey / (double)0 }));
        }

        [Fact]
        public async Task SelectWithDivideString()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT firstName / 3 as firstName FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { FirstName = default(string) }));
        }

        [Fact]
        public async Task SelectWithNegationInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT -userkey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = -x.UserKey }));
        }

        [Fact]
        public async Task SelectWithNegationDouble()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT -DoubleValue FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = -x.DoubleValue }));
        }

        [Fact]
        public async Task SelectWithNegationDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT -Money FROM orders");
            await WaitForUpdate();
            AssertCurrentDataEqual(Orders.Select(x => new { v = -x.Money }));
        }

        [Fact]
        public async Task SelectWithModulusIntInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT userkey % 3 as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = x.UserKey % 3 }));
        }

        [Fact]
        public async Task SelectWithModulusIntFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT userkey % CAST(3 as float) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = x.UserKey % (float)3 }));
        }

        [Fact]
        public async Task SelectWithModulusIntDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT userkey % CAST(3 as decimal) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = x.UserKey % (decimal)3 }));
        }

        [Fact]
        public async Task SelectWithModulusDoubleInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT DoubleValue % 3 as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = x.DoubleValue % 3 }));
        }

        [Fact]
        public async Task SelectWithModulusDoubleFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT DoubleValue % CAST(3 as float) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = x.DoubleValue % 3 }));
        }

        [Fact]
        public async Task SelectWithModulusDoubleDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT DoubleValue % CAST(3 as decimal) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = (decimal)x.DoubleValue % 3 }));
        }

        [Fact]
        public async Task SelectWithModulusDecimalInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT Money % 3 FROM orders");
            await WaitForUpdate();
            var act = GetActualRows();
            AssertCurrentDataEqual(Orders.Select(x => new { Money = x.Money % 3 }));
        }

        [Fact]
        public async Task SelectWithModulusDecimalFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT Money % CAST(3 as float) FROM orders");
            await WaitForUpdate();
            var act = GetActualRows();
            AssertCurrentDataEqual(Orders.Select(x => new { Money = x.Money % 3 }));
        }

        [Fact]
        public async Task SelectWithModulusDecimalDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT Money % CAST(3 as decimal) FROM orders");
            await WaitForUpdate();
            var act = GetActualRows();
            AssertCurrentDataEqual(Orders.Select(x => new { Money = x.Money % 3 }));
        }

        [Fact]
        public async Task SelectWithModulusString()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT firstName % 3 as firstName FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { FirstName = default(string) }));
        }

        [Fact]
        public async Task SelectWithModulusNumberZero()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT userkey % 0 as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = x.UserKey % (double)0 }));
        }

        [Fact]
        public async Task SelectWithPowerIntInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT power(userkey, 2) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = (long)Math.Pow(x.UserKey, 2) }));
        }

        [Fact]
        public async Task SelectWithPowerIntFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT power(userkey, CAST(2 as float)) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Pow(x.UserKey, 2) }));
        }

        [Fact]
        public async Task SelectWithPowerIntDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT power(userkey, CAST(2 as decimal)) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = (decimal)Math.Pow(x.UserKey, 2) }));
        }

        [Fact]
        public async Task SelectWithPowerDoubleInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT power(DoubleValue, 2) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Pow(x.DoubleValue, 2) }));
        }

        [Fact]
        public async Task SelectWithPowerDoubleFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT power(DoubleValue, CAST(2 as float)) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Pow(x.DoubleValue, 2) }));
        }

        [Fact]
        public async Task SelectWithPowerDoubleDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT power(DoubleValue, CAST(2 as decimal)) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = (decimal)Math.Pow(x.DoubleValue, 2) }));
        }

        [Fact]
        public async Task SelectWithPowerDecimalInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT power(Money, 2) FROM orders");
            await WaitForUpdate();
            var act = GetActualRows();
            AssertCurrentDataEqual(Orders.Select(x => new { Money = x.Money == null ? null : (decimal?)Math.Pow((double)x.Money, 2) }));
        }

        [Fact]
        public async Task SelectWithPowerDecimalFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT power(Money, CAST(2 as float)) FROM orders");
            await WaitForUpdate();
            var act = GetActualRows();
            AssertCurrentDataEqual(Orders.Select(x => new { Money = x.Money == null ? null : (decimal?)Math.Pow((double)x.Money, 2) }));
        }

        [Fact]
        public async Task SelectWithPowerDecimalDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT power(Money, CAST(2 as decimal)) FROM orders");
            await WaitForUpdate();
            var act = GetActualRows();
            AssertCurrentDataEqual(Orders.Select(x => new { Money = x.Money == null ? null : (decimal?)Math.Pow((double)x.Money, 2) }));
        }

        [Fact]
        public async Task SelectWithPowerString()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT power(firstName, 2) as firstName FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { FirstName = default(string) }));
        }

        [Fact]
        public async Task SelectWithSqrtInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT sqrt(userkey) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Sqrt(x.UserKey) }));
        }

        [Fact]
        public async Task SelectWithSqrtFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT sqrt(DoubleValue) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Sqrt(x.DoubleValue) }));
        }

        [Fact]
        public async Task SelectWithSqrtDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT sqrt(Money) as UserKey FROM orders");
            await WaitForUpdate();
            AssertCurrentDataEqual(Orders.Select(x => new { v = x.Money == null ? null : (decimal?)Math.Sqrt((double)x.Money) }));
        }

        [Fact]
        public async Task SelectWithSqrtString()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT sqrt(firstName) as firstName FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { FirstName = default(string) }));
        }

        [Fact]
        public async Task SelectWithExpInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT exp(userkey) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Exp(x.UserKey) }));
        }

        [Fact]
        public async Task SelectWithExpFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT exp(doublevalue) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Exp(x.DoubleValue) }));
        }

        [Fact]
        public async Task SelectWithExpDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT exp(Money) as UserKey FROM orders");
            await WaitForUpdate();
            AssertCurrentDataEqual(Orders.Select(x => new { v = x.Money == null ? null : (double?)Math.Exp((double)x.Money) }));
        }

        [Fact]
        public async Task SelectWithExpString()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT exp(firstName) as firstName FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { FirstName = default(string) }));
        }

        [Fact]
        public async Task SelectWithCosInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT cos(userkey) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Cos(x.UserKey) }));
        }

        [Fact]
        public async Task SelectWithCosFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT cos(DoubleValue) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Cos(x.DoubleValue) }));
        }

        [Fact]
        public async Task SelectWithCosDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT cos(Money) as UserKey FROM orders");
            await WaitForUpdate();
            AssertCurrentDataEqual(Orders.Select(x => new { v = x.Money == null ? null : (double?)Math.Cos((double)x.Money) }));
        }

        [Fact]
        public async Task SelectWithCosString()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT cos(firstName) as firstName FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { FirstName = default(string) }));
        }

        [Fact]
        public async Task SelectWithSinInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT sin(userkey) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Sin(x.UserKey) }));
        }

        [Fact]
        public async Task SelectWithSinFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT sin(DoubleValue) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Sin(x.DoubleValue) }));
        }

        [Fact]
        public async Task SelectWithSinDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT sin(Money) as UserKey FROM orders");
            await WaitForUpdate();
            AssertCurrentDataEqual(Orders.Select(x => new { v = x.Money == null ? null : (double?)Math.Sin((double)x.Money) }));
        }

        [Fact]
        public async Task SelectWithSinString()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT sin(firstName) as firstName FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { FirstName = default(string) }));
        }

        [Fact]
        public async Task SelectWithTanInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT tan(userkey) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Tan(x.UserKey) }));
        }

        [Fact]
        public async Task SelectWithTanFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT tan(DoubleValue) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Tan(x.DoubleValue) }));
        }

        [Fact]
        public async Task SelectWithTanDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT tan(Money) as UserKey FROM orders");
            await WaitForUpdate();
            AssertCurrentDataEqual(Orders.Select(x => new { v = x.Money == null ? null : (double?)Math.Tan((double)x.Money) }));
        }

        [Fact]
        public async Task SelectWithTanString()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT tan(firstName) as firstName FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { FirstName = default(string) }));
        }

        [Fact]
        public async Task SelectWithCoshInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT cosh(userkey) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Cosh(x.UserKey) }));
        }

        [Fact]
        public async Task SelectWithCoshFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT cosh(doublevalue) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Cosh(x.DoubleValue) }));
        }

        [Fact]
        public async Task SelectWithCoshDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT cosh(Money) as UserKey FROM orders");
            await WaitForUpdate();
            AssertCurrentDataEqual(Orders.Select(x => new { v = x.Money == null ? null : (double?)Math.Cosh((double)x.Money) }));
        }

        [Fact]
        public async Task SelectWithCoshString()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT cosh(firstName) as firstName FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { FirstName = default(string) }));
        }

        [Fact]
        public async Task SelectWithSinhInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT sinh(userkey) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Sinh(x.UserKey) }));
        }

        [Fact]
        public async Task SelectWithSinhFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT sinh(doublevalue) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Sinh(x.DoubleValue) }));
        }

        [Fact]
        public async Task SelectWithSinhDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT sinh(Money) as UserKey FROM orders");
            await WaitForUpdate();
            AssertCurrentDataEqual(Orders.Select(x => new { v = x.Money == null ? null : (double?)Math.Sinh((double)x.Money) }));
        }

        [Fact]
        public async Task SelectWithSinhString()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT sinh(firstName) as firstName FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { FirstName = default(string) }));
        }

        [Fact]
        public async Task SelectWithTanhInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT tanh(userkey) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Tanh(x.UserKey) }));
        }

        [Fact]
        public async Task SelectWithTanhFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT tanh(doublevalue) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Tanh(x.DoubleValue) }));
        }

        [Fact]
        public async Task SelectWithTanhDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT tanh(Money) as UserKey FROM orders");
            await WaitForUpdate();
            AssertCurrentDataEqual(Orders.Select(x => new { v = x.Money == null ? null : (double?)Math.Tanh((double)x.Money) }));
        }

        [Fact]
        public async Task SelectWithTanhString()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT tanh(firstName) as firstName FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { FirstName = default(string) }));
        }

        [Fact]
        public async Task SelectWithAcosInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT acos(userkey) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Acos(x.UserKey) }));
        }

        [Fact]
        public async Task SelectWithAcosFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT acos(doublevalue) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Acos(x.DoubleValue) }));
        }

        [Fact]
        public async Task SelectWithAcosDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT acos(Money) as UserKey FROM orders");
            await WaitForUpdate();
            AssertCurrentDataEqual(Orders.Select(x => new { v = x.Money == null ? null : (double?)Math.Acos((double)x.Money) }));
        }

        [Fact]
        public async Task SelectWithAcosString()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT acos(firstName) as firstName FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { FirstName = default(string) }));
        }

        [Fact]
        public async Task SelectWithAsinInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT asin(userkey) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Asin(x.UserKey) }));
        }

        [Fact]
        public async Task SelectWithAsinFlat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT asin(doublevalue) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Asin(x.DoubleValue) }));
        }

        [Fact]
        public async Task SelectWithAsinDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT asin(Money) as UserKey FROM orders");
            await WaitForUpdate();
            AssertCurrentDataEqual(Orders.Select(x => new { v = x.Money == null ? null : (double?)Math.Asin((double)x.Money) }));
        }

        [Fact]
        public async Task SelectWithAsinString()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT acos(firstName) as firstName FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { FirstName = default(string) }));
        }

        [Fact]
        public async Task SelectWithAtanInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT atan(userkey) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Atan(x.UserKey) }));
        }

        [Fact]
        public async Task SelectWithAtanFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT atan(doublevalue) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Atan(x.DoubleValue) }));
        }

        [Fact]
        public async Task SelectWithAtanDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT atan(Money) as UserKey FROM orders");
            await WaitForUpdate();
            AssertCurrentDataEqual(Orders.Select(x => new { v = x.Money == null ? null : (double?)Math.Atan((double)x.Money) }));
        }

        [Fact]
        public async Task SelectWithAtanString()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT atan(firstName) as firstName FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { FirstName = default(string) }));
        }

        [Fact]
        public async Task SelectWithAcoshInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT acosh(userkey) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Acosh(x.UserKey) }));
        }

        [Fact]
        public async Task SelectWithAcoshFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT acosh(doublevalue) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Acosh(x.DoubleValue) }));
        }

        [Fact]
        public async Task SelectWithAcoshDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT acosh(Money) as UserKey FROM orders");
            await WaitForUpdate();
            AssertCurrentDataEqual(Orders.Select(x => new { v = x.Money == null ? null : (double?)Math.Acosh((double)x.Money) }));
        }

        [Fact]
        public async Task SelectWithAcoshString()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT acosh(firstName) as firstName FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { FirstName = default(string) }));
        }

        [Fact]
        public async Task SelectWithAsinhInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT asinh(userkey) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Asinh(x.UserKey) }));
        }

        [Fact]
        public async Task SelectWithAsinhFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT asinh(doublevalue) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Asinh(x.DoubleValue) }));
        }

        [Fact]
        public async Task SelectWithAsinhDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT asinh(Money) as UserKey FROM orders");
            await WaitForUpdate();
            AssertCurrentDataEqual(Orders.Select(x => new { v = x.Money == null ? null : (double?)Math.Asinh((double)x.Money) }));
        }

        [Fact]
        public async Task SelectWithAsinhString()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT acosh(firstName) as firstName FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { FirstName = default(string) }));
        }

        [Fact]
        public async Task SelectWithAtanhInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT atanh(userkey) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Atanh(x.UserKey) }));
        }

        [Fact]
        public async Task SelectWithAtanhFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT atanh(doublevalue) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Atanh(x.DoubleValue) }));
        }

        [Fact]
        public async Task SelectWithAtanhDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT atanh(Money) as UserKey FROM orders");
            await WaitForUpdate();
            AssertCurrentDataEqual(Orders.Select(x => new { v = x.Money == null ? null : (double?)Math.Atanh((double)x.Money) }));
        }

        [Fact]
        public async Task SelectWithAtanhString()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT atanh(firstName) as firstName FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { FirstName = default(string) }));
        }

        [Fact]
        public async Task SelectWithAtan2IntInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT atan2(userkey, 2) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Atan2(x.UserKey, 2) }));
        }

        [Fact]
        public async Task SelectWithAtan2IntFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT atan2(userkey, CAST(2 as float)) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Atan2(x.UserKey, 2) }));
        }

        [Fact]
        public async Task SelectWithAtan2IntDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT atan2(userkey, CAST(2 as decimal)) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Atan2(x.UserKey, 2) }));
        }

        [Fact]
        public async Task SelectWithAtan2FloatInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT atan2(doublevalue, 2) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Atan2(x.DoubleValue, 2) }));
        }

        [Fact]
        public async Task SelectWithAtan2FloatFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT atan2(doublevalue, CAST(2 as float)) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Atan2(x.DoubleValue, 2) }));
        }

        [Fact]
        public async Task SelectWithAtan2FloatDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT atan2(doublevalue, CAST(2 as decimal)) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = Math.Atan2(x.DoubleValue, 2) }));
        }

        [Fact]
        public async Task SelectWithAtan2DecimalInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT atan2(money, 2) FROM orders");
            await WaitForUpdate();
            AssertCurrentDataEqual(Orders.Select(x => new { UserKey = x.Money == null ? null : (double?)Math.Atan2((double)x.Money, 2) }));
        }

        [Fact]
        public async Task SelectWithAtan2DecimalFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT atan2(money, CAST(2 as float)) FROM orders");
            await WaitForUpdate();
            AssertCurrentDataEqual(Orders.Select(x => new { UserKey = x.Money == null ? null : (double?)Math.Atan2((double)x.Money, 2) }));
        }

        [Fact]
        public async Task SelectWithAtan2DecimalDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT atan2(money, CAST(2 as decimal)) FROM orders");
            await WaitForUpdate();
            AssertCurrentDataEqual(Orders.Select(x => new { UserKey = x.Money == null ? null : (double?)Math.Atan2((double)x.Money, 2) }));
        }

        [Fact]
        public async Task SelectWithAtan2String()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT atan2(firstName, 2) as firstName FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { FirstName = default(string) }));
        }

        [Fact]
        public async Task SelectWithRadiansInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT radians(userkey) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = (Math.PI / 180) * x.UserKey }));
        }

        [Fact]
        public async Task SelectWithRadiansFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT radians(doublevalue) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = (Math.PI / 180) * x.DoubleValue }));
        }

        [Fact]
        public async Task SelectWithRadiansDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT radians(Money) as UserKey FROM orders");
            await WaitForUpdate();
            AssertCurrentDataEqual(Orders.Select(x => new { v = x.Money == null ? null : (double?)((Math.PI / 180) * (double)x.Money) }));
        }

        [Fact]
        public async Task SelectWithRadiansString()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT radians(firstName) as firstName FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { FirstName = default(string) }));
        }

        [Fact]
        public async Task SelectWithDegreesInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT degrees(userkey) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = (180 / Math.PI) * x.UserKey }));
        }

        [Fact]
        public async Task SelectWithDegreesFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT degrees(doublevalue) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = (180 / Math.PI) * x.DoubleValue }));
        }

        [Fact]
        public async Task SelectWithDegreesDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT degrees(Money) as UserKey FROM orders");
            await WaitForUpdate();
            AssertCurrentDataEqual(Orders.Select(x => new { v = x.Money == null ? null : (double?)((180 / Math.PI) * (double)x.Money) }));
        }

        [Fact]
        public async Task SelectWithDegreesString()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT degrees(firstName) as firstName FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { FirstName = default(string) }));
        }

        [Fact]
        public async Task SelectWithAbsInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT abs(userkey) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { a = Math.Abs(x.UserKey) }));
        }

        [Fact]
        public async Task SelectWithAbsFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT abs(doublevalue) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { a = Math.Abs(x.DoubleValue) }));
        }

        [Fact]
        public async Task SelectWithAbsDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT abs(Money) as UserKey FROM orders");
            await WaitForUpdate();
            AssertCurrentDataEqual(Orders.Select(x => new { v = x.Money == null ? null : (decimal?)Math.Abs(x.Money.Value) }));
        }

        [Fact]
        public async Task SelectWithAbsString()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT abs(firstName) as firstName FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { FirstName = default(string) }));
        }

        [Fact]
        public async Task SelectWithSignInt()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT sign(userkey) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { a = Math.Sign(x.UserKey) }));
        }

        [Fact]
        public async Task SelectWithSignFloat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT sign(doublevalue) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { a = (double)Math.Sign(x.DoubleValue) }));
        }

        [Fact]
        public async Task SelectWithSignDecimal()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT sign(Money) as UserKey FROM orders");
            await WaitForUpdate();
            AssertCurrentDataEqual(Orders.Select(x => new { v = x.Money == null ? null : (decimal?)Math.Sign(x.Money.Value) }));
        }

        [Fact]
        public async Task SelectWithSignInfinity()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT sign(userkey / 0) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { a = (double)Math.Sign(x.UserKey) }));
        }

        [Fact]
        public async Task SelectWithSignNaN()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT sign(userkey % 0) as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { a = x.UserKey % (double)0 }));
        }

        [Fact]
        public async Task SelectSum()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT sum(userkey) FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] {new {Sum = (long)Users.Sum(x => x.UserKey)}} );
        }

        [Fact]
        public async Task SelectSumNoRows()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT sum(userkey) FROM users WHERE userkey = -1");
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] { new { Sum = default(double?) } });
        }

        [Fact]
        public async Task SelectSum0()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT sum0(userkey) FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] { new { Sum = (long)Users.Sum(x => x.UserKey) } });
        }

        [Fact]
        public async Task SelectSum0NoRows()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT sum0(userkey) FROM users WHERE userkey = -1");
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] { new { Sum = default(double) } });
        }

        [Fact]
        public async Task SelectMin()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT min(userkey) FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] { new { Min = (long)Users.Min(x => x.UserKey) } });
        }

        [Fact]
        public async Task SelectMinNonZero()
        {
            GenerateData();
            await StartStream(@"

            CREATE VIEW t AS
            SELECT userkey FROM users WHERE userkey > 200;
            INSERT INTO output SELECT min(userkey) FROM t");
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] { new { Min = (long)Users.Where(x => x.UserKey > 200).Min(x => x.UserKey) } });
        }

        [Fact]
        public async Task MinWithGrouping()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    userkey, min(orderkey)
                FROM orders
                GROUP BY userkey
                ");
            await WaitForUpdate();

            AssertCurrentDataEqual(Orders.GroupBy(x => x.UserKey).Select(x => new { UserKey = x.Key, MinVal = x.Min(y => y.OrderKey) }));
        }

        [Fact]
        public async Task SelectMinWithUpdate()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT min(userkey) FROM users", ignoreSameDataCheck: true);
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] { new { Min = (long)Users.Min(x => x.UserKey) } });
            GenerateData();
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] { new { Min = (long)Users.Min(x => x.UserKey) } });
        }

        [Fact]
        public async Task SelectMinWithNull()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT min(visits) FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] { new { Min = (long)Users.Min(x => x.Visits)! } });
        }

        [Fact]
        public async Task SelectMax()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT max(userkey) FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] { new { Min = (long)Users.Max(x => x.UserKey) } });
        }

        [Fact]
        public async Task SelectMaxNonZero()
        {
            GenerateData();
            await StartStream(@"

            CREATE VIEW t AS
            SELECT userkey FROM users WHERE userkey > 200;
            INSERT INTO output SELECT max(userkey) FROM t");
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] { new { Min = (long)Users.Where(x => x.UserKey > 200).Max(x => x.UserKey) } });
        }

        [Fact]
        public async Task MaxWithGrouping()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    userkey, max(orderkey)
                FROM orders
                GROUP BY userkey
                ");
            await WaitForUpdate();

            AssertCurrentDataEqual(Orders.GroupBy(x => x.UserKey).Select(x => new { UserKey = x.Key, MaxVal = x.Max(y => y.OrderKey) }));
        }

        [Fact]
        public async Task SelectMaxWithUpdate()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT max(userkey) FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] { new { Max = (long)Users.Max(x => x.UserKey) } });
            GenerateData();
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] { new { Max = (long)Users.Max(x => x.UserKey) } });
        }

        [Fact]
        public async Task SelectMaxWithNull()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT max(visits) FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] { new { Min = Users.Max(x => x.Visits) } });
        }
    }
}
