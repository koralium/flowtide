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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests
{
    public class ArithmaticFunctionTests : FlowtideAcceptanceBase
    {
        public ArithmaticFunctionTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        [Fact]
        public async Task SelectWithAdditionNumber()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT userkey + 17 as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = x.UserKey + 17 }));
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
        public async Task SelectWithSubtractionNumber()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT userkey - 17 as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = x.UserKey - 17 }));
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
        public async Task SelectWithMultiplyNumber()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT userkey * 3 as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = x.UserKey * 3 }));
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
        public async Task SelectWithDivideNumber()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT userkey / 3 as UserKey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = x.UserKey / (double)3 }));
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
        public async Task SelectWithNegation()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT -userkey FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { UserKey = -x.UserKey }));
        }

        [Fact]
        public async Task SelectSum()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT sum(userkey) FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] {new {Sum = (double)Users.Sum(x => x.UserKey)}} );
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
            AssertCurrentDataEqual(new[] { new { Sum = (double)Users.Sum(x => x.UserKey) } });
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
            AssertCurrentDataEqual(new[] { new { Min = (double)Users.Min(x => x.UserKey) } });
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
            AssertCurrentDataEqual(new[] { new { Min = (double)Users.Where(x => x.UserKey > 200).Min(x => x.UserKey) } });
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
            await StartStream("INSERT INTO output SELECT min(userkey) FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] { new { Min = (double)Users.Min(x => x.UserKey) } });
            GenerateData();
            await WaitForUpdate();
        }
    }
}
