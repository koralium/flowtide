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
    public class ComparisonFunctionTests : FlowtideAcceptanceBase
    {
        public ComparisonFunctionTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        [Fact]
        public async Task CoalesceTwoArgs()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT coalesce(nullablestring, firstName) FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { val = x.NullableString != null ? x.NullableString : x.FirstName }));
        }

        [Fact]
        public async Task CoalesceOneArg()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT coalesce(nullablestring) FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { val = x.NullableString }));
        }

        [Fact]
        public async Task IsInfinitePositiveInfiniteTrue()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT is_infinite(1 / 0) FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { val = true }));
        }

        [Fact]
        public async Task IsInfiniteNegativeInfiniteTrue()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT is_infinite(-1 / 0) FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { val = true }));
        }

        [Fact]
        public async Task IsInfiniteNaNFalse()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT is_infinite(0 / 0) FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { val = false }));
        }

        [Fact]
        public async Task IsInfiniteStringFalse()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT is_infinite(firstName) FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { val = default(string) }));
        }

        [Fact]
        public async Task IsFinitePositiveInfinite()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT is_finite(1 / 0) FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { val = false }));
        }

        [Fact]
        public async Task IsFiniteNegativeInfinite()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT is_finite(-1 / 0) FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { val = false }));
        }

        [Fact]
        public async Task IsFiniteMuneric()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT is_finite(userkey) FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { val = true }));
        }

        [Fact]
        public async Task Between()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    u.userkey 
                FROM users u
                WHERE u.userkey BETWEEN 100 AND 200");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Where(x => x.UserKey >= 100 && x.UserKey <= 200).Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task IsNull()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    u.userkey 
                FROM users u
                WHERE u.nullablestring is null");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Where(x => x.NullableString == null).Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task IsNan()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    is_nan(0/0), is_nan(1/2), is_nan(userkey), is_nan(nullablestring) 
                FROM users u");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { nan = true, not_nan = false, userkey = false, nullString = default(string) }));
        }

        [Fact]
        public async Task NullIf()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    nullif(u.userkey, 17)
                FROM users u");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { val = x.UserKey == 17 ? default(int?) : x.UserKey }));
        }

        [Fact]
        public async Task EqualNullable()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    u.userkey
                FROM users u
                WHERE u.nullablestring = 'value'");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Where(x => x.NullableString == "value").Select(x => new { val = x.UserKey }));
        }

        [Fact]
        public async Task GreaterThanNullable()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    u.userkey 
                FROM users u
                WHERE u.visits > 3");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Where(x => x.Visits > 3).Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task GreaterThanOrEqualNullable()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    u.userkey 
                FROM users u
                WHERE u.visits >= 3");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Where(x => x.Visits >= 3).Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task LessThanNullable()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    u.userkey 
                FROM users u
                WHERE u.visits < 3");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Where(x => x.Visits < 3).Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task LessThanOrEqualNullable()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    u.userkey 
                FROM users u
                WHERE u.visits <= 3");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Where(x => x.Visits <= 3).Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task GreatestNoNullable()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    greatest(u.userkey, 200, 250)
                FROM users u");
            await WaitForUpdate();

            var act = GetActualRows();
            AssertCurrentDataEqual(Users.Select(x =>
            {
                return new
                {
                    value = Math.Max(x.UserKey, 250)
                };
            }));
        }

        [Fact]
        public async Task GreatestNullable()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    greatest(u.visits, 4)
                FROM users u");
            await WaitForUpdate();

            var act = GetActualRows();
            AssertCurrentDataEqual(Users.Select(x =>
            {
                if (x.Visits.HasValue)
                {
                    return new
                    {
                        value = (int?)Math.Max(x.Visits.Value, 4)
                    };
                }
                return new
                {
                    value = default(int?)
                };
                
            }));
        }
    }
}
