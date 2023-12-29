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
    public class FilterTests : FlowtideAcceptanceBase
    {
        public FilterTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        [Fact]
        public async Task EqualsFilter()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    u.userkey 
                FROM users u
                WHERE u.gender = 0");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Where(x => x.Gender == 0).Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task NotEqualFilter()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    u.userkey 
                FROM users u
                WHERE u.gender != 0");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Where(x => x.Gender != 0).Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task GreaterThanFilter()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    u.userkey 
                FROM users u
                WHERE u.userkey > 200");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Where(x => x.UserKey > 200).Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task GreaterThanOrEqualFilter()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    u.userkey 
                FROM users u
                WHERE u.userkey >= 200");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Where(x => x.UserKey >= 200).Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task LessThanFilter()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    u.userkey 
                FROM users u
                WHERE u.userkey < 200");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Where(x => x.UserKey < 200).Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task LessThanOrEqualFilter()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    u.userkey 
                FROM users u
                WHERE u.userkey <= 200");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Where(x => x.UserKey <= 200).Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task BooleanAndFilter()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    u.userkey 
                FROM users u
                WHERE u.userkey <= 200 AND u.gender = 0");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Where(x => x.UserKey <= 200 && x.Gender == Entities.Gender.Male).Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task BooleanOrFilter()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    u.userkey 
                FROM users u
                WHERE u.userkey <= 200 OR u.gender = 0");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Where(x => x.UserKey <= 200 || x.Gender == Entities.Gender.Male).Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task IsNotNullFilter()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    u.userkey 
                FROM users u
                WHERE u.lastName is not null");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Where(x => x.LastName != null).Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task WhereNull()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    u.userkey 
                FROM users u
                WHERE null");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Where(x => x.UserKey == -999).Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task WhereInSingularOrList()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    u.userkey 
                FROM users u
                WHERE u.userkey IN (1, 5, 17, 325)");
            await WaitForUpdate();
            List<int> list = new List<int>() { 1, 5, 17, 325 };
            AssertCurrentDataEqual(Users.Where(x => list.Contains(x.UserKey)).Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task WhereInSingularOrListOneValue()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    u.userkey 
                FROM users u
                WHERE u.userkey IN (325)");
            await WaitForUpdate();
            List<int> list = new List<int>() { 325 };
            AssertCurrentDataEqual(Users.Where(x => list.Contains(x.UserKey)).Select(x => new { x.UserKey }));
        }
    }
}
