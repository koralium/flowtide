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
    public class TopNTests : FlowtideAcceptanceBase
    {
        public TopNTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        [Fact]
        public async Task TestTop1Asc()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output 
            SELECT TOP 1 userkey 
            FROM users
            ORDER BY userkey");  

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.OrderBy(x => x.UserKey).Take(1).Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task TestTop1Desc()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output 
            SELECT TOP 1 userkey 
            FROM users
            ORDER BY userkey DESC");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.OrderByDescending(x => x.UserKey).Take(1).Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task TestTop10Asc()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output 
            SELECT TOP 10 userkey 
            FROM users
            ORDER BY userkey");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.OrderBy(x => x.UserKey).Take(10).Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task TestTop10Desc()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output 
            SELECT TOP 10 userkey 
            FROM users
            ORDER BY userkey DESC");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.OrderByDescending(x => x.UserKey).Take(10).Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task TestTop63Desc()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output 
            SELECT TOP 63 userkey 
            FROM users
            ORDER BY userkey DESC");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.OrderByDescending(x => x.UserKey).Take(63).Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task TestTop63Asc()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output 
            SELECT TOP 63 userkey 
            FROM users
            ORDER BY userkey");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.OrderBy(x => x.UserKey).Take(63).Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task TestTop1AscNullsFirstWithDuplicates()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output 
            SELECT TOP 1 companyId 
            FROM users
            ORDER BY companyId ASC NULLS FIRST");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.OrderBy(x => x.CompanyId).Take(1).Select(x => new { x.CompanyId }));
        }

        [Fact]
        public async Task TestTop1AscNullsLastWithDuplicates()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output 
            SELECT TOP 1 companyId 
            FROM users
            ORDER BY companyId ASC NULLS LAST");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.OrderByDescending(x => x.CompanyId != null).ThenBy(x => x.CompanyId).Take(1).Select(x => new { x.CompanyId }));
        }

        [Fact]
        public async Task TestTop1DescNullsFirstWithDuplicates()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output 
            SELECT TOP 1 companyId 
            FROM users
            ORDER BY companyId DESC NULLS FIRST");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.OrderBy(x => x.CompanyId != null).ThenByDescending(x => x.CompanyId).Take(1).Select(x => new { x.CompanyId }));
        }

        [Fact]
        public async Task TestTop1DescNullsLastWithDuplicates()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output 
            SELECT TOP 1 companyId 
            FROM users
            ORDER BY companyId DESC NULLS LAST");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.OrderByDescending(x => x.CompanyId != null).ThenByDescending(x => x.CompanyId).Take(1).Select(x => new { x.CompanyId }));
        }

        [Fact]
        public async Task TestTop1AscDeleteFirstRow()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output 
            SELECT TOP 1 userkey 
            FROM users
            ORDER BY userkey");

            await WaitForUpdate();

            var firstUser = Users[0];
            DeleteUser(firstUser);
            await WaitForUpdate();

            AssertCurrentDataEqual(Users.OrderBy(x => x.UserKey).Take(1).Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task TestTop10AscDeleteFirstRow()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output 
            SELECT TOP 10 userkey 
            FROM users
            ORDER BY userkey");

            await WaitForUpdate();

            var firstUser = Users[0];
            DeleteUser(firstUser);
            await WaitForUpdate();

            AssertCurrentDataEqual(Users.OrderBy(x => x.UserKey).Take(10).Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task TestTop1AscNullsFirstWithDuplicatesDeleteNull()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output 
            SELECT TOP 1 companyId 
            FROM users
            ORDER BY companyId ASC NULLS FIRST");

            await WaitForUpdate();
            var firstWithNull = Users.First(x => x.CompanyId == null);
            DeleteUser(firstWithNull);
            await WaitForUpdate();

            AssertCurrentDataEqual(Users.OrderBy(x => x.CompanyId).Take(1).Select(x => new { x.CompanyId }));
        }
    }
}
