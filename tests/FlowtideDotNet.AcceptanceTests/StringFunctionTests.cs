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
    public class StringFunctionTests : FlowtideAcceptanceBase
    {
        public StringFunctionTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        [Fact]
        public async Task SelectWithConcat()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT firstName || lastName as Name FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { Name = x.FirstName + x.LastName }));
        }

        [Fact]
        public async Task SelectWithConcatWithNull()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT firstName || lastName || NULL as Name FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { Name = default(string) }));
        }

        [Fact]
        public async Task SelectWithLower()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT lower(firstName) as Name FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { Name = x.FirstName?.ToLower() }));
        }

        [Fact]
        public async Task SelectWithUpper()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT upper(firstName) as Name FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { Name = x.FirstName?.ToUpper() }));
        }

        [Fact]
        public async Task SelectWithTrim()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT trim(TrimmableNullableString) as Name FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { Name = x.TrimmableNullableString?.Trim() }));
        }
        
        [Fact]
        public async Task SelectWithLTrim()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT ltrim(TrimmableNullableString) as Name FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { Name = x.TrimmableNullableString?.TrimStart() }));
        }

        [Fact]
        public async Task SelectWithRTrim()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT rtrim(TrimmableNullableString) as Name FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { Name = x.TrimmableNullableString?.TrimEnd() }));
        }
    }
}
