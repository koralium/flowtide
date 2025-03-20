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

using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests
{
    [Collection("Acceptance tests")]
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
        public async Task SelectWithConcatFunctionName()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT concat(firstName, ' ', lastName) as Name FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { Name = x.FirstName + " " + x.LastName }));
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

        [Fact]
        public async Task StringAgg()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT string_agg(firstName, ',') as Name FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] { new { Name = string.Join(",", Users.Select(y => y.FirstName).OrderBy(x => x)) } });
        }

        [Fact]
        public async Task StringAggWithUpdate()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT string_agg(firstName, ',') as Name FROM users");
            await WaitForUpdate();
            DeleteUser(Users[0]);
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] { new { Name = string.Join(",", Users.Select(y => y.FirstName).OrderBy(x => x)) } });
        }

        [Fact]
        public async Task SelectWithStartsWith()
        {
            GenerateData();
            var start = Users[0].FirstName!.Substring(0, 2);
            await StartStream($"INSERT INTO output SELECT starts_with(firstName, '{start}') as Name FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { val = x.FirstName!.StartsWith(start) }));
        }

        [Fact]
        public async Task SelectWithSubstringNoLength()
        {
            GenerateData(1000);
            await StartStream($"INSERT INTO output SELECT substring(firstName, 3) as Name FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { val = x.FirstName!.Substring(2) }));
        }

        [Fact]
        public async Task SelectWithSubstringWithLength()
        {
            GenerateData(1000);
            await StartStream($"INSERT INTO output SELECT substring(firstName, 3, 2) as Name FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { val = x.FirstName!.Substring(2, Math.Min(2, x.FirstName.Length - 2)) }));
        }

        [Fact]
        public async Task SelectWithReplace()
        {
            GenerateData();
            var start = Users[0].FirstName!.Substring(0, 1);
            await StartStream($"INSERT INTO output SELECT replace(firstName, '{start}', '_') as Name FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { val = x.FirstName!.Replace(start, "_") }));
        }

        [Fact]
        public async Task StringBase64Encode()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT string_base64_encode(firstName) as Name FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { Name = Convert.ToBase64String(Encoding.UTF8.GetBytes(x.FirstName ?? "")) }));
        }

        [Fact]
        public async Task StringBase64Decode()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT string_base64_decode(string_base64_encode(firstName)) as Name FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { Name = x.FirstName }));
        }

        [Fact]
        public async Task StringLength()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT LEN(firstName) as length FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { Length = x.FirstName?.Length ?? default(int?) }));
        }

        [Fact]
        public async Task StringLengthNullableString()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT LEN(TrimmableNullableString) as length FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { Length = x.TrimmableNullableString?.Length ?? default(int?) }));
        }

        [Fact]
        public async Task SelectWithStrPos()
        {
            GenerateData();
            var start = Users[0].FirstName!.Substring(1, 2);
            await StartStream($"INSERT INTO output SELECT strpos(firstName, '{start}') as Name FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { val = x.FirstName!.IndexOf(start) + 1 }));
        }

        [Fact]
        public async Task StringSplit()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT string_split(concat(firstName, ' ', lastName), ' ') as NameParts FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { NameParts = ($"{x.FirstName} {x.LastName}").Split(' ') }));
        }

        [Fact]
        public async Task RegexStringSplit()
        {
            var pattern = @"\s";
            GenerateData();
            await StartStream($"INSERT INTO output SELECT regexp_string_split(concat(firstName, ' ', lastName), '{pattern}') as NameParts FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { FirstLetter = Regex.Split($"{x.FirstName} {x.LastName}", pattern) }));
        }

        [Fact]
        public async Task ToJsonWithMap()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output 
            SELECT to_json(map('firstName', firstName, 'lastName', lastName)) as json FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { json = JsonSerializer.Serialize(new {firstName = x.FirstName, lastName = x.LastName}) }));
        }
    }
}
