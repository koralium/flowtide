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
    public class LikeTests : FlowtideAcceptanceBase
    {
        public LikeTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        [Fact]
        public async Task LikeStartsWith()
        {
            GenerateData();
            var firstLetterFirstUser = Users[0].FirstName!.Substring(0, 1);
            await StartStream(@"
            INSERT INTO output
            SELECT firstName FROM users WHERE firstName LIKE '" + firstLetterFirstUser + @"%';
            ");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Where(x => x.FirstName!.StartsWith(firstLetterFirstUser)).Select(x => new { firstName = x.FirstName}));
        }

        [Fact]
        public async Task LikeEndsWith()
        {
            GenerateData();
            var lastLetterFirstUser = Users[0].FirstName!.Substring(Users[0].FirstName!.Length - 1, 1);
            await StartStream(@"
            INSERT INTO output
            SELECT firstName FROM users WHERE firstName LIKE '%" + lastLetterFirstUser + @"';
            ");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Where(x => x.FirstName!.EndsWith(lastLetterFirstUser)).Select(x => new { firstName = x.FirstName }));
        }

        [Fact]
        public async Task LikeContains()
        {
            GenerateData();
            var SecondLetterFirstUser = Users[0].FirstName!.Substring(1, 1);
            await StartStream(@"
            INSERT INTO output
            SELECT firstName FROM users WHERE firstName LIKE '%" + SecondLetterFirstUser + @"%';
            ");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Where(x => x.FirstName!.Contains(SecondLetterFirstUser, StringComparison.OrdinalIgnoreCase)).Select(x => new { firstName = x.FirstName }));
        }

        [Fact]
        public async Task LikeSingleCharacterWildcard()
        {
            GenerateData();
            var remainingUserName = Users[0].FirstName!.Substring(1);
            await StartStream(@"
            INSERT INTO output
            SELECT firstName FROM users WHERE firstName LIKE '_" + remainingUserName + @"';
            ");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Where(x => x.FirstName!.EndsWith(remainingUserName) && x.FirstName.Length == (remainingUserName.Length + 1)).Select(x => new { firstName = x.FirstName }));
        }

        [Fact]
        public async Task LikeWithEscapeCharacter()
        {
            GenerateData();
            Users[0].FirstName = "_" + Users[0].FirstName;
            var userName = Users[0].FirstName;
            await StartStream(@"
            INSERT INTO output
            SELECT firstName FROM users WHERE firstName LIKE '!" + userName + @"' ESCAPE '!';
            ");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Where(x => x.FirstName == userName).Select(x => new { firstName = x.FirstName }));
        }

        [Fact]
        public async Task LikeWithCharacterSetTwoChars()
        {
            GenerateData();
            var firstUserChar = Users[0].FirstName!.Substring(0, 1);
            var secondUserChar = Users[0].FirstName!.Substring(0, 1);
            var set = $"[{firstUserChar}{secondUserChar}]";
            var userName = Users[0].FirstName;
            await StartStream(@"
            INSERT INTO output
            SELECT firstName FROM users WHERE firstName LIKE '" + set + @"%';
            ");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Where(x => x.FirstName!.StartsWith(firstUserChar) || x.FirstName!.StartsWith(secondUserChar)).Select(x => new { firstName = x.FirstName }));
        }

        [Fact]
        public async Task NotLike()
        {
            GenerateData();
            var firstLetterFirstUser = Users[0].FirstName!.Substring(0, 1);
            await StartStream(@"
            INSERT INTO output
            SELECT firstName FROM users WHERE firstName NOT LIKE '" + firstLetterFirstUser + @"%';
            ");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Where(x => !x.FirstName!.StartsWith(firstLetterFirstUser)).Select(x => new { firstName = x.FirstName }));
        }
    }
}
