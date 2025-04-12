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
    public class SelectTests : FlowtideAcceptanceBase
    {
        public SelectTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        [Fact]
        public async Task SelectTwoColumnsWithId()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT userkey, firstName FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { x.UserKey, x.FirstName }));
        }

        [Fact]
        public async Task SelectOneColumnsWithoutId()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT firstName FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { x.FirstName }));
        }

        [Fact]
        public async Task SelectOneColumnsWithTableAliasAndBrackets()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT u.[firstName] FROM users u");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { x.FirstName }));
        }

        [Fact]
        public async Task SelectOneColumnsWithoutIdAndUpdate()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT firstName FROM users");
            await WaitForUpdate();
            var firstUser = Users[0];
            firstUser.FirstName = "Updated";
            AddOrUpdateUser(firstUser);

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(x => new { x.FirstName }));
        }

        [Fact]
        public async Task SelectWithCase()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    CASE WHEN Gender = 1 THEN firstName 
                    ELSE lastName 
                    END AS name
                FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { Name = x.Gender == Entities.Gender.Female ? x.FirstName : x.LastName }));
        }

        [Fact]
        public async Task SelectWithCaseNoElse()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    CASE WHEN Gender = 1 THEN firstName
                    END AS name
                FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { Name = x.Gender == Entities.Gender.Female ? x.FirstName : null }));
        }

        [Fact]
        public async Task SelectWithConcat()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    firstName || ' ' || lastName AS name
                FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { Name = x.FirstName + " " + x.LastName }));
        }

        [Fact]
        public async Task SelectWithDistinct()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT DISTINCT
                    userkey
                FROM orders");
            await WaitForUpdate();
            AssertCurrentDataEqual(Orders.Select(x => new { x.UserKey }).Distinct());
        }

        [Fact]
        public async Task SelectSubProperty()
        {
            GenerateData();
            await StartStream(@"
                CREATE VIEW test AS
                SELECT map('userkey', userkey) AS user 
                FROM orders;

                INSERT INTO output 
                SELECT
                    user.userkey
                FROM test");
            await WaitForUpdate();
            AssertCurrentDataEqual(Orders.Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task SelectSubPropertyDifferentCase()
        {
            GenerateData();
            await StartStream(@"
                CREATE VIEW test AS
                SELECT map('userkey', userkey) AS user 
                FROM orders;

                INSERT INTO output 
                SELECT
                    user.userKey
                FROM test");
            await WaitForUpdate();
            // Expect an array where all columns are null since the field was not found in the map
            AssertCurrentDataEqual(Orders.Select(x => new { UserKey = default(int?) }));
        }

        [Fact]
        public async Task SelectWithEqual()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT userkey = 23 FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { val = x.UserKey == 23 }));
        }

        [Fact]
        public async Task SelectWithNotEqual()
        {
            GenerateData();
            await StartStream("INSERT INTO output SELECT userkey != 23 FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.Select(x => new { val = x.UserKey != 23 }));
        }

        [Fact]
        public async Task SelectWithoutFrom()
        {
            await StartStream("INSERT INTO output SELECT 1 as number, 'abc' as str");
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] { new { number = 1, str = "abc" } });
        }

        [Fact]
        public async Task SelectFromValuesList()
        {
            await StartStream(@"
                INSERT INTO output 
                SELECT * FROM 
                (
                    VALUES 
                    (1, 'a'),
                    (2, 'b'),
                    (3, 'c')
                )");

            await WaitForUpdate();

            AssertCurrentDataEqual(new[] { new { val = 1, str = "a" }, new { val = 2, str = "b" }, new { val = 3, str = "c" } });
        }

        [Fact]
        public async Task SelectFromValuesListWithAliases()
        {
            await StartStream(@"
                INSERT INTO output 
                SELECT number, str FROM 
                (
                    VALUES 
                    (1, 'a'),
                    (2, 'b'),
                    (3, 'c')
                ) t(number, str)");

            await WaitForUpdate();

            AssertCurrentDataEqual(new[] { new { val = 1, str = "a" }, new { val = 2, str = "b" }, new { val = 3, str = "c" } });
        }

        [Fact]
        public async Task SelectFromValuesListMakeSureCrashDoesNotResend()
        {
            GenerateData();
            var firstUser = Users.First();
            await StartStream(@"
                INSERT INTO output 
                SELECT u.userkey FROM 
                (
                    VALUES 
                    (" + firstUser.UserKey + @", 'a')
                ) t(number, str)
                INNER JOIN users u ON t.number = u.userkey");

            await WaitForUpdate();

            AssertCurrentDataEqual(new[] { new { userkey = firstUser.UserKey } });

            await Crash();

            GenerateData();

            await WaitForUpdate();

            // The test fails here if the virtual table operator resends data
            AssertCurrentDataEqual(new[] { new { userkey = firstUser.UserKey } });
        }

        [Fact]
        public async Task SelectFromValuesListEmitOnlySecondColumn()
        {
            await StartStream(@"
                INSERT INTO output 
                SELECT str FROM 
                (
                    VALUES 
                    (1, 'a'),
                    (2, 'b'),
                    (3, 'c')
                ) t(number, str)");

            await WaitForUpdate();

            AssertCurrentDataEqual(new[] { new { str = "a" }, new { str = "b" }, new { str = "c" } });
        }

        [Fact]
        public async Task SelectFromValuesListBinaryData()
        {
            await StartStream(@"
                INSERT INTO output 
                SELECT hex FROM 
                (
                    VALUES 
                    (0x544F2041)
                ) t(hex)");

            await WaitForUpdate();

            AssertCurrentDataEqual(new[] { new { hex = new byte[] { 84, 79, 32, 65 } } });
        }

        [Fact]
        public async Task SelectWithNamedStruct()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    named_struct('firstName', firstName, 'lastName', lastName) AS name
                FROM users");
            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(x => new { Name = new { firstName = x.FirstName, lastName = x.LastName } }));
        }

        [Fact]
        public async Task SelectSubPropertyFromNamedStruct()
        {
            GenerateData();
            await StartStream(@"
                CREATE VIEW testview AS
                SELECT 
                    named_struct('firstName', firstName, 'lastName', lastName) AS name
                FROM users;

                INSERT INTO output
                SELECT name.firstName FROM testview;");
            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(x => new { x.FirstName }));
        }

        [Fact]
        public async Task SelectWithEmptyNamedStruct()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    named_struct() AS name
                FROM users");
            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(x => new { Name = new { } }));
        }
    }
}