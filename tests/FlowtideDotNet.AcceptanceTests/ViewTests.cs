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
    public class ViewTests : FlowtideAcceptanceBase
    {
        public ViewTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        [Fact]
        public async Task TestViewInView()
        {
            GenerateData();

            await StartStream(@"
             CREATE VIEW test AS
                SELECT 
                    userKey, 
                    firstName
                FROM users;

            CREATE VIEW test2 AS
                SELECT 
                    userKey, 
                    firstName
                FROM test;

            INSERT INTO output
            SELECT userKey, firstName FROM test2;
            ");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(u => new { u.UserKey, u.FirstName }).ToList());
        }

        [Fact]
        public async Task TestBufferedView()
        {
            GenerateData();

            await StartStream(@"
             CREATE VIEW test WITH (BUFFERED = true) AS
                SELECT 
                    userKey, 
                    firstName
                FROM users;

            INSERT INTO output
            SELECT userKey, firstName FROM test;
            ");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(u => new { u.UserKey, u.FirstName }).ToList());
        }
    }
}
