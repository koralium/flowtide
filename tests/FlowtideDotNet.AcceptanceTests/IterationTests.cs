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

using FlowtideDotNet.AcceptanceTests.Entities;
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests
{
    [Collection("Acceptance tests")]
    public class IterationTests : FlowtideAcceptanceBase
    {
        public IterationTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        /// <summary>
        /// Tests a classic recursive sql which iterates over a tree.
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task TreeIteration()
        {
            GenerateData();
            await this.StartStream(@"
            with user_manager_cte AS (
                SELECT 
                    userKey, 
                    firstName,
                    lastName,
                    managerKey,
                    null as ManagerFirstName,
                    1 as level
                FROM users
                WHERE managerKey is null
                UNION ALL
                SELECT 
                    u.userKey, 
                    u.firstName,
                    u.lastName,
                    u.managerKey,
                    umc.firstName as ManagerFirstName,
                    level + 1 as level 
                FROM users u
                INNER JOIN user_manager_cte umc ON umc.userKey = u.managerKey
            )
            INSERT INTO output
            SELECT userKey, firstName, lastName, managerKey, ManagerFirstName, level
            FROM user_manager_cte");
            await WaitForUpdate();

            List<UserIteration> expected = new List<UserIteration>();
            var firstUser = Users.First(x => x.ManagerKey == null);
            expected.Add(new UserIteration()
            {
                UserKey = firstUser.UserKey,
                FirstName = firstUser.FirstName,
                LastName = firstUser.LastName,
                ManagerKey = null,
                ManagerFirstName = null,
                Level = 1
            });
            CalculateIterationExpectedValue(firstUser, 2, expected);
            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task TreeIterationWithUpdate()
        {
            GenerateData(1000);
            await this.StartStream(@"
            with user_manager_cte AS (
                SELECT 
                    userKey, 
                    firstName,
                    lastName,
                    managerKey,
                    null as ManagerFirstName,
                    1 as level
                FROM users
                WHERE managerKey is null
                UNION ALL
                SELECT 
                    u.userKey, 
                    u.firstName,
                    u.lastName,
                    u.managerKey,
                    umc.firstName as ManagerFirstName,
                    level + 1 as level 
                FROM users u
                INNER JOIN user_manager_cte umc ON umc.userKey = u.managerKey
            )
            INSERT INTO output
            SELECT userKey, firstName, lastName, managerKey, ManagerFirstName, level
            FROM user_manager_cte");
            await WaitForUpdate();

            var user = Users[20];

            user.ManagerKey = Users[10].UserKey;
            AddOrUpdateUser(user);
            await WaitForUpdate();

            List<UserIteration> expected = new List<UserIteration>();
            var firstUser = Users.First(x => x.ManagerKey == null);
            expected.Add(new UserIteration()
            {
                UserKey = firstUser.UserKey,
                FirstName = firstUser.FirstName,
                LastName = firstUser.LastName,
                ManagerKey = null,
                ManagerFirstName = null,
                Level = 1
            });
            CalculateIterationExpectedValue(firstUser, 2, expected);
            AssertCurrentDataEqual(expected);
        }

        private class UserIteration
        {
            public int UserKey { get; set; }
            public string? FirstName { get; set; }
            public string? LastName { get; set; }
            public int? ManagerKey { get; set; }
            public string? ManagerFirstName { get; set; }
            public int Level { get; set; }
        }

        private void CalculateIterationExpectedValue(User current, int depth, List<UserIteration> result)
        {
            var usersWithManager = Users.Where(x => x.ManagerKey == current.UserKey);

            foreach (var user in usersWithManager)
            {
                result.Add(new UserIteration
                {
                    UserKey = user.UserKey,
                    FirstName = user.FirstName,
                    LastName = user.LastName,
                    ManagerKey = user.ManagerKey,
                    ManagerFirstName = current.FirstName,
                    Level = depth
                });

                CalculateIterationExpectedValue(user, depth + 1, result);
            }
        }



    }
}
