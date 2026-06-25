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
    public class StructColumnNullFieldTests : FlowtideAcceptanceBase
    {
        public StructColumnNullFieldTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        [Fact]
        public async Task StructWithNullVsZeroInnerField()
        {
            AddUser(new Entities.User
            {
                UserKey = 1,
                FirstName = "Alice",
                LastName = "Smith",
                Visits = 5,
                Active = false,
                CompanyId = "co1"
            });
            AddUser(new Entities.User
            {
                UserKey = 2,
                FirstName = "Bob",
                LastName = "Jones",
                Visits = 5,
                Active = true,
                CompanyId = "co2"
            });
            AddUser(new Entities.User
            {
                UserKey = 3,
                FirstName = "Carol",
                LastName = "Davis",
                Visits = null,
                Active = true,
                CompanyId = "co1"
            });

            AddUser(new Entities.User
            {
                UserKey = 4,
                FirstName = "Dave",
                LastName = "Evans",
                Visits = 0, 
                Active = true,
                CompanyId = "co1"
            });

            SourceImmutable();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    CASE WHEN active THEN named_struct('company', companyId, 'visits', visits)
                    ELSE NULL
                    END AS s
                FROM users");
            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(x => new
            {
                s = x.Active ? new { company = x.CompanyId, visits = x.Visits } : null
            }));
        }

        [Fact]
        public async Task StructWithNullVsZeroMultipleInnerFields()
        {
            AddUser(new Entities.User
            {
                UserKey = 1,
                FirstName = "Alice",
                LastName = "Smith",
                Visits = 1,
                ManagerKey = 1,
                Active = false,
                CompanyId = "co1"
            });
            AddUser(new Entities.User
            {
                UserKey = 2,
                FirstName = "Bob",
                LastName = "Jones",
                Visits = 5,
                ManagerKey = 10,
                Active = true,
                CompanyId = "co1"
            });
            AddUser(new Entities.User
            {
                UserKey = 3,
                FirstName = "Carol",
                LastName = "Davis",
                Visits = null,
                ManagerKey = null,
                Active = true,
                CompanyId = "co1"
            });
            AddUser(new Entities.User
            {
                UserKey = 4,
                FirstName = "Dave",
                LastName = "Evans",
                Visits = 0,
                ManagerKey = 0,
                Active = true,
                CompanyId = "co1"
            });

            SourceImmutable();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    CASE WHEN active THEN named_struct('company', companyId, 'visits', visits, 'manager', managerKey)
                    ELSE NULL
                    END AS s
                FROM users");
            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(x => new
            {
                s = x.Active
                    ? new { company = x.CompanyId, visits = x.Visits, manager = x.ManagerKey }
                    : null
            }));
        }

        [Fact]
        public async Task StructWithNullVsZeroLargerDataset()
        {
            for (int i = 1; i <= 5; i++)
            {
                AddUser(new Entities.User
                {
                    UserKey = i,
                    FirstName = $"Inactive{i}",
                    LastName = "User",
                    Visits = i,
                    Active = false,
                    CompanyId = $"co{i}"
                });
            }

            for (int i = 6; i <= 15; i++)
            {
                AddUser(new Entities.User
                {
                    UserKey = i,
                    FirstName = $"Active{i}",
                    LastName = "User",
                    Visits = i,
                    Active = true,
                    CompanyId = $"co{i % 3}"
                });
            }

            for (int i = 16; i <= 25; i++)
            {
                AddUser(new Entities.User
                {
                    UserKey = i,
                    FirstName = $"NullVisits{i}",
                    LastName = "User",
                    Visits = null,
                    Active = true,
                    CompanyId = $"co{i % 3}"
                });
            }

            for (int i = 26; i <= 35; i++)
            {
                AddUser(new Entities.User
                {
                    UserKey = i,
                    FirstName = $"ZeroVisits{i}",
                    LastName = "User",
                    Visits = 0,
                    Active = true,
                    CompanyId = $"co{i % 3}"
                });
            }

            SourceImmutable();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    CASE WHEN active THEN named_struct('id', userkey, 'company', companyId, 'visits', visits)
                    ELSE NULL
                    END AS s
                FROM users");
            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(x => new
            {
                s = x.Active
                    ? new { id = x.UserKey, company = x.CompanyId, visits = x.Visits }
                    : null
            }));
        }
    }
}
