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
    /// <summary>
    /// Tests for struct columns with nullable inner fields.
    /// 
    /// The bug: Column.CompareTo(IColumn otherColumn, thisIndex, otherIndex)
    /// does not check validity bitmaps before delegating to the data column's
    /// comparison. This is exposed when:
    /// 1. The outer struct Column has _nullCounter > 0 (some null structs),
    ///    which forces SearchBoundries to use BoundarySearch.SearchBoundriesForDataColumn
    /// 2. The tree's inner data columns have MIXED null/non-null values (type is not
    ///    ArrowTypeId.Null, _nullCounter > 0), which requires a non-null value
    ///    to be inserted before a null value
    /// 3. StructColumn.CompareTo with ReferenceStructValue calls
    ///    _columns[i].CompareTo(IColumn, ...) for inner columns
    /// 4. An inner column has null (raw data=0) compared to actual value 0,
    ///    producing incorrect equality (0 == 0) instead of null &lt; 0
    /// 
    /// Key trigger conditions (ALL required):
    /// - Null outer struct → _nullCounter > 0 on outer Column → SearchBoundriesForDataColumn
    /// - Non-null struct with non-null inner field inserted BEFORE null inner field
    ///   → inner Column type becomes Int64 with _nullCounter > 0
    /// - Search for a struct with inner value = 0 (same raw data as null default)
    /// </summary>
    [Collection("Acceptance tests")]
    public class StructColumnNullFieldTests : FlowtideAcceptanceBase
    {
        public StructColumnNullFieldTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        /// <summary>
        /// Core bug reproduction test.
        /// 
        /// User insertion order matters for triggering the bug:
        /// 1. Inactive user → null outer struct (forces _nullCounter > 0 on outer Column)
        /// 2. Active user with visits=5 → non-null inner visits Column gets Int64 type
        /// 3. Active user with visits=null → inner visits Column stays Int64, _nullCounter > 0
        /// 4. Active user with visits=0 → search for raw value 0, same as null's default
        /// 
        /// Without the fix, when searching for {co1, 0} in a tree containing {co1, null},
        /// the inner Column.CompareTo(IColumn, ...) compares raw data: null(0) == 0,
        /// treating them as the same key. This causes FindIndex to return "found"
        /// for {co1, null} when searching for {co1, 0}, merging distinct keys.
        /// </summary>
        [Fact]
        public async Task StructWithNullVsZeroInnerField()
        {
            // User 1: active=false -> null outer struct (forces _nullCounter > 0)
            AddUser(new Entities.User
            {
                UserKey = 1,
                FirstName = "Alice",
                LastName = "Smith",
                Visits = 5,
                Active = false,
                CompanyId = "co1"
            });
            // User 2: active=true, visits=5 -> non-null struct with non-null visits
            // CRITICAL: must be inserted BEFORE null visits so inner Column type
            // becomes Int64 (not Null), enabling the raw data comparison path
            AddUser(new Entities.User
            {
                UserKey = 2,
                FirstName = "Bob",
                LastName = "Jones",
                Visits = 5,
                Active = true,
                CompanyId = "co2"
            });
            // User 3: active=true, visits=null -> non-null struct with null inner field
            // Inner visits Column now has type=Int64, _nullCounter=1
            AddUser(new Entities.User
            {
                UserKey = 3,
                FirstName = "Carol",
                LastName = "Davis",
                Visits = null,      // null visits -> raw data = 0 in Int64Column
                Active = true,
                CompanyId = "co1"
            });
            // User 4: active=true, visits=0 -> non-null struct with visits=0
            // When searching for this, raw data 0 matches null's raw data 0
            AddUser(new Entities.User
            {
                UserKey = 4,
                FirstName = "Dave",
                LastName = "Evans",
                Visits = 0,         // actual 0 -> raw data = 0 (same as null!)
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

            // Expected: 4 distinct output values:
            // 1. null (User 1, active=false)
            // 2. {co2, 5} (User 2)
            // 3. {co1, null} (User 3)
            // 4. {co1, 0} (User 4) -- MUST be distinct from {co1, null}
            //
            // Without the fix: User 3 and User 4 merge because
            // Column.CompareTo compares raw data: null(0) == 0
            // -> only 3 rows, and {co1, 0} is lost
            AssertCurrentDataEqual(Users.Select(x => new
            {
                s = x.Active ? new { company = x.CompanyId, visits = x.Visits } : null
            }));
        }

        /// <summary>
        /// Same bug pattern but with multiple nullable inner fields.
        /// </summary>
        [Fact]
        public async Task StructWithNullVsZeroMultipleInnerFields()
        {
            // Inactive user -> null outer struct
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
            // Active user with non-null fields -> sets inner Column types to Int64
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
            // Active user with null fields -> inner Columns have _nullCounter > 0
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
            // Active user with zero fields -> raw data matches null's default
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
                    CASE WHEN active THEN named_struct('company', companyId, 'visits', visits, 'manager', managerkey)
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

        /// <summary>
        /// Larger dataset variant to also test internal B+ tree node navigation
        /// which uses EventBatchData.CompareRows -> Column.CompareTo(IColumn, ...).
        /// </summary>
        [Fact]
        public async Task StructWithNullVsZeroLargerDataset()
        {
            // Add inactive users to force null outer structs
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

            // Add active users with non-null visits first (sets inner Column type to Int64)
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

            // Add active users with visits=null (inner Column gets _nullCounter > 0)
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

            // Add active users with visits=0 (raw data matches null's default)
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
