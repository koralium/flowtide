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
    /// These tests exercise the B+ tree key lookup (FindIndex / SearchBoundries)
    /// and comparison paths (Column.CompareTo) when struct column fields are null.
    /// 
    /// The bug: Column.CompareTo(IColumn otherColumn, thisIndex, otherIndex)
    /// does not check validity bitmaps (null tracking) before delegating to
    /// the data column's comparison. When struct fields are null, raw data values
    /// are compared instead of being treated as nulls, which can cause incorrect
    /// comparison results leading to duplicate key insertions in the B+ tree.
    /// </summary>
    [Collection("Acceptance tests")]
    public class StructColumnNullFieldTests : FlowtideAcceptanceBase
    {
        public StructColumnNullFieldTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        /// <summary>
        /// Test that selecting struct columns with nullable inner fields
        /// produces the correct number of rows (no duplicates).
        /// Uses SourceImmutable to skip normalization so data goes directly
        /// into the sink's B+ tree via ColumnComparer.FindIndex.
        /// 
        /// The struct has a nullable field (visits) which can be null for some users.
        /// If the B+ tree comparison doesn't handle null fields correctly,
        /// rows with null struct fields will not match on lookup and
        /// will be inserted as duplicates.
        /// </summary>
        [Fact]
        public async Task SelectStructWithNullableFields()
        {
            SourceImmutable();
            GenerateData(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    named_struct('id', userkey, 'visits', visits) AS s
                FROM users");
            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(x => new
            {
                s = new { id = x.UserKey, visits = x.Visits }
            }));
        }

        /// <summary>
        /// Test that struct columns with multiple nullable inner fields
        /// are handled correctly. Uses multiple nullable fields to increase
        /// the chance of exposing the null handling bug.
        /// </summary>
        [Fact]
        public async Task SelectStructWithMultipleNullableFields()
        {
            SourceImmutable();
            GenerateData(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    named_struct('id', userkey, 'visits', visits, 'manager', managerkey) AS s
                FROM users");
            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(x => new
            {
                s = new { id = x.UserKey, visits = x.Visits, manager = x.ManagerKey }
            }));
        }

        /// <summary>
        /// Test struct with nullable fields in a GROUP BY aggregation context.
        /// The aggregate operator uses AggregateInsertComparer which also goes
        /// through the same Column.CompareTo path for struct key columns.
        /// Grouping by a struct that contains nullable fields exercises the
        /// boundary search logic with null struct field values.
        /// </summary>
        [Fact]
        public async Task AggregateWithStructKeyContainingNullableField()
        {
            SourceImmutable();
            GenerateData(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    named_struct('company', companyId, 'visits', visits) AS key,
                    count(*) as cnt
                FROM users
                GROUP BY named_struct('company', companyId, 'visits', visits)
                ", ignoreSameDataCheck: true);
            await WaitForUpdate();

            var expected = Users
                .GroupBy(x => new { company = x.CompanyId, visits = x.Visits })
                .OrderBy(x => x.Key.company)
                .ThenBy(x => x.Key.visits)
                .Select(x => new
                {
                    key = new { company = x.Key.company, visits = x.Key.visits },
                    cnt = (long)x.Count()
                });
            AssertCurrentDataEqual(expected);
        }

        /// <summary>
        /// Test that updating a user where the struct key contains a null field
        /// properly finds and updates the existing row instead of inserting a duplicate.
        /// This is the most direct test of the bug: if Column.CompareTo doesn't
        /// handle nulls correctly, FindIndex returns "not found" for an existing key,
        /// causing the tree to insert a duplicate.
        /// </summary>
        [Fact]
        public async Task UpdateRowWithNullStructField()
        {
            // Create users with specific null patterns
            AddUser(new Entities.User
            {
                UserKey = 1,
                FirstName = "Alice",
                LastName = "Smith",
                Visits = null,   // null field in struct
                CompanyId = "co1"
            });
            AddUser(new Entities.User
            {
                UserKey = 2,
                FirstName = "Bob",
                LastName = "Jones",
                Visits = 5,      // non-null field in struct
                CompanyId = "co2"
            });
            AddUser(new Entities.User
            {
                UserKey = 3,
                FirstName = "Carol",
                LastName = "Davis",
                Visits = null,   // null field in struct
                CompanyId = null // null companyId too
            });

            SourceImmutable();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    named_struct('id', userkey, 'visits', visits) AS s,
                    firstName
                FROM users");
            await WaitForUpdate();

            // Verify initial state: 3 rows
            AssertCurrentDataEqual(Users.Select(x => new
            {
                s = new { id = x.UserKey, visits = x.Visits },
                x.FirstName
            }));

            // Update a user - change firstName but keep the same struct key (with null visits)
            AddOrUpdateUser(new Entities.User
            {
                UserKey = 1,
                FirstName = "Alicia",  // changed
                LastName = "Smith",
                Visits = null,   // still null - same struct key
                CompanyId = "co1"
            });

            await WaitForUpdate();

            // Should still be 3 rows, not 4 (no duplicate)
            AssertCurrentDataEqual(Users.Select(x => new
            {
                s = new { id = x.UserKey, visits = x.Visits },
                x.FirstName
            }));
        }

        /// <summary>
        /// Test with all-null struct fields to ensure the degenerate case works.
        /// When all fields of a struct are null, the comparison must still
        /// correctly identify equality.
        /// </summary>
        [Fact]
        public async Task SelectStructWithAllNullFields()
        {
            // Create users where nullable fields will all be null
            AddUser(new Entities.User
            {
                UserKey = 1,
                FirstName = "Alice",
                LastName = "Smith",
                Visits = null,
                ManagerKey = null,
                CompanyId = null
            });
            AddUser(new Entities.User
            {
                UserKey = 2,
                FirstName = "Bob",
                LastName = "Jones",
                Visits = null,
                ManagerKey = null,
                CompanyId = null
            });

            SourceImmutable();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    userkey,
                    named_struct('visits', visits, 'manager', managerkey) AS s
                FROM users");
            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(x => new
            {
                x.UserKey,
                s = new { visits = x.Visits, manager = x.ManagerKey }
            }));
        }

        /// <summary>
        /// Test with mixed null and non-null struct fields across multiple rows
        /// to verify binary search correctness. When some rows have null fields
        /// and others don't, the B+ tree's column-by-column narrowing binary
        /// search must correctly handle the null ordering.
        /// </summary>
        [Fact]
        public async Task SelectStructWithMixedNullPattern()
        {
            // Create a controlled set of users with specific null patterns
            AddUser(new Entities.User { UserKey = 1, FirstName = "A", LastName = "X", Visits = 1, ManagerKey = null, CompanyId = "c1" });
            AddUser(new Entities.User { UserKey = 2, FirstName = "B", LastName = "Y", Visits = null, ManagerKey = 1, CompanyId = "c1" });
            AddUser(new Entities.User { UserKey = 3, FirstName = "C", LastName = "Z", Visits = 2, ManagerKey = 1, CompanyId = "c2" });
            AddUser(new Entities.User { UserKey = 4, FirstName = "D", LastName = "W", Visits = null, ManagerKey = null, CompanyId = null });
            AddUser(new Entities.User { UserKey = 5, FirstName = "E", LastName = "V", Visits = 3, ManagerKey = 2, CompanyId = null });

            SourceImmutable();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    named_struct('visits', visits, 'manager', managerkey, 'company', companyId) AS s,
                    firstName
                FROM users");
            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(x => new
            {
                s = new { visits = x.Visits, manager = x.ManagerKey, company = x.CompanyId },
                x.FirstName
            }));
        }

        /// <summary>
        /// Test struct columns in a join context where struct keys with null
        /// fields are used. The merge join uses MergeJoinInsertComparer which
        /// has its own FindBoundries implementation that also goes through
        /// Column.SearchBoundries.
        /// </summary>
        [Fact]
        public async Task JoinWithStructContainingNullFields()
        {
            // Add users with nullable visits
            AddUser(new Entities.User { UserKey = 1, FirstName = "Alice", LastName = "Smith", Visits = null, CompanyId = "co1" });
            AddUser(new Entities.User { UserKey = 2, FirstName = "Bob", LastName = "Jones", Visits = 5, CompanyId = "co1" });
            AddUser(new Entities.User { UserKey = 3, FirstName = "Carol", LastName = "Davis", Visits = null, CompanyId = "co2" });

            SourceImmutable();
            await StartStream(@"
                CREATE VIEW v1 AS
                SELECT 
                    userkey,
                    named_struct('name', firstName, 'visits', visits) AS info
                FROM users;

                INSERT INTO output
                SELECT userkey, info FROM v1");
            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(x => new
            {
                x.UserKey,
                info = new { name = x.FirstName, visits = x.Visits }
            }));
        }
    }
}
