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
    public class SetTests : FlowtideAcceptanceBase
    {
        public SetTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        [Fact]
        public async Task TestUnionAll()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT UserKey FROM users
            UNION ALL
            SELECT UserKey FROM users");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(u => new { u.UserKey }).Concat(Users.Select(u => new { u.UserKey })).ToList());
        }

        [Fact]
        public async Task TestUnionDistinct()
        {
            GenerateData();
            await StartStream(@"
            INSERT INTO output
            SELECT UserKey FROM users
            UNION DISTINCT
            SELECT UserKey FROM users");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(u => new { u.UserKey }).Union(Users.Select(u => new { u.UserKey })).ToList());
        }

        [Fact]
        public async Task TestExceptDistinct()
        {
            GenerateData();
            await StartStream(@"
            CREATE VIEW test AS
            SELECT UserKey FROM users
            UNION
            SELECT UserKey + 1 FROM users;

            INSERT INTO output
            SELECT UserKey FROM test
            EXCEPT DISTINCT
            SELECT UserKey FROM users");

            await WaitForUpdate();

            var expected = Users.Select(x => new { x.UserKey }).Union(Users.Select(u => new { UserKey = u.UserKey + 1 })).Except(Users.Select(x => new { x.UserKey })).Distinct().ToList();
            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task TestExceptDistinctWithUpdate()
        {
            GenerateData();
            await StartStream(@"
            CREATE VIEW test AS
            SELECT UserKey FROM users
            UNION
            SELECT UserKey + 1 FROM users;

            INSERT INTO output
            SELECT UserKey FROM test
            EXCEPT DISTINCT
            SELECT UserKey FROM users");

            await WaitForUpdate();

            GenerateData();

            await WaitForUpdate();
            var expected = Users.Select(x => new { x.UserKey }).Union(Users.Select(u => new { UserKey = u.UserKey + 1 })).Except(Users.Select(x => new { x.UserKey })).Distinct().ToList();
            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task TestExceptAll()
        {
            GenerateData();
            await StartStream(@"
            CREATE VIEW test AS
            SELECT UserKey FROM users
            UNION ALL
            SELECT UserKey + 1 FROM users;

            INSERT INTO output
            SELECT UserKey FROM test
            EXCEPT ALL
            SELECT UserKey FROM users");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(u => new { UserKey = u.UserKey + 1 }));
        }

        [Fact]
        public async Task TestIntersectDistinct()
        {
            GenerateData();
            await StartStream(@"
            CREATE VIEW test AS
            SELECT UserKey FROM users
            UNION
            SELECT UserKey + 1 FROM users;

            INSERT INTO output
            SELECT UserKey FROM test
            INTERSECT DISTINCT
            SELECT UserKey FROM users");

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(u => new { u.UserKey }));
        }

        [Fact]
        public async Task TestIntersectAll()
        {
            GenerateData();

            await StartStream(@"
            CREATE VIEW test AS
            SELECT UserKey FROM users
            UNION ALL
            SELECT UserKey + 1 FROM users;

            CREATE VIEW otherset AS
            SELECT UserKey FROM users
            UNION ALL
            SELECT UserKey FROM users;            

            INSERT INTO output
            SELECT UserKey FROM test
            INTERSECT ALL
            SELECT UserKey FROM otherset");

            await WaitForUpdate();

            var unionOtherset = Users.Select(u => new { u.UserKey }).Concat(Users.Select(u => new { u.UserKey })).ToList();
            var unionTest = Users.Select(u => new { u.UserKey }).Concat(Users.Select(u => new { UserKey = u.UserKey + 1 })).ToList();

            var expected = Users.Take(0).Select(x => new { x.UserKey }).ToList();

            for (int i = 0; i < unionTest.Count; i++)
            {
                if (unionOtherset.Contains(unionTest[i]))
                {
                    expected.Add(unionTest[i]);
                }
            }

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task TestIntersectAllWithUpdate()
        {
            GenerateData();

            await StartStream(@"
            CREATE VIEW test AS
            SELECT UserKey FROM users
            UNION ALL
            SELECT UserKey + 1 FROM users;

            CREATE VIEW otherset AS
            SELECT UserKey FROM users
            UNION ALL
            SELECT UserKey FROM users;            

            INSERT INTO output
            SELECT UserKey FROM test
            INTERSECT ALL
            SELECT UserKey FROM otherset");

            await WaitForUpdate();

            GenerateData();

            await WaitForUpdate();

            var unionOtherset = Users.Select(u => new { u.UserKey }).Concat(Users.Select(u => new { u.UserKey })).ToList();
            var unionTest = Users.Select(u => new { u.UserKey }).Concat(Users.Select(u => new { UserKey = u.UserKey + 1 })).ToList();

            var expected = Users.Take(0).Select(x => new { x.UserKey }).ToList();

            for (int i = 0; i < unionTest.Count; i++)
            {
                if (unionOtherset.Contains(unionTest[i]))
                {
                    expected.Add(unionTest[i]);
                }
            }

            AssertCurrentDataEqual(expected);
        }

        /// <summary>
        /// Tiny pages so a batch hits many leaves.
        /// </summary>
        [Fact]
        public async Task TestUnionDistinctMultipleLeaves()
        {
            SetPageSizeBytes(256);
            for (int i = 0; i < 1000; i++)
            {
                AddUser(new Entities.User { UserKey = i, FirstName = "n" + i });
            }

            await StartStream(@"
            INSERT INTO output
            SELECT UserKey FROM users
            UNION DISTINCT
            SELECT UserKey + 500 FROM users");

            await WaitForUpdate();

            AssertCurrentDataEqual(
                Users.Select(x => new { x.UserKey })
                    .Union(Users.Select(x => new { UserKey = x.UserKey + 500 }))
                    .ToList());

            // Delete every third user in one batch
            foreach (var user in Users.Where(x => x.UserKey % 3 == 0).ToList())
            {
                DeleteUser(user);
            }

            await WaitForUpdate();

            AssertCurrentDataEqual(
                Users.Select(x => new { x.UserKey })
                    .Union(Users.Select(x => new { UserKey = x.UserKey + 500 }))
                    .ToList());
        }

        /// <summary>
        /// Same value ten times, tests duplicates in a batch.
        /// </summary>
        [Fact]
        public async Task TestExceptAllMultipleLeavesWithDuplicateRows()
        {
            SetPageSizeBytes(256);
            for (int i = 0; i < 1000; i++)
            {
                AddUser(new Entities.User { UserKey = i, FirstName = "n" + (i % 100) });
            }

            await StartStream(@"
            INSERT INTO output
            SELECT FirstName FROM users
            EXCEPT ALL
            SELECT FirstName FROM users WHERE UserKey < 500");

            await WaitForUpdate();

            AssertCurrentDataEqual(ExpectedExceptAllByFirstName());

            // Delete rows on both sides at once
            foreach (var user in Users.Where(x => x.UserKey % 10 == 7).ToList())
            {
                DeleteUser(user);
            }

            await WaitForUpdate();

            AssertCurrentDataEqual(ExpectedExceptAllByFirstName());
        }

        private class FirstNameRow
        {
            public string? FirstName { get; set; }
        }

        private List<FirstNameRow> ExpectedExceptAllByFirstName()
        {
            return Users
                .GroupBy(x => x.FirstName)
                .SelectMany(g => Enumerable.Repeat(
                    new FirstNameRow() { FirstName = g.Key },
                    g.Count() - g.Count(u => u.UserKey < 500)))
                .ToList();
        }

        /// <summary>
        /// Values disappear from one input over many leaves.
        /// </summary>
        [Fact]
        public async Task TestIntersectDistinctMultipleLeavesWithDeletes()
        {
            SetPageSizeBytes(256);
            for (int i = 0; i < 600; i++)
            {
                AddUser(new Entities.User { UserKey = i, FirstName = "n" + (i % 200) });
            }

            await StartStream(@"
            INSERT INTO output
            SELECT FirstName FROM users
            INTERSECT DISTINCT
            SELECT FirstName FROM users WHERE UserKey % 2 = 0");

            await WaitForUpdate();

            AssertCurrentDataEqual(ExpectedIntersectByFirstName());

            // These users share parity, removes the value completely
            foreach (var user in Users.Where(x => x.UserKey % 2 == 0 && (x.UserKey % 200) % 4 == 0).ToList())
            {
                DeleteUser(user);
            }

            await WaitForUpdate();

            AssertCurrentDataEqual(ExpectedIntersectByFirstName());
        }

        private List<FirstNameRow> ExpectedIntersectByFirstName()
        {
            var right = Users.Where(x => x.UserKey % 2 == 0).Select(x => x.FirstName).ToHashSet();
            return Users
                .Select(x => x.FirstName)
                .Where(x => right.Contains(x))
                .Distinct()
                .Select(x => new FirstNameRow() { FirstName = x })
                .ToList();
        }

        /// <summary>
        /// Rename rotation deletes and adds a value in one batch.
        /// </summary>
        [Fact]
        public async Task TestUnionDistinctMultipleLeavesWithRenameRotation()
        {
            SetPageSizeBytes(256);
            const int userCount = 600;
            for (int i = 0; i < userCount; i++)
            {
                AddUser(new Entities.User { UserKey = i, FirstName = "n" + i });
            }

            await StartStream(@"
            INSERT INTO output
            SELECT FirstName FROM users WHERE UserKey < 500
            UNION DISTINCT
            SELECT FirstName FROM users WHERE UserKey >= 500");

            await WaitForUpdate();

            var expected = Users.Select(x => new { x.FirstName }).Distinct().ToList();
            AssertCurrentDataEqual(expected);

            var namesByUserKey = Users.OrderBy(x => x.UserKey).Select(x => x.FirstName).ToList();
            foreach (var user in Users.OrderBy(x => x.UserKey).ToList())
            {
                user.FirstName = namesByUserKey[(user.UserKey + 1) % userCount];
                AddOrUpdateUser(user);
            }

            await WaitForUpdate();

            AssertCurrentDataEqual(expected);
        }

        /// <summary>
        /// Partitioned distinct, equal rows must land in the same partition.
        /// </summary>
        [Theory]
        [InlineData(2)]
        [InlineData(4)]
        public async Task TestUnionDistinctParallel(int parallelization)
        {
            for (int i = 0; i < 1000; i++)
            {
                AddUser(new Entities.User { UserKey = i, FirstName = "n" + (i % 250) });
            }

            await StartStream(@"
            INSERT INTO output
            SELECT FirstName FROM users WHERE UserKey < 500
            UNION DISTINCT
            SELECT FirstName FROM users WHERE UserKey >= 500",
            planOptimizerSettings: new Core.Optimizer.PlanOptimizerSettings() { Parallelization = parallelization });

            void AssertExpected()
            {
                AssertCurrentDataEqual(Users.Select(x => new { x.FirstName }).Distinct().ToList());
            }

            await WaitForUpdate();
            AssertExpected();

            // Removes some values completely, others keep members
            foreach (var user in Users.Where(x => (x.UserKey % 250) < 20).ToList())
            {
                DeleteUser(user);
            }
            await WaitForUpdate();
            AssertExpected();
        }

        /// <summary>
        /// Both inputs are the same subplan and share a reference to it.
        /// </summary>
        [Theory]
        [InlineData(2)]
        [InlineData(4)]
        public async Task TestUnionDistinctParallelSharedInput(int parallelization)
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT UserKey FROM users
            UNION DISTINCT
            SELECT UserKey FROM users",
            planOptimizerSettings: new Core.Optimizer.PlanOptimizerSettings() { Parallelization = parallelization });

            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Select(u => new { u.UserKey }).ToList());
        }

        /// <summary>
        /// Both inputs must be scattered the same way for the rows to meet.
        /// </summary>
        [Theory]
        [InlineData(2)]
        [InlineData(4)]
        public async Task TestExceptAllParallel(int parallelization)
        {
            for (int i = 0; i < 1000; i++)
            {
                AddUser(new Entities.User { UserKey = i, FirstName = "n" + (i % 100) });
            }

            await StartStream(@"
            INSERT INTO output
            SELECT FirstName FROM users
            EXCEPT ALL
            SELECT FirstName FROM users WHERE UserKey < 500",
            planOptimizerSettings: new Core.Optimizer.PlanOptimizerSettings() { Parallelization = parallelization });

            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedExceptAllByFirstName());

            foreach (var user in Users.Where(x => x.UserKey % 10 == 7).ToList())
            {
                DeleteUser(user);
            }
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedExceptAllByFirstName());
        }

        [Theory]
        [InlineData(2)]
        [InlineData(4)]
        public async Task TestIntersectDistinctParallel(int parallelization)
        {
            for (int i = 0; i < 600; i++)
            {
                AddUser(new Entities.User { UserKey = i, FirstName = "n" + (i % 200) });
            }

            await StartStream(@"
            INSERT INTO output
            SELECT FirstName FROM users
            INTERSECT DISTINCT
            SELECT FirstName FROM users WHERE UserKey % 2 = 0",
            planOptimizerSettings: new Core.Optimizer.PlanOptimizerSettings() { Parallelization = parallelization });

            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedIntersectByFirstName());

            foreach (var user in Users.Where(x => x.UserKey % 2 == 0 && (x.UserKey % 200) % 4 == 0).ToList())
            {
                DeleteUser(user);
            }
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedIntersectByFirstName());
        }

        /// <summary>
        /// A group by without measures becomes a distinct, it must still partition.
        /// </summary>
        [Theory]
        [InlineData(2)]
        [InlineData(4)]
        public async Task TestGroupByWithoutMeasuresParallel(int parallelization)
        {
            for (int i = 0; i < 1000; i++)
            {
                AddUser(new Entities.User
                {
                    UserKey = i,
                    CompanyId = "co_" + (i % 10),
                    Visits = i % 5 == 0 ? null : i % 4
                });
            }

            await StartStream(@"
            INSERT INTO output
            SELECT companyId, visits
            FROM users
            GROUP BY companyId, visits",
            planOptimizerSettings: new Core.Optimizer.PlanOptimizerSettings() { Parallelization = parallelization });

            void AssertExpected()
            {
                AssertCurrentDataEqual(Users.Select(x => new { x.CompanyId, x.Visits }).Distinct().ToList());
            }

            await WaitForUpdate();
            AssertExpected();

            foreach (var user in Users.Where(x => x.Visits == 3).ToList())
            {
                DeleteUser(user);
            }
            await WaitForUpdate();
            AssertExpected();
        }
    }
}
