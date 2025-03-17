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

using Bogus;
using FlowtideDotNet.AcceptanceTests.Entities;
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests
{
    [Collection("Acceptance tests")]
    public class JoinTests : FlowtideAcceptanceBase
    {
        public JoinTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        [Fact]
        public async Task InnerJoinMergeJoin()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    o.orderkey, firstName, lastName
                FROM orders o
                INNER JOIN users u
                ON o.userkey = u.userkey");
            await WaitForUpdate();

            AssertCurrentDataEqual(Orders.Join(Users, x => x.UserKey, x => x.UserKey, (l, r) => new { l.OrderKey, r.FirstName, r.LastName }));
        }

        /// <summary>
        /// Check that Kleene logic applies to merge joins
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task InnerJoinMergeJoinNullConditionEqual()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    u.userkey, c.name
                FROM users u
                INNER JOIN companies c
                ON u.companyid = c.companyid");
            await WaitForUpdate();

            AssertCurrentDataEqual(Users.Join(Companies, x => x.CompanyId, x => x.CompanyId, (l, r) => new { l.UserKey, r.Name }).ToList());
        }

        [Fact]
        public async Task LeftJoinMergeJoin()
        {
            GenerateData(1000);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    o.orderkey, u.firstName, u.LastName
                FROM orders o
                LEFT JOIN users u
                ON o.userkey = u.userkey");
            await WaitForUpdate();

            AssertCurrentDataEqual(
                from order in Orders
                join user in Users on order.UserKey equals user.UserKey into gj
                from subuser in gj.DefaultIfEmpty()
                select new
                {
                    order.OrderKey,
                    subuser.FirstName,
                    subuser.LastName
                });
        }

        [Fact]
        public async Task LeftJoinMergeJoinWithUpdate()
        {
            GenerateData(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    o.orderkey, u.firstName, u.LastName
                FROM orders o
                LEFT JOIN users u
                ON o.userkey = u.userkey");
            await WaitForUpdate();

            AssertCurrentDataEqual(
                from order in Orders
                join user in Users on order.UserKey equals user.UserKey into gj
                from subuser in gj.DefaultIfEmpty()
                select new
                {
                    order.OrderKey,
                    subuser.FirstName,
                    subuser.LastName
                });

            var uKey = Orders[0].UserKey;
            var firstUser = Users.First(x => x.UserKey == uKey);
            DeleteUser(firstUser);

            await WaitForUpdate();

            AssertCurrentDataEqual(
                from order in Orders
                join user in Users on order.UserKey equals user.UserKey into gj
                from subuser in gj.DefaultIfEmpty()
                select new
                {
                    order.OrderKey,
                    subuser?.FirstName,
                    subuser?.LastName
                });

            // Create a crash to check that the update is persisted
            await Crash();

            AddOrUpdateUser(firstUser);

            await WaitForUpdate();

            AssertCurrentDataEqual(
                from order in Orders
                join user in Users on order.UserKey equals user.UserKey into gj
                from subuser in gj.DefaultIfEmpty()
                select new
                {
                    order.OrderKey,
                    subuser?.FirstName,
                    subuser?.LastName
                });
        }

        [Fact]
        public async Task LeftJoinMergeJoinNullCondition()
        {
            GenerateData(1000);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    u.userkey, c.name
                FROM users u
                LEFT JOIN companies c
                ON u.companyid = c.companyid", pageSize: 64);
            await WaitForUpdate();

            AssertCurrentDataEqual(
                from user in Users
                join company in Companies on user.CompanyId equals company.CompanyId into gj
                from subcompany in gj.DefaultIfEmpty()
                select new
                {
                    user.UserKey,
                    companyName = subcompany?.Name ?? default(string)
                });
        }

        [Fact]
        public async Task LeftJoinUsersAddedBeforeCompanies()
        {
            for (int i = 0; i < 10_000; i++)
            {
                AddOrUpdateUser(new Entities.User()
                {
                    UserKey = i,
                    CompanyId = (i % 10).ToString()
                });
            }

            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    u.userkey, c.name
                FROM users u
                LEFT JOIN companies c
                ON u.companyid = c.companyid", pageSize: 64);
            await WaitForUpdate();

            for (int i = 0; i < 10; i++)
            {
                AddOrUpdateCompany(new Entities.Company()
                {
                    CompanyId = i.ToString(),
                    Name = $"Company {i}"
                });
            }

            await WaitForUpdate();

            AssertCurrentDataEqual(
                from user in Users
                join company in Companies on user.CompanyId equals company.CompanyId into gj
                from subcompany in gj.DefaultIfEmpty()
                select new
                {
                    user.UserKey,
                    companyName = subcompany?.Name ?? default(string)
                });
        }

        [Fact]
        public async Task InnerJoinConditionAlwaysTrue()
        {
            GenerateData(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    o.orderkey, u.firstName, u.LastName
                FROM orders o
                INNER JOIN users u
                ON 1 = 1");
            await WaitForUpdate();

            AssertCurrentDataEqual(
                from order in Orders
                join user in Users on 1 equals 1
                select new
                {
                    order.OrderKey,
                    user.FirstName,
                    user.LastName
                });
        }

        /// <summary>
        /// Uses modulo operation between both datasets to force a block nested loop join
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task InnerJoinBlockLoopModulus()
        {
            GenerateData(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    o.orderkey, u.firstName, u.LastName
                FROM orders o
                INNER JOIN users u
                ON o.userkey % u.userkey = 0");
            await WaitForUpdate();
            var result = from o in Orders
                         from u in Users
                         where o.UserKey % u.UserKey == 0
                         select new
                         {
                             o.OrderKey,
                             u.FirstName,
                             u.LastName
                         };

            AssertCurrentDataEqual(result);

            GenerateData(100);
            await WaitForUpdate();
            result = from o in Orders
                     from u in Users
                     where o.UserKey % u.UserKey == 0
                     select new
                     {
                         o.OrderKey,
                         u.FirstName,
                         u.LastName
                     };

            AssertCurrentDataEqual(result);
        }

        [Fact]
        public async Task InnerJoinBlockLoopModulusUsersFromLeft()
        {
            GenerateCompanies(10);

            GenerateUsers(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    o.orderkey, u.firstName, u.LastName
                FROM users u
                INNER JOIN orders o
                ON o.userkey % u.userkey = 0");
            await WaitForUpdate();

            GenerateOrders(100);

            await WaitForUpdate();

            var result = from o in Orders
                         from u in Users
                         where o.UserKey % u.UserKey == 0
                         select new
                         {
                             o.OrderKey,
                             u.FirstName,
                             u.LastName
                         };

            AssertCurrentDataEqual(result);

            GenerateData(100);
            await WaitForUpdate();
            result = from o in Orders
                     from u in Users
                     where o.UserKey % u.UserKey == 0
                     select new
                     {
                         o.OrderKey,
                         u.FirstName,
                         u.LastName
                     };

            AssertCurrentDataEqual(result);
        }

        [Fact]
        public async Task InnerJoinBlockLoopModulusUsersJoinFromRight()
        {
            GenerateCompanies(10);

            GenerateUsers(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    o.orderkey, u.firstName, u.LastName
                FROM orders o
                INNER JOIN users u
                ON o.userkey % u.userkey = 0");
            await WaitForUpdate();

            GenerateOrders(100);
            GenerateProjects(100);
            GenerateProjectMembers(100);

            await WaitForUpdate();

            var result = from o in Orders
                         from u in Users
                         where o.UserKey % u.UserKey == 0
                         select new
                         {
                             o.OrderKey,
                             u.FirstName,
                             u.LastName
                         };

            AssertCurrentDataEqual(result);

            Randomizer.Seed = new Random(8675309);
            GenerateCompanies(10);
            GenerateUsers(100);
            await WaitForUpdate();
            GenerateOrders(100);
            await WaitForUpdate();
            result = from o in Orders
                     from u in Users
                     where o.UserKey % u.UserKey == 0
                     select new
                     {
                         o.OrderKey,
                         u.FirstName,
                         u.LastName
                     };

            AssertCurrentDataEqual(result);
        }


        private record LeftJoinBlockLoopModulusResult(int? orderkey, string? firstname, string? lastname);

        /// <summary>
        /// Uses modulo operation between both datasets to force a block nested loop join
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task LeftJoinBlockLoopModulus()
        {
            GenerateData(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    o.orderkey, u.firstName, u.LastName
                FROM orders o
                LEFT JOIN users u
                ON o.userkey % u.userkey = 0 AND u.userkey % 2 = 0");
            await WaitForUpdate();

            List<LeftJoinBlockLoopModulusResult> expected = new List<LeftJoinBlockLoopModulusResult>();

            foreach (var order in Orders)
            {
                bool joinFound = false;
                foreach (var user in Users)
                {
                    if (order.UserKey % user.UserKey == 0 && user.UserKey % 2 == 0)
                    {
                        joinFound = true;
                        expected.Add(new LeftJoinBlockLoopModulusResult(order.OrderKey, user.FirstName, user.LastName));
                    }
                }
                if (!joinFound)
                {
                    expected.Add(new LeftJoinBlockLoopModulusResult(order.OrderKey, null, null));
                }
            }

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task LeftJoinBlockLoopModulusUsersFirst()
        {
            GenerateCompanies(10);
            GenerateUsers(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    o.orderkey, u.firstName, u.LastName
                FROM users u
                LEFT JOIN orders o
                ON o.userkey % u.userkey = 0 AND u.userkey % 2 = 0");
            await WaitForUpdate();

            List<LeftJoinBlockLoopModulusResult> expected = new List<LeftJoinBlockLoopModulusResult>();

            foreach (var user in Users)
            {
                bool joinFound = false;
                foreach (var order in Orders)
                {
                    if (order.UserKey % user.UserKey == 0 && user.UserKey % 2 == 0)
                    {
                        joinFound = true;
                        expected.Add(new LeftJoinBlockLoopModulusResult(order.OrderKey, user.FirstName, user.LastName));
                    }
                }
                if (!joinFound)
                {
                    expected.Add(new LeftJoinBlockLoopModulusResult(null, user.FirstName, user.LastName));
                }
            }

            AssertCurrentDataEqual(expected);

            GenerateOrders(100);

            await WaitForUpdate();

            expected = new List<LeftJoinBlockLoopModulusResult>();

            foreach (var user in Users)
            {
                bool joinFound = false;
                foreach (var order in Orders)
                {
                    if (order.UserKey % user.UserKey == 0 && user.UserKey % 2 == 0)
                    {
                        joinFound = true;
                        expected.Add(new LeftJoinBlockLoopModulusResult(order.OrderKey, user.FirstName, user.LastName));
                    }
                }
                if (!joinFound)
                {
                    expected.Add(new LeftJoinBlockLoopModulusResult(null, user.FirstName, user.LastName));
                }
            }

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task LeftJoinBlockLoopModulusDeleteFirstTwoUsers()
        {
            GenerateCompanies(10);
            GenerateData(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    o.orderkey, u.firstName, u.LastName
                FROM users u
                LEFT JOIN orders o
                ON o.userkey % u.userkey = 0 AND u.userkey % 2 = 0");
            await WaitForUpdate();

            List<LeftJoinBlockLoopModulusResult> expected = new List<LeftJoinBlockLoopModulusResult>();

            foreach (var user in Users)
            {
                bool joinFound = false;
                foreach (var order in Orders)
                {
                    if (order.UserKey % user.UserKey == 0 && user.UserKey % 2 == 0)
                    {
                        joinFound = true;
                        expected.Add(new LeftJoinBlockLoopModulusResult(order.OrderKey, user.FirstName, user.LastName));
                    }
                }
                if (!joinFound)
                {
                    expected.Add(new LeftJoinBlockLoopModulusResult(null, user.FirstName, user.LastName));
                }
            }

            AssertCurrentDataEqual(expected);

            var firstUser = Users.First();
            var secondUser = Users.Skip(1).First();
            DeleteUser(firstUser);
            DeleteUser(secondUser);

            await WaitForUpdate();

            expected = new List<LeftJoinBlockLoopModulusResult>();

            foreach (var user in Users)
            {
                bool joinFound = false;
                foreach (var order in Orders)
                {
                    if (order.UserKey % user.UserKey == 0 && user.UserKey % 2 == 0)
                    {
                        joinFound = true;
                        expected.Add(new LeftJoinBlockLoopModulusResult(order.OrderKey, user.FirstName, user.LastName));
                    }
                }
                if (!joinFound)
                {
                    expected.Add(new LeftJoinBlockLoopModulusResult(null, user.FirstName, user.LastName));
                }
            }

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task LeftJoinBlockLoopModulusDeleteAllOrders()
        {
            GenerateCompanies(10);
            GenerateData(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    o.orderkey, u.firstName, u.LastName
                FROM users u
                LEFT JOIN orders o
                ON o.userkey % u.userkey = 0 AND u.userkey % 2 = 0");
            await WaitForUpdate();

            List<LeftJoinBlockLoopModulusResult> expected = new List<LeftJoinBlockLoopModulusResult>();

            foreach (var user in Users)
            {
                bool joinFound = false;
                foreach (var order in Orders)
                {
                    if (order.UserKey % user.UserKey == 0 && user.UserKey % 2 == 0)
                    {
                        joinFound = true;
                        expected.Add(new LeftJoinBlockLoopModulusResult(order.OrderKey, user.FirstName, user.LastName));
                    }
                }
                if (!joinFound)
                {
                    expected.Add(new LeftJoinBlockLoopModulusResult(null, user.FirstName, user.LastName));
                }
            }

            AssertCurrentDataEqual(expected);

            var orderList = Orders.ToList();
            foreach (var order in orderList)
            {
                DeleteOrder(order);
            }

            await WaitForUpdate();

            expected = new List<LeftJoinBlockLoopModulusResult>();

            foreach (var user in Users)
            {
                bool joinFound = false;
                foreach (var order in Orders)
                {
                    if (order.UserKey % user.UserKey == 0 && user.UserKey % 2 == 0)
                    {
                        joinFound = true;
                        expected.Add(new LeftJoinBlockLoopModulusResult(order.OrderKey, user.FirstName, user.LastName));
                    }
                }
                if (!joinFound)
                {
                    expected.Add(new LeftJoinBlockLoopModulusResult(null, user.FirstName, user.LastName));
                }
            }

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task RightJoinBlockLoopModulusDeleteFirstTwo()
        {
            GenerateData(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    o.orderkey, u.firstName, u.LastName
                FROM orders o
                RIGHT JOIN users u
                ON o.userkey % u.userkey = 0 AND u.userkey % 2 = 0");
            await WaitForUpdate();

            List<LeftJoinBlockLoopModulusResult> expected = new List<LeftJoinBlockLoopModulusResult>();

            foreach (var user in Users)
            {
                bool joinFound = false;
                foreach (var order in Orders)
                {
                    if (order.UserKey % user.UserKey == 0 && user.UserKey % 2 == 0)
                    {
                        joinFound = true;
                        expected.Add(new LeftJoinBlockLoopModulusResult(order.OrderKey, user.FirstName, user.LastName));
                    }
                }
                if (!joinFound)
                {
                    expected.Add(new LeftJoinBlockLoopModulusResult(null, user.FirstName, user.LastName));
                }
            }

            AssertCurrentDataEqual(expected);

            var firstUser = Users.First();
            var secondUser = Users.Skip(1).First();
            DeleteUser(firstUser);
            DeleteUser(secondUser);

            await WaitForUpdate();

            expected = new List<LeftJoinBlockLoopModulusResult>();

            foreach (var user in Users)
            {
                bool joinFound = false;
                foreach (var order in Orders)
                {
                    if (order.UserKey % user.UserKey == 0 && user.UserKey % 2 == 0)
                    {
                        joinFound = true;
                        expected.Add(new LeftJoinBlockLoopModulusResult(order.OrderKey, user.FirstName, user.LastName));
                    }
                }
                if (!joinFound)
                {
                    expected.Add(new LeftJoinBlockLoopModulusResult(null, user.FirstName, user.LastName));
                }
            }

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task RightJoinBlockLoopModulusUsersFirst()
        {
            GenerateCompanies(10);
            GenerateUsers(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    o.orderkey, u.firstName, u.LastName
                FROM orders o
                RIGHT JOIN users u
                ON o.userkey % u.userkey = 0 AND u.userkey % 2 = 0");
            await WaitForUpdate();

            List<LeftJoinBlockLoopModulusResult> expected = new List<LeftJoinBlockLoopModulusResult>();

            foreach (var user in Users)
            {
                bool joinFound = false;
                foreach (var order in Orders)
                {
                    if (order.UserKey % user.UserKey == 0 && user.UserKey % 2 == 0)
                    {
                        joinFound = true;
                        expected.Add(new LeftJoinBlockLoopModulusResult(order.OrderKey, user.FirstName, user.LastName));
                    }
                }
                if (!joinFound)
                {
                    expected.Add(new LeftJoinBlockLoopModulusResult(null, user.FirstName, user.LastName));
                }
            }

            AssertCurrentDataEqual(expected);

            GenerateOrders(100);

            await WaitForUpdate();

            expected = new List<LeftJoinBlockLoopModulusResult>();

            foreach (var user in Users)
            {
                bool joinFound = false;
                foreach (var order in Orders)
                {
                    if (order.UserKey % user.UserKey == 0 && user.UserKey % 2 == 0)
                    {
                        joinFound = true;
                        expected.Add(new LeftJoinBlockLoopModulusResult(order.OrderKey, user.FirstName, user.LastName));
                    }
                }
                if (!joinFound)
                {
                    expected.Add(new LeftJoinBlockLoopModulusResult(null, user.FirstName, user.LastName));
                }
            }

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task RightJoinBlockLoopModulusDeleteAllOrders()
        {
            GenerateCompanies(10);
            GenerateData(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    o.orderkey, u.firstName, u.LastName
                FROM orders o
                RIGHT JOIN users u
                ON o.userkey % u.userkey = 0 AND u.userkey % 2 = 0");
            await WaitForUpdate();

            List<LeftJoinBlockLoopModulusResult> expected = new List<LeftJoinBlockLoopModulusResult>();

            foreach (var user in Users)
            {
                bool joinFound = false;
                foreach (var order in Orders)
                {
                    if (order.UserKey % user.UserKey == 0 && user.UserKey % 2 == 0)
                    {
                        joinFound = true;
                        expected.Add(new LeftJoinBlockLoopModulusResult(order.OrderKey, user.FirstName, user.LastName));
                    }
                }
                if (!joinFound)
                {
                    expected.Add(new LeftJoinBlockLoopModulusResult(null, user.FirstName, user.LastName));
                }
            }

            AssertCurrentDataEqual(expected);

            var orderList = Orders.ToList();

            foreach (var order in orderList)
            {
                DeleteOrder(order);
            }

            await WaitForUpdate();

            expected = new List<LeftJoinBlockLoopModulusResult>();

            foreach (var user in Users)
            {
                bool joinFound = false;
                foreach (var order in Orders)
                {
                    if (order.UserKey % user.UserKey == 0 && user.UserKey % 2 == 0)
                    {
                        joinFound = true;
                        expected.Add(new LeftJoinBlockLoopModulusResult(order.OrderKey, user.FirstName, user.LastName));
                    }
                }
                if (!joinFound)
                {
                    expected.Add(new LeftJoinBlockLoopModulusResult(null, user.FirstName, user.LastName));
                }
            }

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task FullOuterJoinBlockLoopModulus()
        {
            GenerateCompanies(10);
            GenerateData(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    o.orderkey, u.firstName, u.LastName
                FROM orders o
                FULL OUTER JOIN users u
                ON o.userkey % u.userkey = 0 AND u.userkey % 2 = 0");
            await WaitForUpdate();

            bool[] joinFoundLeft = new bool[Orders.Count];
            bool[] joinFoundRight = new bool[Users.Count];
            List<LeftJoinBlockLoopModulusResult> expected = new List<LeftJoinBlockLoopModulusResult>();

            for (int orderIndex = 0; orderIndex < Orders.Count; orderIndex++)
            {
                var order = Orders[orderIndex];
                for (int userIndex = 0; userIndex < Users.Count; userIndex++)
                {
                    var user = Users[userIndex];
                    if (order.UserKey % user.UserKey == 0 && user.UserKey % 2 == 0)
                    {
                        joinFoundLeft[orderIndex] = true;
                        joinFoundRight[userIndex] = true;
                        expected.Add(new LeftJoinBlockLoopModulusResult(order.OrderKey, user.FirstName, user.LastName));
                    }
                }
            }

            for (int orderIndex = 0; orderIndex < Orders.Count; orderIndex++)
            {
                if (!joinFoundLeft[orderIndex])
                {
                    expected.Add(new LeftJoinBlockLoopModulusResult(Orders[orderIndex].OrderKey, null, null));
                }
            }

            for (int userIndex = 0; userIndex < Users.Count; userIndex++)
            {
                if (!joinFoundRight[userIndex])
                {
                    expected.Add(new LeftJoinBlockLoopModulusResult(null, Users[userIndex].FirstName, Users[userIndex].LastName));
                }
            }

            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task LargeInnerJoinMergeJoin()
        {
            GenerateData(100_000);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    o.orderkey, u.firstName, u.LastName
                FROM orders o
                INNER JOIN users u
                ON o.userkey = u.userkey");
            await WaitForUpdate();

            AssertCurrentDataEqual(Orders.Join(Users, x => x.UserKey, x => x.UserKey, (l, r) => new { l.OrderKey, r.FirstName, r.LastName }));
        }

        [Fact]
        public async Task MergeJoinWithCrash()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    o.orderkey, u.firstName, u.LastName
                FROM orders o
                INNER JOIN users u
                ON o.userkey = u.userkey");
            await WaitForUpdate();

            Assert.NotNull(LastWatermark);
            Assert.Equal(1000, LastWatermark.Watermarks["users"]);
            Assert.Equal(1000, LastWatermark.Watermarks["orders"]);
            await Crash();

            GenerateData();

            await WaitForUpdate();

            Assert.NotNull(LastWatermark);
            Assert.Equal(2000, LastWatermark.Watermarks["users"]);
            Assert.Equal(2000, LastWatermark.Watermarks["orders"]);

            AssertCurrentDataEqual(Orders.Join(Users, x => x.UserKey, x => x.UserKey, (l, r) => new { l.OrderKey, r.FirstName, r.LastName }));
        }

        [Fact]
        public async Task LeftJoinMergeJoinWithPushdown()
        {
            GenerateData(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    u.userkey, c.name
                FROM users u
                LEFT JOIN companies c
                ON trim(u.companyid) = trim(c.companyid)");
            await WaitForUpdate();

            AssertCurrentDataEqual(
                from user in Users
                join company in Companies on user.CompanyId equals company.CompanyId into gj
                from subcompany in gj.DefaultIfEmpty()
                select new
                {
                    user.UserKey,
                    companyName = subcompany?.Name ?? default(string)
                });
        }

        /// <summary>
        /// Special case for optimizer
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task JoinJoinWithPushdown()
        {
            GenerateData(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    u.userkey, c.name
                FROM users u
                LEFT JOIN companies c
                ON trim(u.companyid) = trim(c.companyid)
                LEFT JOIN companies c2
                ON u.companyid = c2.companyid");
            await WaitForUpdate();

            AssertCurrentDataEqual(
                from user in Users
                join company in Companies on user.CompanyId equals company.CompanyId into gj
                from subcompany in gj.DefaultIfEmpty()
                select new
                {
                    user.UserKey,
                    companyName = subcompany?.Name ?? default(string)
                });
        }

        [Fact]
        public async Task MergeJoinCrashOnEgress()
        {
            EgressCrashOnCheckpoint(3);
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    o.orderkey, u.firstName, u.LastName
                FROM orders o
                INNER JOIN users u
                ON o.userkey = u.userkey");
            await WaitForUpdate();

            AssertCurrentDataEqual(Orders.Join(Users, x => x.UserKey, x => x.UserKey, (l, r) => new { l.OrderKey, r.FirstName, r.LastName }));
        }

        record JoinWithNotResult(int orderkey, string? firstname, string? lastname);

        [Fact]
        public async Task JoinWithNotCauseNLJ()
        {
            GenerateData(10);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    o.orderkey, u.firstName, u.LastName
                FROM orders o
                INNER JOIN users u
                ON NOT o.userkey = u.userkey");
            await WaitForUpdate();

            List<JoinWithNotResult> expected = new List<JoinWithNotResult>();
            for (int i = 0; i < Orders.Count; i++)
            {
                for (int j = 0; j < Users.Count; j++)
                {
                    if (Orders[i].UserKey != Users[j].UserKey)
                    {
                        expected.Add(new JoinWithNotResult(Orders[i].OrderKey, Users[j].FirstName, Users[j].LastName));
                    }
                }
            }
            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task LeftJoinWithConditionOnlyLeft()
        {
            GenerateData(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    u.userkey, c.name
                FROM users u
                LEFT JOIN companies c
                ON u.companyid = '123123'");
            await WaitForUpdate();

            AssertCurrentDataEqual(
                from user in Users
                select new
                {
                    user.UserKey,
                    companyName = default(string)
                });
        }

        [Fact]
        public async Task LeftJoinWithConditionEqualsAndOnlyLeftCondition()
        {
            GenerateData(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    u.userkey, c.name
                FROM users u
                LEFT JOIN companies c
                ON u.companyid = '123123' AND u.companyid = c.companyid");
            await WaitForUpdate();

            AssertCurrentDataEqual(
                from user in Users
                select new
                {
                    user.UserKey,
                    companyName = default(string)
                });
        }

        [Fact]
        public async Task JoinWithSubProperty()
        {
            GenerateData(10000);
            await StartStream(@"
                CREATE VIEW test AS
                SELECT map('userkey', userkey) AS user 
                FROM orders;

                INSERT INTO output 
                SELECT
                    t.user.userkey
                FROM test t
                INNER JOIN users u ON t.user.userkey = u.userkey", pageSize: 1024);
            await WaitForUpdate();
            AssertCurrentDataEqual(Orders.Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task JoinWithMultipleComparisons()
        {
            GenerateData(1000);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    u.userkey, p.name
                FROM users u
                INNER JOIN projectmembers pm
                ON u.userkey = pm.userkey
                INNER JOIN projects p
                ON pm.projectnumber = p.projectnumber AND pm.companyid = p.companyid
                ", pageSize: 64);
            await WaitForUpdate();
            //

            var expected = from user in Users
                           join projectmember in ProjectMembers on user.UserKey equals projectmember.UserKey
                           join project in Projects on new { projectmember.ProjectNumber, projectmember.CompanyId } equals new { project.ProjectNumber, project.CompanyId }
                           select new { user.UserKey, project.Name };

            var expectedList = expected.ToList();

            AssertCurrentDataEqual(expectedList);
        }

        [Fact]
        public async Task LeftJoinWithMultipleComparisons()
        {
            GenerateData(10);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    u.userkey, p.name
                FROM users u
                LEFT JOIN projectmembers pm
                ON u.userkey = pm.userkey
                LEFT JOIN projects p
                ON pm.projectnumber = p.projectnumber AND pm.companyid = p.companyid
                ", pageSize: 8);
            await WaitForUpdate();
            //

            var expected = from user in Users
                           join projectmember in ProjectMembers on user.UserKey equals projectmember.UserKey into gj
                           from subprojectmember in gj.DefaultIfEmpty()
                           join project in Projects on new { subprojectmember?.ProjectNumber, subprojectmember?.CompanyId } equals new { project.ProjectNumber, project.CompanyId } into gj2
                           from subproject in gj2.DefaultIfEmpty()
                           select new { user.UserKey, subproject?.Name };

            var expectedList = expected.ToList();

            AssertCurrentDataEqual(expectedList);
        }

        [Fact]
        public async Task LeftJoinWithMultipleComparisonsProjectUserMembersOrder()
        {
            GenerateCompanies(10);
            GenerateProjects(1000);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    u.userkey, p.name
                FROM users u
                LEFT JOIN projectmembers pm
                ON u.userkey = pm.userkey
                LEFT JOIN projects p
                ON pm.projectnumber = p.projectnumber AND pm.companyid = p.companyid
                ", pageSize: 8);
            await WaitForUpdate();

            Assert.NotNull(LastWatermark);
            Assert.Equal(1000, LastWatermark.Watermarks["projects"]);
            Assert.Equal(-1, LastWatermark.Watermarks["users"]);
            Assert.Equal(-1, LastWatermark.Watermarks["projectmembers"]);

            GenerateUsers(1000);
            await WaitForUpdate();

            Assert.NotNull(LastWatermark);
            Assert.Equal(1000, LastWatermark.Watermarks["projects"]);
            Assert.Equal(1000, LastWatermark.Watermarks["users"]);
            Assert.Equal(-1, LastWatermark.Watermarks["projectmembers"]);

            GenerateProjectMembers(1000);
            await WaitForUpdate();

            Assert.NotNull(LastWatermark);
            Assert.Equal(1000, LastWatermark.Watermarks["projects"]);
            Assert.Equal(1000, LastWatermark.Watermarks["users"]);
            Assert.Equal(1000, LastWatermark.Watermarks["projectmembers"]);

            var expected = from user in Users
                           join projectmember in ProjectMembers on user.UserKey equals projectmember.UserKey into gj
                           from subprojectmember in gj.DefaultIfEmpty()
                           join project in Projects on new { subprojectmember?.ProjectNumber, subprojectmember?.CompanyId } equals new { project.ProjectNumber, project.CompanyId } into gj2
                           from subproject in gj2.DefaultIfEmpty()
                           select new { user.UserKey, subproject?.Name };

            var expectedList = expected.ToList();

            AssertCurrentDataEqual(expectedList);
        }

        [Fact]
        public async Task TestJoinUpdateValueOnPageBorder()
        {
            GenerateCompanies(10);
            GenerateUsers(1000);

            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    u.userkey, u.firstName, o.orderkey
                FROM users u
                LEFT JOIN orders o
                ON u.userkey = o.userkey
                ", pageSize: 512);

            await WaitForUpdate();

            var firstUser = Users[0];
            // Get the user that will be placed at the right side border of a page.
            var keyToFind = firstUser.UserKey + 511;
            var userObj = Users.First(x => x.UserKey == keyToFind);

            // Force so the update of a new object is added after the current object.
            userObj.FirstName = "Zzzzz";
            AddOrUpdateUser(userObj);

            await WaitForUpdate();

            GenerateOrders(1000);

            await WaitForUpdate();

            AssertCurrentDataEqual(
                from user in Users
                join order in Orders on user.UserKey equals order.UserKey into gj
                from suborder in gj.DefaultIfEmpty()
                select new
                {
                    user.UserKey,
                    user.FirstName,
                    suborder?.OrderKey
                });
        }

        [Fact]
        public async Task RightJoinMergeJoin()
        {
            GenerateData(1000);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    o.orderkey, u.firstName, u.LastName
                FROM orders o
                RIGHT JOIN users u
                ON o.userkey = u.userkey");
            await WaitForUpdate();

            AssertCurrentDataEqual(
                from user in Users
                join order in Orders on user.UserKey equals order.UserKey into gj
                from suborder in gj.DefaultIfEmpty()
                select new
                {
                    suborder?.OrderKey,
                    user.FirstName,
                    user.LastName
                });

            GenerateData();

            await WaitForUpdate();

            AssertCurrentDataEqual(
                from user in Users
                join order in Orders on user.UserKey equals order.UserKey into gj
                from suborder in gj.DefaultIfEmpty()
                select new
                {
                    suborder?.OrderKey,
                    user.FirstName,
                    user.LastName
                });
        }

        [Fact]
        public async Task RightJoinMergeJoinFromOrdersUsersFirst()
        {
            GenerateCompanies(10);
            GenerateUsers(1000);
            //GenerateData(1000);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    o.orderkey, u.firstName, u.LastName
                FROM orders o
                RIGHT JOIN users u
                ON o.userkey = u.userkey");
            await WaitForUpdate();

            AssertCurrentDataEqual(
                from user in Users
                join order in Orders on user.UserKey equals order.UserKey into gj
                from suborder in gj.DefaultIfEmpty()
                select new
                {
                    suborder?.OrderKey,
                    user.FirstName,
                    user.LastName
                });

            GenerateOrders(1000);

            await WaitForUpdate();

            AssertCurrentDataEqual(
                from user in Users
                join order in Orders on user.UserKey equals order.UserKey into gj
                from suborder in gj.DefaultIfEmpty()
                select new
                {
                    suborder?.OrderKey,
                    user.FirstName,
                    user.LastName
                });
        }

        [Fact]
        public async Task RightJoinMergeJoinFromUsersUsersFirst()
        {
            GenerateCompanies(10);
            GenerateUsers(1000);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    o.orderkey, u.firstName, u.LastName
                FROM users u
                RIGHT JOIN orders o
                ON o.userkey = u.userkey");
            await WaitForUpdate();

            AssertCurrentDataEqual(
                from order in Orders
                join user in Users on order.UserKey equals user.UserKey into gj
                from subuser in gj.DefaultIfEmpty()
                select new
                {
                    order?.OrderKey,
                    subuser.FirstName,
                    subuser.LastName
                });

            GenerateOrders(1000);

            await WaitForUpdate();

            AssertCurrentDataEqual(
                from order in Orders
                join user in Users on order.UserKey equals user.UserKey into gj
                from subuser in gj.DefaultIfEmpty()
                select new
                {
                    order?.OrderKey,
                    subuser.FirstName,
                    subuser.LastName
                });
        }


        [Fact]
        public async Task FullOuterJoinMergeJoin()
        {
            GenerateData(1000);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    o.orderkey, u.userKey, u.firstName, u.LastName
                FROM orders o
                FULL OUTER JOIN users u
                ON o.userkey = u.userkey");
            await WaitForUpdate();

            var rightJoin = (from user in Users
                             join order in Orders on user.UserKey equals order.UserKey into gj
                             from suborder in gj.DefaultIfEmpty()
                             select new
                             {
                                 suborder?.OrderKey,
                                 user.UserKey,
                                 user.FirstName,
                                 user.LastName
                             });

            var leftJoin = (
                from order in Orders
                join user in Users on order.UserKey equals user.UserKey into gj
                from subuser in gj.DefaultIfEmpty()
                select new
                {
                    order?.OrderKey,
                    subuser.UserKey,
                    subuser.FirstName,
                    subuser.LastName
                });

            var fullOuterJoin = leftJoin.Union(rightJoin).ToList();

            AssertCurrentDataEqual(fullOuterJoin);

            GenerateData();

            await WaitForUpdate();

            rightJoin = (from user in Users
                         join order in Orders on user.UserKey equals order.UserKey into gj
                         from suborder in gj.DefaultIfEmpty()
                         select new
                         {
                             suborder?.OrderKey,
                             user.UserKey,
                             user.FirstName,
                             user.LastName
                         });

            leftJoin = (
                from order in Orders
                join user in Users on order.UserKey equals user.UserKey into gj
                from subuser in gj.DefaultIfEmpty()
                select new
                {
                    order?.OrderKey,
                    subuser.UserKey,
                    subuser.FirstName,
                    subuser.LastName
                });

            fullOuterJoin = leftJoin.Union(rightJoin).ToList();

            AssertCurrentDataEqual(fullOuterJoin);
        }

        [Fact]
        public async Task PostJoinConditionShouldCheckAllRows()
        {
            // Add members first to make sure all the data is inside the tree
            AddOrUpdateProjectMember(new ProjectMember()
            {
                CompanyId = "1",
                ProjectNumber = "123",
                UserKey = 0,
                ProjectMemberKey = 1
            });
            AddOrUpdateProjectMember(new ProjectMember()
            {
                CompanyId = "1",
                ProjectNumber = "123",
                UserKey = 1,
                ProjectMemberKey = 2
            });
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    pm.userkey
                FROM projects p
                LEFT JOIN projectmembers pm
                ON p.projectnumber = pm.projectnumber AND p.companyid = pm.companyid AND pm.userkey % ProjectKey = 1", ignoreSameDataCheck: false);
            await WaitForUpdate();
            // add the project to do the join
            AddOrUpdateProject(new Project()
            {
                ProjectKey = 2,
                CompanyId = "1",
                ProjectNumber = "123",
                Name = "Project 1"
            });
            await WaitForUpdate();

            var actualData = GetActualRows();
            AssertCurrentDataEqual(new[] { new { val = 1 } });
        }
    }
}
