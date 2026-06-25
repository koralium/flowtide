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
    public class AggregateTests : FlowtideAcceptanceBase
    {
        public AggregateTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        [Fact]
        public async Task AggregateCount()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    count(*)
                FROM orders o");
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] { new { Count = Orders.Count() } });
        }

        [Fact]
        public async Task AggregateSum()
        {
            GenerateData(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    companyId, sum(userkey)
                FROM users o
                GROUP BY companyId");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.GroupBy(x => x.CompanyId).OrderBy(x => x.Key).Select(x => new { Key = x.Key, Sum = x.Sum(y => y.UserKey) }));

            //var firstOrder = Orders[0];
            //var toRemove = Orders.Where(x => x.UserKey == firstOrder.UserKey).ToList();
            //EnterDataWriteLock();
            //foreach(var r in toRemove)
            //{
            //    DeleteOrder(r);
            //}
            //ExitDataWriteLock();

            //await WaitForUpdate();
            //AssertCurrentDataEqual(Orders.GroupBy(x => x.UserKey).OrderBy(x => x.Key).Select(x => new { Key = x.Key, Sum = x.Sum(y => y.OrderKey) }));
        }

        [Fact]
        public async Task BulkAggregateMin()
        {
            GenerateData(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    companyId, min(userkey)
                FROM users o
                GROUP BY companyId");
            await WaitForUpdate();
            var expected = Users.GroupBy(x => x.CompanyId).OrderBy(x => x.Key).Select(x => new { Key = x.Key, Min = x.Min(y => y.UserKey) });
            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task BulkAggregateMax()
        {
            GenerateData(10_000);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    companyId, max(userkey)
                FROM users o
                GROUP BY companyId");
            await WaitForUpdate();
            var expected = Users.GroupBy(x => x.CompanyId).OrderBy(x => x.Key).Select(x => new { Key = x.Key, Max = x.Max(y => y.UserKey) });
            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task BulkAggregateAvg()
        {
            GenerateData(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    companyId, avg(userkey)
                FROM users o
                GROUP BY companyId");
            await WaitForUpdate();
            var expected = Users.GroupBy(x => x.CompanyId).OrderBy(x => x.Key).Select(x => new { Key = x.Key, Avg = (double)x.Average(y => y.UserKey) });
            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task BulkAggregateAvgWithUpdatesAndDeletes()
        {
            GenerateData(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    companyId, avg(userkey)
                FROM users o
                GROUP BY companyId");
            await WaitForUpdate();
            
            // Check initial state
            var expected1 = Users.GroupBy(x => x.CompanyId).OrderBy(x => x.Key).Select(x => new { Key = x.Key, Avg = (double)x.Average(y => y.UserKey) });
            AssertCurrentDataEqual(expected1);

            // Now, perform some updates and deletes that affect the avg aggregation
            var firstUser = Users[0];
            var companyId = firstUser.CompanyId;
            DeleteUser(firstUser);

            var newUser = new Entities.User { UserKey = 1000, CompanyId = companyId, FirstName = "New", LastName = "User" };
            AddOrUpdateUser(newUser);

            await WaitForUpdate();

            var expected2 = Users.GroupBy(x => x.CompanyId).OrderBy(x => x.Key).Select(x => new { Key = x.Key, Avg = (double)x.Average(y => y.UserKey) });
            AssertCurrentDataEqual(expected2);
        }

        [Fact]
        public async Task BulkAggregateMinWithUpdatesAndDeletes()
        {
            GenerateData(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    companyId, min(userkey)
                FROM users o
                GROUP BY companyId");
            await WaitForUpdate();
            
            // Check initial state
            var expected1 = Users.GroupBy(x => x.CompanyId).OrderBy(x => x.Key).Select(x => new { Key = x.Key, Min = x.Min(y => y.UserKey) });
            AssertCurrentDataEqual(expected1);

            // Now, perform some updates and deletes that affect the min aggregation
            // 1. Lower a user's key to make it a new min for their company
            var firstUser = Users[0];
            var companyId = firstUser.CompanyId;
            DeleteUser(firstUser);

            var newUser = new Entities.User { UserKey = -100, CompanyId = companyId, FirstName = "New", LastName = "User" };
            AddOrUpdateUser(newUser);

            // 2. Delete some users who currently hold the minimum value
            // Let's find some companies and their current min users
            var groupings = Users.GroupBy(x => x.CompanyId).ToList();
            foreach (var group in groupings.Take(3))
            {
                var minVal = group.Min(x => x.UserKey);
                var minUsers = group.Where(x => x.UserKey == minVal).ToList();
                foreach (var mu in minUsers)
                {
                    DeleteUser(mu);
                }
            }

            await WaitForUpdate();

            var expected2 = Users.GroupBy(x => x.CompanyId).OrderBy(x => x.Key).Select(x => new { Key = x.Key, Min = x.Min(y => y.UserKey) });
            AssertCurrentDataEqual(expected2);
        }

        [Fact]
        public async Task BulkAggregateMaxWithUpdatesAndDeletes()
        {
            GenerateData(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    companyId, max(userkey)
                FROM users o
                GROUP BY companyId");
            await WaitForUpdate();
            
            // Check initial state
            var expected1 = Users.GroupBy(x => x.CompanyId).OrderBy(x => x.Key).Select(x => new { Key = x.Key, Max = x.Max(y => y.UserKey) });
            AssertCurrentDataEqual(expected1);

            // 1. Add a user with a very high key to become the new max for their company
            var firstUser = Users[0];
            var companyId = firstUser.CompanyId;
            DeleteUser(firstUser);

            var newUser = new Entities.User { UserKey = 999999, CompanyId = companyId, FirstName = "New", LastName = "User" };
            AddOrUpdateUser(newUser);

            // 2. Delete users who currently hold the maximum value in their companies
            var groupings = Users.GroupBy(x => x.CompanyId).ToList();
            foreach (var group in groupings.Take(3))
            {
                var maxVal = group.Max(x => x.UserKey);
                var maxUsers = group.Where(x => x.UserKey == maxVal).ToList();
                foreach (var mu in maxUsers)
                {
                    DeleteUser(mu);
                }
            }

            await WaitForUpdate();

            var expected2 = Users.GroupBy(x => x.CompanyId).OrderBy(x => x.Key).Select(x => new { Key = x.Key, Max = x.Max(y => y.UserKey) });
            AssertCurrentDataEqual(expected2);
        }

        /// <summary>
        /// Reproduces an IndexOutOfRangeException in the bulk aggregate operator caused by a group that
        /// is created and then fully retracted (net weight zero) within a single watermark interval,
        /// before its value was ever emitted downstream.
        ///
        /// Root cause: when such a group's weight reaches zero its key is deleted from the persisted
        /// tree, but the entry that was queued for output in the temporary tree is NOT removed (the temp
        /// entry is only cleared when the previous value had already been sent, see BulkAggregateMutator
        /// / the temp update loop in OnRecieve). The temporary tree is left referencing a key that no
        /// longer exists in the persisted tree.
        ///
        /// This single defect surfaces in two places depending on batch layout / build configuration:
        ///   * If the batch carrying the delete has no group to output, the temporary tree receives a
        ///     zero-row ApplyBatch which routes to an empty leaf mapping and throws in
        ///     AggregateInsertComparer.FindBoundriesBulk (OnRecieve). This is what trips first here.
        ///   * Otherwise the dangling entry survives to the watermark, where the bulk search of the
        ///     persisted tree returns a negative (bitwise-complement) lower bound that is then used to
        ///     index PrimitiveList&lt;bool&gt; _previousValueSent via "previousValueSent[lower] = true"
        ///     (BulkAggregateOperator.OnWatermark -> PrimitiveList.set_Item). That is the reported
        ///     production stack (Release build; in Debug the preceding read trips the Debug.Assert in
        ///     PrimitiveList.Get instead).
        ///
        /// To hit it, the insert and the delete of the group must arrive in separate batches but inside
        /// the same watermark interval. The mock source flushes a new batch every time more than 100
        /// operations have accumulated, so the filler rows below push the insert and the delete into
        /// different batches that are still covered by a single watermark.
        /// </summary>
        [Fact]
        public async Task BulkAggregateNewGroupCreatedAndRemovedInSameWatermark()
        {
            // Baseline group so the first watermark emits initial data and the operator switches
            // into incremental (temporary tree) mode.
            AddUser(new Entities.User { UserKey = 1, CompanyId = "baseline" });

            await StartStream(@"
                INSERT INTO output
                SELECT
                    companyId, min(userkey)
                FROM users o
                GROUP BY companyId");
            await WaitForUpdate();
            AssertCurrentDataEqual(Users.GroupBy(x => x.CompanyId).OrderBy(x => x.Key).Select(x => new { Key = x.Key, Min = x.Min(y => y.UserKey) }));

            // Everything below happens within a single watermark interval (no WaitForUpdate in between),
            // so all operations are picked up by one source fetch and covered by one watermark.

            // New group "transient" created here ...
            var transientUser = new Entities.User { UserKey = 1_000_000, CompanyId = "transient" };
            AddUser(transientUser);

            // ... filler rows (each its own new group) to push past the 100 operation batch boundary,
            // so the delete below lands in a different batch than the insert above.
            for (int i = 0; i < 150; i++)
            {
                AddUser(new Entities.User { UserKey = 2_000_000 + i, CompanyId = "filler-" + i });
            }

            // ... and fully retracted before it was ever emitted. Net weight for "transient" is zero.
            DeleteUser(transientUser);

            // On the buggy operator this throws IndexOutOfRangeException inside the bulk aggregate
            // operator (see the summary above for the two surfacing points). On a correct operator the
            // transient group simply produces no output.
            await WaitForUpdate();

            // After the fix the transient group simply never appears and every other group is correct.
            var expected = Users.GroupBy(x => x.CompanyId).OrderBy(x => x.Key).Select(x => new { Key = x.Key, Min = x.Min(y => y.UserKey) });
            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task AggregateCountDistinct()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    count(DISTINCT UserKey)
                FROM orders o");
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] { new { Count = Orders.Select(x => x.UserKey).Distinct().Count() } });
        }

        [Fact]
        public async Task AggregateCountWithGroup()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    userkey, count(*)
                FROM orders
                GROUP BY userkey");
            await WaitForUpdate();

            AssertCurrentDataEqual(Orders.GroupBy(x => x.UserKey).Select(x => new { UserKey = x.Key, Count = x.Count() }));
        }

        /// <summary>
        /// Tests that checks that emit is working correctly from aggregate operator
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task AggregateWithGroupOnlyAggregate()
        {
            GenerateData(10000);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    '1' as c, count(*)
                FROM orders
                GROUP BY userkey");
            await WaitForUpdate();

            AssertCurrentDataEqual(Orders.GroupBy(x => x.UserKey).Select(x => new { c = "1", Count = x.Count() }));
        }

        [Fact]
        public async Task TestAggregateInt8ConvertsToInt16()
        {
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 0, UserKey = sbyte.MaxValue - 9 });
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = sbyte.MaxValue + 10 });

            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    '1' as c, count(*)
                FROM orders
                GROUP BY userkey");
            await WaitForUpdate();

            AssertCurrentDataEqual(Orders.GroupBy(x => x.UserKey).Select(x => new { c = "1", Count = x.Count() }));
        }

        [Fact]
        public async Task TestAggregateInt16ConvertsToInt32()
        {
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 0, UserKey = short.MaxValue - 9 });
            AddOrUpdateOrder(new Entities.Order() { OrderKey = 1, UserKey = short.MaxValue + 10 });


            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    '1' as c, count(*)
                FROM orders
                GROUP BY userkey");
            await WaitForUpdate();

            AssertCurrentDataEqual(Orders.GroupBy(x => x.UserKey).Select(x => new { c = "1", Count = x.Count() }));
        }

        [Fact]
        public async Task AggregateOnJoinedData()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    u.firstName, count(*)
                FROM orders o
                INNER JOIN users u
                ON o.userkey = u.userkey
                GROUP BY u.firstName");
            await WaitForUpdate();

            AssertCurrentDataEqual(Orders.Join(Users, x => x.UserKey, x => x.UserKey, (l, r) => new { l.OrderKey, r.FirstName }).GroupBy(x => x.FirstName).Select(x => new { FirstName = x.Key, Count = x.Count() }));
        }

        [Fact]
        public async Task MultipleAggregates()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    userkey, sum(orderkey), count(*)
                FROM orders
                GROUP BY userkey
                ");
            await WaitForUpdate();

            AssertCurrentDataEqual(Orders
                .GroupBy(x => x.UserKey)
                .Select(x => new { Userkey = x.Key, Sum = (long)x.Sum(y => y.OrderKey), Count = x.Count() }));
        }

        /// <summary>
        /// Test case to solve bug when using multiple aggregates and group by's
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task MultipleMaxAggregates()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    userkey, max(orderkey), max(orderkey)
                FROM orders
                GROUP BY userkey, orderkey
                ", ignoreSameDataCheck: true);
            await WaitForUpdate();

            for (int i = 0; i < 10; i++)
            {
                GenerateData();
                await WaitForUpdate();
            }

            AssertCurrentDataEqual(Orders
                .GroupBy(x => $"{x.UserKey}:{x.OrderKey}")
                .Select(x => new { Userkey = int.Parse(x.Key.Substring(0, x.Key.IndexOf(':'))), Max1 = (long)x.Max(y => y.OrderKey), Max2 = x.Max(y => y.OrderKey) }));
        }

        [Fact]
        public async Task HavingSameAggregate()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    userkey, count(*)
                FROM orders
                GROUP BY userkey
                HAVING count(*) > 1
                ");
            await WaitForUpdate();

            AssertCurrentDataEqual(Orders.GroupBy(x => x.UserKey).Select(x => new { FirstName = x.Key, Count = x.Count() }).Where(x => x.Count > 1));
        }

        [Fact]
        public async Task HavingDifferentAggregate()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    userkey, sum(orderkey)
                FROM orders
                GROUP BY userkey
                HAVING count(*) > 1
                ");
            await WaitForUpdate();

            AssertCurrentDataEqual(Orders
                .GroupBy(x => x.UserKey)
                .Select(x => new { Userkey = x.Key, Count = x.Count(), Sum = (long)x.Sum(y => y.OrderKey) })
                .Where(x => x.Count > 1)
                .Select(x => new { x.Userkey, x.Sum }));
        }

        [Fact]
        public async Task AggregateWithStateCrash()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    userkey, min(orderkey)
                FROM orders
                GROUP BY userkey
                ", ignoreSameDataCheck: true);
            await WaitForUpdate();

            await Crash();

            GenerateData(1000);

            await WaitForUpdate();



            AssertCurrentDataEqual(Orders.GroupBy(x => x.UserKey).Select(x => new { UserKey = x.Key, MinVal = x.Min(y => y.OrderKey) }));
        }

        [Fact]
        public async Task MaxAggregateWithStateCrash()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    userkey, max(orderkey)
                FROM orders
                GROUP BY userkey
                ", ignoreSameDataCheck: true);
            await WaitForUpdate();

            await Crash();

            GenerateData(1000);

            await WaitForUpdate();

            AssertCurrentDataEqual(Orders.GroupBy(x => x.UserKey).Select(x => new { UserKey = x.Key, MaxVal = x.Max(y => y.OrderKey) }));
        }

        [Fact]
        public async Task AvgAggregateWithStateCrash()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    userkey, avg(orderkey)
                FROM orders
                GROUP BY userkey
                ", ignoreSameDataCheck: true);
            await WaitForUpdate();

            await Crash();

            GenerateData(1000);

            await WaitForUpdate();

            AssertCurrentDataEqual(Orders.GroupBy(x => x.UserKey).Select(x => new { UserKey = x.Key, AvgVal = (double)x.Average(y => y.OrderKey) }));
        }

        //[Fact]
        //public async Task AggregateStopAndStartStream()
        //{
        //    GenerateData();
        //    await StartStream(@"
        //        INSERT INTO output 
        //        SELECT 
        //            userkey, min(orderkey)
        //        FROM orders
        //        GROUP BY userkey
        //        ");
        //    await WaitForUpdate();

        //    await this.StopStream();

        //    GenerateData(1000);

        //    await StartStream();

        //    await WaitForUpdate();

        //    AssertCurrentDataEqual(Orders.GroupBy(x => x.UserKey).Select(x => new { UserKey = x.Key, MinVal = x.Min(y => y.OrderKey) }));
        //}

        [Fact]
        public async Task AggregateWithGroupByOnly()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    userkey
                FROM orders
                GROUP BY userkey
                ");
            await WaitForUpdate();

            AssertCurrentDataEqual(Orders.GroupBy(x => x.UserKey).Select(x => new { UserKey = x.Key }));
        }

        [Fact]
        public async Task ListAggWithMapAndUpdates()
        {
            GenerateData(1000);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    list_agg(map('userkey', userkey, 'company', u.companyId))
                FROM users u
                ");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[] { new { list = Users.OrderBy(x => x.CompanyId).Select(x => new Dictionary<string, object>(){
                { "userkey", x.UserKey },
                { "company", x.CompanyId! }
            }).ToList() } });

            GenerateData(1000);

            await WaitForUpdate();

            AssertCurrentDataEqual(new[] { new { list = Users.OrderBy(x => x.CompanyId).ThenBy(x => x.UserKey).Select(x => new Dictionary<string, object> {
                { "userkey", x.UserKey },
                { "company", x.CompanyId! }
            }).ToList() } });

            Users[0].CompanyId = "newCompany";
            AddOrUpdateUser(Users[0]);

            await WaitForUpdate();

            AssertCurrentDataEqual(new[] { new { list = Users.OrderBy(x => x.CompanyId).ThenBy(x => x.UserKey).Select(x => new Dictionary<string, object> {
                { "userkey", x.UserKey },
                { "company", x.CompanyId! }
            }).ToList() } });

            DeleteUser(Users[10]);

            await WaitForUpdate();

            AssertCurrentDataEqual(new[] { new { list = Users.OrderBy(x => x.CompanyId).ThenBy(x => x.UserKey).Select(x => new Dictionary<string, object> {
                { "userkey", x.UserKey },
                { "company", x.CompanyId! }
            }).ToList() } });
        }

        [Fact]
        public async Task ListAggWithNamedStructAndUpdates()
        {
            GenerateData(1000);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    list_agg(named_struct('userkey', userkey, 'company', u.companyId))
                FROM users u
                ");
            await WaitForUpdate();
            
            AssertCurrentDataEqual(new[] { new { list = Users.OrderBy(x => x.UserKey).Select(x => new { userkey = x.UserKey, company = x.CompanyId }).ToList() } });

            GenerateData(1000);

            await WaitForUpdate();

            AssertCurrentDataEqual(new[] { new { list = Users.OrderBy(x => x.UserKey).Select(x => new { userkey = x.UserKey, company = x.CompanyId }).ToList() } });

            Users[0].CompanyId = "newCompany";
            AddOrUpdateUser(Users[0]);

            await WaitForUpdate();

            AssertCurrentDataEqual(new[] { new { list = Users.OrderBy(x => x.UserKey).Select(x => new { userkey = x.UserKey, company = x.CompanyId }).ToList() } });

            DeleteUser(Users[10]);

            await WaitForUpdate();

            AssertCurrentDataEqual(new[] { new { list = Users.OrderBy(x => x.UserKey).Select(x => new { userkey = x.UserKey, company = x.CompanyId }).ToList() } });
        }

        [Fact]
        public async Task TestSurrogateKeyInt64()
        {
            GenerateData();

            await StartStream(@"
            INSERT INTO output
            SELECT
                surrogate_key_int64() as key,
                userkey
            FROM users
            GROUP BY userkey
            ");

            await WaitForUpdate();

            long counter = 0;
            AssertCurrentDataEqual(Users.Select(x =>
            {
                return new
                {
                    key = counter++,
                    x.UserKey
                };
            }));
        }

        [Fact]
        public async Task MinAggregateWithFilterAndGroup()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    userKey, min(orderkey) FILTER (WHERE orderkey % 2 = 0)
                FROM orders
                GROUP BY userkey
                ", ignoreSameDataCheck: true);
            await WaitForUpdate();

            await Crash();

            GenerateData(1000);

            await WaitForUpdate();

            AssertCurrentDataEqual(Orders
                .GroupBy(x => x.UserKey)
                .Select(x =>
                {
                    var minSequence = x.Where(x => x.OrderKey % 2 == 0).ToList();
                    int? output = null;
                    if (minSequence.Count > 0)
                    {
                        output = minSequence.Min(y => y.OrderKey);
                    }
                    return new
                    {
                        UserKey = x.Key,
                        MinVal = output
                    };
                })
            );
        }

        [Fact]
        public async Task MinAggregateWithDifferentFilters()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    userKey, min(orderkey) FILTER (WHERE orderkey % 2 = 0), min(orderkey) FILTER (WHERE orderkey % 3 = 0)
                FROM orders
                GROUP BY userkey
                ", ignoreSameDataCheck: true);

            await WaitForUpdate();

            AssertCurrentDataEqual(Orders
                .GroupBy(x => x.UserKey)
                .Select(x =>
                {
                    var minSequence = x.Where(x => x.OrderKey % 2 == 0).ToList();
                    var minSequence2 = x.Where(x => x.OrderKey % 3 == 0).ToList();
                    int? output = null;
                    if (minSequence.Count > 0)
                    {
                        output = minSequence.Min(y => y.OrderKey);
                    }
                    int? output2 = null;
                    if (minSequence2.Count > 0)
                    {
                        output2 = minSequence2.Min(y => y.OrderKey);
                    }
                    return new
                    {
                        UserKey = x.Key,
                        MinVal = output,
                        MinVal2 = output2
                    };
                })
            );
        }

        [Fact]
        public async Task MinMaxAggregateWithSameFilters()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    userKey, min(orderkey) FILTER (WHERE orderkey % 2 = 0), max(orderkey) FILTER (WHERE orderkey % 2 = 0)
                FROM orders
                GROUP BY userkey
                ", ignoreSameDataCheck: true);

            await WaitForUpdate();

            AssertCurrentDataEqual(Orders
                .GroupBy(x => x.UserKey)
                .Select(x =>
                {
                    var minSequence = x.Where(x => x.OrderKey % 2 == 0).ToList();
                    var minSequence2 = x.Where(x => x.OrderKey % 2 == 0).ToList();
                    int? output = null;
                    if (minSequence.Count > 0)
                    {
                        output = minSequence.Min(y => y.OrderKey);
                    }
                    int? output2 = null;
                    if (minSequence2.Count > 0)
                    {
                        output2 = minSequence2.Max(y => y.OrderKey);
                    }
                    return new
                    {
                        UserKey = x.Key,
                        MinVal = output,
                        MinVal2 = output2
                    };
                })
            );
        }

        [Fact]
        public async Task MinByAggregate()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    userkey, min_by(Orderdate, orderkey)
                FROM orders
                GROUP BY userkey
                ", ignoreSameDataCheck: true);
            await WaitForUpdate();

            await Crash();

            GenerateData(1000);

            await WaitForUpdate();

            AssertCurrentDataEqual(Orders
                .GroupBy(x => x.UserKey)
                .Select(x =>
                {
                    var outputrow = x.Min(y => y.OrderKey);
                    var order = x.First(x => x.OrderKey == outputrow);
                    return new
                    {
                        UserKey = x.Key,
                        MinVal = order.Orderdate
                    };
                })
            );
        }

        [Fact]
        public async Task MaxByAggregate()
        {
            GenerateData();
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    userkey, max_by(Orderdate, orderkey)
                FROM orders
                GROUP BY userkey
                ");
            await WaitForUpdate();

            await Crash();

            GenerateData(1000);

            await WaitForUpdate();

            AssertCurrentDataEqual(Orders
                .GroupBy(x => x.UserKey)
                .Select(x =>
                {
                    var outputrow = x.Max(y => y.OrderKey);
                    var order = x.First(x => x.OrderKey == outputrow);
                    return new
                    {
                        UserKey = x.Key,
                        MaxVal = order.Orderdate
                    };
                })
            );
        }

        /// <summary>
        /// Tests MIN on a nullable column (Visits) which exercises the SharedGroupValueTree's
        /// _ignoreNulls path. When Visits is null for some rows, the shared tree must skip those
        /// rows during StoreAsync, creating a gap between the full batch length and the actualLength
        /// passed to ApplyBatch. This verifies that null-skipping does not corrupt the index mapping.
        /// </summary>
        [Fact]
        public async Task MinAggregateOnNullableColumnWithSharedTree()
        {
            GenerateData(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    companyId, min(visits)
                FROM users
                GROUP BY companyId
                ");
            await WaitForUpdate();

            var expected = Users
                .GroupBy(x => x.CompanyId)
                .OrderBy(x => x.Key)
                .Select(x => new
                {
                    Key = x.Key,
                    Min = x.Where(u => u.Visits.HasValue).Any()
                        ? (int?)x.Where(u => u.Visits.HasValue).Min(u => u.Visits!.Value)
                        : (int?)null
                });
            AssertCurrentDataEqual(expected);
        }

        /// <summary>
        /// Tests MIN and MAX on a nullable column with shared tree, then performs updates and deletes.
        /// The shared tree is shared between MIN and MAX (same value expression). Null values in Visits
        /// cause non-contiguous indices in the shared tree's StoreAsync. After mutations, the tree must
        /// correctly reflect the new min/max values.
        /// </summary>
        [Fact]
        public async Task MinMaxAggregateOnNullableColumnWithUpdatesAndDeletes()
        {
            GenerateData(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    companyId, min(visits), max(visits)
                FROM users
                GROUP BY companyId
                ", ignoreSameDataCheck: true);
            await WaitForUpdate();

            // Verify initial state
            var expected1 = Users
                .GroupBy(x => x.CompanyId)
                .OrderBy(x => x.Key)
                .Select(x =>
                {
                    var nonNulls = x.Where(u => u.Visits.HasValue).ToList();
                    return new
                    {
                        Key = x.Key,
                        Min = nonNulls.Any() ? (int?)nonNulls.Min(u => u.Visits!.Value) : (int?)null,
                        Max = nonNulls.Any() ? (int?)nonNulls.Max(u => u.Visits!.Value) : (int?)null
                    };
                });
            AssertCurrentDataEqual(expected1);

            // Delete the user holding the current min for their company, and add a new user
            // with a very low Visits value to become the new min
            var firstUser = Users[0];
            var companyId = firstUser.CompanyId;
            DeleteUser(firstUser);

            var newUser = new Entities.User
            {
                UserKey = 99999,
                CompanyId = companyId,
                FirstName = "NullTest",
                LastName = "User",
                Visits = -50  // Should become new min
            };
            AddOrUpdateUser(newUser);

            // Also add a user with null Visits to exercise the null-skipping path during update
            var nullUser = new Entities.User
            {
                UserKey = 99998,
                CompanyId = companyId,
                FirstName = "NullVisits",
                LastName = "User",
                Visits = null
            };
            AddOrUpdateUser(nullUser);

            await WaitForUpdate();

            var expected2 = Users
                .GroupBy(x => x.CompanyId)
                .OrderBy(x => x.Key)
                .Select(x =>
                {
                    var nonNulls = x.Where(u => u.Visits.HasValue).ToList();
                    return new
                    {
                        Key = x.Key,
                        Min = nonNulls.Any() ? (int?)nonNulls.Min(u => u.Visits!.Value) : (int?)null,
                        Max = nonNulls.Any() ? (int?)nonNulls.Max(u => u.Visits!.Value) : (int?)null
                    };
                });
            AssertCurrentDataEqual(expected2);
        }

        /// <summary>
        /// Tests MIN with a FILTER clause on a nullable column. This exercises both the filter
        /// predicate path and the null-skipping path in SharedGroupValueTree.StoreAsync simultaneously,
        /// maximizing the gap between the full batch length and the actualLength that reaches ApplyBatch.
        /// </summary>
        [Fact]
        public async Task MinAggregateWithFilterOnNullableColumn()
        {
            GenerateData(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    companyId, min(visits) FILTER (WHERE visits > 3)
                FROM users
                GROUP BY companyId
                ", ignoreSameDataCheck: true);
            await WaitForUpdate();

            var expected = Users
                .GroupBy(x => x.CompanyId)
                .OrderBy(x => x.Key)
                .Select(x =>
                {
                    var filtered = x.Where(u => u.Visits.HasValue && u.Visits.Value > 3).ToList();
                    return new
                    {
                        Key = x.Key,
                        Min = filtered.Any()
                            ? (int?)filtered.Min(u => u.Visits!.Value)
                            : (int?)null
                    };
                });
            AssertCurrentDataEqual(expected);

            // Now add a user with Visits=null (should be ignored by both null-skip and filter)
            // and one with Visits=1 (should be filtered out by FILTER WHERE visits > 3)
            // and one with Visits=100 (should pass filter)
            var company = Users[0].CompanyId;

            AddOrUpdateUser(new Entities.User
            {
                UserKey = 88881,
                CompanyId = company,
                FirstName = "NullV",
                LastName = "User",
                Visits = null
            });
            AddOrUpdateUser(new Entities.User
            {
                UserKey = 88882,
                CompanyId = company,
                FirstName = "LowV",
                LastName = "User",
                Visits = 1  // Below filter threshold, should be excluded
            });
            AddOrUpdateUser(new Entities.User
            {
                UserKey = 88883,
                CompanyId = company,
                FirstName = "HighV",
                LastName = "User",
                Visits = 100  // Passes filter, but not a new min
            });

            await WaitForUpdate();

            var expected2 = Users
                .GroupBy(x => x.CompanyId)
                .OrderBy(x => x.Key)
                .Select(x =>
                {
                    var filtered = x.Where(u => u.Visits.HasValue && u.Visits.Value > 3).ToList();
                    return new
                    {
                        Key = x.Key,
                        Min = filtered.Any()
                            ? (int?)filtered.Min(u => u.Visits!.Value)
                            : (int?)null
                    };
                });
            AssertCurrentDataEqual(expected2);
        }

        /// <summary>
        /// Tests that weight accumulation is correct at group boundaries in OnRecieve.
        /// When a batch contains deletes (weight=-1) for one company and inserts (weight=+1)
        /// for a different company, after sorting by companyId, the group boundary transition
        /// must correctly initialize the weight counter for the new group.
        /// Bug scenario: if the previous group's representative row has weight=-1,
        /// the next group's weight counter could be initialized to -1 instead of +1.
        /// </summary>
        [Fact]
        public async Task BulkAggregateSumWithCrossGroupDeletesAndInserts()
        {
            GenerateData(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    companyId, sum(userkey)
                FROM users
                GROUP BY companyId
                ", ignoreSameDataCheck: true);
            await WaitForUpdate();

            // Verify initial state
            var expected1 = Users
                .GroupBy(x => x.CompanyId)
                .OrderBy(x => x.Key)
                .Select(x => new { Key = x.Key, Sum = x.Sum(y => y.UserKey) });
            AssertCurrentDataEqual(expected1);

            // Now create a scenario with cross-group deletes and inserts in the same batch.
            // Delete ALL users from one company, and add new users to a DIFFERENT company.
            // This ensures the batch has both weight=-1 and weight=+1 rows for different groups.
            var companiesWithUsers = Users
                .Where(x => x.CompanyId != null)
                .GroupBy(x => x.CompanyId)
                .OrderBy(x => x.Key)
                .ToList();

            if (companiesWithUsers.Count >= 2)
            {
                var companyToDelete = companiesWithUsers[0];
                var companyToAddTo = companiesWithUsers[companiesWithUsers.Count - 1];

                // Delete all users from the first company (sorted alphabetically)
                foreach (var user in companyToDelete.ToList())
                {
                    DeleteUser(user);
                }

                // Add new users to the last company (sorted alphabetically)
                // These will be in the same batch as the deletes
                for (int i = 0; i < 5; i++)
                {
                    AddOrUpdateUser(new Entities.User
                    {
                        UserKey = 70000 + i,
                        CompanyId = companyToAddTo.Key,
                        FirstName = $"CrossGroup{i}",
                        LastName = "Test",
                        Visits = 5
                    });
                }
            }

            await WaitForUpdate();

            var expected2 = Users
                .GroupBy(x => x.CompanyId)
                .OrderBy(x => x.Key)
                .Select(x => new { Key = x.Key, Sum = x.Sum(y => y.UserKey) });
            AssertCurrentDataEqual(expected2);
        }

        /// <summary>
        /// Tests min aggregation with cross-group deletes and inserts.
        /// Similar to the sum variant, but exercises the stateful (shared tree) path.
        /// Deletes all users from one company and adds users to another company in
        /// the same batch, which creates mixed weight=-1 and weight=+1 rows at
        /// group boundaries.
        /// </summary>
        [Fact]
        public async Task BulkAggregateMinWithCrossGroupDeletesAndInserts()
        {
            SourceImmutable();
            GenerateData(100);
            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    companyId, min(userkey)
                FROM users
                GROUP BY companyId
                ", ignoreSameDataCheck: true);
            await WaitForUpdate();

            // Verify initial state
            var expected1 = Users
                .GroupBy(x => x.CompanyId)
                .OrderBy(x => x.Key)
                .Select(x => new { Key = x.Key, Min = x.Min(y => y.UserKey) });
            AssertCurrentDataEqual(expected1);

            // Cross-group mutation: delete from one company, insert to another
            var companiesWithUsers = Users
                .Where(x => x.CompanyId != null)
                .GroupBy(x => x.CompanyId)
                .OrderBy(x => x.Key)
                .ToList();

            if (companiesWithUsers.Count >= 2)
            {
                var companyToDelete = companiesWithUsers[0];
                var companyToAddTo = companiesWithUsers[companiesWithUsers.Count - 1];

                // Delete all users from the first company
                foreach (var user in companyToDelete.ToList())
                {
                    DeleteUser(user);
                }

                // Add users with very low keys to the last company
                for (int i = 0; i < 3; i++)
                {
                    AddOrUpdateUser(new Entities.User
                    {
                        UserKey = -(500 + i),
                        CompanyId = companyToAddTo.Key,
                        FirstName = $"CrossMin{i}",
                        LastName = "Test"
                    });
                }
            }

            await WaitForUpdate();

            var expected2 = Users
                .GroupBy(x => x.CompanyId)
                .OrderBy(x => x.Key)
                .Select(x => new { Key = x.Key, Min = x.Min(y => y.UserKey) });
            AssertCurrentDataEqual(expected2);
        }

        /// <summary>
        /// Targeted test for the weight counter initialization bug at group boundaries (line 723).
        /// Uses SourceImmutable to force deletes and inserts into the same batch (no normalization,
        /// no 50ms polling split). 
        /// Sets up: company "aaa_test" with 1 user and company "zzz_test" with 1 user.
        /// On GenerateData, the immutable source retracts old data and inserts new data in one batch.
        /// After deleting zzz_test's only user and adding to aaa_test, the batch contains:
        /// - Old aaa_test user retracted (weight=-1)
        /// - Old zzz_test user retracted (weight=-1)  
        /// - New aaa_test users inserted (weight=+1 each)
        /// After sorting by companyId, groups interleave. The weight counter at group boundaries
        /// could be initialized from the previous group's representative weight instead of the
        /// current group's first row weight.
        /// </summary>
        [Fact]
        public async Task BulkAggregateWeightAtGroupBoundaryDeletion()
        {
            // Create exactly 2 users in 2 different companies - no random data
            var userA = new Entities.User
            {
                UserKey = 50001,
                CompanyId = "aaa_test",
                FirstName = "UserA",
                LastName = "Test"
            };
            var userZ = new Entities.User
            {
                UserKey = 50002,
                CompanyId = "zzz_test",
                FirstName = "UserZ",
                LastName = "Test"
            };
            AddUser(userA);
            AddUser(userZ);

            SourceImmutable();

            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    companyId, sum(userkey)
                FROM users
                GROUP BY companyId
                ", ignoreSameDataCheck: true);
            await WaitForUpdate();

            // Verify both companies appear
            var expected1 = Users
                .GroupBy(x => x.CompanyId)
                .OrderBy(x => x.Key)
                .Select(x => new { Key = x.Key, Sum = x.Sum(y => y.UserKey) });
            AssertCurrentDataEqual(expected1);

            // Delete the only user from zzz_test (group should be removed)
            // and add a new user to aaa_test
            // With SourceImmutable, the next GenerateData/WaitForUpdate will send
            // the full new snapshot with both retractions and insertions in one batch.
            DeleteUser(userZ);
            AddOrUpdateUser(new Entities.User
            {
                UserKey = 50003,
                CompanyId = "aaa_test",
                FirstName = "NewA",
                LastName = "Test"
            });

            GenerateData(0);
            await WaitForUpdate();

            // zzz_test should be gone; aaa_test should have 2 users
            var expected2 = Users
                .GroupBy(x => x.CompanyId)
                .OrderBy(x => x.Key)
                .Select(x => new { Key = x.Key, Sum = x.Sum(y => y.UserKey) });
            AssertCurrentDataEqual(expected2);
        }

        [Fact]
        public async Task BulkListAggWithCrossGroupDeletesAndInserts()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "aaa_co", FirstName = "Alice", LastName = "A" });
            AddUser(new Entities.User { UserKey = 2, CompanyId = "aaa_co", FirstName = "Bob", LastName = "A" });
            AddUser(new Entities.User { UserKey = 3, CompanyId = "zzz_co", FirstName = "Zara", LastName = "Z" });
            AddUser(new Entities.User { UserKey = 4, CompanyId = "zzz_co", FirstName = "Zoe", LastName = "Z" });

            SourceImmutable();

            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    companyId, list_agg(firstname)
                FROM users
                GROUP BY companyId
                ", ignoreSameDataCheck: true);
            await WaitForUpdate();

            var expected1 = Users
                .GroupBy(x => x.CompanyId)
                .OrderBy(x => x.Key)
                .Select(x => new { Key = x.Key, Names = x.Select(y => y.FirstName).OrderBy(n => n).ToList() });
            AssertCurrentDataEqual(expected1);

            DeleteUser(Users.First(u => u.UserKey == 3)); // Remove Zara from zzz_co
            AddOrUpdateUser(new Entities.User { UserKey = 5, CompanyId = "aaa_co", FirstName = "Charlie", LastName = "A" });

            GenerateData(0);
            await WaitForUpdate();

            var expected2 = Users
                .GroupBy(x => x.CompanyId)
                .OrderBy(x => x.Key)
                .Select(x => new { Key = x.Key, Names = x.Select(y => y.FirstName).OrderBy(n => n).ToList() });
            AssertCurrentDataEqual(expected2);
        }

        [Fact]
        public async Task BulkStringAggWithCrossGroupDeletesAndInserts()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "aaa_co", FirstName = "Alice", LastName = "A" });
            AddUser(new Entities.User { UserKey = 2, CompanyId = "aaa_co", FirstName = "Bob", LastName = "A" });
            AddUser(new Entities.User { UserKey = 3, CompanyId = "zzz_co", FirstName = "Zara", LastName = "Z" });
            AddUser(new Entities.User { UserKey = 4, CompanyId = "zzz_co", FirstName = "Zoe", LastName = "Z" });

            SourceImmutable();

            await StartStream(@"
                INSERT INTO output 
                SELECT 
                    companyId, string_agg(firstname, ',')
                FROM users
                GROUP BY companyId
                ", ignoreSameDataCheck: true);
            await WaitForUpdate();

            // Verify initial output
            var expected1 = Users
                .GroupBy(x => x.CompanyId)
                .OrderBy(x => x.Key)
                .Select(x => new { Key = x.Key, Names = string.Join(",", x.Select(y => y.FirstName).OrderBy(n => n)) });
            AssertCurrentDataEqual(expected1);

            DeleteUser(Users.First(u => u.UserKey == 3)); // Remove Zara from zzz_co
            AddOrUpdateUser(new Entities.User { UserKey = 5, CompanyId = "aaa_co", FirstName = "Charlie", LastName = "A" });

            GenerateData(0);
            await WaitForUpdate();

            var expected2 = Users
                .GroupBy(x => x.CompanyId)
                .OrderBy(x => x.Key)
                .Select(x => new { Key = x.Key, Names = string.Join(",", x.Select(y => y.FirstName).OrderBy(n => n)) });
            AssertCurrentDataEqual(expected2);
        }
    }
}
