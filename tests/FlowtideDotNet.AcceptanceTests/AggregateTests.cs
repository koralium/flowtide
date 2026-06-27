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

        /// <summary>
        /// An empty batch must not crash a grouped bulk aggregate. Empty batches are normally absorbed
        /// by the upstream normalization operator, but an immutable source has no normalization step, so
        /// the empty batch reaches the operator directly. With dataCount == 0 the operator applies a
        /// zero-row batch to the persisted aggregate tree; AggregateInsertComparer.FindBoundriesBulk then
        /// indexes sortedLookup[0] on an empty span and throws IndexOutOfRangeException
        /// (BulkMinInsertComparer guards this case, but AggregateInsertComparer did not).
        /// </summary>
        [Fact]
        public async Task BulkAggregateEmptyBatchDoesNotCrash()
        {
            SourceImmutable();
            AddUser(new Entities.User { UserKey = 1, CompanyId = "a" });
            AddUser(new Entities.User { UserKey = 2, CompanyId = "b" });
            await StartStream(@"
                INSERT INTO output
                SELECT companyId, min(userkey)
                FROM users o
                GROUP BY companyId");
            await WaitForUpdate();

            var expected = Users.GroupBy(x => x.CompanyId).OrderBy(x => x.Key).Select(x => new { Key = x.Key, Min = x.Min(y => y.UserKey) });
            AssertCurrentDataEqual(expected);

            // Push an empty batch through the operator. On the buggy operator this throws
            // IndexOutOfRangeException in AggregateInsertComparer.FindBoundriesBulk during OnRecieve.
            WaitForUpdateDoesNotRequireDataChange();
            await Trigger("send_empty_batch", "users");
            await WaitForUpdate();

            // The empty batch changes nothing.
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
        public async Task BulkAggregateGrouplessSurrogateKeyOverEmptyInput()
        {
            SourceImmutable();
            await StartStream(@"
                INSERT INTO output
                SELECT surrogate_key_int64() as key
                FROM users");

            // An immutable source has no normalization step, so an empty batch reaches the operator and
            // fires a watermark while the aggregate tree is still empty, hitting the groupless empty path.
            WaitForUpdateDoesNotRequireDataChange();
            await Trigger("send_empty_batch", "users");
            await WaitForUpdate();

            // The scalar aggregate emits exactly one row, with the first surrogate key.
            AssertCurrentDataEqual(new[] { new { key = 0L } });
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

        /// <summary>
        /// Regression test for an IndexOutOfRangeException in <c>BulkAggregateOperator.OnWatermark</c>
        /// when the persisted aggregate tree spans multiple leaves. When updates touch many groups, the
        /// bulk search of the persisted tree can return a not-found (negative LowerBound) result for a
        /// key that is routed to a leaf it does not belong to (a leaf-boundary artifact); the key is
        /// found again in its actual leaf. The watermark loop used that negative LowerBound to index the
        /// leaf's _previousValueSent list and threw (the reported crash). Forcing a tiny page size makes
        /// the main tree split into many leaves so the boundary case is hit.
        ///
        /// This variant uses list_agg (and other shared-tree measures): the forward value comes from
        /// FetchValuesAsync, so the crash was in the retraction / previousValueSent loop.
        /// </summary>
        [Fact]
        public async Task BulkAggregateMultiLeafBoundary_ListAgg()
        {
            SetPageSizeBytes(256); // tiny leaves -> many separators in the main tree
            // 250 groups, 2 members each (a member update never empties a group).
            for (int i = 0; i < 500; i++)
            {
                AddUser(new Entities.User { UserKey = i, CompanyId = "co_" + (i / 2).ToString("D4"), FirstName = "n" + i });
            }
            await StartStream(@"
                INSERT INTO output
                SELECT companyId, list_agg(firstName)
                FROM users
                GROUP BY companyId");
            await WaitForUpdate();

            // Update one member of every group in one batch -> all groups become temp keys searched
            // against the multi-leaf persisted tree.
            for (int i = 0; i < 500; i += 2)
            {
                var u = Users.First(x => x.UserKey == i);
                u.FirstName = "updated" + i;
                AddOrUpdateUser(u);
            }
            await WaitForUpdate();

            var expected = Users
                .GroupBy(x => x.CompanyId)
                .OrderBy(x => x.Key)
                .Select(x => new { Key = x.Key, Names = x.Select(y => y.FirstName).OrderBy(n => n).ToList() });
            AssertCurrentDataEqual(expected);
        }

        /// <summary>
        /// Multi-leaf scenario with a stateless measure (sum) whose value actually changes. Stateless
        /// measures build their forward value from GetValuesAsync per persisted leaf; when a temp leaf
        /// spans multiple persisted leaves those per-leaf forward values must not be interleaved with the
        /// retraction rows, otherwise the output columns misalign and the sink ends up with wrong/negative
        /// weight rows. (The update must change the aggregated value, otherwise retract == insert and the
        /// misalignment cancels out, hiding the bug.)
        /// </summary>
        [Fact]
        public async Task BulkAggregateMultiLeafBoundary_Sum()
        {
            SetPageSizeBytes(256);
            for (int i = 0; i < 1000; i++)
            {
                AddUser(new Entities.User { UserKey = i, CompanyId = "co_" + (i / 2).ToString("D4"), FirstName = "n" + i, Visits = i });
            }
            await StartStream(@"
                INSERT INTO output
                SELECT companyId, sum(visits)
                FROM users
                GROUP BY companyId");
            await WaitForUpdate();

            for (int i = 0; i < 1000; i += 2)
            {
                var u = Users.First(x => x.UserKey == i);
                u.Visits = (u.Visits ?? 0) + 100000;
                AddOrUpdateUser(u);
            }
            await WaitForUpdate();

            var expected = Users
                .GroupBy(x => x.CompanyId)
                .OrderBy(x => x.Key)
                .Select(x => new { Key = x.Key, Sum = x.Sum(y => (long)(y.Visits ?? 0)) });
            AssertCurrentDataEqual(expected);
        }

        /// <summary>
        /// Multi-leaf scenario where whole groups are deleted in the same watermark that other groups are
        /// updated. A deleted group disappears from the persisted tree; the temporary tree must not retain
        /// a reference to it (Fix #1), otherwise the watermark would bulk-search a key that no longer
        /// exists and read a negative bound. Stresses the temp-tree invariant under deletions with a
        /// stateless measure (sum) on a multi-leaf tree.
        /// </summary>
        [Fact]
        public async Task BulkAggregateMultiLeafBoundary_GroupDeletions_Sum()
        {
            SetPageSizeBytes(256);
            for (int i = 0; i < 1000; i++)
            {
                AddUser(new Entities.User { UserKey = i, CompanyId = "co_" + (i / 2).ToString("D4"), FirstName = "n" + i, Visits = i });
            }
            await StartStream(@"
                INSERT INTO output
                SELECT companyId, sum(visits)
                FROM users
                GROUP BY companyId");
            await WaitForUpdate();

            // In one batch: delete BOTH members of every 4th group (group dies), update one member of
            // every other group (group survives). The deletions and survivors are interleaved across the
            // whole multi-leaf tree.
            for (int g = 0; g < 500; g++)
            {
                var u0 = Users.First(x => x.UserKey == g * 2);
                var u1 = Users.First(x => x.UserKey == g * 2 + 1);
                if (g % 4 == 0)
                {
                    DeleteUser(u0);
                    DeleteUser(u1);
                }
                else
                {
                    u0.Visits = (u0.Visits ?? 0) + 100000;
                    AddOrUpdateUser(u0);
                }
            }
            await WaitForUpdate();

            var expected = Users
                .GroupBy(x => x.CompanyId)
                .OrderBy(x => x.Key)
                .Select(x => new { Key = x.Key, Sum = x.Sum(y => (long)(y.Visits ?? 0)) });
            AssertCurrentDataEqual(expected);
        }

        /// <summary>
        /// Multi-leaf scenario mixing a stateless measure (sum, forward value via per-leaf GetValuesAsync)
        /// with a shared-tree measure (list_agg, forward value laid down up front via FetchValuesAsync).
        /// The two forward orderings must not be allowed to misalign with each other or with the
        /// retraction rows.
        /// </summary>
        [Fact]
        public async Task BulkAggregateMultiLeafBoundary_MixedMeasures()
        {
            SetPageSizeBytes(256);
            for (int i = 0; i < 1000; i++)
            {
                AddUser(new Entities.User { UserKey = i, CompanyId = "co_" + (i / 2).ToString("D4"), FirstName = "longfirstname_value_" + i, Visits = i });
            }
            await StartStream(@"
                INSERT INTO output
                SELECT companyId, sum(visits), list_agg(firstName)
                FROM users
                GROUP BY companyId");
            await WaitForUpdate();

            for (int i = 0; i < 1000; i += 2)
            {
                var u = Users.First(x => x.UserKey == i);
                u.Visits = (u.Visits ?? 0) + 100000;
                u.FirstName = "updated_longfirstname_value_" + i;
                AddOrUpdateUser(u);
            }
            await WaitForUpdate();

            var expected = Users
                .GroupBy(x => x.CompanyId)
                .OrderBy(x => x.Key)
                .Select(x => new { Key = x.Key, Sum = x.Sum(y => (long)(y.Visits ?? 0)), Names = x.Select(y => y.FirstName).OrderBy(n => n).ToList() });
            AssertCurrentDataEqual(expected);
        }

        /// <summary>
        /// Min/max/list_agg over a multi-leaf shared tree on the initial load, with min and max that
        /// differ for every group (visits = g*100 + ...). The distinct-per-group values are the point: a
        /// column reversal or row-misalignment between the measure columns and the group column (e.g. the
        /// shared-measure output being front-inserted per persisted leaf, or a partial-key routing miss)
        /// produces wrong values and is caught here. Uniform values would hide it.
        /// </summary>
        [Fact]
        public async Task BulkAggregateMinMaxListAgg_MultiLeaf_VaryingValues()
        {
            SetPageSizeBytes(256);
            for (int g = 0; g < 50; g++)
                for (int m = 0; m < 5; m++)
                    AddUser(new Entities.User { UserKey = g * 5 + m, CompanyId = "co_" + g.ToString("D4"), FirstName = "name_" + (g * 5 + m), Visits = g * 100 + (m + 1) * 10 });
            await StartStream("INSERT INTO output SELECT companyId, min(visits), max(visits), list_agg(firstName) FROM users GROUP BY companyId");
            await WaitForUpdate();
            var expected = Users.GroupBy(x => x.CompanyId).OrderBy(x => x.Key).Select(x => new { Key = x.Key, Min = x.Min(y => y.Visits), Max = x.Max(y => y.Visits), Names = x.Select(y => y.FirstName).OrderBy(n => n).ToList() });
            AssertCurrentDataEqual(expected);
        }

        /// <summary>
        /// Same multi-leaf, distinct-per-group setup, but deletes each group's min member so min must rise
        /// to the next value (still distinct per group). Exercises shared-tree retraction on the
        /// incremental path with values that vary across groups, so a reversal/misalignment is observable.
        /// </summary>
        [Fact]
        public async Task BulkAggregateMinMaxListAgg_MultiLeaf_DeleteMinMember()
        {
            SetPageSizeBytes(256);
            for (int g = 0; g < 50; g++)
                for (int m = 0; m < 5; m++)
                    AddUser(new Entities.User { UserKey = g * 5 + m, CompanyId = "co_" + g.ToString("D4"), FirstName = "name_" + (g * 5 + m), Visits = g * 100 + (m + 1) * 10 });
            await StartStream("INSERT INTO output SELECT companyId, min(visits), max(visits), list_agg(firstName) FROM users GROUP BY companyId");

            void AssertExpected()
            {
                var expected = Users.GroupBy(x => x.CompanyId).OrderBy(x => x.Key).Select(x => new { Key = x.Key, Min = x.Min(y => y.Visits), Max = x.Max(y => y.Visits), Names = x.Select(y => y.FirstName).OrderBy(n => n).ToList() });
                AssertCurrentDataEqual(expected);
            }

            await WaitForUpdate();
            AssertExpected();

            // Delete each group's smallest member (m == 0) so min must rise to the next value per group.
            for (int g = 0; g < 50; g++)
                DeleteUser(Users.First(u => u.UserKey == g * 5));
            await WaitForUpdate();
            AssertExpected();
        }

        /// <summary>
        /// min_by over a multi-leaf shared tree, distinct value per group. min_by reads the value at the
        /// group's smallest orderBy (its leftmost shared-tree entry); like min it routes left and needs
        /// forward carry to find groups that begin on a leaf boundary. Varying per-group results make any
        /// boundary miss or column misalignment observable.
        /// </summary>
        [Fact]
        public async Task BulkAggregateMinBy_MultiLeaf_VaryingValues()
        {
            SetPageSizeBytes(256);
            for (int g = 0; g < 50; g++)
                for (int m = 0; m < 5; m++)
                    AddUser(new Entities.User { UserKey = g * 5 + m, CompanyId = "co_" + g.ToString("D4"), FirstName = "g" + g + "_m" + m, Visits = g * 100 + (m + 1) * 10 });
            await StartStream("INSERT INTO output SELECT companyId, min_by(firstName, visits) FROM users GROUP BY companyId");

            void AssertExpected()
            {
                var expected = Users.GroupBy(x => x.CompanyId).OrderBy(x => x.Key).Select(x =>
                {
                    var minV = x.Min(y => y.Visits);
                    return new { Key = x.Key, MinByName = x.First(y => y.Visits == minV).FirstName };
                });
                AssertCurrentDataEqual(expected);
            }

            await WaitForUpdate();
            AssertExpected();

            // Delete each group's smallest-orderBy member so min_by must move to the next member, and the
            // deletes shift leaf boundaries (which is what surfaced the min boundary-miss).
            for (int g = 0; g < 50; g++)
                DeleteUser(Users.First(u => u.UserKey == g * 5));
            await WaitForUpdate();
            AssertExpected();
        }

        /// <summary>
        /// max_by over a multi-leaf shared tree, distinct value per group. max_by gathers per leaf and
        /// emits via InsertFrom; with multiple leaves the column must not be front-inserted (reversed).
        /// Varying per-group results make any reversal/misalignment observable.
        /// </summary>
        [Fact]
        public async Task BulkAggregateMaxBy_MultiLeaf_VaryingValues()
        {
            SetPageSizeBytes(256);
            for (int g = 0; g < 50; g++)
                for (int m = 0; m < 5; m++)
                    AddUser(new Entities.User { UserKey = g * 5 + m, CompanyId = "co_" + g.ToString("D4"), FirstName = "g" + g + "_m" + m, Visits = g * 100 + (m + 1) * 10 });
            await StartStream("INSERT INTO output SELECT companyId, max_by(firstName, visits) FROM users GROUP BY companyId");
            await WaitForUpdate();
            var expected = Users.GroupBy(x => x.CompanyId).OrderBy(x => x.Key).Select(x =>
            {
                var maxV = x.Max(y => y.Visits);
                return new { Key = x.Key, MaxByName = x.First(y => y.Visits == maxV).FirstName };
            });
            AssertCurrentDataEqual(expected);
        }

        /// <summary>
        /// Hammers the shared-tree measures (min/max/count_distinct/list_agg) under member churn on a
        /// multi-leaf tree: removing the current min member must raise min, removing the current max member
        /// must drop max, and updating a member must retract its old contribution from the shared trees.
        /// This is the closest stress to the original production failure (list_agg member update).
        /// </summary>
        [Fact]
        public async Task BulkAggregateSharedTreeRetractionChurn_MultiLeaf()
        {
            SetPageSizeBytes(256);
            for (int g = 0; g < 200; g++)
            {
                for (int m = 0; m < 5; m++)
                {
                    int key = g * 5 + m;
                    AddUser(new Entities.User { UserKey = key, CompanyId = "co_" + g.ToString("D4"), FirstName = "name_" + key, Visits = (m + 1) * 10 });
                }
            }
            await StartStream(@"
                INSERT INTO output
                SELECT companyId, min(visits), max(visits), count(DISTINCT visits), list_agg(firstName)
                FROM users
                GROUP BY companyId");

            void AssertExpected()
            {
                var expected = Users
                    .GroupBy(x => x.CompanyId)
                    .OrderBy(x => x.Key)
                    .Select(x => new
                    {
                        Key = x.Key,
                        Min = x.Min(y => y.Visits),
                        Max = x.Max(y => y.Visits),
                        DistinctVisits = x.Select(y => y.Visits).Distinct().Count(),
                        Names = x.Select(y => y.FirstName).OrderBy(n => n).ToList()
                    });
                AssertCurrentDataEqual(expected);
            }

            await WaitForUpdate();
            AssertExpected();

            // Remove the current MIN member (visits=10) of every group -> min must rise to 20.
            for (int g = 0; g < 200; g++)
            {
                DeleteUser(Users.First(u => u.UserKey == g * 5));
            }
            await WaitForUpdate();
            AssertExpected();

            // Remove the current MAX member (visits=50) of every group -> max must drop to 40.
            for (int g = 0; g < 200; g++)
            {
                DeleteUser(Users.First(u => u.UserKey == g * 5 + 4));
            }
            await WaitForUpdate();
            AssertExpected();

            // Update a surviving member to a new global extreme and change its name -> both the min/max
            // shared tree and the list_agg shared tree must retract the old value and add the new one.
            for (int g = 0; g < 200; g++)
            {
                var u = Users.First(uu => uu.UserKey == g * 5 + 2);
                u.Visits = 1000;
                u.FirstName = "updated_" + u.UserKey;
                AddOrUpdateUser(u);
            }
            await WaitForUpdate();
            AssertExpected();
        }

        /// <summary>
        /// Stresses the numeric promotion paths in SumAggregation.DoSum: within each group the summed
        /// expression yields an int for some rows (visits) and a double for others (DoubleValue), so the
        /// running state changes type. Flipping a row's branch on update must retract the old typed value
        /// correctly. DoubleValue uses a 0.25 fraction (exactly representable) so the sum is order
        /// independent and can be compared exactly.
        /// </summary>
        [Fact]
        public async Task BulkAggregateSumMixedNumericTypes()
        {
            for (int i = 0; i < 200; i++)
            {
                AddUser(new Entities.User { UserKey = i, CompanyId = "co_" + (i / 10).ToString("D3"), Gender = (Entities.Gender)(i % 2), Visits = i, DoubleValue = i + 0.25 });
            }
            await StartStream(@"
                INSERT INTO output
                SELECT companyId, sum(CASE WHEN Gender = 1 THEN visits ELSE DoubleValue END)
                FROM users
                GROUP BY companyId");

            void AssertExpected()
            {
                var expected = Users
                    .GroupBy(x => x.CompanyId)
                    .OrderBy(x => x.Key)
                    .Select(x => new
                    {
                        Key = x.Key,
                        Sum = x.Sum(y => y.Gender == Entities.Gender.Female ? (double)(y.Visits ?? 0) : y.DoubleValue)
                    });
                AssertCurrentDataEqual(expected);
            }

            await WaitForUpdate();
            AssertExpected();

            // Flip the gender of every even user: rows that contributed a double now contribute an int and
            // vice versa, forcing the per-group state through mixed-type retract + add.
            for (int i = 0; i < 200; i += 2)
            {
                var u = Users.First(uu => uu.UserKey == i);
                u.Gender = u.Gender == Entities.Gender.Male ? Entities.Gender.Female : Entities.Gender.Male;
                AddOrUpdateUser(u);
            }
            await WaitForUpdate();
            AssertExpected();
        }

        /// <summary>
        /// Multi-column group key (companyId, lastName) on a multi-leaf tree with updates and deletes. The
        /// group columns must stay aligned across the two key columns and the measures.
        /// </summary>
        [Fact]
        public async Task BulkAggregateMultiColumnGroupKey_MultiLeaf()
        {
            SetPageSizeBytes(256);
            for (int i = 0; i < 600; i++)
            {
                AddUser(new Entities.User { UserKey = i, CompanyId = "co_" + (i / 30).ToString("D3"), LastName = "L" + (i % 3), FirstName = "n" + i, Visits = i });
            }
            await StartStream(@"
                INSERT INTO output
                SELECT companyId, LastName, sum(visits), max(visits), list_agg(firstName)
                FROM users
                GROUP BY companyId, LastName");

            void AssertExpected()
            {
                var expected = Users
                    .GroupBy(x => new { x.CompanyId, x.LastName })
                    .OrderBy(x => x.Key.CompanyId).ThenBy(x => x.Key.LastName)
                    .Select(x => new
                    {
                        x.Key.CompanyId,
                        x.Key.LastName,
                        Sum = x.Sum(y => (long)(y.Visits ?? 0)),
                        Max = x.Max(y => y.Visits),
                        Names = x.Select(y => y.FirstName).OrderBy(n => n).ToList()
                    });
                AssertCurrentDataEqual(expected);
            }

            await WaitForUpdate();
            AssertExpected();

            for (int i = 0; i < 600; i += 2)
            {
                if (i % 6 == 0)
                {
                    DeleteUser(Users.First(u => u.UserKey == i));
                }
                else
                {
                    var u = Users.First(uu => uu.UserKey == i);
                    u.Visits = (u.Visits ?? 0) + 100000;
                    AddOrUpdateUser(u);
                }
            }
            await WaitForUpdate();
            AssertExpected();
        }

        /// <summary>
        /// Group by a computed expression (concat) rather than a direct field reference, on a multi-leaf
        /// tree. This exercises the non-direct-field group path (groupExpressions / ColumnProjectCompiler)
        /// instead of the direct-column path.
        /// </summary>
        [Fact]
        public async Task BulkAggregateComputedGroupKey_MultiLeaf()
        {
            SetPageSizeBytes(256);
            for (int i = 0; i < 600; i++)
            {
                AddUser(new Entities.User { UserKey = i, CompanyId = "co_" + (i / 3).ToString("D4"), FirstName = "n" + i, Visits = i });
            }
            await StartStream(@"
                INSERT INTO output
                SELECT concat(companyId, '_grp'), sum(visits)
                FROM users
                GROUP BY concat(companyId, '_grp')");

            void AssertExpected()
            {
                var expected = Users
                    .GroupBy(x => x.CompanyId + "_grp")
                    .OrderBy(x => x.Key)
                    .Select(x => new { Key = x.Key, Sum = x.Sum(y => (long)(y.Visits ?? 0)) });
                AssertCurrentDataEqual(expected);
            }

            await WaitForUpdate();
            AssertExpected();

            for (int i = 0; i < 600; i += 3)
            {
                var u = Users.First(uu => uu.UserKey == i);
                u.Visits = (u.Visits ?? 0) + 50000;
                AddOrUpdateUser(u);
            }
            await WaitForUpdate();
            AssertExpected();
        }

        /// <summary>
        /// Crash and restore with a multi-leaf persisted tree AND shared trees (min/max/list_agg), then
        /// continue with member churn. Verifies the serialized persisted + shared tree state restores
        /// correctly and the operator keeps incrementally updating afterwards.
        /// </summary>
        [Fact]
        public async Task BulkAggregateCrashRestore_MultiLeaf_SharedTree()
        {
            SetPageSizeBytes(256);
            for (int g = 0; g < 200; g++)
            {
                for (int m = 0; m < 3; m++)
                {
                    int key = g * 3 + m;
                    AddUser(new Entities.User { UserKey = key, CompanyId = "co_" + g.ToString("D4"), FirstName = "n_" + key, Visits = (m + 1) * 10 });
                }
            }
            await StartStream(@"
                INSERT INTO output
                SELECT companyId, min(visits), max(visits), sum(visits), list_agg(firstName)
                FROM users
                GROUP BY companyId");

            void AssertExpected()
            {
                var expected = Users
                    .GroupBy(x => x.CompanyId)
                    .OrderBy(x => x.Key)
                    .Select(x => new
                    {
                        Key = x.Key,
                        Min = x.Min(y => y.Visits),
                        Max = x.Max(y => y.Visits),
                        Sum = x.Sum(y => (long)(y.Visits ?? 0)),
                        Names = x.Select(y => y.FirstName).OrderBy(n => n).ToList()
                    });
                AssertCurrentDataEqual(expected);
            }

            await WaitForUpdate();
            AssertExpected();

            await Crash();

            // After restore: remove the min member of each group and update another, so the restored
            // persisted + shared trees must be correct and keep updating.
            for (int g = 0; g < 200; g++)
            {
                DeleteUser(Users.First(u => u.UserKey == g * 3));
                var u = Users.First(uu => uu.UserKey == g * 3 + 1);
                u.Visits = 999;
                u.FirstName = "upd_" + u.UserKey;
                AddOrUpdateUser(u);
            }
            await WaitForUpdate();
            AssertExpected();
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

        /// <summary>
        /// count(DISTINCT visits) over a multi-leaf shared tree with a distinct count that varies per group
        /// (2..5), plus a delete round. Varying per-group counts make any boundary miss / misalignment in
        /// the shared distinct tree observable.
        /// </summary>
        [Fact]
        public async Task BulkAggregateCountDistinct_MultiLeaf_VaryingCounts()
        {
            SetPageSizeBytes(256);
            for (int g = 0; g < 50; g++)
            {
                int distinct = (g % 4) + 2; // 2..5 distinct visit values per group
                for (int m = 0; m < 6; m++)
                    AddUser(new Entities.User { UserKey = g * 6 + m, CompanyId = "co_" + g.ToString("D4"), FirstName = "n" + (g * 6 + m), Visits = g * 1000 + (m % distinct) });
            }
            await StartStream("INSERT INTO output SELECT companyId, count(DISTINCT visits) FROM users GROUP BY companyId");

            void AssertExpected()
            {
                var expected = Users.GroupBy(x => x.CompanyId).OrderBy(x => x.Key)
                    .Select(x => new { Key = x.Key, DistinctVisits = x.Select(y => y.Visits).Distinct().Count() });
                AssertCurrentDataEqual(expected);
            }

            await WaitForUpdate();
            AssertExpected();

            for (int g = 0; g < 50; g++)
                DeleteUser(Users.First(u => u.UserKey == g * 6));
            await WaitForUpdate();
            AssertExpected();
        }

        /// <summary>
        /// string_agg over a multi-leaf shared tree with distinct names per group, plus a delete round.
        /// Varying per-group strings make any reversal/misalignment observable.
        /// </summary>
        [Fact]
        public async Task BulkAggregateStringAgg_MultiLeaf_VaryingValues()
        {
            SetPageSizeBytes(256);
            for (int g = 0; g < 50; g++)
                for (int m = 0; m < 5; m++)
                    AddUser(new Entities.User { UserKey = g * 5 + m, CompanyId = "co_" + g.ToString("D4"), FirstName = "g" + g.ToString("D4") + "_m" + m });
            await StartStream("INSERT INTO output SELECT companyId, string_agg(firstName, ',') FROM users GROUP BY companyId");

            void AssertExpected()
            {
                var expected = Users.GroupBy(x => x.CompanyId).OrderBy(x => x.Key)
                    .Select(x => new { Key = x.Key, Names = string.Join(",", x.Select(y => y.FirstName).OrderBy(n => n)) });
                AssertCurrentDataEqual(expected);
            }

            await WaitForUpdate();
            AssertExpected();

            for (int g = 0; g < 50; g++)
                DeleteUser(Users.First(u => u.UserKey == g * 5));
            await WaitForUpdate();
            AssertExpected();
        }

        /// <summary>
        /// Regression: list_union_distinct_agg with a GROUP BY used to crash during insert with
        /// NotSupportedException. The list argument is flattened (each element a shared-tree entry) and the
        /// group column was wrapped again with 'new ColumnWithOffset'; since the incoming group column is
        /// already a ColumnWithOffset under GROUP BY, that nested ColumnWithOffset and tripped
        /// GetColumnState in the insert boundary search. Fixed by using ColumnWithOffset.CreateFlattened
        /// (composes offsets instead of nesting).
        /// </summary>
        [Fact]
        public async Task BulkAggregateListUnionDistinctAgg_Grouped()
        {
            for (int g = 0; g < 3; g++)
                for (int m = 0; m < 2; m++)
                    AddUser(new Entities.User { UserKey = g * 2 + m, CompanyId = "co_" + g.ToString("D4"), FirstName = "g" + g.ToString("D4") + "_m" + m });
            await StartStream("INSERT INTO output SELECT companyId, list_union_distinct_agg(list(firstName)) FROM users GROUP BY companyId");

            void AssertExpected()
            {
                var expected = Users.GroupBy(x => x.CompanyId).OrderBy(x => x.Key)
                    .Select(x => new { Key = x.Key, Names = x.Select(y => y.FirstName).Distinct().OrderBy(n => n).ToList() });
                AssertCurrentDataEqual(expected);
            }

            await WaitForUpdate();
            AssertExpected();
        }

        /// <summary>
        /// Differential stress for list_union_distinct_agg under churn + crash on a multi-leaf tree. Each row
        /// contributes a single-element list, so the per-group result is the distinct set of firstNames. A
        /// small name pool forces frequent duplicates: deleting one holder of a duplicated name must NOT drop
        /// it from the union (another holder remains) — exercising weighted dedup and partial retraction
        /// through the flattened-offset insert path that was the recent fix.
        /// </summary>
        [Theory]
        [InlineData(5)]
        [InlineData(9)]
        public async Task BulkAggregateListUnionDistinctAgg_RandomizedStress_WithCrash(int seed)
        {
            SetPageSizeBytes(160);
            var rnd = new Random(seed);
            int nextKey = 0;
            // Small pool => frequent duplicate names within a group (tests weighted dedup under retraction).
            string NewName() => "p" + rnd.Next(15).ToString("D2");

            for (int g = 0; g < 60; g++)
                for (int m = 0; m < 3; m++)
                    AddUser(new Entities.User { UserKey = nextKey++, CompanyId = "co_" + g.ToString("D4"), FirstName = NewName() });

            await StartStream("INSERT INTO output SELECT companyId, list_union_distinct_agg(list(firstName)) FROM users GROUP BY companyId");

            void AssertExpected()
            {
                var expected = Users.GroupBy(x => x.CompanyId).OrderBy(x => x.Key)
                    .Select(x => new { Key = x.Key, Names = x.Select(y => y.FirstName).Distinct().OrderBy(n => n).ToList() });
                AssertCurrentDataEqual(expected);
            }

            await WaitForUpdate();
            AssertExpected();

            for (int round = 0; round < 20; round++)
            {
                int ops = rnd.Next(10, 40);
                for (int o = 0; o < ops; o++)
                {
                    var action = Users.Count < 30 ? 0 : rnd.Next(3);
                    if (action == 0)
                    {
                        int g = rnd.Next(60);
                        AddUser(new Entities.User { UserKey = nextKey++, CompanyId = "co_" + g.ToString("D4"), FirstName = NewName() });
                    }
                    else if (action == 1)
                    {
                        var u = Users[rnd.Next(Users.Count)];
                        u.FirstName = NewName();
                        AddOrUpdateUser(u);
                    }
                    else
                    {
                        DeleteUser(Users[rnd.Next(Users.Count)]);
                    }
                }

                if (round == 10)
                {
                    await Crash();
                }

                await WaitForUpdate();
                AssertExpected();
            }
        }

        /// <summary>
        /// Stateless measures (count/avg/sum) over a multi-leaf tree with values that vary per group, plus
        /// churn. These forward their value per persisted leaf via GetValuesAsync (appended by the operator,
        /// not via InsertFrom), so this confirms the stateless path aligns on multi-leaf the same way the
        /// shared-tree measures now do.
        /// </summary>
        [Fact]
        public async Task BulkAggregateStatelessMeasures_MultiLeaf_VaryingValues()
        {
            SetPageSizeBytes(256);
            for (int g = 0; g < 80; g++)
                for (int m = 0; m < 4; m++)
                    AddUser(new Entities.User { UserKey = g * 4 + m, CompanyId = "co_" + g.ToString("D4"), Visits = g * 100 + (m + 1) * 10 });
            await StartStream("INSERT INTO output SELECT companyId, count(*), avg(visits), sum(visits) FROM users GROUP BY companyId");

            void AssertExpected()
            {
                var expected = Users.GroupBy(x => x.CompanyId).OrderBy(x => x.Key).Select(x => new
                {
                    Key = x.Key,
                    Cnt = x.Count(),
                    Avg = (double)x.Average(y => y.Visits!.Value),
                    Sum = x.Sum(y => (long)(y.Visits ?? 0))
                });
                AssertCurrentDataEqual(expected);
            }

            await WaitForUpdate();
            AssertExpected();

            for (int g = 0; g < 80; g++)
            {
                if (g % 3 == 0)
                {
                    DeleteUser(Users.First(u => u.UserKey == g * 4));
                }
                else
                {
                    var u = Users.First(uu => uu.UserKey == g * 4 + 1);
                    u.Visits = (u.Visits ?? 0) + 100000;
                    AddOrUpdateUser(u);
                }
            }
            await WaitForUpdate();
            AssertExpected();
        }

        /// <summary>
        /// Randomized differential stress: many random add/update/delete operations across many watermarks
        /// on a multi-leaf tree, with min/max/sum/count/list_agg together, each round checked against a
        /// LINQ ground truth. Exercises carry-over, leaf split/merge, retraction and every measure in
        /// combinations no targeted test covers. Avg is omitted (floating-point order sensitivity).
        /// </summary>
        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        public async Task BulkAggregateRandomizedStress_MultiLeaf(int seed)
        {
            SetPageSizeBytes(256);
            var rnd = new Random(seed);
            int nextKey = 0;
            string NewName() => "n" + rnd.Next(1_000_000).ToString("D7");

            for (int g = 0; g < 100; g++)
                for (int m = 0; m < 3; m++)
                    AddUser(new Entities.User { UserKey = nextKey++, CompanyId = "co_" + g.ToString("D4"), FirstName = NewName(), Visits = rnd.Next(1, 1000) });

            await StartStream(@"
                INSERT INTO output
                SELECT companyId, min(visits), max(visits), sum(visits), count(*), list_agg(firstName)
                FROM users
                GROUP BY companyId");

            void AssertExpected()
            {
                var expected = Users.GroupBy(x => x.CompanyId).OrderBy(x => x.Key).Select(x => new
                {
                    Key = x.Key,
                    Min = x.Min(y => y.Visits),
                    Max = x.Max(y => y.Visits),
                    Sum = x.Sum(y => (long)(y.Visits ?? 0)),
                    Cnt = x.Count(),
                    Names = x.Select(y => y.FirstName).OrderBy(n => n).ToList()
                });
                AssertCurrentDataEqual(expected);
            }

            await WaitForUpdate();
            AssertExpected();

            for (int round = 0; round < 20; round++)
            {
                int ops = rnd.Next(10, 40);
                for (int o = 0; o < ops; o++)
                {
                    var action = Users.Count < 50 ? 0 : rnd.Next(3);
                    if (action == 0)
                    {
                        int g = rnd.Next(100);
                        AddUser(new Entities.User { UserKey = nextKey++, CompanyId = "co_" + g.ToString("D4"), FirstName = NewName(), Visits = rnd.Next(1, 1000) });
                    }
                    else if (action == 1)
                    {
                        var u = Users[rnd.Next(Users.Count)];
                        u.Visits = rnd.Next(1, 1000);
                        u.FirstName = NewName();
                        AddOrUpdateUser(u);
                    }
                    else
                    {
                        DeleteUser(Users[rnd.Next(Users.Count)]);
                    }
                }
                await WaitForUpdate();
                AssertExpected();
            }
        }

        /// <summary>
        /// Regression: sum must not be history-dependent for an all-null group. A group whose member never
        /// had a non-null value and a group whose value was set then retracted to null are logically
        /// identical (one null-valued row each), so both must report sum = NULL. Previously the second
        /// reported 0, because the stateless sum landed at numeric 0 after the last contributor was retracted
        /// (5 + 5*-1 = 0) with no way to revert to null. sum now tracks a non-null contributor count (like
        /// avg) and emits null once it returns to 0. (sum0 is the always-0 variant.)
        /// </summary>
        [Fact]
        public async Task BulkAggregateSum_AllNullGroup_HistoryDependent()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", Visits = null });
            AddUser(new Entities.User { UserKey = 2, CompanyId = "B", Visits = 5 });
            await StartStream("INSERT INTO output SELECT companyId, sum(visits) FROM users GROUP BY companyId");
            await WaitForUpdate();

            var u = Users.First(x => x.UserKey == 2);
            u.Visits = null;
            AddOrUpdateUser(u);
            await WaitForUpdate();

            // Both groups now contain only null values, so SQL SUM should be null for both.
            AssertCurrentDataEqual(new[]
            {
                new { CompanyId = "A", Sum = (long?)null },
                new { CompanyId = "B", Sum = (long?)null },
            });
        }

        /// <summary>
        /// Regression: a FILTER clause on a STATELESS aggregate (sum/count/avg) must be honoured. These
        /// measures aggregate in the Compute path (driven by BulkAggregateMutator), which previously used the
        /// full per-group index range and ignored the filter; the mutator now compacts each group's rows to
        /// those passing the measure's filter before calling Compute. A: visits {2,5,10}, >3 -> {5,10}
        /// (sum=15, count=2, avg=7.5). B: visits {1,2}, nothing passes (sum=null, count=0, avg=null).
        /// </summary>
        [Fact]
        public async Task BulkAggregateSum_WithFilter_Stateless()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", Visits = 2 });
            AddUser(new Entities.User { UserKey = 2, CompanyId = "A", Visits = 5 });
            AddUser(new Entities.User { UserKey = 3, CompanyId = "A", Visits = 10 });
            AddUser(new Entities.User { UserKey = 4, CompanyId = "B", Visits = 1 });
            AddUser(new Entities.User { UserKey = 5, CompanyId = "B", Visits = 2 });
            await StartStream(@"
                INSERT INTO output
                SELECT companyId,
                       sum(visits) FILTER (WHERE visits > 3),
                       count(*) FILTER (WHERE visits > 3),
                       avg(visits) FILTER (WHERE visits > 3)
                FROM users GROUP BY companyId");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new { CompanyId = "A", Sum = (long?)15, Cnt = 2L, Avg = (double?)7.5 },
                new { CompanyId = "B", Sum = (long?)null, Cnt = 0L, Avg = (double?)null },
            });
        }

        /// <summary>
        /// The stateless FILTER must also be honoured on retractions: updating a row across the filter
        /// boundary retracts/adds its contribution only when the row's own value passes the predicate (the
        /// filter is evaluated per row, weights carry insert/retract).
        /// </summary>
        [Fact]
        public async Task BulkAggregateSum_WithFilter_Churn()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", Visits = 5 });
            AddUser(new Entities.User { UserKey = 2, CompanyId = "A", Visits = 10 });
            await StartStream("INSERT INTO output SELECT companyId, sum(visits) FILTER (WHERE visits > 3) FROM users GROUP BY companyId");
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] { new { CompanyId = "A", Sum = (long?)15 } });

            // 5 -> 2 : drops below the filter, contribution removed -> only 10 remains.
            var u = Users.First(x => x.UserKey == 1);
            u.Visits = 2;
            AddOrUpdateUser(u);
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] { new { CompanyId = "A", Sum = (long?)10 } });

            // 2 -> 20 : rises above the filter, contribution added back -> 10 + 20 = 30.
            u = Users.First(x => x.UserKey == 1);
            u.Visits = 20;
            AddOrUpdateUser(u);
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] { new { CompanyId = "A", Sum = (long?)30 } });
        }

        /// <summary>
        /// DISTINCT is not honoured for sum/avg/sum0 (the planner only routes count(DISTINCT) to a
        /// distinct-aware implementation). Rather than silently summing duplicates, sum(DISTINCT) must now be
        /// rejected at plan time. (min/max DISTINCT remains a valid no-op.)
        /// </summary>
        [Fact]
        public async Task BulkAggregateSum_Distinct_Throws()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", Visits = 5 });
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await StartStream("INSERT INTO output SELECT companyId, sum(DISTINCT visits) FROM users GROUP BY companyId");
            });
            Assert.Equal("sum does not support DISTINCT.", ex.Message);
        }

        [Fact]
        public async Task BulkAggregateAvg_Distinct_Throws()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", Visits = 5 });
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await StartStream("INSERT INTO output SELECT companyId, avg(DISTINCT visits) FROM users GROUP BY companyId");
            });
            Assert.Equal("avg does not support DISTINCT.", ex.Message);
        }

        /// <summary>
        /// DISTINCT on min/max is a semantic no-op (min of distinct values == min of all values), so it must
        /// keep working rather than being rejected alongside sum/avg.
        /// </summary>
        [Fact]
        public async Task BulkAggregateMin_Distinct_NoOp()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", Visits = 5 });
            AddUser(new Entities.User { UserKey = 2, CompanyId = "A", Visits = 5 });
            AddUser(new Entities.User { UserKey = 3, CompanyId = "A", Visits = 10 });
            await StartStream("INSERT INTO output SELECT companyId, min(DISTINCT visits) FROM users GROUP BY companyId");
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] { new { CompanyId = "A", Min = (int?)5 } });
        }

        /// <summary>
        /// list_agg/string_agg order by the aggregated value (their shared tree is keyed by it), so a
        /// user-specified ORDER BY cannot be honoured. Rather than silently producing value-order, it is now
        /// rejected at plan time.
        /// </summary>
        [Fact]
        public async Task BulkAggregateListAgg_OrderBy_Throws()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", FirstName = "c", Visits = 1 });
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await StartStream("INSERT INTO output SELECT companyId, list_agg(firstName ORDER BY visits) FROM users GROUP BY companyId");
            });
            Assert.Equal("list_agg does not support ORDER BY.", ex.Message);
        }

        /// <summary>
        /// list_agg/string_agg do not honour DISTINCT (they keep weighted duplicates), so it is rejected at
        /// plan time rather than silently keeping duplicates. (list_union_distinct_agg covers distinct lists.)
        /// </summary>
        [Fact]
        public async Task BulkAggregateListAgg_Distinct_Throws()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", FirstName = "a" });
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await StartStream("INSERT INTO output SELECT companyId, list_agg(DISTINCT firstName) FROM users GROUP BY companyId");
            });
            Assert.Equal("list_agg does not support DISTINCT.", ex.Message);
        }

        /// <summary>
        /// count(DISTINCT) combined with FILTER works (the filter is applied by the shared tree, the distinct
        /// count by the count_distinct measure). Regression guard for the combination.
        /// </summary>
        [Fact]
        public async Task BulkAggregateCountDistinct_WithFilter()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", Visits = 2 });
            AddUser(new Entities.User { UserKey = 2, CompanyId = "A", Visits = 5 });
            AddUser(new Entities.User { UserKey = 3, CompanyId = "A", Visits = 5 });
            AddUser(new Entities.User { UserKey = 4, CompanyId = "A", Visits = 10 });
            await StartStream("INSERT INTO output SELECT companyId, count(DISTINCT visits) FILTER (WHERE visits > 3) FROM users GROUP BY companyId");
            await WaitForUpdate();

            // Distinct visits > 3 are {5, 10} -> 2.
            AssertCurrentDataEqual(new[] { new { CompanyId = "A", Cnt = 2L } });
        }

        /// <summary>
        /// Stresses the incremental watermark path's exact-match search of the persisted tree across many
        /// leaf boundaries: tiny pages (one/few groups per leaf) + state-bearing measures, then EVERY group is
        /// modified in a single batch so the temp tree spans the whole persisted tree and the bulk search
        /// crosses every leaf boundary (the carry / "continues to next leaf" path). Verifies values stay
        /// correct and nothing reads a not-found/negative state index.
        /// </summary>
        [Theory]
        [InlineData(128)]
        [InlineData(200)]
        public async Task BulkAggregateIncrementalWatermark_ManyLeafBoundaries(int pageBytes)
        {
            SetPageSizeBytes(pageBytes);
            int nextKey = 0;
            for (int g = 0; g < 150; g++)
                for (int m = 0; m < 2; m++)
                    AddUser(new Entities.User { UserKey = nextKey++, CompanyId = "co_" + g.ToString("D4"), Visits = g * 10 + m + 1 });

            await StartStream(@"
                INSERT INTO output
                SELECT companyId, sum(visits), count(*), avg(visits), count(DISTINCT visits)
                FROM users GROUP BY companyId");

            void AssertExpected()
            {
                var expected = Users.GroupBy(x => x.CompanyId).OrderBy(x => x.Key).Select(x =>
                {
                    var nn = x.Where(y => y.Visits.HasValue).ToList();
                    return new
                    {
                        Key = x.Key,
                        Sum = nn.Count > 0 ? (long?)nn.Sum(y => (long)y.Visits!.Value) : null,
                        Cnt = (long)x.Count(),
                        Avg = nn.Count > 0 ? (double?)nn.Average(y => (double)y.Visits!.Value) : null,
                        DCnt = (long)nn.Select(y => y.Visits!.Value).Distinct().Count()
                    };
                });
                AssertCurrentDataEqual(expected);
            }

            await WaitForUpdate();
            AssertExpected();

            // Modify EVERY group in one batch -> every key lands in the temp tree -> the watermark search
            // walks the entire persisted tree, crossing every leaf boundary.
            foreach (var u in Users.Where(x => x.UserKey % 2 == 0).ToList())
            {
                u.Visits = u.Visits!.Value + 1000;
                AddOrUpdateUser(u);
            }
            await WaitForUpdate();
            AssertExpected();

            // Delete one member of every group in one batch (shifts boundaries) and re-verify.
            foreach (var u in Users.Where(x => x.UserKey % 2 == 1).ToList())
            {
                DeleteUser(u);
            }
            await WaitForUpdate();
            AssertExpected();
        }

        [Fact]
        public async Task BulkAggregateStringAgg_OrderBy_Throws()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", FirstName = "c", Visits = 1 });
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await StartStream("INSERT INTO output SELECT companyId, string_agg(firstName, ',' ORDER BY visits) FROM users GROUP BY companyId");
            });
            Assert.Equal("string_agg does not support ORDER BY.", ex.Message);
        }

        [Fact]
        public async Task BulkAggregateStringAgg_Distinct_Throws()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", FirstName = "a" });
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await StartStream("INSERT INTO output SELECT companyId, string_agg(DISTINCT firstName, ',') FROM users GROUP BY companyId");
            });
            Assert.Equal("string_agg does not support DISTINCT.", ex.Message);
        }

        /// <summary>
        /// min_by returns the value at the smallest order-by row even when that value is null (the order-by is
        /// non-null, so the row is not skipped). Regression guard.
        /// </summary>
        [Fact]
        public async Task BulkAggregateMinBy_NullValue()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", FirstName = null, Visits = 1 });
            AddUser(new Entities.User { UserKey = 2, CompanyId = "A", FirstName = "x", Visits = 2 });
            await StartStream("INSERT INTO output SELECT companyId, min_by(firstName, visits) FROM users GROUP BY companyId");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[] { new { CompanyId = "A", Name = default(string) } });
        }

        [Fact]
        public async Task BulkAggregateAllMeasureTypes_OneQuery()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", FirstName = "c", Visits = 10 });
            AddUser(new Entities.User { UserKey = 2, CompanyId = "A", FirstName = "a", Visits = 30 });
            AddUser(new Entities.User { UserKey = 3, CompanyId = "A", FirstName = "b", Visits = 20 });
            await StartStream(@"
                INSERT INTO output
                SELECT companyId, min(visits), max(visits), sum(visits), count(*), count(DISTINCT visits),
                       list_agg(firstName), string_agg(firstName, ','), min_by(firstName, visits),
                       max_by(firstName, visits), avg(visits)
                FROM users GROUP BY companyId");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new
                {
                    CompanyId = "A",
                    Min = (int?)10,
                    Max = (int?)30,
                    Sum = (long?)60,
                    Cnt = 3L,
                    DCnt = 3L,
                    Names = new List<string> { "a", "b", "c" },
                    Str = "a,b,c",
                    MnBy = "c",  // value at min visits (10)
                    MxBy = "a",  // value at max visits (30)
                    Avg = (double?)20.0
                }
            });
        }

        [Fact]
        public async Task BulkAggregateFilter_OnDifferentColumn()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", Visits = 5 });
            AddUser(new Entities.User { UserKey = 2, CompanyId = "A", Visits = 10 });
            AddUser(new Entities.User { UserKey = 3, CompanyId = "A", Visits = 20 });
            AddUser(new Entities.User { UserKey = 4, CompanyId = "A", Visits = 40 });
            await StartStream("INSERT INTO output SELECT companyId, sum(visits) FILTER (WHERE userkey > 2) FROM users GROUP BY companyId");
            await WaitForUpdate();
            // userkey > 2 -> uk 3,4 -> 20+40 = 60
            AssertCurrentDataEqual(new[] { new { CompanyId = "A", S = (long?)60 } });
        }

        [Fact]
        public async Task BulkAggregateTwoDifferentFiltersOnSameMeasure()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", Visits = 5 });
            AddUser(new Entities.User { UserKey = 2, CompanyId = "A", Visits = 10 });
            await StartStream("INSERT INTO output SELECT companyId, sum(visits) FILTER (WHERE visits > 3), sum(visits) FILTER (WHERE visits > 8) FROM users GROUP BY companyId");
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] { new { CompanyId = "A", S1 = (long?)15, S2 = (long?)10 } });
        }

        [Fact]
        public async Task BulkAggregateFilter_StatelessAndSharedMixed()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", Visits = 2 });
            AddUser(new Entities.User { UserKey = 2, CompanyId = "A", Visits = 5 });
            AddUser(new Entities.User { UserKey = 3, CompanyId = "A", Visits = 10 });
            await StartStream("INSERT INTO output SELECT companyId, sum(visits) FILTER (WHERE visits > 3), min(visits) FILTER (WHERE visits > 3) FROM users GROUP BY companyId");
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] { new { CompanyId = "A", Sum = (long?)15, Min = (int?)5 } });
        }

        [Fact]
        public async Task BulkAggregateDecimalSum_RetractToNull()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", Visits = null });
            AddUser(new Entities.User { UserKey = 2, CompanyId = "B", Visits = 5 });
            await StartStream("INSERT INTO output SELECT companyId, sum(CAST(visits AS DECIMAL)) FROM users GROUP BY companyId");
            await WaitForUpdate();

            var u = Users.First(x => x.UserKey == 2);
            u.Visits = null;
            AddOrUpdateUser(u);
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new { CompanyId = "A", Sum = (decimal?)null },
                new { CompanyId = "B", Sum = (decimal?)null },
            });
        }

        [Fact]
        public async Task BulkAggregateAvgDecimal()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", Visits = 5 });
            AddUser(new Entities.User { UserKey = 2, CompanyId = "A", Visits = 10 });
            await StartStream("INSERT INTO output SELECT companyId, avg(CAST(visits AS DECIMAL)) FROM users GROUP BY companyId");
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] { new { CompanyId = "A", Avg = (decimal?)7.5m } });
        }

        [Fact]
        public async Task BulkAggregateGroupByExpression_WithNull()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", Visits = 2 });
            AddUser(new Entities.User { UserKey = 2, CompanyId = "A", Visits = 4 });
            AddUser(new Entities.User { UserKey = 3, CompanyId = "A", Visits = null });
            await StartStream("INSERT INTO output SELECT visits % 2, count(*) FROM users GROUP BY visits % 2");
            await WaitForUpdate();
            var expected = Users.GroupBy(x => x.Visits.HasValue ? (long?)(x.Visits.Value % 2) : null)
                .OrderBy(g => g.Key)
                .Select(g => new { M = g.Key, Cnt = (long)g.Count() });
            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task BulkAggregateGroupByMultipleColumnsWithExpression()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", Visits = 2 });
            AddUser(new Entities.User { UserKey = 2, CompanyId = "A", Visits = 3 });
            AddUser(new Entities.User { UserKey = 3, CompanyId = "A", Visits = 4 });
            AddUser(new Entities.User { UserKey = 4, CompanyId = "B", Visits = 5 });
            await StartStream("INSERT INTO output SELECT companyId, visits % 2, count(*) FROM users GROUP BY companyId, visits % 2");
            await WaitForUpdate();
            var expected = Users.GroupBy(x => new { x.CompanyId, M = (long)(x.Visits!.Value % 2) })
                .OrderBy(g => g.Key.CompanyId).ThenBy(g => g.Key.M)
                .Select(g => new { g.Key.CompanyId, g.Key.M, Cnt = (long)g.Count() });
            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task BulkAggregateZeroMeasureGroupBy()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A" });
            AddUser(new Entities.User { UserKey = 2, CompanyId = "A" });
            AddUser(new Entities.User { UserKey = 3, CompanyId = "B" });
            await StartStream("INSERT INTO output SELECT companyId FROM users GROUP BY companyId");
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] { new { CompanyId = "A" }, new { CompanyId = "B" } });
        }

        [Fact]
        public async Task BulkAggregateDecimalMinMaxSum()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", Visits = 5 });
            AddUser(new Entities.User { UserKey = 2, CompanyId = "A", Visits = 10 });
            await StartStream("INSERT INTO output SELECT companyId, min(CAST(visits AS DECIMAL)), max(CAST(visits AS DECIMAL)), sum(CAST(visits AS DECIMAL)) FROM users GROUP BY companyId");
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] { new { CompanyId = "A", Mn = 5m, Mx = 10m, Sum = 15m } });
        }

        [Fact]
        public async Task BulkAggregateNetZeroGroupInBatch()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", Visits = 5 });
            await StartStream("INSERT INTO output SELECT companyId, sum(visits) FROM users GROUP BY companyId");
            await WaitForUpdate();

            // In a single batch: create a brand-new group then immediately delete it (net zero).
            var z = new Entities.User { UserKey = 100, CompanyId = "Z", Visits = 99 };
            AddUser(z);
            DeleteUser(z);
            await WaitForUpdate();

            // Z must not appear.
            AssertCurrentDataEqual(new[] { new { CompanyId = "A", Sum = (long?)5 } });
        }

        [Fact]
        public async Task BulkAggregateGroupByBooleanExpression()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", Visits = 2 });
            AddUser(new Entities.User { UserKey = 2, CompanyId = "A", Visits = 5 });
            AddUser(new Entities.User { UserKey = 3, CompanyId = "A", Visits = 8 });
            AddUser(new Entities.User { UserKey = 4, CompanyId = "A", Visits = 10 });
            await StartStream("INSERT INTO output SELECT visits > 5, count(*) FROM users GROUP BY visits > 5");
            await WaitForUpdate();
            var expected = Users.GroupBy(x => x.Visits!.Value > 5).OrderBy(g => g.Key)
                .Select(g => new { B = g.Key, Cnt = (long)g.Count() });
            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task BulkAggregateOverExpression()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", Visits = 5 });
            AddUser(new Entities.User { UserKey = 2, CompanyId = "A", Visits = 10 });
            await StartStream("INSERT INTO output SELECT companyId, sum(visits * 2), min(visits + userkey), max(visits + userkey) FROM users GROUP BY companyId");
            await WaitForUpdate();
            // sum(visits*2)=30; visits+userkey = {6,12} -> min 6, max 12
            AssertCurrentDataEqual(new[] { new { CompanyId = "A", S = 30L, Mn = 6L, Mx = 12L } });
        }

        [Fact]
        public async Task BulkAggregateGroupByExpression()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", Visits = 2 });
            AddUser(new Entities.User { UserKey = 2, CompanyId = "A", Visits = 4 });
            AddUser(new Entities.User { UserKey = 3, CompanyId = "A", Visits = 5 });
            AddUser(new Entities.User { UserKey = 4, CompanyId = "A", Visits = 7 });
            await StartStream("INSERT INTO output SELECT visits % 2, count(*), sum(visits) FROM users GROUP BY visits % 2");
            await WaitForUpdate();
            var expected = Users.GroupBy(x => (long)(x.Visits!.Value % 2)).OrderBy(g => g.Key)
                .Select(g => new { M = g.Key, Cnt = (long)g.Count(), Sum = (long)g.Sum(x => x.Visits!.Value) });
            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task BulkAggregateMinMaxString_WithChurn()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", FirstName = "banana" });
            AddUser(new Entities.User { UserKey = 2, CompanyId = "A", FirstName = "apple" });
            AddUser(new Entities.User { UserKey = 3, CompanyId = "A", FirstName = "cherry" });
            await StartStream("INSERT INTO output SELECT companyId, min(firstName), max(firstName) FROM users GROUP BY companyId");
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] { new { CompanyId = "A", Mn = "apple", Mx = "cherry" } });

            DeleteUser(Users.First(x => x.UserKey == 2)); // remove current min "apple"
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] { new { CompanyId = "A", Mn = "banana", Mx = "cherry" } });
        }

        [Fact]
        public async Task BulkAggregateCountDistinctString()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", FirstName = "x" });
            AddUser(new Entities.User { UserKey = 2, CompanyId = "A", FirstName = "x" });
            AddUser(new Entities.User { UserKey = 3, CompanyId = "A", FirstName = "y" });
            await StartStream("INSERT INTO output SELECT companyId, count(DISTINCT firstName) FROM users GROUP BY companyId");
            await WaitForUpdate();
            AssertCurrentDataEqual(new[] { new { CompanyId = "A", Cnt = 2L } });
        }

        [Fact]
        public async Task BulkAggregateMinBy_StringOrderBy()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", FirstName = "first", LastName = "zeta" });
            AddUser(new Entities.User { UserKey = 2, CompanyId = "A", FirstName = "second", LastName = "alpha" });
            AddUser(new Entities.User { UserKey = 3, CompanyId = "A", FirstName = "third", LastName = "mid" });
            await StartStream("INSERT INTO output SELECT companyId, min_by(firstName, lastName), max_by(firstName, lastName) FROM users GROUP BY companyId");
            await WaitForUpdate();
            // min lastName = "alpha" -> "second"; max lastName = "zeta" -> "first"
            AssertCurrentDataEqual(new[] { new { CompanyId = "A", MnBy = "second", MxBy = "first" } });
        }

        [Fact]
        public async Task BulkAggregateGroupByNullableColumn()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", Visits = 5 });
            AddUser(new Entities.User { UserKey = 2, CompanyId = "B", Visits = 5 });
            AddUser(new Entities.User { UserKey = 3, CompanyId = "C", Visits = null });
            AddUser(new Entities.User { UserKey = 4, CompanyId = "D", Visits = null });
            await StartStream("INSERT INTO output SELECT visits, count(*) FROM users GROUP BY visits");
            await WaitForUpdate();

            var expected = Users.GroupBy(x => x.Visits).OrderBy(x => x.Key)
                .Select(x => new { Visits = x.Key, Cnt = x.Count() });
            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task BulkAggregateAvg_RetractToNull()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", Visits = null });
            AddUser(new Entities.User { UserKey = 2, CompanyId = "B", Visits = 10 });
            await StartStream("INSERT INTO output SELECT companyId, avg(visits) FROM users GROUP BY companyId");
            await WaitForUpdate();

            var u = Users.First(x => x.UserKey == 2);
            u.Visits = null;
            AddOrUpdateUser(u);
            await WaitForUpdate();

            // Both groups all-null now -> avg null for both.
            AssertCurrentDataEqual(new[]
            {
                new { CompanyId = "A", Avg = (double?)null },
                new { CompanyId = "B", Avg = (double?)null },
            });
        }

        [Fact]
        public async Task BulkAggregateDecimalSum_ThroughCrash()
        {
            for (int g = 0; g < 5; g++)
                for (int m = 0; m < 3; m++)
                    AddUser(new Entities.User { UserKey = g * 3 + m, CompanyId = "co_" + g, Visits = (g + 1) * 10 + m });
            await StartStream("INSERT INTO output SELECT companyId, sum(CAST(visits AS DECIMAL)) FROM users GROUP BY companyId");
            await WaitForUpdate();
            await Crash();
            var u = Users.First(x => x.UserKey == 0);
            u.Visits = 999;
            AddOrUpdateUser(u);
            await WaitForUpdate();

            var expected = Users.GroupBy(x => x.CompanyId).OrderBy(x => x.Key)
                .Select(x => new { Key = x.Key, Sum = (decimal)x.Sum(y => (long)y.Visits!.Value) });
            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task BulkAggregateEmptyGlobal_AfterDeletingAllRows()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", Visits = 5 });
            AddUser(new Entities.User { UserKey = 2, CompanyId = "B", Visits = 7 });
            await StartStream("INSERT INTO output SELECT count(*), sum(visits), min(visits) FROM users");
            await WaitForUpdate();

            DeleteUser(Users.First(x => x.UserKey == 1));
            DeleteUser(Users.First(x => x.UserKey == 2));
            await WaitForUpdate();

            // SQL semantics for an empty global aggregate: one row, count=0, sum=null, min=null.
            AssertCurrentDataEqual(new[] { new { Cnt = 0L, Sum = (long?)null, Min = (long?)null } });
        }

        /// <summary>
        /// Regression: sum0 of an empty/all-null group must emit a zero TYPED to the declared return type, so
        /// it matches valued groups. A never-valued group has no value to infer a type from, so the zero is
        /// taken from the aggregate's output type (Substrait AggregateFunction.output_type). For an integer
        /// sum0, both a never-valued group and a valued-then-retracted group must be Int64 0 (previously the
        /// never-valued one leaked a Double 0.0 fallback into the Int64 column).
        /// </summary>
        [Fact]
        public async Task BulkAggregateSum0_AllNullGroup_TypeConsistency()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", Visits = null });
            AddUser(new Entities.User { UserKey = 2, CompanyId = "B", Visits = 5 });
            await StartStream("INSERT INTO output SELECT companyId, sum0(visits) FROM users GROUP BY companyId");
            await WaitForUpdate();

            var u = Users.First(x => x.UserKey == 2);
            u.Visits = null;
            AddOrUpdateUser(u);
            await WaitForUpdate();

            // Both groups are logically identical (one null-valued row each); both must be Int64 0.
            AssertCurrentDataEqual(new[]
            {
                new { CompanyId = "A", S = 0L },
                new { CompanyId = "B", S = 0L },
            });
        }

        /// <summary>
        /// Same as above for a Decimal sum0 (reached via CAST). Decimal previously fell through the planner's
        /// type mapping to AnyType, so the empty-group zero used the Double 0.0 floor and mismatched valued
        /// Decimal groups. With the Decimal arm added, a never-valued group must emit a Decimal 0 matching the
        /// retracted-to-zero group.
        /// </summary>
        [Fact]
        public async Task BulkAggregateSum0_AllNullGroup_Decimal_TypeConsistency()
        {
            AddUser(new Entities.User { UserKey = 1, CompanyId = "A", Visits = null });
            AddUser(new Entities.User { UserKey = 2, CompanyId = "B", Visits = 5 });
            await StartStream("INSERT INTO output SELECT companyId, sum0(CAST(visits AS DECIMAL)) FROM users GROUP BY companyId");
            await WaitForUpdate();

            var u = Users.First(x => x.UserKey == 2);
            u.Visits = null;
            AddOrUpdateUser(u);
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new { CompanyId = "A", S = 0m },
                new { CompanyId = "B", S = 0m },
            });
        }

        /// <summary>
        /// Deepest stress: nullable values (null-handling under churn), smaller pages (more leaves), and a
        /// crash/restore mid-stream (restore from an arbitrary random state). min/max/sum return null for an
        /// all-null group; count(*) still counts the rows.
        /// </summary>
        [Theory]
        [InlineData(11)]
        [InlineData(22)]
        public async Task BulkAggregateRandomizedStress_Nullable_WithCrash(int seed)
        {
            SetPageSizeBytes(160);
            var rnd = new Random(seed);
            int nextKey = 0;
            string NewName() => "n" + rnd.Next(1_000_000).ToString("D7");
            int? RandVisits() => rnd.Next(4) == 0 ? (int?)null : rnd.Next(1, 1000);

            for (int g = 0; g < 80; g++)
                for (int m = 0; m < 3; m++)
                    AddUser(new Entities.User { UserKey = nextKey++, CompanyId = "co_" + g.ToString("D4"), FirstName = NewName(), Visits = RandVisits() });

            await StartStream(@"
                INSERT INTO output
                SELECT companyId, min(visits), max(visits), sum(visits), count(DISTINCT visits), count(*), list_agg(firstName), string_agg(firstName, ',')
                FROM users
                GROUP BY companyId");

            void AssertExpected()
            {
                var expected = Users.GroupBy(x => x.CompanyId).OrderBy(x => x.Key).Select(x =>
                {
                    var nn = x.Where(y => y.Visits.HasValue).ToList();
                    return new
                    {
                        Key = x.Key,
                        Min = nn.Count > 0 ? (int?)nn.Min(y => y.Visits!.Value) : null,
                        Max = nn.Count > 0 ? (int?)nn.Max(y => y.Visits!.Value) : null,
                        // SQL SUM over an all-null group is null (not 0); sum0 is the always-0 variant.
                        Sum = nn.Count > 0 ? (long?)nn.Sum(y => (long)y.Visits!.Value) : null,
                        // COUNT(DISTINCT visits) excludes null.
                        DistinctVisits = nn.Select(y => y.Visits!.Value).Distinct().Count(),
                        Cnt = x.Count(),
                        Names = x.Select(y => y.FirstName).OrderBy(n => n).ToList(),
                        NamesStr = string.Join(",", x.Select(y => y.FirstName).OrderBy(n => n))
                    };
                });
                AssertCurrentDataEqual(expected);
            }

            await WaitForUpdate();
            AssertExpected();

            for (int round = 0; round < 20; round++)
            {
                int ops = rnd.Next(10, 40);
                for (int o = 0; o < ops; o++)
                {
                    var action = Users.Count < 40 ? 0 : rnd.Next(3);
                    if (action == 0)
                    {
                        int g = rnd.Next(80);
                        AddUser(new Entities.User { UserKey = nextKey++, CompanyId = "co_" + g.ToString("D4"), FirstName = NewName(), Visits = RandVisits() });
                    }
                    else if (action == 1)
                    {
                        var u = Users[rnd.Next(Users.Count)];
                        u.Visits = RandVisits();
                        u.FirstName = NewName();
                        AddOrUpdateUser(u);
                    }
                    else
                    {
                        DeleteUser(Users[rnd.Next(Users.Count)]);
                    }
                }

                if (round == 10)
                {
                    await Crash();
                }

                await WaitForUpdate();
                AssertExpected();
            }
        }

        /// <summary>
        /// Differential stress for a GLOBAL aggregate (no GROUP BY) under churn + crash. With grouping length
        /// 0 the shared-tree comparers take a dedicated branch (whole-leaf boundaries, single logical group),
        /// so the carry across many leaves is exercised differently than the grouped case. A row floor avoids
        /// the separate empty-global-aggregate question. Checks min/max/sum/count/count_distinct/list_agg/
        /// string_agg against LINQ over all rows.
        /// </summary>
        [Theory]
        [InlineData(4)]
        [InlineData(8)]
        public async Task BulkAggregateGlobalNoGroupBy_RandomizedStress_WithCrash(int seed)
        {
            SetPageSizeBytes(160);
            var rnd = new Random(seed);
            int nextKey = 0;
            string NewName() => "n" + rnd.Next(1_000_000).ToString("D7");
            int? RandVisits() => rnd.Next(4) == 0 ? (int?)null : rnd.Next(1, 1000);

            for (int i = 0; i < 200; i++)
                AddUser(new Entities.User { UserKey = nextKey++, CompanyId = "c", FirstName = NewName(), Visits = RandVisits() });

            await StartStream(@"
                INSERT INTO output
                SELECT min(visits), max(visits), sum(visits), count(DISTINCT visits), count(*), list_agg(firstName), string_agg(firstName, ',')
                FROM users");

            void AssertExpected()
            {
                var nn = Users.Where(y => y.Visits.HasValue).ToList();
                var row = new
                {
                    Min = nn.Count > 0 ? (int?)nn.Min(y => y.Visits!.Value) : null,
                    Max = nn.Count > 0 ? (int?)nn.Max(y => y.Visits!.Value) : null,
                    Sum = nn.Count > 0 ? (long?)nn.Sum(y => (long)y.Visits!.Value) : null,
                    DistinctVisits = nn.Select(y => y.Visits!.Value).Distinct().Count(),
                    Cnt = Users.Count,
                    Names = Users.Select(y => y.FirstName).OrderBy(n => n).ToList(),
                    NamesStr = string.Join(",", Users.Select(y => y.FirstName).OrderBy(n => n))
                };
                AssertCurrentDataEqual(new[] { row });
            }

            await WaitForUpdate();
            AssertExpected();

            for (int round = 0; round < 20; round++)
            {
                int ops = rnd.Next(10, 40);
                for (int o = 0; o < ops; o++)
                {
                    var action = Users.Count < 20 ? 0 : rnd.Next(3);
                    if (action == 0)
                    {
                        AddUser(new Entities.User { UserKey = nextKey++, CompanyId = "c", FirstName = NewName(), Visits = RandVisits() });
                    }
                    else if (action == 1)
                    {
                        var u = Users[rnd.Next(Users.Count)];
                        u.Visits = RandVisits();
                        u.FirstName = NewName();
                        AddOrUpdateUser(u);
                    }
                    else
                    {
                        DeleteUser(Users[rnd.Next(Users.Count)]);
                    }
                }

                if (round == 10)
                {
                    await Crash();
                }

                await WaitForUpdate();
                AssertExpected();
            }
        }

        /// <summary>
        /// Differential stress for min_by/max_by under churn + crash on a multi-leaf own-tree. Order-by
        /// (visits) values are globally unique so there are no ties — min_by/max_by are then unambiguous and
        /// can be checked against LINQ. visits is nullable (a null order-by member is ignored, like min);
        /// a group with no non-null order-by yields null. Deletes regularly remove the current min/max member
        /// so the result must move to the next member, and shifting leaf boundaries exercise the forward/
        /// backward carry.
        /// </summary>
        [Theory]
        [InlineData(7)]
        [InlineData(13)]
        public async Task BulkAggregateMinByMaxBy_RandomizedStress_WithCrash(int seed)
        {
            SetPageSizeBytes(160);
            var rnd = new Random(seed);
            int nextKey = 0;
            int nextVisit = 1; // monotonically increasing => globally unique, no order-by ties
            string NewName() => "n" + rnd.Next(1_000_000).ToString("D7");
            int? RandVisits() => rnd.Next(4) == 0 ? (int?)null : nextVisit++;

            for (int g = 0; g < 80; g++)
                for (int m = 0; m < 3; m++)
                    AddUser(new Entities.User { UserKey = nextKey++, CompanyId = "co_" + g.ToString("D4"), FirstName = NewName(), Visits = RandVisits() });

            await StartStream(@"
                INSERT INTO output
                SELECT companyId, min_by(firstName, visits), max_by(firstName, visits)
                FROM users
                GROUP BY companyId");

            void AssertExpected()
            {
                var expected = Users.GroupBy(x => x.CompanyId).OrderBy(x => x.Key).Select(x =>
                {
                    var nn = x.Where(y => y.Visits.HasValue).OrderBy(y => y.Visits!.Value).ToList();
                    return new
                    {
                        Key = x.Key,
                        MinByName = nn.Count > 0 ? nn.First().FirstName : null,
                        MaxByName = nn.Count > 0 ? nn.Last().FirstName : null,
                    };
                });
                AssertCurrentDataEqual(expected);
            }

            await WaitForUpdate();
            AssertExpected();

            for (int round = 0; round < 20; round++)
            {
                int ops = rnd.Next(10, 40);
                for (int o = 0; o < ops; o++)
                {
                    var action = Users.Count < 40 ? 0 : rnd.Next(3);
                    if (action == 0)
                    {
                        int g = rnd.Next(80);
                        AddUser(new Entities.User { UserKey = nextKey++, CompanyId = "co_" + g.ToString("D4"), FirstName = NewName(), Visits = RandVisits() });
                    }
                    else if (action == 1)
                    {
                        var u = Users[rnd.Next(Users.Count)];
                        u.Visits = RandVisits();
                        u.FirstName = NewName();
                        AddOrUpdateUser(u);
                    }
                    else
                    {
                        DeleteUser(Users[rnd.Next(Users.Count)]);
                    }
                }

                if (round == 10)
                {
                    await Crash();
                }

                await WaitForUpdate();
                AssertExpected();
            }
        }
    }
}
