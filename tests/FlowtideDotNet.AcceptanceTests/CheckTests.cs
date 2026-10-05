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
using FlowtideDotNet.AcceptanceTests.Internal;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests
{
    public class CheckTests : FlowtideAcceptanceBase
    {
        private static readonly TimeSpan IssueWaitTimeout = TimeSpan.FromSeconds(60);

        private const string UserKeyCheckName = "Userkey: {userkey} is too large";

        private const string UserKeyCheckSql = @"
            INSERT INTO output
            SELECT CHECK_VALUE(UserKey, UserKey < 900, 'Userkey: {userkey} is too large', userkey => UserKey)
            FROM users";

        private const string VisitsCheckSql = @"
            INSERT INTO output
            SELECT CHECK_VALUE(UserKey, Visits < 100, 'User {userkey} has {visits} visits', userkey => UserKey, visits => Visits)
            FROM users";

        public CheckTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        [Fact]
        public async Task CheckValue()
        {
            GenerateData();

            var listener = new CheckIssueListener();
            await StartStream(UserKeyCheckSql, failureListener: listener);

            await WaitForUpdate();

            AssertIssues(listener, ExpectedUserKeyIssues());
            // Listeners get the template, the tags carry the row values
            Assert.Equal(new[] { UserKeyCheckName }, listener.CheckNames());
            AssertCurrentDataEqual(Users.Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task CheckValueStartResetsThenRaisesInitialIssues()
        {
            GenerateData();

            var listener = new CheckIssueListener();
            await StartStream(UserKeyCheckSql, failureListener: listener);

            await WaitForUpdate();

            var checkId = Assert.Single(listener.CheckIds());
            Assert.Matches("^[0-9]+:0$", checkId);
            Assert.Equal(1, listener.ResetCount);
            Assert.Equal(0, listener.ResolvedCount);
            var expected = ExpectedUserKeyIssues();
            Assert.Equal(expected.Count, listener.RaisedCount);
            AssertIssues(listener, expected);
        }

        [Fact]
        public async Task CheckWithoutFailuresOnlyResets()
        {
            GenerateData();

            var listener = new CheckIssueListener();
            await StartStream(@"
                INSERT INTO output
                SELECT CHECK_VALUE(UserKey, UserKey > 0, 'Userkey is not positive')
                FROM users", failureListener: listener);

            await WaitForUpdate();

            Assert.Single(listener.CheckIds());
            Assert.Equal(1, listener.ResetCount);
            AssertIssues(listener, []);
        }

        [Fact]
        public async Task CheckTrue()
        {
            GenerateData();

            var listener = new CheckIssueListener();
            await StartStream(@"
               INSERT INTO output
                SELECT
                  UserKey
                FROM users
                WHERE CHECK_TRUE(userkey < 900, 'Userkey: {userkey} is too large', userkey => UserKey)
            ", failureListener: listener);

            await WaitForUpdate();

            AssertIssues(listener, ExpectedUserKeyIssues());
            Assert.Equal(new[] { UserKeyCheckName }, listener.CheckNames());
            AssertCurrentDataEqual(Users.Where(x => x.UserKey < 900).Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task CheckValueDeleteResolvesIssue()
        {
            GenerateData();

            var listener = new CheckIssueListener();
            await StartStream(UserKeyCheckSql, failureListener: listener);

            await WaitForUpdate();
            AssertIssues(listener, ExpectedUserKeyIssues());

            foreach (var user in Users.Where(x => x.UserKey >= 900).Take(3).ToList())
            {
                DeleteUser(user);
            }

            await WaitForUpdate();

            AssertIssues(listener, ExpectedUserKeyIssues());
            Assert.Equal(3, listener.ResolvedCount);
            Assert.Equal(1, listener.ResetCount);
            AssertCurrentDataEqual(Users.Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task CheckValueUpdateRaisesAndResolves()
        {
            GenerateData();
            var resolvedUser = Users[10];
            var changedUser = Users[11];
            var raisedUser = WithVisits(Users[12], 5);
            AddOrUpdateUser(WithVisits(resolvedUser, 500));
            AddOrUpdateUser(WithVisits(changedUser, 500));
            AddOrUpdateUser(raisedUser);

            var listener = new CheckIssueListener();
            await StartStream(VisitsCheckSql, failureListener: listener);

            await WaitForUpdate();
            AssertIssues(listener, ExpectedVisitsIssues());
            Assert.Equal(2, listener.RaisedCount);

            // Fail to pass, new tag values, pass to fail
            AddOrUpdateUser(WithVisits(resolvedUser, 5));
            AddOrUpdateUser(WithVisits(changedUser, 600));
            AddOrUpdateUser(WithVisits(raisedUser, 700));

            await WaitForUpdate();

            AssertIssues(listener, ExpectedVisitsIssues());
            Assert.Contains($"User {raisedUser.UserKey} has 700 visits", listener.ActiveIssues());
            Assert.Contains($"User {changedUser.UserKey} has 600 visits", listener.ActiveIssues());
            Assert.Equal(2, listener.ResolvedCount);
            Assert.Equal(4, listener.RaisedCount);
            Assert.Equal(1, listener.ResetCount);
            AssertCurrentDataEqual(Users.Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task CheckValueSameTagsResolvesWhenAllRowsAreGone()
        {
            GenerateData();
            var first = WithVisits(Users[10], 500);
            var second = WithVisits(Users[11], 500);
            AddOrUpdateUser(first);
            AddOrUpdateUser(second);
            AddOrUpdateUser(WithVisits(Users[12], 700));

            var listener = new CheckIssueListener();
            var status = new CheckStatusListener(listener);
            await StartStream(@"
                INSERT INTO output
                SELECT CHECK_VALUE(UserKey, Visits < 100, 'Too many visits: {visits}', visits => Visits)
                FROM users", failureListener: listener, statusListener: status);

            await WaitForUpdate();
            AssertIssues(listener, ["Too many visits: 500", "Too many visits: 700"]);
            Assert.Equal(2, listener.RaisedCount);
            AssertLatestStatus(status, activeIssues: 2, failingRows: 3);

            DeleteUser(first);
            await WaitForUpdate();

            // The other row still has the same tag values
            AssertIssues(listener, ["Too many visits: 500", "Too many visits: 700"]);
            Assert.Equal(0, listener.ResolvedCount);
            AssertLatestStatus(status, activeIssues: 2, failingRows: 2);

            DeleteUser(second);
            await WaitForUpdate();

            AssertIssues(listener, ["Too many visits: 700"]);
            Assert.Equal(1, listener.ResolvedCount);
            AssertLatestStatus(status, activeIssues: 1, failingRows: 1);
            AssertCurrentDataEqual(Users.Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task CheckValueStopStartReannouncesIssues()
        {
            GenerateData();

            var listener = new CheckIssueListener();
            await StartStream(UserKeyCheckSql, failureListener: listener);

            await WaitForUpdate();
            AssertIssues(listener, ExpectedUserKeyIssues());
            Assert.Equal(1, listener.ResetCount);

            await StopStream();

            DeleteUser(Users.First(x => x.UserKey >= 900));
            AddUser(new User() { UserKey = 5000 });

            await StartStream();

            // The restart snapshot replaces the view, then the changes follow
            await WaitForIssues(listener, ExpectedUserKeyIssues(), minResets: 2);
            Assert.Equal(2, listener.ResetCount);
            Assert.Equal(ExpectedUserKeyIssues().Count, ReadActiveIssuesGauge());
        }

        [Fact]
        public async Task CheckValueIdleRestartReannouncesIssues()
        {
            GenerateData();

            var listener = new CheckIssueListener();
            await StartStream(UserKeyCheckSql, failureListener: listener);

            await WaitForUpdate();
            var raised = listener.RaisedCount;

            await StopStream();
            await StartStream();

            // No new data, so no checkpoint publishes anything
            await WaitForIssues(listener, ExpectedUserKeyIssues(), minResets: 2);
            Assert.Equal(2, listener.ResetCount);
            Assert.Equal(raised + ExpectedUserKeyIssues().Count, listener.RaisedCount);
            Assert.Equal(0, listener.ResolvedCount);
        }

        [Fact]
        public async Task CheckValueCrashRecoveryReannouncesIssues()
        {
            GenerateData();

            var listener = new CheckIssueListener();
            var status = new CheckStatusListener(listener);
            await StartStream(UserKeyCheckSql, failureListener: listener, statusListener: status);

            await WaitForUpdate();
            AssertIssues(listener, ExpectedUserKeyIssues());
            Assert.Equal(1, listener.ResetCount);

            // May or may not be committed before the crash
            DeleteUser(Users.First(x => x.UserKey >= 900));

            await Crash();

            DeleteUser(Users.First(x => x.UserKey >= 900));
            AddUser(new User() { UserKey = 5000 });

            await WaitForIssues(listener, ExpectedUserKeyIssues(), minResets: 2);
            Assert.Equal(2, listener.ResetCount);
            Assert.Equal(ExpectedUserKeyIssues().Count, ReadActiveIssuesGauge());

            var expectedCount = ExpectedUserKeyIssues().Count;
            await WaitForLatestStatus(status, expectedCount, expectedCount);
            Assert.Empty(status.Violations());
        }

        [Fact]
        public async Task CheckActiveIssuesGaugeCountsActiveIssues()
        {
            GenerateData();

            var listener = new CheckIssueListener();
            await StartStream(UserKeyCheckSql, failureListener: listener);

            await WaitForUpdate();
            Assert.Equal(ExpectedUserKeyIssues().Count, ReadActiveIssuesGauge());
            Assert.Equal(ExpectedUserKeyIssues().Count, ReadFailingRowsGauge());

            foreach (var user in Users.Where(x => x.UserKey >= 900).Take(5).ToList())
            {
                DeleteUser(user);
            }

            await WaitForUpdate();

            AssertIssues(listener, ExpectedUserKeyIssues());
            Assert.Equal(ExpectedUserKeyIssues().Count, ReadActiveIssuesGauge());
            Assert.Equal(ExpectedUserKeyIssues().Count, ReadFailingRowsGauge());
        }

        [Fact]
        public async Task CheckFailedCheckpointsAreNotPublished()
        {
            EgressCrashOnCheckpoint(2);
            GenerateData();

            var listener = new CheckIssueListener();
            var status = new CheckStatusListener(listener);
            await StartStream(UserKeyCheckSql, failureListener: listener, statusListener: status);

            await WaitForUpdate();

            // Each restart resets to the empty committed state, failed epochs are discarded
            Assert.Equal(2, FailureNotificationCount);
            Assert.Equal(3, listener.ResetCount);
            Assert.Equal(ExpectedUserKeyIssues().Count, listener.RaisedCount);
            Assert.Equal(0, listener.ResolvedCount);
            AssertIssues(listener, ExpectedUserKeyIssues());
            AssertCurrentDataEqual(Users.Select(x => new { x.UserKey }));

            // Every start passes, only the committed epoch fails
            var expectedCount = ExpectedUserKeyIssues().Count;
            AssertStatuses(status, (0, 0), (0, 0), (0, 0), (expectedCount, expectedCount));
        }

        [Fact]
        public async Task CheckTrueNullConditionPassesAndFiltersRow()
        {
            GenerateData();
            AddOrUpdateUser(WithVisits(Users[10], 500));
            AddOrUpdateUser(WithVisits(Users[11], 600));
            AddOrUpdateUser(WithVisits(Users[12], null));
            AddOrUpdateUser(WithVisits(Users[13], 5));

            var listener = new CheckIssueListener();
            await StartStream(@"
                INSERT INTO output
                SELECT UserKey
                FROM users
                WHERE CHECK_TRUE(Visits < 100, 'User {userkey} has {visits} visits', userkey => UserKey, visits => Visits)", failureListener: listener);

            await WaitForUpdate();

            AssertIssues(listener, ExpectedVisitsIssues());
            AssertCurrentDataEqual(Users.Where(x => x.Visits != null && x.Visits < 100).Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task CheckTrueReturnsTrueOnlyForBooleanTrue()
        {
            GenerateData();
            AddOrUpdateUser(WithVisits(Users[10], 500));
            AddOrUpdateUser(WithVisits(Users[11], 600));
            AddOrUpdateUser(WithVisits(Users[12], null));
            AddOrUpdateUser(WithVisits(Users[13], 5));

            var listener = new CheckIssueListener();
            await StartStream(@"
                INSERT INTO output
                SELECT
                    UserKey,
                    CHECK_TRUE(Visits < 100, 'User {userkey} has {visits} visits', userkey => UserKey, visits => Visits) AS underLimit,
                    CHECK_TRUE(Visits, 'Visits is not a boolean') AS fromInteger
                FROM users", failureListener: listener);

            await WaitForUpdate();

            // Null and integer conditions return false without an issue
            AssertIssues(listener, ExpectedVisitsIssues());
            AssertCurrentDataEqual(Users.Select(x => new { x.UserKey, underLimit = x.Visits != null && x.Visits < 100, fromInteger = false }));
        }

        [Fact]
        public async Task CheckValueRowsWithEqualTagValuesShareOneIssue()
        {
            GenerateData();
            // The failing users share three companies, one of them null
            string?[] companies = [null, "company_a", "company_b"];
            var failingUsers = Users.Where(x => x.UserKey >= 900).ToList();
            for (int i = 0; i < failingUsers.Count; i++)
            {
                AddOrUpdateUser(WithCompany(failingUsers[i], companies[i % companies.Length]));
            }

            var listener = new CheckIssueListener();
            var status = new CheckStatusListener(listener);
            await StartStream(@"
                INSERT INTO output
                SELECT CHECK_VALUE(UserKey, UserKey < 900, 'Company {company} has a too large userkey', company => CompanyId)
                FROM users", failureListener: listener, statusListener: status);

            await WaitForUpdate();

            var failing = Users.Where(x => x.UserKey >= 900).ToList();
            var expected = failing
                .Select(x => $"Company {x.CompanyId ?? "null"} has a too large userkey")
                .Distinct()
                .ToList();
            // A null tag value is a value of its own
            Assert.Contains("Company null has a too large userkey", expected);
            Assert.Equal(3, expected.Count);
            Assert.True(expected.Count < failing.Count);
            AssertIssues(listener, expected);
            AssertLatestStatus(status, activeIssues: expected.Count, failingRows: failing.Count);
            AssertCurrentDataEqual(Users.Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task CheckValueUnderCaseOnlyRunsOnItsBranch()
        {
            GenerateData();

            var listener = new CheckIssueListener();
            await StartStream(@"
                INSERT INTO output
                SELECT
                    CASE WHEN UserKey % 2 = 0
                    THEN CHECK_VALUE(UserKey, UserKey < 900, 'Even userkey: {userkey} is too large', userkey => UserKey)
                    ELSE CHECK_VALUE(UserKey, UserKey < 950, 'Odd userkey: {userkey} is too large', userkey => UserKey)
                    END AS UserKey
                FROM users", failureListener: listener);

            await WaitForUpdate();

            var expected = Users
                .Where(x => x.UserKey % 2 == 0 && x.UserKey >= 900)
                .Select(x => $"Even userkey: {x.UserKey} is too large")
                .Concat(Users
                    .Where(x => x.UserKey % 2 != 0 && x.UserKey >= 950)
                    .Select(x => $"Odd userkey: {x.UserKey} is too large"))
                .ToList();
            AssertIssues(listener, expected);
            Assert.Equal(2, listener.CheckIds().Count);
            Assert.Equal(2, listener.ResetCount);
            Assert.Equal(
                new[] { "Even userkey: {userkey} is too large", "Odd userkey: {userkey} is too large" },
                listener.CheckNames().OrderBy(x => x, StringComparer.Ordinal));
            AssertCurrentDataEqual(Users.Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task CheckTrueInWhereOnlySeesRowsPassingOtherConjuncts()
        {
            GenerateData();

            var listener = new CheckIssueListener();
            await StartStream(@"
                INSERT INTO output
                SELECT UserKey
                FROM users
                WHERE CHECK_TRUE(UserKey < 900, 'Userkey: {userkey} is too large', userkey => UserKey) AND UserKey % 2 = 0", failureListener: listener);

            await WaitForUpdate();

            var expected = Users
                .Where(x => x.UserKey % 2 == 0 && x.UserKey >= 900)
                .Select(x => $"Userkey: {x.UserKey} is too large")
                .ToList();
            AssertIssues(listener, expected);
            AssertCurrentDataEqual(Users.Where(x => x.UserKey % 2 == 0 && x.UserKey < 900).Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task CheckTrueOverExistsSubquery()
        {
            GenerateData();

            var listener = new CheckIssueListener();
            await StartStream(@"
                INSERT INTO output
                SELECT u.UserKey
                FROM users u
                WHERE CHECK_TRUE(EXISTS (SELECT 1 FROM orders o WHERE o.UserKey = u.UserKey), 'User {userkey} has no orders', userkey => u.UserKey)", failureListener: listener);

            await WaitForUpdate();

            var withoutOrders = Users.Where(u => !Orders.Any(o => o.UserKey == u.UserKey)).ToList();
            Assert.NotEmpty(withoutOrders);
            AssertIssues(listener, withoutOrders.Select(u => $"User {u.UserKey} has no orders"));
            AssertCurrentDataEqual(Users.Where(u => Orders.Any(o => o.UserKey == u.UserKey)).Select(u => new { u.UserKey }));

            // A new order resolves the issue of its user
            var user = withoutOrders[0];
            AddOrUpdateOrder(new Order()
            {
                OrderKey = Orders.Max(o => o.OrderKey) + 1,
                UserKey = user.UserKey,
                Orderdate = DateTime.UtcNow,
                GuidVal = Guid.NewGuid(),
                Money = 1
            });
            await WaitForUpdate();

            AssertIssues(listener, withoutOrders.Skip(1).Select(u => $"User {u.UserKey} has no orders"));
            Assert.Equal(1, listener.ResolvedCount);
            AssertCurrentDataEqual(Users.Where(u => Orders.Any(o => o.UserKey == u.UserKey)).Select(u => new { u.UserKey }));
        }

        [Fact]
        public async Task CheckUsingGetTimestampRaisesAndResolvesAsTimeAdvances()
        {
            GenerateData();
            var user = Users[0];
            // Born a few seconds after the stream starts
            AddOrUpdateUser(WithBirthDate(user, DateTime.UtcNow.AddSeconds(20)));
            var futureIssue = $"User {user.UserKey} is born in the future";
            var bornIssue = $"User {user.UserKey} is already born";

            var listener = new CheckIssueListener();
            await StartStream($@"
                INSERT INTO output
                SELECT
                    UserKey,
                    CHECK_TRUE(CAST(BirthDate AS TIMESTAMP) > gettimestamp(), 'User {{userkey}} is already born', userkey => UserKey) AS Unborn
                FROM users
                WHERE UserKey = {user.UserKey} AND CHECK_TRUE(CAST(BirthDate AS TIMESTAMP) < gettimestamp(), 'User {{userkey}} is born in the future', userkey => UserKey)", failureListener: listener);

            await WaitForUpdate();

            // The where check filters the row out before the birth date
            Assert.Empty(GetActualRows());
            await WaitUntil(() => listener.ActiveIssues().SequenceEqual(new[] { futureIssue }));
            AssertIssues(listener, new[] { futureIssue });

            await WaitUntil(() => listener.ResolvedCount == 1 && listener.ActiveIssues().SequenceEqual(new[] { bornIssue }));
            AssertIssues(listener, new[] { bornIssue });
            Assert.Equal(1, listener.ResolvedCount);

            await WaitForUpdate();
            AssertCurrentDataEqual(new[] { new { user.UserKey, Unborn = false } });
        }

        [Fact]
        public async Task CheckValueInAggregateMeasure()
        {
            GenerateData();

            var listener = new CheckIssueListener();
            await StartStream(@"
                INSERT INTO output
                SELECT
                    CompanyId,
                    sum(CHECK_VALUE(UserKey, UserKey < 900, 'Userkey: {userkey} is too large', userkey => UserKey)) FILTER (WHERE UserKey % 2 = 0)
                FROM users
                GROUP BY CompanyId", failureListener: listener);

            await WaitForUpdate();

            // The measure filter guards the check
            var expected = Users
                .Where(x => x.UserKey % 2 == 0 && x.UserKey >= 900)
                .Select(x => $"Userkey: {x.UserKey} is too large")
                .ToList();
            AssertIssues(listener, expected);
            AssertCurrentDataEqual(Users
                .GroupBy(x => x.CompanyId)
                .OrderBy(x => x.Key)
                .Select(x => new { Key = x.Key, Sum = x.Where(y => y.UserKey % 2 == 0).Sum(y => y.UserKey) }));
        }

        [Fact]
        public async Task CheckValueWithWindowFunction()
        {
            GenerateData();
            // Duplicates in a fixed company and the null company
            AddOrUpdateUser(WithCompany(Users[10], "window_company"));
            AddOrUpdateUser(WithCompany(Users[11], "window_company"));
            AddOrUpdateUser(WithCompany(Users[12], "window_company"));
            AddOrUpdateUser(WithCompany(Users[13], null));
            AddOrUpdateUser(WithCompany(Users[14], null));

            var listener = new CheckIssueListener();
            await StartStream(@"
               INSERT INTO output
                SELECT CHECK_VALUE(UserKey, ROW_NUMBER() OVER (PARTITION BY CompanyId ORDER BY UserKey) = 1, 'Duplicate user: {userkey} found for company {companyId}', userkey, companyId)
                FROM users", failureListener: listener);

            await WaitForUpdate();

            var expected = Users.GroupBy(x => x.CompanyId)
                .SelectMany(x =>
                {
                    bool first = true;
                    List<string> output = new List<string>();

                    foreach (var row in x.OrderBy(y => y.UserKey))
                    {
                        if (first)
                        {
                            first = false;
                            continue;
                        }
                        output.Add($"Duplicate user: {row.UserKey} found for company {row.CompanyId ?? "null"}");
                    }
                    return output;
                }).ToList();

            Assert.Contains($"Duplicate user: {Users[11].UserKey} found for company window_company", expected);
            Assert.Contains($"Duplicate user: {Users[12].UserKey} found for company window_company", expected);
            Assert.Contains($"Duplicate user: {Users[14].UserKey} found for company null", expected);
            AssertIssues(listener, expected);
            AssertCurrentDataEqual(Users.Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task CheckTrueInJoinConditionRunsOnTheInputItUses()
        {
            GenerateData();

            var listener = new CheckIssueListener();
            await StartStream(@"
                INSERT INTO output
                SELECT o.OrderKey, u.UserKey
                FROM orders o
                INNER JOIN users u ON o.UserKey = u.UserKey AND CHECK_TRUE(o.OrderKey < 1900, 'Order {orderkey} is too large', orderkey => o.OrderKey)", failureListener: listener);

            await WaitForUpdate();

            AssertIssues(listener, ExpectedOrderIssues());
            AssertCurrentDataEqual(Orders.Where(x => x.OrderKey < 1900).Select(x => new { x.OrderKey, x.UserKey }));

            DeleteOrder(Orders.First(x => x.OrderKey >= 1900));

            await WaitForUpdate();

            AssertIssues(listener, ExpectedOrderIssues());
            Assert.Equal(1, listener.ResolvedCount);
        }

        [Fact]
        public async Task CheckInJoinConditionUsingBothInputsIsRejected()
        {
            GenerateData();

            var exception = await Assert.ThrowsAsync<NotSupportedException>(() => StartStream(@"
                INSERT INTO output
                SELECT o.OrderKey
                FROM orders o
                INNER JOIN users u ON o.UserKey = u.UserKey AND CHECK_TRUE(o.OrderKey > u.UserKey, 'bad order')", failureListener: new CheckIssueListener()));
            Assert.Contains("references both inputs", exception.Message);
        }

        [Fact]
        public async Task CheckWithoutFromIsRejected()
        {
            var exception = await Assert.ThrowsAsync<NotSupportedException>(() => StartStream(@"
                INSERT INTO output
                SELECT CHECK_VALUE(1, 1 = 2, 'never true') AS v", failureListener: new CheckIssueListener()));
            Assert.Contains("VALUES", exception.Message);
        }

        [Theory]
        [InlineData("SELECT CHECK_VALUE(UserKey, UserKey < 900, concat('Userkey: ', UserKey, ' is too large')) FROM users")]
        [InlineData("SELECT CHECK_VALUE(UserKey, UserKey < 900, 'Userkey: ' || FirstName) FROM users")]
        [InlineData("SELECT CHECK_VALUE(UserKey, UserKey < 900, FirstName) FROM users")]
        [InlineData("SELECT CHECK_VALUE(UserKey, UserKey < 900, NULL) FROM users")]
        [InlineData("SELECT UserKey FROM users WHERE CHECK_TRUE(UserKey < 900, concat('Userkey: ', UserKey))")]
        public async Task CheckComputedMessageIsRejected(string query)
        {
            GenerateData();

            var exception = await Assert.ThrowsAsync<NotSupportedException>(() => StartStream(
                "INSERT INTO output " + query,
                failureListener: new CheckIssueListener()));
            Assert.Contains("must be a string literal", exception.Message);
            Assert.Contains("{tag}", exception.Message);
        }

        [Fact]
        public async Task CheckInsideCheckMessageIsRejected()
        {
            GenerateData();

            var exception = await Assert.ThrowsAsync<NotSupportedException>(() => StartStream(@"
                INSERT INTO output
                SELECT CHECK_VALUE(UserKey, UserKey < 900, CHECK_VALUE('inner', UserKey < 10, 'inner message'))
                FROM users", failureListener: new CheckIssueListener()));
            Assert.Contains("must be a string literal", exception.Message);
        }

        [Fact]
        public async Task CheckInsideCheckTagIsRejected()
        {
            GenerateData();

            var exception = await Assert.ThrowsAsync<NotSupportedException>(() => StartStream(@"
                INSERT INTO output
                SELECT CHECK_VALUE(UserKey, UserKey < 900, 'Userkey {innerValue} is too large', innerValue => CHECK_VALUE(UserKey, UserKey < 10, 'inner message'))
                FROM users", failureListener: new CheckIssueListener()));
            Assert.Contains("tag arguments", exception.Message);
        }

        [Fact]
        public async Task CheckValueWithTags()
        {
            GenerateData();

            var listener = new CheckIssueListener();
            await StartStream(@"
               INSERT INTO output
                SELECT CHECK_VALUE(UserKey, UserKey < 900, 'Userkey: {userkey} is too large', userkey => UserKey)
                FROM users", failureListener: listener);

            await WaitForUpdate();

            AssertIssues(listener, ExpectedUserKeyIssues());
            AssertCurrentDataEqual(Users.Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task CheckValueWithTagsNullValue()
        {
            GenerateData();

            var listener = new CheckIssueListener();
            await StartStream(@"
               INSERT INTO output
                SELECT CHECK_VALUE(UserKey, UserKey < 900, 'Userkey: {userkey} is too large', userkey => null)
                FROM users", failureListener: listener);

            await WaitForUpdate();

            // Same tag values is one issue
            AssertIssues(listener, ["Userkey: null is too large"]);
            AssertCurrentDataEqual(Users.Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task CheckValueWithTagsNonNamed()
        {
            GenerateData();

            var listener = new CheckIssueListener();
            await StartStream(@"
               INSERT INTO output
                SELECT CHECK_VALUE(UserKey, UserKey < 900, 'Userkey: {userkey} is too large', userkey)
                FROM users", failureListener: listener);

            await WaitForUpdate();

            AssertIssues(listener, ExpectedUserKeyIssues());
            AssertCurrentDataEqual(Users.Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task CheckTrueWithTags()
        {
            GenerateData();

            var listener = new CheckIssueListener();
            await StartStream(@"
               INSERT INTO output
                SELECT
                  UserKey
                FROM users
                WHERE CHECK_TRUE(userkey < 900, 'Userkey: {userkey} is too large', userkey => UserKey)
            ", failureListener: listener);

            await WaitForUpdate();

            AssertIssues(listener, ExpectedUserKeyIssues());
            AssertCurrentDataEqual(Users.Where(x => x.UserKey < 900).Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task CheckTrueWithTagsNoNamed()
        {
            GenerateData();

            var listener = new CheckIssueListener();
            await StartStream(@"
               INSERT INTO output
                SELECT
                  UserKey
                FROM users
                WHERE CHECK_TRUE(userkey < 900, 'Userkey: {userkey} is too large', UserKey)
            ", failureListener: listener);

            await WaitForUpdate();

            AssertIssues(listener, ExpectedUserKeyIssues());
            AssertCurrentDataEqual(Users.Where(x => x.UserKey < 900).Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task CheckStatusPassingCheckReportsPassedAtStart()
        {
            GenerateData();

            var status = new CheckStatusListener();
            await StartStream(@"
                INSERT INTO output
                SELECT CHECK_VALUE(UserKey, UserKey > 0, 'Userkey is not positive')
                FROM users", statusListener: status);

            await WaitForUpdate();

            // The checkpoint left the counts at zero, so only the start reports
            var reported = Assert.Single(status.Statuses());
            Assert.True(reported.Passed);
            Assert.Equal(0, reported.ActiveIssues);
            Assert.Equal(0, reported.FailingRows);
            Assert.Equal("Userkey is not positive", reported.CheckName);
            Assert.Matches("^[0-9]+:0$", reported.CheckId);
            Assert.Equal(StreamName, reported.StreamName);
            Assert.Empty(status.Violations());
        }

        [Fact]
        public async Task CheckStatusFailingCheckReportsCounts()
        {
            GenerateData();

            var listener = new CheckIssueListener();
            var status = new CheckStatusListener(listener);
            await StartStream(UserKeyCheckSql, failureListener: listener, statusListener: status);

            await WaitForUpdate();

            // The empty committed state passes at start, the first checkpoint fails
            var expectedCount = ExpectedUserKeyIssues().Count;
            AssertStatuses(status, (0, 0), (expectedCount, expectedCount));
            var failed = status.Statuses()[^1];
            Assert.False(failed.Passed);
            Assert.Equal(UserKeyCheckName, failed.CheckName);
            Assert.Equal(Assert.Single(listener.CheckIds()), failed.CheckId);
        }

        [Fact]
        public async Task CheckStatusReportsEveryCountChange()
        {
            GenerateData();

            var listener = new CheckIssueListener();
            var status = new CheckStatusListener(listener);
            await StartStream(UserKeyCheckSql, failureListener: listener, statusListener: status);

            await WaitForUpdate();
            var initialCount = ExpectedUserKeyIssues().Count;
            AssertStatuses(status, (0, 0), (initialCount, initialCount));

            foreach (var user in Users.Where(x => x.UserKey >= 900).Take(3).ToList())
            {
                DeleteUser(user);
            }
            await WaitForUpdate();

            AssertStatuses(status, (0, 0), (initialCount, initialCount), (initialCount - 3, initialCount - 3));

            // Fail to pass
            foreach (var user in Users.Where(x => x.UserKey >= 900).ToList())
            {
                DeleteUser(user);
            }
            await WaitForUpdate();

            AssertStatuses(status, (0, 0), (initialCount, initialCount), (initialCount - 3, initialCount - 3), (0, 0));
            Assert.True(status.Statuses()[^1].Passed);
            AssertIssues(listener, []);

            // Pass to fail
            AddUser(new User() { UserKey = 5000 });
            await WaitForUpdate();

            AssertStatuses(status, (0, 0), (initialCount, initialCount), (initialCount - 3, initialCount - 3), (0, 0), (1, 1));
            Assert.False(status.Statuses()[^1].Passed);
            AssertIssues(listener, ["Userkey: 5000 is too large"]);
        }

        [Fact]
        public async Task CheckStatusNotReportedWhenCountsDoNotChange()
        {
            GenerateData();
            var movedUser = WithVisits(Users[10], 500);
            AddOrUpdateUser(movedUser);
            AddOrUpdateUser(WithVisits(Users[11], 500));

            var listener = new CheckIssueListener();
            var status = new CheckStatusListener(listener);
            await StartStream(VisitsCheckSql, failureListener: listener, statusListener: status);

            await WaitForUpdate();
            AssertStatuses(status, (0, 0), (2, 2));

            // A passing row changes
            AddOrUpdateUser(WithVisits(Users[20], 50));
            await WaitForUpdate();

            AssertStatuses(status, (0, 0), (2, 2));
            Assert.Equal(2, listener.RaisedCount);

            // An issue is replaced by another, the counts stay the same
            AddOrUpdateUser(WithVisits(movedUser, 600));
            await WaitForUpdate();

            AssertIssues(listener, ExpectedVisitsIssues());
            Assert.Equal(1, listener.ResolvedCount);
            Assert.Equal(3, listener.RaisedCount);
            AssertStatuses(status, (0, 0), (2, 2));
        }

        [Fact]
        public async Task CheckTaglessCheckHasOneIssueAndCountsEveryFailingRow()
        {
            GenerateData();

            var listener = new CheckIssueListener();
            var status = new CheckStatusListener(listener);
            await StartStream(@"
                INSERT INTO output
                SELECT CHECK_VALUE(UserKey, UserKey < 900, 'Userkey too large')
                FROM users", failureListener: listener, statusListener: status);

            await WaitForUpdate();

            var initialCount = ExpectedUserKeyIssues().Count;
            Assert.True(initialCount > 1);
            AssertIssues(listener, ["Userkey too large"]);
            Assert.Equal(1, listener.RaisedCount);
            AssertStatuses(status, (0, 0), (1, initialCount));

            foreach (var user in Users.Where(x => x.UserKey >= 900).Take(3).ToList())
            {
                DeleteUser(user);
            }
            await WaitForUpdate();

            // The issue stays active while any row fails
            AssertIssues(listener, ["Userkey too large"]);
            Assert.Equal(1, listener.RaisedCount);
            Assert.Equal(0, listener.ResolvedCount);
            AssertStatuses(status, (0, 0), (1, initialCount), (1, initialCount - 3));

            foreach (var user in Users.Where(x => x.UserKey >= 900).ToList())
            {
                DeleteUser(user);
            }
            await WaitForUpdate();

            AssertIssues(listener, []);
            Assert.Equal(1, listener.ResolvedCount);
            AssertStatuses(status, (0, 0), (1, initialCount), (1, initialCount - 3), (0, 0));
        }

        [Fact]
        public async Task CheckStatusWithoutIssueListener()
        {
            GenerateData();

            var status = new CheckStatusListener();
            await StartStream(UserKeyCheckSql, statusListener: status);

            await WaitForUpdate();
            var initialCount = ExpectedUserKeyIssues().Count;
            AssertStatuses(status, (0, 0), (initialCount, initialCount));
            Assert.All(status.Statuses(), x => Assert.Equal(UserKeyCheckName, x.CheckName));

            foreach (var user in Users.Where(x => x.UserKey >= 900).Take(3).ToList())
            {
                DeleteUser(user);
            }
            await WaitForUpdate();

            AssertStatuses(status, (0, 0), (initialCount, initialCount), (initialCount - 3, initialCount - 3));
            Assert.Equal(initialCount - 3, ReadActiveIssuesGauge());
            AssertCurrentDataEqual(Users.Select(x => new { x.UserKey }));
        }

        [Fact]
        public async Task CheckStatusRestartReportsCommittedStatus()
        {
            GenerateData();

            var listener = new CheckIssueListener();
            var status = new CheckStatusListener(listener);
            await StartStream(UserKeyCheckSql, failureListener: listener, statusListener: status);

            await WaitForUpdate();
            var initialCount = ExpectedUserKeyIssues().Count;
            AssertStatuses(status, (0, 0), (initialCount, initialCount));

            // An idle restart reports the committed counts once more
            await StopStream();
            await StartStream();
            await WaitForStatusCount(status, 3);

            AssertStatuses(status, (0, 0), (initialCount, initialCount), (initialCount, initialCount));

            await StopStream();
            DeleteUser(Users.First(x => x.UserKey >= 900));
            await StartStream();

            // The start reports the committed counts, then the change follows
            await WaitForStatusCount(status, 5);
            AssertStatuses(
                status,
                (0, 0),
                (initialCount, initialCount),
                (initialCount, initialCount),
                (initialCount, initialCount),
                (initialCount - 1, initialCount - 1));
            AssertIssues(listener, ExpectedUserKeyIssues());
            Assert.Equal(3, listener.ResetCount);
        }

        [Fact]
        public async Task CheckGaugesReportEveryCheckWithItsName()
        {
            GenerateData();
            AddOrUpdateUser(WithVisits(Users[10], 500));
            AddOrUpdateUser(WithVisits(Users[11], 600));
            AddOrUpdateUser(WithVisits(Users[12], 700));

            var listener = new CheckIssueListener();
            await StartStream(@"
                INSERT INTO output
                SELECT
                    CHECK_VALUE(UserKey, UserKey < 900, 'Userkey: {userkey} is too large', userkey => UserKey) AS UserKey,
                    CHECK_VALUE(Visits, Visits < 100, 'Too many visits') AS Visits
                FROM users", failureListener: listener);

            await WaitForUpdate();

            var userKeyCount = ExpectedUserKeyIssues().Count;
            var gauges = ReadCheckGauges();
            Assert.Equal(4, gauges.Count);

            var userKeyIssues = gauges[("flowtide_check_active_issues", UserKeyCheckName)];
            var userKeyRows = gauges[("flowtide_check_failing_rows", UserKeyCheckName)];
            var visitsIssues = gauges[("flowtide_check_active_issues", "Too many visits")];
            var visitsRows = gauges[("flowtide_check_failing_rows", "Too many visits")];
            Assert.Equal(userKeyCount, userKeyIssues.Value);
            Assert.Equal(userKeyCount, userKeyRows.Value);
            Assert.Equal(1, visitsIssues.Value);
            Assert.Equal(3, visitsRows.Value);

            // Both checks share one operator, the check id tells them apart
            var operatorName = Assert.IsType<string>(userKeyIssues.Tags["operator"]);
            Assert.Equal($"{operatorName}:0", userKeyIssues.Tags["check_id"]);
            Assert.Equal($"{operatorName}:1", visitsIssues.Tags["check_id"]);
            Assert.Equal(operatorName, visitsRows.Tags["operator"]);
            Assert.Equal(StreamName, userKeyRows.Tags["stream"]);
            Assert.Equal("Check", visitsIssues.Tags["title"]);
            Assert.Equal(
                new[] { $"{operatorName}:0", $"{operatorName}:1" },
                listener.CheckIds().OrderBy(x => x, StringComparer.Ordinal));

            var node = Assert.Single(GetDiagnosticsGraph().Nodes.Values, x => x.DisplayName == "Check");
            Assert.Single(node.Gauges, x => x.Name == "flowtide_check_active_issues");
            Assert.Single(node.Gauges, x => x.Name == "flowtide_check_failing_rows");
            AssertCurrentDataEqual(Users.Select(x => new { x.UserKey, x.Visits }));
        }

        private List<string> ExpectedUserKeyIssues()
        {
            return Users.Where(x => x.UserKey >= 900)
                .Select(x => $"Userkey: {x.UserKey} is too large")
                .ToList();
        }

        private List<string> ExpectedVisitsIssues()
        {
            return Users.Where(x => x.Visits >= 100)
                .Select(x => $"User {x.UserKey} has {x.Visits} visits")
                .ToList();
        }

        private List<string> ExpectedOrderIssues()
        {
            return Orders.Where(x => x.OrderKey >= 1900)
                .Select(x => $"Order {x.OrderKey} is too large")
                .ToList();
        }

        private static User WithVisits(User user, int? visits)
        {
            return new User()
            {
                UserKey = user.UserKey,
                Gender = user.Gender,
                FirstName = user.FirstName,
                LastName = user.LastName,
                NullableString = user.NullableString,
                CompanyId = user.CompanyId,
                Visits = visits,
                ManagerKey = user.ManagerKey,
                TrimmableNullableString = user.TrimmableNullableString,
                DoubleValue = user.DoubleValue,
                Active = user.Active,
                BirthDate = user.BirthDate
            };
        }

        private static User WithBirthDate(User user, DateTime birthDate)
        {
            var copy = WithVisits(user, user.Visits);
            copy.BirthDate = birthDate;
            return copy;
        }

        private static User WithCompany(User user, string? companyId)
        {
            var copy = WithVisits(user, user.Visits);
            copy.CompanyId = companyId;
            return copy;
        }

        private decimal ReadActiveIssuesGauge()
        {
            return ReadSingleCheckGauge("flowtide_check_active_issues");
        }

        private decimal ReadFailingRowsGauge()
        {
            return ReadSingleCheckGauge("flowtide_check_failing_rows");
        }

        /// <summary>
        /// Reads a check gauge from the diagnostics graph, which keeps one value per gauge.
        /// </summary>
        private decimal ReadSingleCheckGauge(string name)
        {
            var node = Assert.Single(GetDiagnosticsGraph().Nodes.Values, x => x.DisplayName == "Check");
            var gauge = Assert.Single(node.Gauges, x => x.Name == name);
            return Assert.Single(gauge.Dimensions.Values).Value;
        }

        /// <summary>
        /// Collects the check gauges of this stream with their tags, keyed by gauge and check name.
        /// </summary>
        private Dictionary<(string Gauge, string CheckName), (long Value, Dictionary<string, object?> Tags)> ReadCheckGauges()
        {
            var meterPrefix = $"flowtide.{StreamName}.operator.";
            var readings = new List<(string Gauge, long Value, Dictionary<string, object?> Tags)>();
            using (var meterListener = new MeterListener())
            {
                meterListener.InstrumentPublished = (instrument, listener) =>
                {
                    if (instrument.Meter.Name.StartsWith(meterPrefix, StringComparison.Ordinal) &&
                        (instrument.Name == "flowtide_check_active_issues" || instrument.Name == "flowtide_check_failing_rows"))
                    {
                        listener.EnableMeasurementEvents(instrument);
                    }
                };
                meterListener.SetMeasurementEventCallback<long>((instrument, value, tags, state) =>
                {
                    var tagValues = new Dictionary<string, object?>(StringComparer.Ordinal);
                    foreach (var tag in tags)
                    {
                        tagValues.Add(tag.Key, tag.Value);
                    }
                    readings.Add((instrument.Name, value, tagValues));
                });
                meterListener.Start();
                meterListener.RecordObservableInstruments();
            }

            var gauges = new Dictionary<(string Gauge, string CheckName), (long Value, Dictionary<string, object?> Tags)>();
            foreach (var reading in readings)
            {
                var checkName = Assert.IsType<string>(reading.Tags["check_name"]);
                gauges.Add((reading.Gauge, checkName), (reading.Value, reading.Tags));
            }
            return gauges;
        }

        private static void AssertIssues(CheckIssueListener listener, IEnumerable<string> expected)
        {
            Assert.Equal(expected.OrderBy(x => x, StringComparer.Ordinal), listener.ActiveIssues());
            Assert.Empty(listener.Violations());
        }

        private static void AssertStatuses(CheckStatusListener listener, params (long ActiveIssues, long FailingRows)[] expected)
        {
            Assert.Equal(expected, listener.Statuses().Select(x => (x.ActiveIssues, x.FailingRows)));
            Assert.Empty(listener.Violations());
        }

        private static void AssertLatestStatus(CheckStatusListener listener, long activeIssues, long failingRows)
        {
            var latest = listener.Statuses()[^1];
            Assert.Equal((activeIssues, failingRows), (latest.ActiveIssues, latest.FailingRows));
            Assert.Empty(listener.Violations());
        }

        /// <summary>
        /// Ticks the stream until the active issues match, for waits across a restart.
        /// </summary>
        private async Task WaitForIssues(CheckIssueListener listener, IEnumerable<string> expectedIssues, int minResets)
        {
            var expected = expectedIssues.OrderBy(x => x, StringComparer.Ordinal).ToList();
            await WaitUntil(() => listener.ResetCount >= minResets && expected.SequenceEqual(listener.ActiveIssues()));
            Assert.True(listener.ResetCount >= minResets, $"Expected at least {minResets} resets, got {listener.ResetCount}.");
            AssertIssues(listener, expected);
        }

        private async Task WaitForStatusCount(CheckStatusListener listener, int count)
        {
            await WaitUntil(() => listener.Count >= count);
            Assert.True(listener.Count >= count, $"Expected at least {count} statuses, got {listener.Count}.");
        }

        private async Task WaitForLatestStatus(CheckStatusListener listener, long activeIssues, long failingRows)
        {
            await WaitUntil(() =>
            {
                var statuses = listener.Statuses();
                return statuses.Count > 0 && statuses[^1].ActiveIssues == activeIssues && statuses[^1].FailingRows == failingRows;
            });
            AssertLatestStatus(listener, activeIssues, failingRows);
        }

        /// <summary>
        /// Ticks the stream until the condition holds or the wait times out.
        /// </summary>
        private async Task WaitUntil(Func<bool> condition)
        {
            var stopwatch = Stopwatch.StartNew();
            while (stopwatch.Elapsed < IssueWaitTimeout && !condition())
            {
                await SchedulerTick();
                await Task.Delay(10);
            }
        }
    }
}
