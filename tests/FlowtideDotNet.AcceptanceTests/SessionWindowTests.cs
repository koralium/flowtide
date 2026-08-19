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
    /// <summary>
    /// Session windows assign each row the start of its run.
    /// Expected values are recomputed from the current dataset.
    /// </summary>
    public class SessionWindowTests : FlowtideAcceptanceBase
    {
        public SessionWindowTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        private static readonly DateTime Base = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);

        // A 10 second gap between neighbouring rows.
        private const string SessionQuery = @"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                session_window(10, 'SECOND') OVER (PARTITION BY CompanyId ORDER BY BirthDate)
            FROM users";

        public record SessionResult(string? companyId, int userkey, DateTime? sessionStart);

        private void AddUser(string companyId, int userKey, int secondsFromBase)
        {
            AddOrUpdateUser(new User()
            {
                UserKey = userKey,
                CompanyId = companyId,
                BirthDate = Base.AddSeconds(secondsFromBase)
            });
        }

        private void AddUserWithoutDate(string companyId, int userKey)
        {
            AddOrUpdateUser(new User()
            {
                UserKey = userKey,
                CompanyId = companyId,
                BirthDate = null
            });
        }

        private void RemoveUser(int userKey)
        {
            DeleteUser(Users.First(x => x.UserKey == userKey));
        }

        /// <summary>
        /// The oracle, cut whenever the distance exceeds the gap.
        /// </summary>
        private List<SessionResult> ExpectedSessions(int gapSeconds = 10)
        {
            var gap = TimeSpan.FromSeconds(gapSeconds);
            return Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var output = new List<SessionResult>();
                    DateTime? sessionStart = null;
                    DateTime? previous = null;

                    // Null timestamps sort first and carry nothing.
                    foreach (var user in g.Where(x => x.BirthDate == null).OrderBy(x => x.UserKey))
                    {
                        output.Add(new SessionResult(user.CompanyId, user.UserKey, null));
                    }

                    foreach (var user in g.Where(x => x.BirthDate != null).OrderBy(x => x.BirthDate).ThenBy(x => x.UserKey))
                    {
                        var current = user.BirthDate!.Value;
                        if (previous == null || current - previous.Value > gap)
                        {
                            sessionStart = current;
                        }
                        previous = current;
                        output.Add(new SessionResult(user.CompanyId, user.UserKey, sessionStart));
                    }
                    return output;
                }).ToList();
        }

        [Fact]
        public async Task AppendExtendingAndStartingSessions()
        {
            // Two sessions: 0,5,10 then 100,105.
            AddUser("1", 1, 0);
            AddUser("1", 2, 5);
            AddUser("1", 3, 10);
            AddUser("1", 4, 100);
            AddUser("1", 5, 105);

            await StartStream(SessionQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());

            // Within the gap, extends the second session.
            AddUser("1", 6, 110);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());

            // Beyond the gap, starts a third session.
            AddUser("1", 7, 200);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());
        }

        [Fact]
        public async Task InsertBridgingTwoSessions()
        {
            // 0,5 and 30,35 are separate, 25 seconds apart.
            AddUser("1", 1, 0);
            AddUser("1", 2, 5);
            AddUser("1", 3, 30);
            AddUser("1", 4, 35);

            await StartStream(SessionQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());

            // 5 to 20 is 15, still two sessions.
            AddUser("1", 5, 20);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());

            // 12 bridges 5 and 20 into one session.
            AddUser("1", 6, 12);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());
        }

        [Fact]
        public async Task InsertEarlierThanSessionStartMovesIt()
        {
            AddUser("1", 1, 50);
            AddUser("1", 2, 55);
            AddUser("1", 3, 60);

            await StartStream(SessionQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());

            // Within the gap of 50, the start moves back.
            AddUser("1", 4, 45);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());
        }

        [Fact]
        public async Task DeleteOpeningGapSplitsSession()
        {
            // One session: 0,8,16,24.
            AddUser("1", 1, 0);
            AddUser("1", 2, 8);
            AddUser("1", 3, 16);
            AddUser("1", 4, 24);

            await StartStream(SessionQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());

            // Removing 8 leaves 0 and 16, so it splits.
            RemoveUser(2);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());
        }

        [Fact]
        public async Task DeleteFirstRowOfSessionMovesStart()
        {
            AddUser("1", 1, 0);
            AddUser("1", 2, 5);
            AddUser("1", 3, 10);

            await StartStream(SessionQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());

            // The start moves to 5, every row changes.
            RemoveUser(1);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());
        }

        [Fact]
        public async Task TransitiveThreeWayMergeFromOneInsert()
        {
            // Three sessions, each further apart than the gap.
            AddUser("1", 1, 0);
            AddUser("1", 2, 20);
            AddUser("1", 3, 40);

            await StartStream(SessionQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());

            // 10 and 30 together collapse all three.
            AddUser("1", 4, 10);
            AddUser("1", 5, 30);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());
        }

        [Fact]
        public async Task ExactlyAtGapBoundary()
        {
            // Exactly the gap joins, more starts a new one.
            AddUser("1", 1, 0);
            AddUser("1", 2, 10);
            AddUser("1", 3, 21);

            await StartStream(SessionQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());
        }

        [Fact]
        public async Task NullTimestampsBelongToNoSession()
        {
            AddUser("1", 1, 0);
            AddUser("1", 2, 5);
            AddUserWithoutDate("1", 3);
            AddUserWithoutDate("1", 4);

            await StartStream(SessionQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());

            // A null row must not split the run.
            AddUser("1", 5, 10);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());
        }

        [Fact]
        public async Task SessionsAreIndependentPerPartition()
        {
            // Sessions must not bleed across partitions.
            for (int i = 0; i < 5; i++)
            {
                AddUser("1", i, i * 5);
                AddUser("2", 100 + i, i * 50);
            }

            await StartStream(SessionQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());

            AddUser("2", 200, 25);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());
        }

        [Fact]
        public async Task ChangeAtHeadOfLongPartitionRelabelsTail()
        {
            // One long session, 200 rows five seconds apart.
            for (int i = 0; i < 200; i++)
            {
                AddUser("1", i, i * 5);
            }

            await StartStream(SessionQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());

            // Inserting before the head moves every start.
            AddUser("1", 500, -3);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());
        }

        /// <summary>
        /// Three sessions with a large middle one.
        /// A full re emission would be around 120 rows.
        /// </summary>
        private void AddThreeSessionsWithLargeMiddle()
        {
            for (int i = 0; i < 5; i++)
            {
                AddUser("1", i, i * 5);
            }
            for (int i = 0; i < 50; i++)
            {
                AddUser("1", 100 + i, 1000 + i * 5);
            }
            for (int i = 0; i < 5; i++)
            {
                AddUser("1", 200 + i, 2000 + i * 5);
            }
        }

        [Fact]
        public async Task ChangeInOneSessionLeavesOtherSessionsAlone()
        {
            AddThreeSessionsWithLargeMiddle();

            await StartStream(SessionQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());

            // An append changes nothing stored, one row out.
            ResetChangeRowsRecieved();
            AddUser("1", 500, 1000 + 50 * 5);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());
            Assert.Equal(1, ChangeRowsRecieved);
        }

        /// <summary>
        /// Two changes far apart in the same partition.
        /// The rows between them must not be re emitted.
        /// </summary>
        [Fact]
        public async Task TwoChangesFarApartInOnePartition()
        {
            AddThreeSessionsWithLargeMiddle();

            await StartStream(SessionQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());

            ResetChangeRowsRecieved();
            // Two appends in one batch, one new row each.
            AddUser("1", 500, 25);
            AddUser("1", 501, 2025);
            await WaitForUpdate();

            AssertCurrentDataEqual(ExpectedSessions());
            Assert.Equal(2, ChangeRowsRecieved);
        }

        /// <summary>
        /// A cascade must not swallow the second marker.
        /// </summary>
        [Fact]
        public async Task SecondChangeAppliedWhenFirstCascades()
        {
            AddThreeSessionsWithLargeMiddle();

            await StartStream(SessionQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());

            ResetChangeRowsRecieved();
            // The first session's start moves, all five rows change.
            AddUser("1", 500, -5);
            AddUser("1", 501, 2025);
            await WaitForUpdate();

            AssertCurrentDataEqual(ExpectedSessions());
            // Five retracts, five inserts, plus two new rows.
            Assert.Equal(12, ChangeRowsRecieved);
        }

        public record SessionAggResult(string? companyId, long count, DateTime? windowStart, DateTime? windowEnd);

        // GROUP BY SESSION(...) exposes the grouping as window_start.
        private const string SessionGroupByQuery = @"
            INSERT INTO output
            SELECT
                CompanyId,
                count(*) AS visits,
                window_start,
                SESSION_END(BirthDate, 10, 'SECOND') AS endtime
            FROM users
            GROUP BY CompanyId, SESSION(BirthDate, 10, 'SECOND')";

        // The same query using the accessor instead.
        private const string SessionGroupByAccessorQuery = @"
            INSERT INTO output
            SELECT
                CompanyId,
                count(*) AS visits,
                SESSION_START(BirthDate, 10, 'SECOND') AS starttime,
                SESSION_END(BirthDate, 10, 'SECOND') AS endtime
            FROM users
            GROUP BY CompanyId, SESSION(BirthDate, 10, 'SECOND')";

        private List<SessionAggResult> ExpectedSessionAggregate(int gapSeconds = 10)
        {
            var gap = TimeSpan.FromSeconds(gapSeconds);
            var results = new List<SessionAggResult>();
            foreach (var g in Users.Where(x => x.BirthDate != null).GroupBy(x => x.CompanyId))
            {
                DateTime? sessionStart = null;
                DateTime? previous = null;
                DateTime last = default;
                long count = 0;

                foreach (var user in g.OrderBy(x => x.BirthDate).ThenBy(x => x.UserKey))
                {
                    var current = user.BirthDate!.Value;
                    if (previous == null || current - previous.Value > gap)
                    {
                        if (sessionStart != null)
                        {
                            results.Add(new SessionAggResult(g.Key, count, sessionStart, last + gap));
                        }
                        sessionStart = current;
                        count = 0;
                    }
                    previous = current;
                    last = current;
                    count++;
                }
                if (sessionStart != null)
                {
                    results.Add(new SessionAggResult(g.Key, count, sessionStart, last + gap));
                }
            }
            return results;
        }

        [Fact]
        public async Task GroupBySessionAggregates()
        {
            // Two sessions in company 1, one in company 2.
            AddUser("1", 1, 0);
            AddUser("1", 2, 5);
            AddUser("1", 3, 10);
            AddUser("1", 4, 100);
            AddUser("1", 5, 105);
            AddUser("2", 10, 0);
            AddUser("2", 11, 3);

            await StartStream(SessionGroupByQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessionAggregate());
        }

        public record SessionCountResult(string? companyId, long count, DateTime? windowStart);

        /// <summary>
        /// The minimal form, the aggregate has no max measure.
        /// </summary>
        [Fact]
        public async Task GroupBySessionWithoutSessionEnd()
        {
            const string query = @"
            INSERT INTO output
            SELECT
                CompanyId,
                count(*) AS visits,
                window_start
            FROM users
            GROUP BY CompanyId, SESSION(BirthDate, 10, 'SECOND')";

            AddUser("1", 1, 0);
            AddUser("1", 2, 5);
            AddUser("1", 3, 100);

            await StartStream(query);
            await WaitForUpdate();
            AssertCurrentDataEqual(new List<SessionCountResult>()
            {
                new SessionCountResult("1", 2, Base),
                new SessionCountResult("1", 1, Base.AddSeconds(100))
            });
        }

        [Fact]
        public async Task GroupBySessionMergeCollapsesTwoGroupsIntoOne()
        {
            AddUser("1", 1, 0);
            AddUser("1", 2, 5);
            AddUser("1", 3, 30);
            AddUser("1", 4, 35);

            await StartStream(SessionGroupByQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessionAggregate());

            // Bridging retracts both rows and emits one merged.
            AddUser("1", 5, 20);
            AddUser("1", 6, 12);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessionAggregate());
        }

        [Fact]
        public async Task GroupBySessionSplitOnDelete()
        {
            AddUser("1", 1, 0);
            AddUser("1", 2, 8);
            AddUser("1", 3, 16);
            AddUser("1", 4, 24);

            await StartStream(SessionGroupByQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessionAggregate());

            // Removing the middle row splits one row into two.
            RemoveUser(2);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessionAggregate());
        }

        [Fact]
        public async Task SessionStartAccessorMatchesWindowStart()
        {
            AddUser("1", 1, 0);
            AddUser("1", 2, 5);
            AddUser("1", 3, 100);

            await StartStream(SessionGroupByAccessorQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessionAggregate());
        }

        [Theory]
        // A different timestamp column than the grouping uses
        [InlineData("SESSION_START(FirstName, 10, 'SECOND')")]
        [InlineData("SESSION_END(FirstName, 10, 'SECOND')")]
        // A different gap than the grouping uses
        [InlineData("SESSION_START(BirthDate, 30, 'SECOND')")]
        [InlineData("SESSION_END(BirthDate, 30, 'SECOND')")]
        [InlineData("SESSION_START(BirthDate, 10, 'MINUTE')")]
        [InlineData("SESSION_END(BirthDate, 10, 'MINUTE')")]
        public async Task SessionAccessorArgumentsMustMatchTheGrouping(string accessor)
        {
            AddUser("1", 1, 0);

            var query = $@"
            INSERT INTO output
            SELECT CompanyId, count(*) AS visits, {accessor} AS value
            FROM users
            GROUP BY CompanyId, SESSION(BirthDate, 10, 'SECOND')";

            var exception = await Assert.ThrowsAnyAsync<Exception>(() => StartStream(query));
            Assert.Contains("does not match the 'SESSION(...)' in the GROUP BY", exception.Message);
        }

        [Theory]
        [InlineData("SESSION_START(BirthDate, 10, 'SECOND')")]
        [InlineData("SESSION_END(BirthDate, 10, 'SECOND')")]
        public async Task SessionAccessorRequiresASessionGrouping(string accessor)
        {
            AddUser("1", 1, 0);

            var query = $@"
            INSERT INTO output
            SELECT CompanyId, count(*) AS visits, {accessor} AS value
            FROM users
            GROUP BY CompanyId";

            var exception = await Assert.ThrowsAnyAsync<Exception>(() => StartStream(query));
            Assert.Contains("requires a 'SESSION(...)' expression in the GROUP BY", exception.Message);
        }

        [Fact]
        public async Task OnlyOneSessionAllowedInAGroupBy()
        {
            AddUser("1", 1, 0);

            const string query = @"
            INSERT INTO output
            SELECT CompanyId, count(*) AS visits
            FROM users
            GROUP BY CompanyId, SESSION(BirthDate, 10, 'SECOND'), SESSION(BirthDate, 30, 'SECOND')";

            var exception = await Assert.ThrowsAnyAsync<Exception>(() => StartStream(query));
            Assert.Contains("Only one SESSION expression is supported", exception.Message);
        }

        [Fact]
        public async Task SessionsSurviveCrashRecovery()
        {
            AddUser("1", 1, 0);
            AddUser("1", 2, 5);
            AddUser("1", 3, 100);

            await StartStream(SessionQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());

            await Crash();

            // A merge across the crash uses the restored values.
            AddUser("1", 4, 95);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());
        }

        [Fact]
        public async Task DuplicateTimestampsShareSessionAndSurviveSingleDelete()
        {
            AddUser("1", 1, 0);
            AddUser("1", 2, 5);
            AddUser("1", 3, 5);
            AddUser("1", 4, 10);

            await StartStream(SessionQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());

            RemoveUser(2);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());
        }

        [Fact]
        public async Task GlobalPartitionAcrossAllRows()
        {
            const string globalQuery = @"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                session_window(10, 'SECOND') OVER (ORDER BY BirthDate)
            FROM users";

            AddUser("1", 1, 0);
            AddUser("2", 2, 5);
            AddUser("1", 3, 20);

            await StartStream(globalQuery);
            await WaitForUpdate();

            // Spelled out, the oracle groups by company.
            // 0 and 5 share a session, 20 starts its own.
            AssertCurrentDataEqual(new List<SessionResult>()
            {
                new SessionResult("1", 1, Base),
                new SessionResult("2", 2, Base),
                new SessionResult("1", 3, Base.AddSeconds(20))
            });
        }

        [Fact]
        public async Task DeleteIsolatedSingleRowSession()
        {
            AddUser("1", 1, 0);
            AddUser("1", 2, 100);

            await StartStream(SessionQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());

            RemoveUser(2);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());
        }

        public record MultiSessionResult(string? companyId, int userkey, DateTime? session5s, DateTime? session20s);

        private List<MultiSessionResult> ExpectedMultiSessions()
        {
            var sessions5s = ExpectedSessions(5).ToDictionary(x => (x.companyId, x.userkey), x => x.sessionStart);
            var sessions20s = ExpectedSessions(20).ToDictionary(x => (x.companyId, x.userkey), x => x.sessionStart);

            return Users.Select(u => new MultiSessionResult(
                u.CompanyId,
                u.UserKey,
                sessions5s[(u.CompanyId, u.UserKey)],
                sessions20s[(u.CompanyId, u.UserKey)]
            )).ToList();
        }

        [Fact]
        public async Task MultipleSessionWindowsInSameQuery()
        {
            const string multiWindowQuery = @"
                INSERT INTO output
                SELECT
                    CompanyId,
                    UserKey,
                    session_window(5, 'SECOND') OVER (PARTITION BY CompanyId ORDER BY BirthDate) as s1,
                    session_window(20, 'SECOND') OVER (PARTITION BY CompanyId ORDER BY BirthDate) as s2
                FROM users";

            AddUser("1", 1, 0);
            AddUser("1", 2, 8);
            AddUser("1", 3, 16);
            AddUser("1", 4, 40);

            await StartStream(multiWindowQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedMultiSessions());

            AddUser("1", 5, 30);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedMultiSessions());

            RemoveUser(2);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedMultiSessions());
        }
    }
}
