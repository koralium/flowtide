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
    /// Session windows assign each row the start of the gap free run it belongs to.
    /// A run grows, merges and splits as rows are inserted and deleted, so the expected
    /// values are recomputed from the current dataset rather than hardcoded.
    /// </summary>
    public class SessionWindowTests : FlowtideAcceptanceBase
    {
        public SessionWindowTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        private static readonly DateTime Base = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);

        // A 10 second gap, so rows further apart than 10 seconds start a new session.
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
        /// The oracle: group by partition, order by the timestamp, cut whenever the distance to the
        /// previous row is greater than the gap. Rows without a timestamp belong to no session.
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

                    // Null timestamps sort first and carry nothing forward.
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
            // Two sessions in company 1: 0,5,10 then 100,105.
            AddUser("1", 1, 0);
            AddUser("1", 2, 5);
            AddUser("1", 3, 10);
            AddUser("1", 4, 100);
            AddUser("1", 5, 105);

            await StartStream(SessionQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());

            // Append within the gap of the last row, extends the second session.
            AddUser("1", 6, 110);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());

            // Append beyond the gap, starts a third session.
            AddUser("1", 7, 200);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());
        }

        [Fact]
        public async Task InsertBridgingTwoSessions()
        {
            // 0,5 and 30,35 are separate, the distance from 5 to 30 is 25 seconds.
            AddUser("1", 1, 0);
            AddUser("1", 2, 5);
            AddUser("1", 3, 30);
            AddUser("1", 4, 35);

            await StartStream(SessionQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());

            // 20 is within 10 of neither... 5->20 is 15. Still two sessions, now 0,5 and 20,30,35.
            AddUser("1", 5, 20);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());

            // 12 bridges 5 and 20, collapsing everything into one session starting at 0.
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

            // Within the gap of 50, so the whole session start moves back to 45.
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

            // Removing 8 leaves 0 and 16 which are 16 apart, so the session splits.
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

            // The session start moves from 0 to 5, so every remaining row changes.
            RemoveUser(1);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());
        }

        [Fact]
        public async Task TransitiveThreeWayMergeFromOneInsert()
        {
            // Three sessions, each pair separated by more than the gap.
            AddUser("1", 1, 0);
            AddUser("1", 2, 20);
            AddUser("1", 3, 40);

            await StartStream(SessionQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());

            // 10 bridges 0 and 20, 30 bridges 20 and 40. Added together they collapse all three.
            AddUser("1", 4, 10);
            AddUser("1", 5, 30);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());
        }

        [Fact]
        public async Task ExactlyAtGapBoundary()
        {
            // Exactly the gap joins the session, one tick more starts a new one.
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

            // A null row must not split the run of real timestamps around it.
            AddUser("1", 5, 10);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());
        }

        [Fact]
        public async Task SessionsAreIndependentPerPartition()
        {
            // Same timestamps in two companies, sessions must not bleed across partitions.
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

            // Inserting before the head moves the start of every row in the session.
            AddUser("1", 500, -3);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());
        }

        /// <summary>
        /// Three sessions in one partition, with a large middle one. A full re emission would be
        /// around 120 rows, so the change counts below are what separate an incremental scan from a
        /// scan that recomputes and re emits everything it walks over.
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

            // Appending to the end of the middle session gives the new row the existing start, so
            // no stored row changes and exactly one row should reach the sink.
            ResetChangeRowsRecieved();
            AddUser("1", 500, 1000 + 50 * 5);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());
            Assert.Equal(1, ChangeRowsRecieved);
        }

        /// <summary>
        /// Two changes far apart in the same partition. The scan cannot stop between them because a
        /// marker is still outstanding, but it must not re emit the rows it walks over, and it must
        /// still apply the second change. A wrong stability declaration shows up here as either a
        /// row count blow up or a silently missing update.
        /// </summary>
        [Fact]
        public async Task TwoChangesFarApartInOnePartition()
        {
            AddThreeSessionsWithLargeMiddle();

            await StartStream(SessionQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());

            ResetChangeRowsRecieved();
            // Extend the first session and the last session in the same batch. Both are appends
            // within the gap, so each contributes exactly one new row and nothing else moves.
            AddUser("1", 500, 25);
            AddUser("1", 501, 2025);
            await WaitForUpdate();

            AssertCurrentDataEqual(ExpectedSessions());
            Assert.Equal(2, ChangeRowsRecieved);
        }

        /// <summary>
        /// The first change cascades through its own session while the second sits far away. The
        /// cascade must not swallow the second marker.
        /// </summary>
        [Fact]
        public async Task SecondChangeAppliedWhenFirstCascades()
        {
            AddThreeSessionsWithLargeMiddle();

            await StartStream(SessionQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedSessions());

            ResetChangeRowsRecieved();
            // Inserting before the first session's start moves it, so all five of its rows are
            // retracted and re emitted. The append to the last session must still be applied.
            AddUser("1", 500, -5);
            AddUser("1", 501, 2025);
            await WaitForUpdate();

            AssertCurrentDataEqual(ExpectedSessions());
            // Five retracts, five replacing inserts, plus the two new rows.
            Assert.Equal(12, ChangeRowsRecieved);
        }

        public record SessionAggResult(string? companyId, long count, DateTime? windowStart, DateTime? windowEnd);

        // GROUP BY SESSION(...) exposes the grouping as window_start, matching hopping and tumbling.
        private const string SessionGroupByQuery = @"
            INSERT INTO output
            SELECT
                CompanyId,
                count(*) AS visits,
                window_start,
                SESSION_END(BirthDate, 10, 'SECOND') AS endtime
            FROM users
            GROUP BY CompanyId, SESSION(BirthDate, 10, 'SECOND')";

        // The same query written with the Flink style accessor instead of the column name.
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
        /// The documented minimal form, where nothing but the grouping references the timestamp so
        /// the aggregate has no max measure at all.
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

            // Bridging the two sessions must retract both aggregate rows and emit one merged row.
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

            // Removing the middle row splits one aggregate row into two.
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
        // A different timestamp column than the one the grouping sessionizes on
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

            // A merge across the crash boundary exercises the restored stored values.
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

            // Spelled out rather than derived, because ExpectedSessions groups by company and the
            // point of this test is that without a PARTITION BY the companies share one run.
            // 0 and 5 are five seconds apart so they share a session, 20 is fifteen after 5 so it
            // starts its own.
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
