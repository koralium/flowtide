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

using System.Diagnostics;
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests
{
    /// <summary>
    /// Timing oriented tests for the bulk window operator. Every run asserts the query's result, the
    /// half million row datasets that make the printed timings meaningful are opt in through
    /// FLOWTIDE_RUN_PROBES=1 so the normal suite stays cheap.
    /// </summary>
    public class BulkWindowPerformanceTests : FlowtideAcceptanceBase
    {
        private readonly ITestOutputHelper _output;

        public BulkWindowPerformanceTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
            _output = testOutputHelper;
        }

        private static bool FullScale => Environment.GetEnvironmentVariable("FLOWTIDE_RUN_PROBES") == "1";

        private static int UserRowCount => FullScale ? 500_000 : 5_000;

        public record SumResult(string? companyId, int userkey, int value);
        public record TopRowResult(string? companyId, int userkey);

        private List<SumResult> ExpectedRunningSum(Func<int, bool> userKeyFilter)
        {
            return Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var ordered = g.OrderBy(x => x.UserKey).ToList();
                    var output = new List<SumResult>();
                    double sum = 0;
                    for (int i = 0; i < ordered.Count; i++)
                    {
                        sum += ordered[i].DoubleValue;
                        if (userKeyFilter(ordered[i].UserKey))
                        {
                            output.Add(new SumResult(ordered[i].CompanyId, ordered[i].UserKey, (int)sum));
                        }
                    }
                    return output;
                }).ToList();
        }

        [Fact]
        public async Task YearLowYearHighInitialLoadAndAppend()
        {
            GenerateTpcDi(FullScale ? 100 : 10, FullScale ? 3000 : 300);

            var stopwatch = Stopwatch.StartNew();
            await StartStream(@"
            INSERT INTO output
            SELECT
                dm.DM_S_SYMB as DM_S_SYMB,
                DM_CLOSE as ClosePrice,
                DM_DATE as DM_DATE,
                min_by(
                    named_struct(
                        'low', dm.DM_LOW,
                        'date', dm.DM_DATE
                    ), DM_LOW) OVER (PARTITION BY dm.DM_S_SYMB ORDER BY dm.DM_DATE ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) as YearLow,
                MAX_BY(named_struct(
                        'high', dm.DM_HIGH,
                        'date', dm.DM_DATE
                    ), DM_HIGH) OVER (PARTITION BY dm.DM_S_SYMB ORDER BY dm.DM_DATE ROWS BETWEEN 364 PRECEDING AND CURRENT ROW) as YearHigh
            FROM dailymarkets dm
            ", ignoreSameDataCheck: true);
            await WaitForUpdate();
            stopwatch.Stop();
            _output.WriteLine($"Initial load {DailyMarkets.Count} rows: {stopwatch.ElapsedMilliseconds} ms");

            // A window function emits one row per input row, a dropped or duplicated row is the failure to catch here.
            Assert.Equal(DailyMarkets.Count, GetActualRows().Count);

            // Append one day of data at the end of every partition.
            for (int i = 0; i < 5; i++)
            {
                GenerateDailyMarkets(1);
                stopwatch.Restart();
                await WaitForUpdate();
                stopwatch.Stop();
                _output.WriteLine($"Append day {i + 1}: {stopwatch.ElapsedMilliseconds} ms");
                Assert.Equal(DailyMarkets.Count, GetActualRows().Count);
            }
        }

        [Fact]
        public async Task RowNumberTopOneFilterTopInsert()
        {
            // A single large partition. The filter keeps the sink at one row, so the measured time is
            // dominated by the window operator work plus the fixed pipeline cost.
            GenerateData(UserRowCount);

            var stopwatch = Stopwatch.StartNew();
            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey
            FROM users
            WHERE ROW_NUMBER() OVER (ORDER BY UserKey) = 1
            ", ignoreSameDataCheck: true);
            await WaitForUpdate();
            stopwatch.Stop();
            _output.WriteLine($"Initial load {UserRowCount} rows: {stopwatch.ElapsedMilliseconds} ms");

            AssertTopRowIsSmallestUserKey();

            // Each iteration inserts a new smallest key, shifting every row number in the partition. With
            // the max_row_number hint only the top rows are recomputed.
            for (int i = 0; i < 5; i++)
            {
                AddOrUpdateUser(new Entities.User()
                {
                    UserKey = -1 - i,
                    CompanyId = "1",
                    DoubleValue = i
                });
                stopwatch.Restart();
                await WaitForUpdate();
                stopwatch.Stop();
                _output.WriteLine($"Top insert {i + 1} (1 row into {UserRowCount} partition): {stopwatch.ElapsedMilliseconds} ms");

                // Suppressing the row that the filter keeps would leave an empty sink, which used to pass.
                AssertTopRowIsSmallestUserKey();
            }
        }

        private void AssertTopRowIsSmallestUserKey()
        {
            var top = Users.OrderBy(x => x.UserKey).First();
            AssertCurrentDataEqual(new[] { new TopRowResult(top.CompanyId, top.UserKey) });
        }

        [Fact]
        public async Task RunningSumAppendSmallSinkControl()
        {
            // Same window workload as RunningSumInitialLoadAndAppend, but the output is filtered to a
            // small set of rows after the window. This isolates the window operator's append cost from the
            // test sink, which copies its whole table on every checkpoint.
            GenerateData(UserRowCount);

            var stopwatch = Stopwatch.StartNew();
            await StartStream(@"
            INSERT INTO output
            SELECT companyid, userkey, value FROM (
                SELECT
                    CompanyId as companyid,
                    UserKey as userkey,
                    CAST(SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS INT) as value
                FROM users) t
            WHERE t.userkey % 1000 = 0
            ", ignoreSameDataCheck: true);
            await WaitForUpdate();
            stopwatch.Stop();
            _output.WriteLine($"Initial load {UserRowCount} rows (small sink): {stopwatch.ElapsedMilliseconds} ms");

            AssertCurrentDataEqual(ExpectedRunningSum(k => k % 1000 == 0));

            for (int i = 0; i < 5; i++)
            {
                GenerateData(100);
                stopwatch.Restart();
                await WaitForUpdate();
                stopwatch.Stop();
                _output.WriteLine($"Append batch {i + 1} (100 rows, small sink): {stopwatch.ElapsedMilliseconds} ms");
                AssertCurrentDataEqual(ExpectedRunningSum(k => k % 1000 == 0));
            }
        }

        [Fact]
        public async Task RunningSumInitialLoadAndAppend()
        {
            GenerateData(UserRowCount);

            var stopwatch = Stopwatch.StartNew();
            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                CAST(SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS INT) as value
            FROM users
            ", ignoreSameDataCheck: true);
            await WaitForUpdate();
            stopwatch.Stop();
            _output.WriteLine($"Initial load {UserRowCount} rows: {stopwatch.ElapsedMilliseconds} ms");

            // Every running sum is checked, an append that corrupts earlier rows used to pass silently.
            AssertCurrentDataEqual(ExpectedRunningSum(_ => true));

            for (int i = 0; i < 5; i++)
            {
                GenerateData(100);
                stopwatch.Restart();
                await WaitForUpdate();
                stopwatch.Stop();
                _output.WriteLine($"Append batch {i + 1} (100 rows): {stopwatch.ElapsedMilliseconds} ms");
                AssertCurrentDataEqual(ExpectedRunningSum(_ => true));
            }
        }
    }
}
