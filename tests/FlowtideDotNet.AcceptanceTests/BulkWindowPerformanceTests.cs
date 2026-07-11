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
    /// Timing oriented tests for the bulk window operator. They assert correct completion and print
    /// timings for the initial load and for incremental appends, which are the scenarios the bulk
    /// operator optimizes.
    /// </summary>
    public class BulkWindowPerformanceTests : FlowtideAcceptanceBase
    {
        private readonly ITestOutputHelper _output;

        public BulkWindowPerformanceTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
            _output = testOutputHelper;
        }

        [Fact]
        public async Task YearLowYearHighInitialLoadAndAppend()
        {
            GenerateTpcDi(100, 3000);

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
            _output.WriteLine($"Initial load 300000 rows: {stopwatch.ElapsedMilliseconds} ms");

            // Append one day of data at the end of every partition.
            for (int i = 0; i < 5; i++)
            {
                GenerateDailyMarkets(1);
                stopwatch.Restart();
                await WaitForUpdate();
                stopwatch.Stop();
                _output.WriteLine($"Append day {i + 1} (100 rows): {stopwatch.ElapsedMilliseconds} ms");
            }
        }

        [Fact]
        public async Task RowNumberTopOneFilterTopInsert()
        {
            // A single 500k row partition. The filter keeps the sink at one row, so the measured time is
            // dominated by the window operator work plus the fixed pipeline cost.
            GenerateData(500_000);

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
            _output.WriteLine($"Initial load 500000 rows: {stopwatch.ElapsedMilliseconds} ms");

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
                _output.WriteLine($"Top insert {i + 1} (1 row into 500k partition): {stopwatch.ElapsedMilliseconds} ms");
            }
        }

        [Fact]
        public async Task RunningSumAppendSmallSinkControl()
        {
            // Same window workload as RunningSumInitialLoadAndAppend, but the output is filtered to a
            // small set of rows after the window. This isolates the window operator's append cost from the
            // test sink, which copies its whole table on every checkpoint.
            GenerateData(500_000);

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
            _output.WriteLine($"Initial load 500000 rows (small sink): {stopwatch.ElapsedMilliseconds} ms");

            for (int i = 0; i < 5; i++)
            {
                GenerateData(100);
                stopwatch.Restart();
                await WaitForUpdate();
                stopwatch.Stop();
                _output.WriteLine($"Append batch {i + 1} (100 rows, small sink): {stopwatch.ElapsedMilliseconds} ms");
            }
        }

        [Fact]
        public async Task RunningSumInitialLoadAndAppend()
        {
            GenerateData(500_000);

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
            _output.WriteLine($"Initial load 500000 rows: {stopwatch.ElapsedMilliseconds} ms");

            for (int i = 0; i < 5; i++)
            {
                GenerateData(100);
                stopwatch.Restart();
                await WaitForUpdate();
                stopwatch.Stop();
                _output.WriteLine($"Append batch {i + 1} (100 rows): {stopwatch.ElapsedMilliseconds} ms");
            }
        }
    }
}
