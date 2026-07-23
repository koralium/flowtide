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
    /// Tests for the bulk window operator's incremental recompute paths: appends at the end of a
    /// partition, inserts and deletes in the middle, updates of existing rows and crash recovery, all
    /// across multiple watermarks.
    /// </summary>
    public class BulkWindowTests : FlowtideAcceptanceBase
    {
        public BulkWindowTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        public record SumResult(string? companyId, int userkey, long? value);
        public record RowNumberResult(string? companyId, int userkey, long value);
        public record MinByResult(string? companyId, int userkey, double? value);

        private void AddUser(string companyId, int userKey, double doubleValue)
        {
            AddOrUpdateUser(new User()
            {
                UserKey = userKey,
                CompanyId = companyId,
                DoubleValue = doubleValue
            });
        }

        private List<SumResult> ExpectedRunningSum()
        {
            return Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var sum = 0.0;
                    var ordered = g.OrderBy(x => x.UserKey).ToList();
                    var output = new List<SumResult>();
                    for (int i = 0; i < ordered.Count; i++)
                    {
                        sum += ordered[i].DoubleValue;
                        output.Add(new SumResult(ordered[i].CompanyId, ordered[i].UserKey, (long)sum));
                    }
                    return output;
                }).ToList();
        }

        private List<SumResult> ExpectedBoundedSum(int precedingStart, int precedingEnd)
        {
            return Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var ordered = g.OrderBy(x => x.UserKey).ToList();
                    var output = new List<SumResult>();
                    for (int i = 0; i < ordered.Count; i++)
                    {
                        double? sum = default;
                        for (int k = Math.Max(0, i - precedingStart); k <= i - precedingEnd; k++)
                        {
                            sum = (sum ?? 0) + ordered[k].DoubleValue;
                        }
                        output.Add(new SumResult(ordered[i].CompanyId, ordered[i].UserKey, sum == null ? null : (long)sum));
                    }
                    return output;
                }).ToList();
        }

        private List<RowNumberResult> ExpectedRowNumbers()
        {
            return Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var ordered = g.OrderBy(x => x.UserKey).ToList();
                    var output = new List<RowNumberResult>();
                    for (int i = 0; i < ordered.Count; i++)
                    {
                        output.Add(new RowNumberResult(ordered[i].CompanyId, ordered[i].UserKey, i + 1));
                    }
                    return output;
                }).ToList();
        }

        private List<MinByResult> ExpectedBoundedMin(int preceding)
        {
            return Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var ordered = g.OrderBy(x => x.UserKey).ToList();
                    var output = new List<MinByResult>();
                    for (int i = 0; i < ordered.Count; i++)
                    {
                        double? min = default;
                        for (int k = Math.Max(0, i - preceding); k <= i; k++)
                        {
                            if (min == null || ordered[k].DoubleValue < min)
                            {
                                min = ordered[k].DoubleValue;
                            }
                        }
                        output.Add(new MinByResult(ordered[i].CompanyId, ordered[i].UserKey, min));
                    }
                    return output;
                }).ToList();
        }

        private const string RunningSumQuery = @"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                CAST(SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS INT) as value
            FROM users";

        private const string RowNumberQuery = @"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                ROW_NUMBER() OVER (PARTITION BY CompanyId ORDER BY UserKey)
            FROM users";

        [Fact]
        public async Task RunningSumAppendAtPartitionEnd()
        {
            for (int i = 0; i < 10; i++)
            {
                AddUser("1", i, i + 1);
                AddUser("2", 100 + i, i + 2);
            }

            await StartStream(RunningSumQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedRunningSum());

            // Append rows at the end of both partitions, the scan should only need the previous stored sum.
            for (int i = 10; i < 15; i++)
            {
                AddUser("1", i, i + 1);
                AddUser("2", 100 + i, i + 2);
            }
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedRunningSum());

            // Append to only one partition.
            AddUser("1", 20, 55);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedRunningSum());
        }

        [Fact]
        public async Task RunningSumInsertMiddleOfPartition()
        {
            for (int i = 0; i < 10; i++)
            {
                AddUser("1", i * 10, i + 1);
            }

            await StartStream(RunningSumQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedRunningSum());

            // Insert in the middle, all sums after the insert position change.
            AddUser("1", 45, 1000);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedRunningSum());
        }

        [Fact]
        public async Task RunningSumUpdateExistingRow()
        {
            for (int i = 0; i < 10; i++)
            {
                AddUser("1", i, i + 1);
            }

            await StartStream(RunningSumQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedRunningSum());

            // Update the value of a middle row, which is a delete and insert at the same sort position.
            AddUser("1", 5, 500);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedRunningSum());

            // Update that does not change the value of the sum column should produce no changes.
            AddUser("1", 5, 500);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedRunningSum());
        }

        [Fact]
        public async Task RowNumberInsertAtTopOfPartition()
        {
            for (int i = 10; i < 20; i++)
            {
                AddUser("1", i, i);
            }

            await StartStream(RowNumberQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedRowNumbers());

            // A new first row shifts every row number in the partition.
            AddUser("1", 1, 1);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedRowNumbers());
        }

        [Fact]
        public async Task RowNumberDeleteAtTopOfPartition()
        {
            for (int i = 0; i < 10; i++)
            {
                AddUser("1", i, i);
            }

            await StartStream(RowNumberQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedRowNumbers());

            DeleteUser(Users.First(x => x.UserKey == 0));
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedRowNumbers());

            // Delete at the end of the partition only retracts the last row.
            DeleteUser(Users.First(x => x.UserKey == 9));
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedRowNumbers());
        }

        [Fact]
        public async Task RowNumberAppendManySmallBatches()
        {
            AddUser("1", 0, 0);

            await StartStream(RowNumberQuery);
            await WaitForUpdate();

            for (int i = 1; i <= 10; i++)
            {
                AddUser("1", i, i);
                await WaitForUpdate();
                AssertCurrentDataEqual(ExpectedRowNumbers());
            }
        }

        [Fact]
        public async Task BoundedSumPrecedingToPrecedingIncremental()
        {
            for (int i = 0; i < 12; i++)
            {
                AddUser("1", i * 10, i + 1);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                CAST(SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING) AS INT) as value
            FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedBoundedSum(4, 1));

            // Append at the end, only the last rows need to recompute.
            AddUser("1", 200, 50);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedBoundedSum(4, 1));

            // Insert in the middle, only the following four rows are affected.
            AddUser("1", 45, 1000);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedBoundedSum(4, 1));

            // Delete in the middle.
            DeleteUser(Users.First(x => x.UserKey == 60));
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedBoundedSum(4, 1));
        }

        [Fact]
        public async Task MinByBoundedDeleteMinInsideWindow()
        {
            // Values chosen so user 30 is the minimum for several windows.
            AddUser("1", 0, 50);
            AddUser("1", 10, 40);
            AddUser("1", 20, 60);
            AddUser("1", 30, 10);
            AddUser("1", 40, 70);
            AddUser("1", 50, 80);
            AddUser("1", 60, 90);

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                min_by(DoubleValue, DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as value
            FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedBoundedMin(2));

            // Delete the row that is the minimum for the following windows, forcing a frame rescan.
            DeleteUser(Users.First(x => x.UserKey == 30));
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedBoundedMin(2));
        }

        [Fact]
        public async Task MinByBoundedAppendRows()
        {
            for (int i = 0; i < 8; i++)
            {
                AddUser("1", i, 100 - i);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                min_by(DoubleValue, DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) as value
            FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedBoundedMin(3));

            // Append a new minimum, only the new row needs computing.
            AddUser("1", 8, 1);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedBoundedMin(3));

            // Append a non minimum value.
            AddUser("1", 9, 500);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedBoundedMin(3));

            // Monotonically increasing values keep dropping the minimum out of the frame.
            for (int i = 10; i < 16; i++)
            {
                AddUser("1", i, 500 + i);
            }
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedBoundedMin(3));
        }

        [Fact]
        public async Task MinByUnboundedFromIncremental()
        {
            for (int i = 0; i < 8; i++)
            {
                AddUser("1", i, 100 - i);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                min_by(DoubleValue, DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as value
            FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedBoundedMin(int.MaxValue - 1));

            // Append a row that does not change the minimum, the scan should stop immediately.
            AddUser("1", 8, 1000);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedBoundedMin(int.MaxValue - 1));

            // Append a new global minimum.
            AddUser("1", 9, 0.5);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedBoundedMin(int.MaxValue - 1));

            // Delete the minimum, all rows from its position recompute.
            DeleteUser(Users.First(x => x.UserKey == 9));
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedBoundedMin(int.MaxValue - 1));
        }

        [Fact]
        public async Task RunningSumSingleBatchLargerThanChunkSize()
        {
            // Batches over the operator's 2048 row chunk size are applied in chunks, every chunk
            // must insert its own rows and not the first chunk's.
            SourceBatchSize = 10000;
            for (int i = 0; i < 3000; i++)
            {
                AddUser("1", i, 1);
            }

            await StartStream(RunningSumQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedRunningSum());

            // A single update batch over the chunk size after initial data.
            for (int i = 3000; i < 6000; i++)
            {
                AddUser("1", i, 1);
            }
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedRunningSum());
        }

        [Fact]
        public async Task RunningSumExpressionPartitionSingleBatchLargerThanChunkSize()
        {
            // A computed PARTITION BY expression is stored in a plain column, which the key container
            // inserts by row index. Every chunk after the first must insert its own rows.
            SourceBatchSize = 10000;
            for (int i = 0; i < 3000; i++)
            {
                AddUser("1", i, 1);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                CAST(SUM(DoubleValue) OVER (PARTITION BY UserKey % 3 ORDER BY UserKey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS INT) as value
            FROM users");
            await WaitForUpdate();

            List<SumResult> Expected()
            {
                return Users.GroupBy(x => x.UserKey % 3)
                    .SelectMany(g =>
                    {
                        var sum = 0.0;
                        var ordered = g.OrderBy(x => x.UserKey).ToList();
                        var output = new List<SumResult>();
                        for (int i = 0; i < ordered.Count; i++)
                        {
                            sum += ordered[i].DoubleValue;
                            output.Add(new SumResult(ordered[i].CompanyId, ordered[i].UserKey, (long)sum));
                        }
                        return output;
                    }).ToList();
            }

            AssertCurrentDataEqual(Expected());

            // A single update batch over the chunk size after initial data.
            for (int i = 3000; i < 6000; i++)
            {
                AddUser("1", i, 1);
            }
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());
        }

        [Fact]
        public async Task FollowingFrameChangeNearTreeStart()
        {
            // A following frame walks backwards from the first marker to find the scan anchor.
            // A change near the start of the tree's first partition ends that walk at the tree
            // start, where the backward iterator must not be read as a row.
            for (int i = 0; i < 10; i++)
            {
                AddUser("1", i, 1);
            }
            await StartStream(@"
            INSERT INTO output
            SELECT CompanyId, UserKey,
            CAST(SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING) AS INT) as value
            FROM users");
            await WaitForUpdate();

            List<SumResult> Expected()
            {
                var ordered = Users.OrderBy(x => x.UserKey).ToList();
                var output = new List<SumResult>();
                for (int i = 0; i < ordered.Count; i++)
                {
                    double sum = 0;
                    for (int k = Math.Max(0, i - 1); k <= Math.Min(ordered.Count - 1, i + 2); k++)
                    {
                        sum += ordered[k].DoubleValue;
                    }
                    output.Add(new SumResult(ordered[i].CompanyId, ordered[i].UserKey, (long)sum));
                }
                return output;
            }

            AssertCurrentDataEqual(Expected());

            // Update the partition's second row, the anchor walk reaches the tree start.
            AddUser("1", 1, 5);
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());
        }

        [Fact]
        public async Task RunningSumCrashRecovery()
        {
            GenerateData(500);

            await StartStream(RunningSumQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedRunningSum());

            await Crash();

            GenerateData(200);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedRunningSum());
        }

        public record DuplicateRowResult(string? companyId, double value, long rowNumber);

        [Fact]
        public async Task RowNumberDuplicateRowsDeleteOne()
        {
            // Users 1 and 3 are identical after the projection (CompanyId, DoubleValue), so they become one
            // stored row with weight two and separate row numbers.
            AddUser("1", 1, 10);
            AddUser("1", 3, 10);
            AddUser("1", 2, 20);

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                DoubleValue,
                ROW_NUMBER() OVER (PARTITION BY CompanyId ORDER BY DoubleValue)
            FROM users");
            await WaitForUpdate();

            List<DuplicateRowResult> Expected()
            {
                return Users.GroupBy(x => x.CompanyId)
                    .SelectMany(g =>
                    {
                        var ordered = g.OrderBy(x => x.DoubleValue).ToList();
                        var output = new List<DuplicateRowResult>();
                        for (int i = 0; i < ordered.Count; i++)
                        {
                            output.Add(new DuplicateRowResult(ordered[i].CompanyId, ordered[i].DoubleValue, i + 1));
                        }
                        return output;
                    }).ToList();
            }

            AssertCurrentDataEqual(Expected());

            // Deleting one duplicate should retract the highest duplicate row number.
            DeleteUser(Users.First(x => x.UserKey == 3));
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());
        }

        public record RowNumberValueResult(string? companyId, int userkey, double value, long rowNumber);

        [Fact]
        public async Task RowNumberUpdateNonKeyColumn()
        {
            for (int i = 10; i < 20; i++)
            {
                AddUser("1", i, i);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                DoubleValue,
                ROW_NUMBER() OVER (PARTITION BY CompanyId ORDER BY UserKey)
            FROM users");
            await WaitForUpdate();

            List<RowNumberValueResult> Expected()
            {
                return Users.GroupBy(x => x.CompanyId)
                    .SelectMany(g =>
                    {
                        var ordered = g.OrderBy(x => x.UserKey).ToList();
                        var output = new List<RowNumberValueResult>();
                        for (int i = 0; i < ordered.Count; i++)
                        {
                            output.Add(new RowNumberValueResult(ordered[i].CompanyId, ordered[i].UserKey, ordered[i].DoubleValue, i + 1));
                        }
                        return output;
                    }).ToList();
            }

            AssertCurrentDataEqual(Expected());

            // Update a column that is not part of the sort order, this deletes the old row and inserts a
            // new one at the same position in the same batch.
            AddUser("1", 15, 5000);
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());

            // A mix of a new top row, an update at an existing position and a delete in the same interval.
            AddUser("1", 1, 1);
            AddUser("1", 16, 6000);
            DeleteUser(Users.First(x => x.UserKey == 17));
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());
        }

        public record EvictionPressureResult(string? companyId, int userkey, long sumValue, long rowNumber);

        [Fact]
        public async Task WindowUnderCacheEvictionPressure()
        {
            // Tiny cache keeps eviction serializing pages during the scan, exercising the page write
            // lock. Descending order by runs the descending radix and boundary paths under the same pressure.
            CachePageCount = 256;
            SetPageSizeBytes(2048);
            GenerateData(10_000);

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                SUM(UserKey) OVER (PARTITION BY CompanyId ORDER BY UserKey DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as sumValue,
                ROW_NUMBER() OVER (PARTITION BY CompanyId ORDER BY UserKey DESC)
            FROM users");
            await WaitForUpdate();

            List<EvictionPressureResult> Expected()
            {
                return Users.GroupBy(x => x.CompanyId)
                    .SelectMany(g =>
                    {
                        var ordered = g.OrderByDescending(x => x.UserKey).ToList();
                        var output = new List<EvictionPressureResult>();
                        long sum = 0;
                        for (int i = 0; i < ordered.Count; i++)
                        {
                            sum += ordered[i].UserKey;
                            output.Add(new EvictionPressureResult(ordered[i].CompanyId, ordered[i].UserKey, sum, i + 1));
                        }
                        return output;
                    }).ToList();
            }

            AssertCurrentDataEqual(Expected());

            // Incremental changes while pages of the existing state are being evicted and reloaded.
            GenerateData(2_000);
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());

            // Recovery must restore from pages that were written under eviction pressure.
            await Crash();

            GenerateData(2_000);
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());
        }

        public record TopRowResult(string? companyId, int userkey);

        [Fact]
        public async Task FilterRowNumberEqualsOneIncremental()
        {
            for (int i = 10; i < 20; i++)
            {
                AddUser("1", i, i);
                AddUser("2", 100 + i, i);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey
            FROM users
            WHERE ROW_NUMBER() OVER (PARTITION BY CompanyId ORDER BY UserKey) = 1");
            await WaitForUpdate();

            List<TopRowResult> Expected()
            {
                return Users.GroupBy(x => x.CompanyId)
                    .Select(g =>
                    {
                        var first = g.OrderBy(x => x.UserKey).First();
                        return new TopRowResult(first.CompanyId, first.UserKey);
                    }).ToList();
            }

            AssertCurrentDataEqual(Expected());

            // A new top row replaces the previous one, without scanning the whole partition.
            AddUser("1", 1, 1);
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());

            // Deleting the top row brings the previous top back.
            DeleteUser(Users.First(x => x.UserKey == 1));
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());

            // Appends at the end do not change the top row.
            AddUser("1", 50, 50);
            AddUser("2", 150, 50);
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());

            // Deleting a middle row does not change the top row.
            DeleteUser(Users.First(x => x.UserKey == 15));
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());
        }

        public record TopRowsResult(string? companyId, int userkey, long rowNumber);

        [Fact]
        public async Task FilterRowNumberLessThanOrEqualTwoIncremental()
        {
            for (int i = 10; i < 20; i++)
            {
                AddUser("1", i, i);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                ROW_NUMBER() OVER (PARTITION BY CompanyId ORDER BY UserKey) as rn
            FROM users
            WHERE ROW_NUMBER() OVER (PARTITION BY CompanyId ORDER BY UserKey) <= 2");
            await WaitForUpdate();

            List<TopRowsResult> Expected()
            {
                return Users.GroupBy(x => x.CompanyId)
                    .SelectMany(g =>
                    {
                        return g.OrderBy(x => x.UserKey)
                            .Take(2)
                            .Select((x, i) => new TopRowsResult(x.CompanyId, x.UserKey, i + 1));
                    }).ToList();
            }

            AssertCurrentDataEqual(Expected());

            AddUser("1", 1, 1);
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());

            DeleteUser(Users.First(x => x.UserKey == 10));
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());
        }

        [Fact]
        public async Task FilterRowNumberEqualsTwoIncremental()
        {
            // With rn = 2 the emission suppression drops rows beyond the bound, while the kept filter
            // still removes the rn = 1 row from the emitted ones.
            for (int i = 10; i < 20; i++)
            {
                AddUser("1", i, i);
                AddUser("2", 100 + i, i);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey
            FROM users
            WHERE ROW_NUMBER() OVER (PARTITION BY CompanyId ORDER BY UserKey) = 2");
            await WaitForUpdate();

            List<TopRowResult> Expected()
            {
                return Users.GroupBy(x => x.CompanyId)
                    .Where(g => g.Count() >= 2)
                    .Select(g =>
                    {
                        var second = g.OrderBy(x => x.UserKey).Skip(1).First();
                        return new TopRowResult(second.CompanyId, second.UserKey);
                    }).ToList();
            }

            AssertCurrentDataEqual(Expected());

            // A new top row shifts which row is number two.
            AddUser("1", 1, 1);
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());

            // Deleting the top row shifts it back.
            DeleteUser(Users.First(x => x.UserKey == 1));
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());

            // Deleting the current number two promotes the next row.
            DeleteUser(Users.First(x => x.UserKey == 11));
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());

            // Appends at the end change nothing.
            AddUser("1", 50, 50);
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());
        }

        [Fact]
        public async Task RunningSumLargeInitialThenSingleAppend()
        {
            GenerateData(5000);

            await StartStream(RunningSumQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedRunningSum());

            // Single row appended into a large existing state.
            AddUser("1", 1_000_000, 42);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedRunningSum());
        }

        private void AddUserVisits(string companyId, int userKey, int? visits)
        {
            AddOrUpdateUser(new User()
            {
                UserKey = userKey,
                CompanyId = companyId,
                Visits = visits
            });
        }

        public record LeadLagResult(string? companyId, int userkey, long? value);
        public record LastValueResult(string? companyId, int userkey, long? value);
        public record AvgResult(string? companyId, int userkey, double? value);
        public record SurrogateKeyResult(string? companyId, int userkey, long key);

        private List<LeadLagResult> ExpectedLead(int offset)
        {
            return Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var ordered = g.OrderBy(x => x.UserKey).ToList();
                    var output = new List<LeadLagResult>();
                    for (int i = 0; i < ordered.Count; i++)
                    {
                        long? val = i + offset < ordered.Count ? ordered[i + offset].UserKey : null;
                        output.Add(new LeadLagResult(ordered[i].CompanyId, ordered[i].UserKey, val));
                    }
                    return output;
                }).ToList();
        }

        private List<LeadLagResult> ExpectedLag(int offset, long? defaultValue)
        {
            return Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var ordered = g.OrderBy(x => x.UserKey).ToList();
                    var output = new List<LeadLagResult>();
                    for (int i = 0; i < ordered.Count; i++)
                    {
                        long? val = i - offset >= 0 ? ordered[i - offset].UserKey : defaultValue;
                        output.Add(new LeadLagResult(ordered[i].CompanyId, ordered[i].UserKey, val));
                    }
                    return output;
                }).ToList();
        }

        [Fact]
        public async Task LeadIncremental()
        {
            for (int i = 0; i < 10; i++)
            {
                AddUser("1", i * 10, i);
                AddUser("2", 100 + i * 10, i);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                LEAD(UserKey, 2) OVER (PARTITION BY CompanyId ORDER BY UserKey)
            FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedLead(2));

            // Append at the end, the previous two rows get new lead values.
            AddUser("1", 200, 0);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedLead(2));

            // Insert in the middle, the two rows before the insert change.
            AddUser("1", 45, 0);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedLead(2));

            // Delete in the middle.
            DeleteUser(Users.First(x => x.UserKey == 30));
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedLead(2));
        }

        [Fact]
        public async Task LagWithDefaultIncremental()
        {
            for (int i = 0; i < 10; i++)
            {
                AddUser("1", 100 + i * 10, i);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                LAG(UserKey, 2, 0) OVER (PARTITION BY CompanyId ORDER BY UserKey)
            FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedLag(2, 0));

            // Insert at the top of the partition, the following two rows change.
            AddUser("1", 1, 0);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedLag(2, 0));

            // Delete in the middle, the two rows after the deleted position change.
            DeleteUser(Users.First(x => x.UserKey == 140));
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedLag(2, 0));

            // Append at the end only computes the new row.
            AddUser("1", 300, 0);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedLag(2, 0));
        }

        private List<AvgResult> ExpectedRunningAverage()
        {
            return Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var ordered = g.OrderBy(x => x.UserKey).ToList();
                    var output = new List<AvgResult>();
                    double sum = 0;
                    for (int i = 0; i < ordered.Count; i++)
                    {
                        sum += ordered[i].DoubleValue;
                        output.Add(new AvgResult(ordered[i].CompanyId, ordered[i].UserKey, sum / (i + 1)));
                    }
                    return output;
                }).ToList();
        }

        private List<AvgResult> ExpectedBoundedAverage(int preceding, int following)
        {
            return Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var ordered = g.OrderBy(x => x.UserKey).ToList();
                    var output = new List<AvgResult>();
                    for (int i = 0; i < ordered.Count; i++)
                    {
                        double sum = 0;
                        int count = 0;
                        for (int k = Math.Max(0, i - preceding); k <= Math.Min(ordered.Count - 1, i + following); k++)
                        {
                            sum += ordered[k].DoubleValue;
                            count++;
                        }
                        output.Add(new AvgResult(ordered[i].CompanyId, ordered[i].UserKey, count == 0 ? null : sum / count));
                    }
                    return output;
                }).ToList();
        }

        [Fact]
        public async Task RunningAverageIncremental()
        {
            for (int i = 0; i < 10; i++)
            {
                AddUser("1", i, i + 1);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                AVG(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as value
            FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedRunningAverage());

            // Append at the end seeds the sum and count from the previous row's stored state.
            AddUser("1", 20, 100);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedRunningAverage());

            // Insert in the middle recomputes all rows after the insert position.
            AddUser("1", 5, 50);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedRunningAverage());

            // Update an existing value.
            AddUser("1", 3, 7);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedRunningAverage());
        }

        [Fact]
        public async Task BoundedFollowingAverageIncremental()
        {
            for (int i = 0; i < 12; i++)
            {
                AddUser("1", i * 10, i + 1);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                AVG(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) as value
            FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedBoundedAverage(2, 2));

            // Append at the end, the two previous rows gain a frame member.
            AddUser("1", 200, 60);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedBoundedAverage(2, 2));

            // Insert in the middle affects two rows on each side.
            AddUser("1", 45, 1000);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedBoundedAverage(2, 2));

            // Delete in the middle.
            DeleteUser(Users.First(x => x.UserKey == 60));
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedBoundedAverage(2, 2));
        }

        private List<LastValueResult> ExpectedLastValueIgnoreNulls(int from, int to)
        {
            return Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var ordered = g.OrderBy(x => x.UserKey).ToList();
                    var output = new List<LastValueResult>();
                    for (int i = 0; i < ordered.Count; i++)
                    {
                        long? val = null;
                        for (int k = Math.Max(0, i + from); k <= Math.Min(ordered.Count - 1, i + to); k++)
                        {
                            if (ordered[k].Visits != null)
                            {
                                val = ordered[k].Visits;
                            }
                        }
                        output.Add(new LastValueResult(ordered[i].CompanyId, ordered[i].UserKey, val));
                    }
                    return output;
                }).ToList();
        }

        [Fact]
        public async Task LastValueIgnoreNullsBoundedIncremental()
        {
            for (int i = 0; i < 12; i++)
            {
                // Every third row has a value, the rest are null.
                AddUserVisits("1", i * 10, i % 3 == 0 ? i : null);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                LAST_VALUE(Visits) IGNORE NULLS OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) as value
            FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedLastValueIgnoreNulls(-4, 0));

            // Turn a null into a value, the following rows pick it up as their new last value.
            AddUserVisits("1", 40, 100);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedLastValueIgnoreNulls(-4, 0));

            // Turn a value into a null, the following rows fall back to an earlier candidate.
            AddUserVisits("1", 60, null);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedLastValueIgnoreNulls(-4, 0));

            // Delete a candidate row.
            DeleteUser(Users.First(x => x.UserKey == 90));
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedLastValueIgnoreNulls(-4, 0));
        }

        [Fact]
        public async Task LastValueIgnoreNullsFollowingIncremental()
        {
            for (int i = 0; i < 12; i++)
            {
                AddUserVisits("1", i * 10, i % 3 == 0 ? i : null);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                LAST_VALUE(Visits) IGNORE NULLS OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 4 PRECEDING AND 2 FOLLOWING) as value
            FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedLastValueIgnoreNulls(-4, 2));

            // Append a non null value at the end, the two previous rows see it through the lookahead.
            AddUserVisits("1", 200, 500);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedLastValueIgnoreNulls(-4, 2));

            // Insert a null in the middle shifts frames without adding a candidate.
            AddUserVisits("1", 45, null);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedLastValueIgnoreNulls(-4, 2));

            // Delete a candidate that rows before it were seeing through the lookahead.
            DeleteUser(Users.First(x => x.UserKey == 60));
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedLastValueIgnoreNulls(-4, 2));
        }

        [Fact]
        public async Task LastValueRespectNullsPrecedingIncremental()
        {
            for (int i = 0; i < 10; i++)
            {
                AddUserVisits("1", i * 10, i % 2 == 0 ? i : null);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                LAST_VALUE(Visits) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING) as value
            FROM users");
            await WaitForUpdate();

            List<LastValueResult> Expected()
            {
                return Users.GroupBy(x => x.CompanyId)
                    .SelectMany(g =>
                    {
                        var ordered = g.OrderBy(x => x.UserKey).ToList();
                        var output = new List<LastValueResult>();
                        for (int i = 0; i < ordered.Count; i++)
                        {
                            // With respect nulls the result is the value of the row at the frame end.
                            long? val = i - 1 >= 0 ? ordered[i - 1].Visits : null;
                            output.Add(new LastValueResult(ordered[i].CompanyId, ordered[i].UserKey, val));
                        }
                        return output;
                    }).ToList();
            }

            AssertCurrentDataEqual(Expected());

            // Insert in the middle, the following row's frame end changes.
            AddUserVisits("1", 45, 999);
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());

            // Delete in the middle.
            DeleteUser(Users.First(x => x.UserKey == 70));
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());

            // Append at the end.
            AddUserVisits("1", 200, 7);
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());
        }

        [Fact]
        public async Task SurrogateKeyIncrementalAndPartitionCleanup()
        {
            for (int i = 0; i < 5; i++)
            {
                AddUser("1", i, i);
                AddUser("2", 100 + i, i);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                surrogate_key_int64() OVER (PARTITION BY CompanyId) as key
            FROM users");
            await WaitForUpdate();

            // Keys are assigned per partition in scan order, so partition 1 gets 0 and partition 2 gets 1.
            List<SurrogateKeyResult> Expected(Dictionary<string, long> keys)
            {
                return Users.Select(x => new SurrogateKeyResult(x.CompanyId, x.UserKey, keys[x.CompanyId!])).ToList();
            }

            var expectedKeys = new Dictionary<string, long> { { "1", 0 }, { "2", 1 } };
            AssertCurrentDataEqual(Expected(expectedKeys));

            // Adding rows to an existing partition keeps its key.
            AddUser("1", 50, 0);
            AddUser("2", 150, 0);
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected(expectedKeys));

            // A new partition gets the next key from the durable counter.
            AddUser("3", 200, 0);
            expectedKeys["3"] = 2;
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected(expectedKeys));

            // Deleting every row in a partition removes its stored key entry.
            foreach (var user in Users.Where(x => x.CompanyId == "3").ToList())
            {
                DeleteUser(user);
            }
            expectedKeys.Remove("3");
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected(expectedKeys));

            // Recreating the partition allocates a fresh key, the counter never reuses values.
            AddUser("3", 201, 0);
            expectedKeys["3"] = 3;
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected(expectedKeys));

            // The counter and key entries survive a crash.
            await Crash();
            AddUser("4", 300, 0);
            expectedKeys["4"] = 4;
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected(expectedKeys));
        }

        private List<MinByResult> ExpectedMinMaxBy(long from, long to, bool isMin)
        {
            return Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var ordered = g.OrderBy(x => x.UserKey).ToList();
                    var output = new List<MinByResult>();
                    for (int i = 0; i < ordered.Count; i++)
                    {
                        double? best = default;
                        long start = from == long.MinValue ? 0 : Math.Max(0, i + from);
                        long end = Math.Min(ordered.Count - 1, i + to);
                        for (long k = start; k <= end; k++)
                        {
                            var value = ordered[(int)k].DoubleValue;
                            if (best == null || (isMin ? value < best : value > best))
                            {
                                best = value;
                            }
                        }
                        output.Add(new MinByResult(ordered[i].CompanyId, ordered[i].UserKey, best));
                    }
                    return output;
                }).ToList();
        }

        [Fact]
        public async Task MinByPrecedingToPrecedingIncremental()
        {
            // Values with a distinct minimum that moves around.
            double[] values = { 50, 40, 60, 10, 70, 80, 90, 30, 20, 100, 55, 65 };
            for (int i = 0; i < values.Length; i++)
            {
                AddUser("1", i * 10, values[i]);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                min_by(DoubleValue, DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 5 PRECEDING AND 2 PRECEDING) as value
            FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedMinMaxBy(-5, -2, isMin: true));

            // Append at the end.
            AddUser("1", 200, 5);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedMinMaxBy(-5, -2, isMin: true));

            // Delete the minimum inside several frames, forcing a ring rescan.
            DeleteUser(Users.First(x => x.UserKey == 30));
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedMinMaxBy(-5, -2, isMin: true));

            // Insert in the middle.
            AddUser("1", 45, 1);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedMinMaxBy(-5, -2, isMin: true));

            // Update the value of an existing row.
            AddUser("1", 80, 2);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedMinMaxBy(-5, -2, isMin: true));
        }

        [Fact]
        public async Task MinByPrecedingToFollowingIncremental()
        {
            double[] values = { 50, 40, 60, 10, 70, 80, 90, 30, 20, 100, 55, 65 };
            for (int i = 0; i < values.Length; i++)
            {
                AddUser("1", i * 10, values[i]);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                min_by(DoubleValue, DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 2 PRECEDING AND 3 FOLLOWING) as value
            FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedMinMaxBy(-2, 3, isMin: true));

            // Append a new minimum at the end, rows before it see it through the lookahead.
            AddUser("1", 200, 1);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedMinMaxBy(-2, 3, isMin: true));

            // Delete a minimum in the middle.
            DeleteUser(Users.First(x => x.UserKey == 30));
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedMinMaxBy(-2, 3, isMin: true));

            // Insert in the middle.
            AddUser("1", 45, 3);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedMinMaxBy(-2, 3, isMin: true));
        }

        [Fact]
        public async Task MaxByUnboundedToPrecedingIncremental()
        {
            double[] values = { 50, 40, 60, 10, 70, 80, 90, 30, 20, 100 };
            for (int i = 0; i < values.Length; i++)
            {
                AddUser("1", i * 10, values[i]);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                max_by(DoubleValue, DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) as value
            FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedMinMaxBy(long.MinValue, -2, isMin: false));

            // Append at the end seeds the running best from the previous row's stored state.
            AddUser("1", 200, 5);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedMinMaxBy(long.MinValue, -2, isMin: false));

            // Append a new maximum, it only enters frames two rows later.
            AddUser("1", 210, 500);
            AddUser("1", 220, 6);
            AddUser("1", 230, 7);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedMinMaxBy(long.MinValue, -2, isMin: false));

            // Delete the maximum, all rows after its position recompute.
            DeleteUser(Users.First(x => x.UserKey == 210));
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedMinMaxBy(long.MinValue, -2, isMin: false));
        }

        [Fact]
        public async Task LagDynamicOffsetIncremental()
        {
            // Visits is used as a per-row offset: null falls back to 1 and a negative offset reaches
            // forward, matching the non bulk implementation.
            int?[] offsets = { 2, null, 1, -1, 3, 0, 5, -2, 1, 2 };
            for (int i = 0; i < offsets.Length; i++)
            {
                AddUserVisits("1", i * 10, offsets[i]);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                LAG(UserKey, Visits) OVER (PARTITION BY CompanyId ORDER BY UserKey)
            FROM users");
            await WaitForUpdate();

            List<LeadLagResult> Expected()
            {
                return Users.GroupBy(x => x.CompanyId)
                    .SelectMany(g =>
                    {
                        var ordered = g.OrderBy(x => x.UserKey).ToList();
                        var output = new List<LeadLagResult>();
                        for (int i = 0; i < ordered.Count; i++)
                        {
                            long offset = ordered[i].Visits ?? 1;
                            long target = i - offset;
                            long? val = target >= 0 && target < ordered.Count ? ordered[(int)target].UserKey : null;
                            output.Add(new LeadLagResult(ordered[i].CompanyId, ordered[i].UserKey, val));
                        }
                        return output;
                    }).ToList();
            }

            AssertCurrentDataEqual(Expected());

            // Change an offset value, the whole partition recomputes.
            AddUserVisits("1", 40, 1);
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());

            // Insert a row in the middle shifts every target.
            AddUserVisits("1", 45, 2);
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());

            // Delete a row.
            DeleteUser(Users.First(x => x.UserKey == 20));
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());
        }

        [Fact]
        public async Task LeadDynamicOffsetWithDefaultIncremental()
        {
            int?[] offsets = { 1, 3, null, 2, -1, 0, 4, 1, 2, 1 };
            for (int i = 0; i < offsets.Length; i++)
            {
                AddUserVisits("1", i * 10, offsets[i]);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                LEAD(UserKey, Visits, 0) OVER (PARTITION BY CompanyId ORDER BY UserKey)
            FROM users");
            await WaitForUpdate();

            List<LeadLagResult> Expected()
            {
                return Users.GroupBy(x => x.CompanyId)
                    .SelectMany(g =>
                    {
                        var ordered = g.OrderBy(x => x.UserKey).ToList();
                        var output = new List<LeadLagResult>();
                        for (int i = 0; i < ordered.Count; i++)
                        {
                            long offset = ordered[i].Visits ?? 1;
                            long target = i + offset;
                            long? val = target >= 0 && target < ordered.Count ? ordered[(int)target].UserKey : 0;
                            output.Add(new LeadLagResult(ordered[i].CompanyId, ordered[i].UserKey, val));
                        }
                        return output;
                    }).ToList();
            }

            AssertCurrentDataEqual(Expected());

            // Append at the end changes which rows fall off the partition.
            AddUserVisits("1", 200, 1);
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());

            // Delete in the middle.
            DeleteUser(Users.First(x => x.UserKey == 30));
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());
        }

        [Fact]
        public async Task LeadDynamicOffsetTargetsLastRowAfterExhaustion()
        {
            // The offset 10 at position 6 runs the reader past the partition end.
            // The next rows target the last row exactly, which must still return its value.
            int?[] offsets = { 1, 1, 1, 1, 1, 1, 10, 2, 1, 1 };
            for (int i = 0; i < offsets.Length; i++)
            {
                AddUserVisits("1", i * 10, offsets[i]);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                LEAD(UserKey, Visits) OVER (PARTITION BY CompanyId ORDER BY UserKey)
            FROM users");
            await WaitForUpdate();

            List<LeadLagResult> Expected()
            {
                return Users.GroupBy(x => x.CompanyId)
                    .SelectMany(g =>
                    {
                        var ordered = g.OrderBy(x => x.UserKey).ToList();
                        var output = new List<LeadLagResult>();
                        for (int i = 0; i < ordered.Count; i++)
                        {
                            long offset = ordered[i].Visits ?? 1;
                            long target = i + offset;
                            long? val = target >= 0 && target < ordered.Count ? ordered[(int)target].UserKey : null;
                            output.Add(new LeadLagResult(ordered[i].CompanyId, ordered[i].UserKey, val));
                        }
                        return output;
                    }).ToList();
            }

            AssertCurrentDataEqual(Expected());

            // Append a row and make the row after the exhausting one target the new last row.
            AddUserVisits("1", 100, 1);
            AddUserVisits("1", 70, 3);
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());
        }

        [Fact]
        public async Task SumSuffixFollowingIncremental()
        {
            for (int i = 0; i < 10; i++)
            {
                AddUser("1", i * 10, i + 1);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                CAST(SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 2 FOLLOWING AND UNBOUNDED FOLLOWING) AS INT) as value
            FROM users");
            await WaitForUpdate();

            List<SumResult> Expected()
            {
                return Users.GroupBy(x => x.CompanyId)
                    .SelectMany(g =>
                    {
                        var ordered = g.OrderBy(x => x.UserKey).ToList();
                        var output = new List<SumResult>();
                        for (int i = 0; i < ordered.Count; i++)
                        {
                            double? sum = default;
                            for (int k = i + 2; k < ordered.Count; k++)
                            {
                                sum = (sum ?? 0) + ordered[k].DoubleValue;
                            }
                            output.Add(new SumResult(ordered[i].CompanyId, ordered[i].UserKey, sum == null ? null : (long)sum));
                        }
                        return output;
                    }).ToList();
            }

            AssertCurrentDataEqual(Expected());

            // Append at the end changes every row before it.
            AddUser("1", 200, 50);
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());

            // Delete in the middle.
            DeleteUser(Users.First(x => x.UserKey == 40));
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());
        }

        [Fact]
        public async Task AverageSuffixFollowingIncremental()
        {
            for (int i = 0; i < 10; i++)
            {
                AddUser("1", i * 10, i + 1);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                AVG(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 2 FOLLOWING AND UNBOUNDED FOLLOWING) as value
            FROM users");
            await WaitForUpdate();

            List<AvgResult> Expected()
            {
                return Users.GroupBy(x => x.CompanyId)
                    .SelectMany(g =>
                    {
                        var ordered = g.OrderBy(x => x.UserKey).ToList();
                        var output = new List<AvgResult>();
                        for (int i = 0; i < ordered.Count; i++)
                        {
                            double sum = 0;
                            int count = 0;
                            for (int k = i + 2; k < ordered.Count; k++)
                            {
                                sum += ordered[k].DoubleValue;
                                count++;
                            }
                            output.Add(new AvgResult(ordered[i].CompanyId, ordered[i].UserKey, count == 0 ? null : sum / count));
                        }
                        return output;
                    }).ToList();
            }

            AssertCurrentDataEqual(Expected());

            AddUser("1", 200, 42);
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());

            DeleteUser(Users.First(x => x.UserKey == 50));
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());
        }

        [Fact]
        public async Task MinBySuffixIncremental()
        {
            double[] values = { 50, 40, 60, 10, 70, 80, 90, 30, 20, 100 };
            for (int i = 0; i < values.Length; i++)
            {
                AddUser("1", i * 10, values[i]);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                min_by(DoubleValue, DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING) as value
            FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedMinMaxBy(-2, int.MaxValue, isMin: true));

            // Delete the global minimum, the suffix change points rebuild.
            DeleteUser(Users.First(x => x.UserKey == 30));
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedMinMaxBy(-2, int.MaxValue, isMin: true));

            // Append a new global minimum at the end, every row sees it.
            AddUser("1", 200, 1);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedMinMaxBy(-2, int.MaxValue, isMin: true));

            // Insert in the middle.
            AddUser("1", 45, 5);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedMinMaxBy(-2, int.MaxValue, isMin: true));
        }

        [Fact]
        public async Task MaxByFollowingSuffixEmptyFrameAtEnd()
        {
            double[] values = { 50, 40, 60, 10, 70, 80 };
            for (int i = 0; i < values.Length; i++)
            {
                AddUser("1", i * 10, values[i]);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                max_by(DoubleValue, DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) as value
            FROM users");
            await WaitForUpdate();

            List<MinByResult> Expected()
            {
                return Users.GroupBy(x => x.CompanyId)
                    .SelectMany(g =>
                    {
                        var ordered = g.OrderBy(x => x.UserKey).ToList();
                        var output = new List<MinByResult>();
                        for (int i = 0; i < ordered.Count; i++)
                        {
                            double? best = default;
                            for (int k = i + 1; k < ordered.Count; k++)
                            {
                                if (best == null || ordered[k].DoubleValue > best)
                                {
                                    best = ordered[k].DoubleValue;
                                }
                            }
                            output.Add(new MinByResult(ordered[i].CompanyId, ordered[i].UserKey, best));
                        }
                        return output;
                    }).ToList();
            }

            AssertCurrentDataEqual(Expected());

            // Delete the maximum at the end, the last row's frame is empty and stays null.
            DeleteUser(Users.First(x => x.UserKey == 50));
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());
        }

        [Fact]
        public async Task LastValueSuffixIncremental()
        {
            for (int i = 0; i < 10; i++)
            {
                // The last non null value sits away from the partition end so late rows can miss it.
                AddUserVisits("1", i * 10, i is 0 or 3 or 5 ? i : null);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                LAST_VALUE(Visits) IGNORE NULLS OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING) as value
            FROM users");
            await WaitForUpdate();

            List<LastValueResult> Expected()
            {
                return Users.GroupBy(x => x.CompanyId)
                    .SelectMany(g =>
                    {
                        var ordered = g.OrderBy(x => x.UserKey).ToList();
                        int lastNonNull = -1;
                        for (int i = 0; i < ordered.Count; i++)
                        {
                            if (ordered[i].Visits != null)
                            {
                                lastNonNull = i;
                            }
                        }
                        var output = new List<LastValueResult>();
                        for (int i = 0; i < ordered.Count; i++)
                        {
                            long? val = lastNonNull >= i - 2 && lastNonNull >= 0 ? ordered[lastNonNull].Visits : null;
                            output.Add(new LastValueResult(ordered[i].CompanyId, ordered[i].UserKey, val));
                        }
                        return output;
                    }).ToList();
            }

            AssertCurrentDataEqual(Expected());

            // Turn the last candidate into a null, the last non null position moves backwards.
            AddUserVisits("1", 50, null);
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());

            // Append a non null at the end.
            AddUserVisits("1", 200, 42);
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());
        }

        [Fact]
        public async Task LastValueRespectNullsSuffixGivesPartitionLastValue()
        {
            for (int i = 0; i < 8; i++)
            {
                AddUserVisits("1", i * 10, i % 2 == 0 ? i : null);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                LAST_VALUE(Visits) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 2 PRECEDING AND UNBOUNDED FOLLOWING) as value
            FROM users");
            await WaitForUpdate();

            List<LastValueResult> Expected()
            {
                return Users.GroupBy(x => x.CompanyId)
                    .SelectMany(g =>
                    {
                        var ordered = g.OrderBy(x => x.UserKey).ToList();
                        long? last = ordered[ordered.Count - 1].Visits;
                        return ordered.Select(x => new LastValueResult(x.CompanyId, x.UserKey, last));
                    }).ToList();
            }

            AssertCurrentDataEqual(Expected());

            // Append a null at the end, every row's value becomes null with respect nulls.
            AddUserVisits("1", 200, null);
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());

            AddUserVisits("1", 210, 99);
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());
        }

        [Fact]
        public async Task EmptyFrameFromGreaterThanToIsNull()
        {
            for (int i = 0; i < 6; i++)
            {
                AddUser("1", i, i + 1);
            }

            // The frame start lies after the frame end, so the frame is always empty.
            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                CAST(SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) AS INT) as sumValue,
                min_by(DoubleValue, DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 1 PRECEDING AND 3 PRECEDING) as minValue
            FROM users");
            await WaitForUpdate();

            List<EmptyFrameResult> Expected()
            {
                return Users.Select(x => new EmptyFrameResult(x.CompanyId, x.UserKey, null, null)).ToList();
            }

            AssertCurrentDataEqual(Expected());

            AddUser("1", 10, 7);
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());
        }

        public record EmptyFrameResult(string? companyId, int userkey, long? sumValue, double? minValue);

        [Fact]
        public async Task HugeFrameOnSmallPartition()
        {
            for (int i = 0; i < 20; i++)
            {
                AddUser("1", i, i + 1);
            }

            // A frame much larger than the partition must behave like a running sum without
            // preallocating the declared frame size.
            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                CAST(SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 5000000 PRECEDING AND CURRENT ROW) AS INT) as value
            FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedRunningSum());

            AddUser("1", 100, 42);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedRunningSum());
        }

        [Fact]
        public async Task MinByUnboundedToFollowingIncremental()
        {
            double[] values = { 50, 40, 60, 10, 70, 80, 90, 30, 20, 100 };
            for (int i = 0; i < values.Length; i++)
            {
                AddUser("1", i * 10, values[i]);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                min_by(DoubleValue, DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN UNBOUNDED PRECEDING AND 2 FOLLOWING) as value
            FROM users");
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedMinMaxBy(long.MinValue, 2, isMin: true));

            // Append a row that is not a new minimum, only the last rows recompute.
            AddUser("1", 200, 500);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedMinMaxBy(long.MinValue, 2, isMin: true));

            // Append a new global minimum, the two rows before it pick it up through the lookahead.
            AddUser("1", 210, 1);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedMinMaxBy(long.MinValue, 2, isMin: true));

            // Delete the global minimum.
            DeleteUser(Users.First(x => x.UserKey == 210));
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedMinMaxBy(long.MinValue, 2, isMin: true));
        }

        public record UnboundedSumResult(string? companyId, int userkey, string? firstName, long? total);

        private const string UnboundedSumQuery = @"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                FirstName,
                CAST(SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS INT) as total
            FROM users";

        private List<UnboundedSumResult> ExpectedUnboundedSum()
        {
            return Users.GroupBy(x => x.CompanyId)
                .SelectMany(g =>
                {
                    var total = (long)g.Sum(x => x.DoubleValue);
                    return g.OrderBy(x => x.UserKey)
                        .Select(x => new UnboundedSumResult(x.CompanyId, x.UserKey, x.FirstName, total));
                }).ToList();
        }

        private User AddUserWithName(string companyId, int userKey, double doubleValue, string firstName)
        {
            var user = new User
            {
                UserKey = userKey,
                CompanyId = companyId,
                DoubleValue = doubleValue,
                FirstName = firstName
            };
            AddOrUpdateUser(user);
            return user;
        }

        /// <summary>
        /// Inserting a few wide rows into the middle of a persistent-tree leaf makes the leaf's N-way
        /// split leave its last node without any rows: the split boundaries land inside the wide run,
        /// and the minimum-rows-per-node deficit loop then drains the leaf's remaining rows and the
        /// rest of the batch into the middle node, so an empty page is persisted and stays linked in
        /// the middle of the tree. The whole-partition sum pre-scans the partition with
        /// BulkWindowForwardPartitionReader, which treats an empty page as the end of the partition,
        /// so every row stored after the empty page is missing from the total emitted for all rows.
        /// </summary>
        [Fact]
        public async Task UnboundedSumWideRowSplitMidPartition()
        {
            // Layout phase: one partition whose rows span two leaves at the default 32KiB page size.
            // 20 small rows (keys 1..20), 8 filler rows of ~4200 bytes (keys 30,32..44) and 20 small
            // witness rows (keys 100..119) load as one batch of ~35KiB, which splits into a first
            // leaf holding the small rows plus four fillers (separator at the key-36 filler) and a
            // second leaf holding the remaining fillers and all witnesses.
            for (int i = 1; i <= 20; i++)
            {
                AddUserWithName("a", i, 1, "s");
            }
            for (int i = 0; i < 8; i++)
            {
                AddUserWithName("a", 30 + i * 2, 1, new string('f', 4200));
            }
            for (int i = 0; i < 20; i++)
            {
                AddUserWithName("a", 100 + i, 1, "w");
            }

            await StartStream(UnboundedSumQuery);
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedUnboundedSum());

            // Split phase: insert four ~20KiB rows at keys 21..24, between the small rows and the
            // fillers of the first leaf. The leaf (~17KiB + ~80KiB incoming) splits three ways: the
            // first boundary lands after one wide row, the second boundary's deficit loop then
            // consumes the remaining wide rows and the leaf's trailing fillers for the middle node,
            // and the third node is persisted as an empty page sitting before the leaf that holds
            // the remaining fillers and the witnesses.
            for (int i = 0; i < 4; i++)
            {
                AddUserWithName("a", 21 + i, 1000, new string('h', 20000));
            }

            // The partition scan recomputes every row of the partition: the whole-partition pre-scan
            // stops at the empty page, so the remaining fillers and all witness rows are missing from
            // the emitted total on every row.
            await WaitForUpdate();
            AssertCurrentDataEqual(ExpectedUnboundedSum());
        }

        // ---- Regression tests for bugs found by BulkWindowOracleTests ----

        // Bug 1: a FOLLOWING-start frame loads positions 0..to on the very first row before any eviction,
        // which is to + 1 entries, more than the frame width. The frame ring was sized for the frame width
        // and overflowed. Found on min_by/max_by; the same latent overflow existed in sum and avg.

        [Fact]
        public async Task MinByFollowingOnlyFrameDoesNotOverflow()
        {
            for (int i = 0; i < 10; i++)
            {
                AddUser("1", i, i);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT CompanyId, UserKey,
                min_by(DoubleValue, DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 4 FOLLOWING AND 4 FOLLOWING) as value
            FROM users");
            await WaitForUpdate();

            // The frame is the single row 4 ahead, so the value is that row's DoubleValue or null past the end.
            var expected = Enumerable.Range(0, 10)
                .Select(i => new MinByResult("1", i, i + 4 <= 9 ? (double?)(i + 4) : null));
            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task MaxByFollowingOnlyFrameDoesNotOverflow()
        {
            for (int i = 0; i < 10; i++)
            {
                AddUser("1", i, i);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT CompanyId, UserKey,
                max_by(DoubleValue, DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 4 FOLLOWING AND 4 FOLLOWING) as value
            FROM users");
            await WaitForUpdate();

            var expected = Enumerable.Range(0, 10)
                .Select(i => new MinByResult("1", i, i + 4 <= 9 ? (double?)(i + 4) : null));
            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task SumFollowingOnlyFrameDoesNotOverflow()
        {
            for (int i = 0; i < 10; i++)
            {
                AddUser("1", i, i);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT CompanyId, UserKey,
                CAST(SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 4 FOLLOWING AND 4 FOLLOWING) AS INT) as value
            FROM users");
            await WaitForUpdate();

            var expected = Enumerable.Range(0, 10)
                .Select(i => new SumResult("1", i, i + 4 <= 9 ? (long?)(i + 4) : null));
            AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task AverageFollowingOnlyFrameDoesNotOverflow()
        {
            for (int i = 0; i < 10; i++)
            {
                AddUser("1", i, i);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT CompanyId, UserKey,
                AVG(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 4 FOLLOWING AND 4 FOLLOWING) as value
            FROM users");
            await WaitForUpdate();

            var expected = Enumerable.Range(0, 10)
                .Select(i => new AvgResult("1", i, i + 4 <= 9 ? (double?)(i + 4) : null));
            AssertCurrentDataEqual(expected);
        }

        // Bug 3: SQL SUM over a frame with no non-null values is null, but the incremental sum reaches a
        // typed 0 when values are added then all subtracted out again. The fix tracks a non-null count.

        [Fact]
        public async Task SumBoundedFrameAllNullReturnsNull()
        {
            // The only non null value leaves the frame at row 3, whose frame is then all null.
            AddUserVisits("1", 0, 3);
            AddUserVisits("1", 1, null);
            AddUserVisits("1", 2, null);
            AddUserVisits("1", 3, null);
            AddUserVisits("1", 4, null);
            AddUserVisits("1", 5, null);

            await StartStream(@"
            INSERT INTO output
            SELECT CompanyId, UserKey,
                CAST(SUM(CAST(Visits AS DOUBLE)) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS INT) as value
            FROM users");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new SumResult("1", 0, 3),
                new SumResult("1", 1, 3),
                new SumResult("1", 2, 3),
                new SumResult("1", 3, null),
                new SumResult("1", 4, null),
                new SumResult("1", 5, null)
            });
        }

        [Fact]
        public async Task SumFollowingFrameAllNullReturnsNull()
        {
            // The only non null value is at row 3; it is inside the frame for rows 1 and 2, then leaves.
            AddUserVisits("1", 0, null);
            AddUserVisits("1", 1, null);
            AddUserVisits("1", 2, null);
            AddUserVisits("1", 3, 5);
            AddUserVisits("1", 4, null);
            AddUserVisits("1", 5, null);

            await StartStream(@"
            INSERT INTO output
            SELECT CompanyId, UserKey,
                CAST(SUM(CAST(Visits AS DOUBLE)) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS INT) as value
            FROM users");
            await WaitForUpdate();

            AssertCurrentDataEqual(new[]
            {
                new SumResult("1", 0, null),
                new SumResult("1", 1, 5),
                new SumResult("1", 2, 5),
                new SumResult("1", 3, null),
                new SumResult("1", 4, null),
                new SumResult("1", 5, null)
            });
        }

        [Fact]
        public async Task SumSuffixFrameAllNullReturnsNull()
        {
            AddUserVisits("1", 0, 7);
            AddUserVisits("1", 1, null);
            AddUserVisits("1", 2, null);
            AddUserVisits("1", 3, null);

            await StartStream(@"
            INSERT INTO output
            SELECT CompanyId, UserKey,
                CAST(SUM(CAST(Visits AS DOUBLE)) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING) AS INT) as value
            FROM users");
            await WaitForUpdate();

            // Rows 0 and 1 include position 0 (value 7); rows 2 and 3 exclude it and see only nulls.
            AssertCurrentDataEqual(new[]
            {
                new SumResult("1", 0, 7),
                new SumResult("1", 1, 7),
                new SumResult("1", 2, null),
                new SumResult("1", 3, null)
            });
        }

        [Fact]
        public async Task SumSuffixFollowingFrameAllNullReturnsNull()
        {
            AddUserVisits("1", 0, null);
            AddUserVisits("1", 1, 7);
            AddUserVisits("1", 2, null);
            AddUserVisits("1", 3, null);
            AddUserVisits("1", 4, null);

            await StartStream(@"
            INSERT INTO output
            SELECT CompanyId, UserKey,
                CAST(SUM(CAST(Visits AS DOUBLE)) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING) AS INT) as value
            FROM users");
            await WaitForUpdate();

            // Only row 0's frame [1..end] includes position 1 (value 7); every later frame is all null.
            AssertCurrentDataEqual(new[]
            {
                new SumResult("1", 0, 7),
                new SumResult("1", 1, null),
                new SumResult("1", 2, null),
                new SumResult("1", 3, null),
                new SumResult("1", 4, null)
            });
        }

        // General coverage for min_by suffix across several partitions with nullable compare values and a
        // mutation. The specific stack-column desync crash (bug 2) is guarded by
        // MinBySuffixReusedStackColumnsSurviveCrash, which reproduces the exact re-initialization scenario.
        [Fact]
        public async Task MinBySuffixAcrossPartitionsWithNullsAndMutation()
        {
            // Three partitions with different value shapes so the suffix stack grows to different depths.
            int key = 0;
            AddUserVisits("a", key++, 5);
            AddUserVisits("a", key++, 4);
            AddUserVisits("a", key++, 3);
            AddUserVisits("a", key++, 2);
            AddUserVisits("b", key++, 1);
            AddUserVisits("b", key++, null);
            AddUserVisits("b", key++, 9);
            AddUserVisits("c", key++, null);
            AddUserVisits("c", key++, 7);
            AddUserVisits("c", key++, null);
            AddUserVisits("c", key++, 8);

            await StartStream(@"
            INSERT INTO output
            SELECT CompanyId, UserKey,
                min_by(CAST(Visits AS DOUBLE), CAST(Visits AS DOUBLE)) OVER (PARTITION BY CompanyId ORDER BY UserKey DESC ROWS BETWEEN 3 PRECEDING AND UNBOUNDED FOLLOWING) as value
            FROM users");
            await WaitForUpdate();

            List<MinByResult> Expected()
            {
                return Users.GroupBy(x => x.CompanyId)
                    .SelectMany(g =>
                    {
                        var ordered = g.OrderByDescending(x => x.UserKey).ToList();
                        int n = ordered.Count;
                        var output = new List<MinByResult>();
                        for (int i = 0; i < n; i++)
                        {
                            double? best = null;
                            for (int j = Math.Max(0, i - 3); j < n; j++)
                            {
                                if (ordered[j].Visits.HasValue && (best == null || ordered[j].Visits.Value < best))
                                {
                                    best = ordered[j].Visits.Value;
                                }
                            }
                            output.Add(new MinByResult(ordered[i].CompanyId, ordered[i].UserKey, best));
                        }
                        return output;
                    }).ToList();
            }

            AssertCurrentDataEqual(Expected());

            // A mutation forces a re-scan of a partition, reusing the stack columns.
            AddUserVisits("b", 5, 0);
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());
        }

        [Fact]
        public async Task MinBySuffixReusedStackColumnsSurviveCrash()
        {
            // The suffix stack columns are reused across scans. A stale allocation counter that survived a
            // re-initialization (crash recovery re-inits the function while the counter kept the pre-crash
            // depth) made the reused columns index past their real length. Build a stack, crash, then re-scan.
            void Add(int key, double compare, int value) =>
                AddOrUpdateUser(new User { UserKey = key, CompanyId = "a", DoubleValue = compare, Visits = value });

            // Increasing compare so nothing pops: the stack grows to full depth.
            for (int i = 0; i < 8; i++)
            {
                Add(i, i, i);
            }

            await StartStream(@"
            INSERT INTO output
            SELECT CompanyId, UserKey,
                min_by(CAST(Visits AS DOUBLE), DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as value
            FROM users");
            await WaitForUpdate();

            List<MinByResult> Expected()
            {
                return Users.GroupBy(x => x.CompanyId)
                    .SelectMany(g =>
                    {
                        var ordered = g.OrderBy(x => x.UserKey).ToList();
                        int n = ordered.Count;
                        var output = new List<MinByResult>();
                        for (int i = 0; i < n; i++)
                        {
                            double bestCompare = double.MaxValue;
                            double? best = null;
                            for (int j = i; j < n; j++)
                            {
                                if (ordered[j].DoubleValue < bestCompare)
                                {
                                    bestCompare = ordered[j].DoubleValue;
                                    best = ordered[j].Visits;
                                }
                            }
                            output.Add(new MinByResult(ordered[i].CompanyId, ordered[i].UserKey, best));
                        }
                        return output;
                    }).ToList();
            }

            AssertCurrentDataEqual(Expected());

            await Crash();

            // A mutation after recovery forces the partition to be re-scanned, reusing the stack columns.
            Add(8, 8, 8);
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());
        }

        public record RowNumberDoubleResult(string? companyId, int userkey, double? value);

        private static User NullOrderUser(int key, string company, int? visits) => new User
        {
            UserKey = key,
            CompanyId = company,
            DoubleValue = 0,
            Visits = visits
        };

        [Fact]
        public async Task DeleteNullOrderKeyRowDoesNotLeavePhantom()
        {
            // Regression for the phantom left behind when a row whose ORDER BY key is null is deleted under
            // DESC NULLS FIRST. The deleted null row lingered in the row number ordinal walk, so every later
            // row kept its old number and the sequence skipped a value. An all null incoming order column is
            // a Null typed column that reported no null flag, so the boundary search wrongly collapsed the
            // swapped null direction and placed the null probe last. Found by randomized nullable order probing.
            var sql = @"
                INSERT INTO output
                SELECT CompanyId, UserKey, CAST(ROW_NUMBER() OVER (PARTITION BY CompanyId ORDER BY Visits DESC NULLS FIRST) AS DOUBLE) as val
                FROM users";

            // Five nulls plus a sixth (key 63) that is the last entry in the null bucket, plus non null rows
            // across three value groups.
            AddOrUpdateUser(NullOrderUser(8, "c0", 0));
            AddOrUpdateUser(NullOrderUser(18, "c0", 3));
            AddOrUpdateUser(NullOrderUser(20, "c0", 1));
            AddOrUpdateUser(NullOrderUser(38, "c0", 0));
            AddOrUpdateUser(NullOrderUser(41, "c0", null));
            AddOrUpdateUser(NullOrderUser(43, "c0", null));
            AddOrUpdateUser(NullOrderUser(51, "c0", 1));
            AddOrUpdateUser(NullOrderUser(52, "c0", null));
            AddOrUpdateUser(NullOrderUser(53, "c0", null));
            AddOrUpdateUser(NullOrderUser(54, "c0", 0));
            AddOrUpdateUser(NullOrderUser(56, "c0", 1));
            AddOrUpdateUser(NullOrderUser(59, "c0", 2));
            AddOrUpdateUser(NullOrderUser(62, "c0", null));
            AddOrUpdateUser(NullOrderUser(63, "c0", null));

            await StartStream(sql);
            await WaitForUpdate();

            List<RowNumberDoubleResult> Expected()
            {
                var ordered = Users
                    .OrderBy(u => u.Visits.HasValue ? 1 : 0)          // nulls first
                    .ThenByDescending(u => u.Visits ?? 0)             // then value descending
                    .ThenBy(u => u.UserKey)                           // tie break
                    .ToList();
                var output = new List<RowNumberDoubleResult>();
                for (int i = 0; i < ordered.Count; i++)
                {
                    output.Add(new RowNumberDoubleResult(ordered[i].CompanyId, ordered[i].UserKey, i + 1));
                }
                return output;
            }

            AssertCurrentDataEqual(Expected());

            // In one batch add a new non null row and delete the last null row, reproducing the trigger.
            AddOrUpdateUser(NullOrderUser(64, "c0", 2));
            DeleteUser(NullOrderUser(63, "c0", null));

            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());
        }
    }
}
