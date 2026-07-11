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
    }
}
