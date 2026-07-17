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
    /// Randomized differential probes for the bulk window operator: seeded mutation churn asserted
    /// against brute force recomputation after every cycle. Seeds are fixed so failures reproduce.
    /// </summary>
    public class BulkWindowProbeTests : FlowtideAcceptanceBase
    {
        public BulkWindowProbeTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
        {
        }

        public record SumRow(string? companyId, int userkey, long? value);
        public record MinByKeyRow(string? companyId, int userkey, long? best);
        public record MultiRow(string? companyId, int userkey, long? sum, long rn, double? minValue);
        public record DupRow(string? companyId, double value, long rn);
        public record LeadRow(string? companyId, int userkey, long? value);

        private void AddUser(string? companyId, int key, double value, int? visits = null)
        {
            AddOrUpdateUser(new User()
            {
                UserKey = key,
                CompanyId = companyId,
                DoubleValue = value,
                Visits = visits
            });
        }

        /// <summary>
        /// Applies a cycle of random mutations: inserts, updates, deletes, same batch replaces,
        /// no op updates and partition moves. Always performs at least three inserts so every
        /// cycle produces a visible change.
        /// </summary>
        private void Churn(Random rng, string?[] partitions, int ops, int keySpace, int valueDomain, bool mutateVisits = false)
        {
            int? RandomVisits()
            {
                if (!mutateVisits)
                {
                    return null;
                }
                var roll = rng.Next(10);
                if (roll < 2)
                {
                    return null;
                }
                if (roll < 4)
                {
                    return -rng.Next(1, 4);
                }
                return rng.Next(0, 6);
            }

            for (int i = 0; i < 3; i++)
            {
                AddUser(partitions[rng.Next(partitions.Length)], rng.Next(keySpace), rng.Next(1, valueDomain + 1), RandomVisits());
            }

            for (int i = 0; i < ops; i++)
            {
                var roll = rng.Next(100);
                if (roll < 40 || Users.Count == 0)
                {
                    AddUser(partitions[rng.Next(partitions.Length)], rng.Next(keySpace), rng.Next(1, valueDomain + 1), RandomVisits());
                }
                else if (roll < 55)
                {
                    var existing = Users[rng.Next(Users.Count)];
                    AddUser(existing.CompanyId, existing.UserKey, rng.Next(1, valueDomain + 1), RandomVisits());
                }
                else if (roll < 70)
                {
                    DeleteUser(Users[rng.Next(Users.Count)]);
                }
                else if (roll < 80)
                {
                    // Delete and reinsert the same key within the same cycle.
                    var existing = Users[rng.Next(Users.Count)];
                    DeleteUser(existing);
                    AddUser(existing.CompanyId, existing.UserKey, rng.Next(1, valueDomain + 1), RandomVisits());
                }
                else if (roll < 88)
                {
                    // No op update, the row is replaced with identical values.
                    var existing = Users[rng.Next(Users.Count)];
                    AddUser(existing.CompanyId, existing.UserKey, existing.DoubleValue, existing.Visits);
                }
                else
                {
                    // Move the row to another partition.
                    var existing = Users[rng.Next(Users.Count)];
                    AddUser(partitions[rng.Next(partitions.Length)], existing.UserKey, existing.DoubleValue, existing.Visits);
                }
            }
        }

        private static readonly string?[] DefaultPartitions = { "a", "b", "c", "d", null };

        [Fact]
        public async Task RunningSumChurn()
        {
            var rng = new Random(9001);
            Churn(rng, DefaultPartitions, 60, 400, 50);
            await StartStream(@"
            INSERT INTO output
            SELECT CompanyId, UserKey,
            CAST(SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS INT)
            FROM users");

            List<SumRow> Expected()
            {
                return Users.GroupBy(x => x.CompanyId)
                    .SelectMany(g =>
                    {
                        var ordered = g.OrderBy(x => x.UserKey).ToList();
                        var output = new List<SumRow>();
                        double sum = 0;
                        for (int i = 0; i < ordered.Count; i++)
                        {
                            sum += ordered[i].DoubleValue;
                            output.Add(new SumRow(ordered[i].CompanyId, ordered[i].UserKey, (long)sum));
                        }
                        return output;
                    }).ToList();
            }

            for (int cycle = 0; cycle < 15; cycle++)
            {
                await WaitForUpdate();
                AssertCurrentDataEqual(Expected());
                Churn(rng, DefaultPartitions, 60, 400, 50);
            }
        }

        [Fact]
        public async Task BoundedFollowingSumChurn()
        {
            var rng = new Random(9002);
            Churn(rng, DefaultPartitions, 60, 400, 50);
            await StartStream(@"
            INSERT INTO output
            SELECT CompanyId, UserKey,
            CAST(SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 2 PRECEDING AND 3 FOLLOWING) AS INT)
            FROM users");

            List<SumRow> Expected()
            {
                return Users.GroupBy(x => x.CompanyId)
                    .SelectMany(g =>
                    {
                        var ordered = g.OrderBy(x => x.UserKey).ToList();
                        var output = new List<SumRow>();
                        for (int i = 0; i < ordered.Count; i++)
                        {
                            double sum = 0;
                            for (int k = Math.Max(0, i - 2); k <= Math.Min(ordered.Count - 1, i + 3); k++)
                            {
                                sum += ordered[k].DoubleValue;
                            }
                            output.Add(new SumRow(ordered[i].CompanyId, ordered[i].UserKey, (long)sum));
                        }
                        return output;
                    }).ToList();
            }

            for (int cycle = 0; cycle < 15; cycle++)
            {
                await WaitForUpdate();
                AssertCurrentDataEqual(Expected());
                Churn(rng, DefaultPartitions, 60, 400, 50);
            }
        }

        [Fact]
        public async Task MinByTiesChurn()
        {
            // A small value domain makes compare ties frequent, the oldest row must win a tie.
            var rng = new Random(9003);
            Churn(rng, DefaultPartitions, 50, 300, 4);
            await StartStream(@"
            INSERT INTO output
            SELECT CompanyId, UserKey,
            min_by(UserKey, DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
            FROM users");

            List<MinByKeyRow> Expected()
            {
                return Users.GroupBy(x => x.CompanyId)
                    .SelectMany(g =>
                    {
                        var ordered = g.OrderBy(x => x.UserKey).ToList();
                        var output = new List<MinByKeyRow>();
                        for (int i = 0; i < ordered.Count; i++)
                        {
                            double? best = default;
                            long? bestKey = default;
                            for (int k = Math.Max(0, i - 4); k <= i; k++)
                            {
                                if (best == null || ordered[k].DoubleValue < best)
                                {
                                    best = ordered[k].DoubleValue;
                                    bestKey = ordered[k].UserKey;
                                }
                            }
                            output.Add(new MinByKeyRow(ordered[i].CompanyId, ordered[i].UserKey, bestKey));
                        }
                        return output;
                    }).ToList();
            }

            for (int cycle = 0; cycle < 15; cycle++)
            {
                await WaitForUpdate();
                AssertCurrentDataEqual(Expected());
                Churn(rng, DefaultPartitions, 50, 300, 4);
            }
        }

        [Fact]
        public async Task MaxBySuffixTiesChurn()
        {
            var rng = new Random(9004);
            Churn(rng, DefaultPartitions, 50, 300, 4);
            await StartStream(@"
            INSERT INTO output
            SELECT CompanyId, UserKey,
            max_by(UserKey, DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
            FROM users");

            List<MinByKeyRow> Expected()
            {
                return Users.GroupBy(x => x.CompanyId)
                    .SelectMany(g =>
                    {
                        var ordered = g.OrderBy(x => x.UserKey).ToList();
                        var output = new List<MinByKeyRow>();
                        for (int i = 0; i < ordered.Count; i++)
                        {
                            double? best = default;
                            long? bestKey = default;
                            for (int k = i; k < ordered.Count; k++)
                            {
                                if (best == null || ordered[k].DoubleValue > best)
                                {
                                    best = ordered[k].DoubleValue;
                                    bestKey = ordered[k].UserKey;
                                }
                            }
                            output.Add(new MinByKeyRow(ordered[i].CompanyId, ordered[i].UserKey, bestKey));
                        }
                        return output;
                    }).ToList();
            }

            for (int cycle = 0; cycle < 15; cycle++)
            {
                await WaitForUpdate();
                AssertCurrentDataEqual(Expected());
                Churn(rng, DefaultPartitions, 50, 300, 4);
            }
        }

        [Fact]
        public async Task MultiFunctionChurn()
        {
            var rng = new Random(9005);
            Churn(rng, DefaultPartitions, 60, 400, 20);
            await StartStream(@"
            INSERT INTO output
            SELECT CompanyId, UserKey,
            CAST(SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS INT),
            ROW_NUMBER() OVER (PARTITION BY CompanyId ORDER BY UserKey),
            min_by(DoubleValue, DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
            FROM users");

            List<MultiRow> Expected()
            {
                return Users.GroupBy(x => x.CompanyId)
                    .SelectMany(g =>
                    {
                        var ordered = g.OrderBy(x => x.UserKey).ToList();
                        var output = new List<MultiRow>();
                        double sum = 0;
                        for (int i = 0; i < ordered.Count; i++)
                        {
                            sum += ordered[i].DoubleValue;
                            double? minValue = default;
                            for (int k = Math.Max(0, i - 3); k <= i; k++)
                            {
                                if (minValue == null || ordered[k].DoubleValue < minValue)
                                {
                                    minValue = ordered[k].DoubleValue;
                                }
                            }
                            output.Add(new MultiRow(ordered[i].CompanyId, ordered[i].UserKey, (long)sum, i + 1, minValue));
                        }
                        return output;
                    }).ToList();
            }

            for (int cycle = 0; cycle < 15; cycle++)
            {
                await WaitForUpdate();
                AssertCurrentDataEqual(Expected());
                Churn(rng, DefaultPartitions, 60, 400, 20);
            }
        }

        [Fact]
        public async Task DescOrderBoundedSumChurn()
        {
            var rng = new Random(9006);
            Churn(rng, DefaultPartitions, 60, 400, 50);
            await StartStream(@"
            INSERT INTO output
            SELECT CompanyId, UserKey,
            CAST(SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey DESC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS INT)
            FROM users");

            List<SumRow> Expected()
            {
                return Users.GroupBy(x => x.CompanyId)
                    .SelectMany(g =>
                    {
                        var ordered = g.OrderByDescending(x => x.UserKey).ToList();
                        var output = new List<SumRow>();
                        for (int i = 0; i < ordered.Count; i++)
                        {
                            double sum = 0;
                            for (int k = Math.Max(0, i - 2); k <= i; k++)
                            {
                                sum += ordered[k].DoubleValue;
                            }
                            output.Add(new SumRow(ordered[i].CompanyId, ordered[i].UserKey, (long)sum));
                        }
                        return output;
                    }).ToList();
            }

            for (int cycle = 0; cycle < 15; cycle++)
            {
                await WaitForUpdate();
                AssertCurrentDataEqual(Expected());
                Churn(rng, DefaultPartitions, 60, 400, 50);
            }
        }

        [Fact]
        public async Task DuplicateRowsRowNumberChurn()
        {
            // Only company and value are projected, so rows sharing both become one stored row with
            // a weight above one. A tiny value domain makes that constant.
            var rng = new Random(9007);
            Churn(rng, DefaultPartitions, 40, 120, 3);
            await StartStream(@"
            INSERT INTO output
            SELECT CompanyId, DoubleValue,
            ROW_NUMBER() OVER (PARTITION BY CompanyId ORDER BY DoubleValue)
            FROM users");

            List<DupRow> Expected()
            {
                return Users.GroupBy(x => x.CompanyId)
                    .SelectMany(g =>
                    {
                        var ordered = g.OrderBy(x => x.DoubleValue).ToList();
                        var output = new List<DupRow>();
                        for (int i = 0; i < ordered.Count; i++)
                        {
                            output.Add(new DupRow(ordered[i].CompanyId, ordered[i].DoubleValue, i + 1));
                        }
                        return output;
                    }).ToList();
            }

            for (int cycle = 0; cycle < 15; cycle++)
            {
                await WaitForUpdate();
                AssertCurrentDataEqual(Expected());
                Churn(rng, DefaultPartitions, 40, 120, 3);
            }
        }

        [Fact]
        public async Task CrashRecoveryMinByChurn()
        {
            // min_by carries auxiliary state columns, churn continues over restored state.
            var rng = new Random(9008);
            Churn(rng, DefaultPartitions, 50, 300, 10);
            await StartStream(@"
            INSERT INTO output
            SELECT CompanyId, UserKey,
            min_by(UserKey, DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 4 PRECEDING AND CURRENT ROW)
            FROM users");

            List<MinByKeyRow> Expected()
            {
                return Users.GroupBy(x => x.CompanyId)
                    .SelectMany(g =>
                    {
                        var ordered = g.OrderBy(x => x.UserKey).ToList();
                        var output = new List<MinByKeyRow>();
                        for (int i = 0; i < ordered.Count; i++)
                        {
                            double? best = default;
                            long? bestKey = default;
                            for (int k = Math.Max(0, i - 4); k <= i; k++)
                            {
                                if (best == null || ordered[k].DoubleValue < best)
                                {
                                    best = ordered[k].DoubleValue;
                                    bestKey = ordered[k].UserKey;
                                }
                            }
                            output.Add(new MinByKeyRow(ordered[i].CompanyId, ordered[i].UserKey, bestKey));
                        }
                        return output;
                    }).ToList();
            }

            for (int cycle = 0; cycle < 6; cycle++)
            {
                await WaitForUpdate();
                AssertCurrentDataEqual(Expected());
                Churn(rng, DefaultPartitions, 50, 300, 10);
            }

            await Crash();

            for (int cycle = 0; cycle < 6; cycle++)
            {
                await WaitForUpdate();
                AssertCurrentDataEqual(Expected());
                Churn(rng, DefaultPartitions, 50, 300, 10);
            }
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());
        }

        [Fact]
        public async Task LargeBatchExpressionPartitionChurn()
        {
            // Batches above the operator's chunk size with a computed partition expression, so the
            // chunked apply path with sorted order chunks runs on every cycle.
            var rng = new Random(9009);
            SourceBatchSize = 10000;
            var partitions = new string?[] { "x" };
            Churn(rng, partitions, 3000, 20000, 50);
            await StartStream(@"
            INSERT INTO output
            SELECT CompanyId, UserKey,
            CAST(SUM(DoubleValue) OVER (PARTITION BY UserKey % 7 ORDER BY UserKey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS INT)
            FROM users");

            List<SumRow> Expected()
            {
                return Users.GroupBy(x => x.UserKey % 7)
                    .SelectMany(g =>
                    {
                        var ordered = g.OrderBy(x => x.UserKey).ToList();
                        var output = new List<SumRow>();
                        double sum = 0;
                        for (int i = 0; i < ordered.Count; i++)
                        {
                            sum += ordered[i].DoubleValue;
                            output.Add(new SumRow(ordered[i].CompanyId, ordered[i].UserKey, (long)sum));
                        }
                        return output;
                    }).ToList();
            }

            for (int cycle = 0; cycle < 5; cycle++)
            {
                await WaitForUpdate();
                AssertCurrentDataEqual(Expected());
                Churn(rng, partitions, 3000, 20000, 50);
            }
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());
        }

        public record AvgRow(string? companyId, int userkey, double? value);
        public record DupSumRow(string? companyId, double value, long sum);
        public record MixedRow(string? companyId, int userkey, long? followingSum, long? runningSum, double? minValue);
        public record TopRow(string? companyId, int userkey);
        public record LastRow(string? companyId, int userkey, long? value);

        [Fact]
        public async Task AverageRunningCrashChurn()
        {
            // Average carries sum and count auxiliary columns, churn continues over restored state.
            var rng = new Random(9011);
            Churn(rng, DefaultPartitions, 50, 300, 20);
            await StartStream(@"
            INSERT INTO output
            SELECT CompanyId, UserKey,
            AVG(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            FROM users");

            List<AvgRow> Expected()
            {
                return Users.GroupBy(x => x.CompanyId)
                    .SelectMany(g =>
                    {
                        var ordered = g.OrderBy(x => x.UserKey).ToList();
                        var output = new List<AvgRow>();
                        double sum = 0;
                        for (int i = 0; i < ordered.Count; i++)
                        {
                            sum += ordered[i].DoubleValue;
                            output.Add(new AvgRow(ordered[i].CompanyId, ordered[i].UserKey, sum / (i + 1)));
                        }
                        return output;
                    }).ToList();
            }

            for (int cycle = 0; cycle < 6; cycle++)
            {
                await WaitForUpdate();
                AssertCurrentDataEqual(Expected());
                Churn(rng, DefaultPartitions, 50, 300, 20);
            }

            await Crash();

            for (int cycle = 0; cycle < 6; cycle++)
            {
                await WaitForUpdate();
                AssertCurrentDataEqual(Expected());
                Churn(rng, DefaultPartitions, 50, 300, 20);
            }
            await WaitForUpdate();
            AssertCurrentDataEqual(Expected());
        }

        [Fact]
        public async Task FollowingFrameDuplicatesChurn()
        {
            // Only company and value are projected, so rows merge with weights above one, and the
            // following frame's lookahead must skip already fed duplicates correctly.
            var rng = new Random(9012);
            Churn(rng, DefaultPartitions, 40, 120, 3);
            await StartStream(@"
            INSERT INTO output
            SELECT CompanyId, DoubleValue,
            CAST(SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY DoubleValue ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING) AS INT)
            FROM users");

            List<DupSumRow> Expected()
            {
                return Users.GroupBy(x => x.CompanyId)
                    .SelectMany(g =>
                    {
                        var ordered = g.OrderBy(x => x.DoubleValue).ToList();
                        var output = new List<DupSumRow>();
                        for (int i = 0; i < ordered.Count; i++)
                        {
                            double sum = 0;
                            for (int k = Math.Max(0, i - 1); k <= Math.Min(ordered.Count - 1, i + 2); k++)
                            {
                                sum += ordered[k].DoubleValue;
                            }
                            output.Add(new DupSumRow(ordered[i].CompanyId, ordered[i].DoubleValue, (long)sum));
                        }
                        return output;
                    }).ToList();
            }

            for (int cycle = 0; cycle < 15; cycle++)
            {
                await WaitForUpdate();
                AssertCurrentDataEqual(Expected());
                Churn(rng, DefaultPartitions, 40, 120, 3);
            }
        }

        [Fact]
        public async Task MultiFunctionMixedFramesChurn()
        {
            // A following frame forces backward anchor walks while bounded and running functions
            // share the same scan, mixing every seeding path in one relation.
            var rng = new Random(9013);
            Churn(rng, DefaultPartitions, 60, 400, 20);
            await StartStream(@"
            INSERT INTO output
            SELECT CompanyId, UserKey,
            CAST(SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 1 PRECEDING AND 2 FOLLOWING) AS INT),
            CAST(SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS INT),
            min_by(DoubleValue, DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
            FROM users");

            List<MixedRow> Expected()
            {
                return Users.GroupBy(x => x.CompanyId)
                    .SelectMany(g =>
                    {
                        var ordered = g.OrderBy(x => x.UserKey).ToList();
                        var output = new List<MixedRow>();
                        double runningSum = 0;
                        for (int i = 0; i < ordered.Count; i++)
                        {
                            runningSum += ordered[i].DoubleValue;
                            double followingSum = 0;
                            for (int k = Math.Max(0, i - 1); k <= Math.Min(ordered.Count - 1, i + 2); k++)
                            {
                                followingSum += ordered[k].DoubleValue;
                            }
                            double? minValue = default;
                            for (int k = Math.Max(0, i - 3); k <= i; k++)
                            {
                                if (minValue == null || ordered[k].DoubleValue < minValue)
                                {
                                    minValue = ordered[k].DoubleValue;
                                }
                            }
                            output.Add(new MixedRow(ordered[i].CompanyId, ordered[i].UserKey, (long)followingSum, (long)runningSum, minValue));
                        }
                        return output;
                    }).ToList();
            }

            for (int cycle = 0; cycle < 15; cycle++)
            {
                await WaitForUpdate();
                AssertCurrentDataEqual(Expected());
                Churn(rng, DefaultPartitions, 60, 400, 20);
            }
        }

        [Fact]
        public async Task RowNumberMaxHintChurn()
        {
            // The filter gives the row number function a max bound, rows past it become null and
            // are dropped, churn moves rows in and out of the top three constantly.
            var rng = new Random(9014);
            Churn(rng, DefaultPartitions, 50, 300, 30);
            await StartStream(@"
            INSERT INTO output
            SELECT CompanyId, UserKey
            FROM users
            WHERE ROW_NUMBER() OVER (PARTITION BY CompanyId ORDER BY UserKey) <= 3");

            List<TopRow> Expected()
            {
                return Users.GroupBy(x => x.CompanyId)
                    .SelectMany(g => g.OrderBy(x => x.UserKey).Take(3).Select(x => new TopRow(x.CompanyId, x.UserKey)))
                    .ToList();
            }

            for (int cycle = 0; cycle < 15; cycle++)
            {
                await WaitForUpdate();
                AssertCurrentDataEqual(Expected());
                Churn(rng, DefaultPartitions, 50, 300, 30);
            }
        }

        [Fact]
        public async Task LagConstantChurn()
        {
            var rng = new Random(9015);
            Churn(rng, DefaultPartitions, 50, 300, 30);
            await StartStream(@"
            INSERT INTO output
            SELECT CompanyId, UserKey,
            lag(UserKey, 2) OVER (PARTITION BY CompanyId ORDER BY UserKey)
            FROM users");

            List<LeadRow> Expected()
            {
                return Users.GroupBy(x => x.CompanyId)
                    .SelectMany(g =>
                    {
                        var ordered = g.OrderBy(x => x.UserKey).ToList();
                        var output = new List<LeadRow>();
                        for (int i = 0; i < ordered.Count; i++)
                        {
                            long? value = i >= 2 ? ordered[i - 2].UserKey : null;
                            output.Add(new LeadRow(ordered[i].CompanyId, ordered[i].UserKey, value));
                        }
                        return output;
                    }).ToList();
            }

            for (int cycle = 0; cycle < 15; cycle++)
            {
                await WaitForUpdate();
                AssertCurrentDataEqual(Expected());
                Churn(rng, DefaultPartitions, 50, 300, 30);
            }
        }

        [Fact]
        public async Task LastValueIgnoreNullsChurn()
        {
            // Delay frame with the ignore nulls candidate chain, Visits carries frequent nulls.
            var rng = new Random(9016);
            Churn(rng, DefaultPartitions, 50, 300, 30, mutateVisits: true);
            await StartStream(@"
            INSERT INTO output
            SELECT CompanyId, UserKey,
            LAST_VALUE(Visits) IGNORE NULLS OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING)
            FROM users");

            List<LastRow> Expected()
            {
                return Users.GroupBy(x => x.CompanyId)
                    .SelectMany(g =>
                    {
                        var ordered = g.OrderBy(x => x.UserKey).ToList();
                        var output = new List<LastRow>();
                        for (int i = 0; i < ordered.Count; i++)
                        {
                            long? value = default;
                            for (int k = Math.Min(ordered.Count - 1, i - 1); k >= Math.Max(0, i - 4); k--)
                            {
                                if (k <= i - 1 && ordered[k].Visits != null)
                                {
                                    value = ordered[k].Visits;
                                    break;
                                }
                            }
                            output.Add(new LastRow(ordered[i].CompanyId, ordered[i].UserKey, value));
                        }
                        return output;
                    }).ToList();
            }

            for (int cycle = 0; cycle < 15; cycle++)
            {
                await WaitForUpdate();
                AssertCurrentDataEqual(Expected());
                Churn(rng, DefaultPartitions, 50, 300, 30, mutateVisits: true);
            }
        }

        [Fact]
        public async Task DynamicLeadChurn()
        {
            // Per row offsets from Visits: null falls back to one, negative reaches backwards.
            var rng = new Random(9010);
            Churn(rng, DefaultPartitions, 50, 300, 30, mutateVisits: true);
            await StartStream(@"
            INSERT INTO output
            SELECT CompanyId, UserKey,
            lead(UserKey, Visits) OVER (PARTITION BY CompanyId ORDER BY UserKey)
            FROM users");

            List<LeadRow> Expected()
            {
                return Users.GroupBy(x => x.CompanyId)
                    .SelectMany(g =>
                    {
                        var ordered = g.OrderBy(x => x.UserKey).ToList();
                        var output = new List<LeadRow>();
                        for (int i = 0; i < ordered.Count; i++)
                        {
                            long offset = ordered[i].Visits ?? 1;
                            long target = i + offset;
                            long? value = target >= 0 && target < ordered.Count ? ordered[(int)target].UserKey : null;
                            output.Add(new LeadRow(ordered[i].CompanyId, ordered[i].UserKey, value));
                        }
                        return output;
                    }).ToList();
            }

            for (int cycle = 0; cycle < 15; cycle++)
            {
                await WaitForUpdate();
                AssertCurrentDataEqual(Expected());
                Churn(rng, DefaultPartitions, 50, 300, 30, mutateVisits: true);
            }
        }
    }
}
