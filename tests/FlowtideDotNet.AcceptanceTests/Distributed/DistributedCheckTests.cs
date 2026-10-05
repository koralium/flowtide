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
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter;
using FlowtideDotNet.Core.Engine.Distributed;
using FlowtideDotNet.Core.Optimizer;
using FlowtideDotNet.Core.Optimizer.DistributedMode;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Sql;
using System.Collections.Concurrent;
using System.Diagnostics;

namespace FlowtideDotNet.AcceptanceTests.Distributed
{
    public class DistributedCheckTests : IAsyncLifetime
    {
        private const string TestName = "e2e_check_lanes";

        private readonly MockDatabase _db;
        private readonly DatasetGenerator _generator;
        private DistributedFlowtideStream? _stream;

        public DistributedCheckTests()
        {
            FastEngineTimings.Apply();
            _db = new MockDatabase();
            _generator = new DatasetGenerator(_db);
        }

        public Task InitializeAsync()
        {
            return Task.CompletedTask;
        }

        public async Task DisposeAsync()
        {
            if (_stream != null)
            {
                await _stream.DisposeAsync();
            }
        }

        [Fact]
        public async Task CheckInPartitionedJoinReportsFromEveryLane()
        {
            _generator.Generate(500);
            // Failing orders spread over every fifth user
            var users = _generator.Users;
            var failingOrders = _generator.Orders.Where(x => x.OrderKey >= 900).ToList();
            for (int i = 0; i < failingOrders.Count; i++)
            {
                failingOrders[i] = WithUserKey(failingOrders[i], users[i * 5 % users.Count].UserKey);
                _generator.AddOrUpdateOrder(failingOrders[i]);
            }

            var listener = new CheckIssueListener();
            var status = new CheckStatusListener(listener);
            var latestData = new ConcurrentDictionary<string, EventBatchData>();
            var failures = new ConcurrentBag<(string Substream, Exception? Exception)>();

            _stream = new DistributedStreamBuilder(TestName)
                .AddPlan(() =>
                {
                    var sqlPlanBuilder = new SqlPlanBuilder();
                    sqlPlanBuilder.AddTableProvider(new DatasetTableProvider(_db));
                    sqlPlanBuilder.Sql(@"
                    INSERT INTO output
                    SELECT CHECK_VALUE(u.userkey, o.orderkey < 900, 'Order {orderkey} is too large', orderkey => o.orderkey) AS userkey
                    FROM users u
                    INNER JOIN orders o ON u.userkey = o.userkey;
                    ");
                    var plan = PlanOptimizer.Optimize(sqlPlanBuilder.GetPlan(), new PlanOptimizerSettings()
                    {
                        DistributedPlanOptions = new DistributedPlanOptions() { SubstreamCount = 2 }
                    });
                    // Substream grains receive the distributed plan as json
                    return new SubstraitDeserializer().Deserialize(SubstraitSerializer.SerializeToJson(plan));
                }, optimize: false)
                .WithStateOptionsFactory((streamName, substreamName) => DistributedStreamE2ETests.CreateStateOptions(TestName, substreamName))
                .ConfigureSubstream((substreamName, substreamBuilder) =>
                {
                    var connectorManager = new ConnectorManager();
                    connectorManager.AddSource(new MockSourceFactory("*", _db, false));
                    connectorManager.AddSink(new MockSinkFactory("*", data => latestData[substreamName] = data, 0, watermark => { }));
                    substreamBuilder.AddConnectorManager(connectorManager);
                    substreamBuilder.WithFailureListener(e => failures.Add((substreamName, e)));
                    substreamBuilder.WithCheckFailureListener(listener);
                    substreamBuilder.WithCheckStatusListener(status);
                })
                .Build();

            await _stream.StartAsync();

            await WaitForSinkData(latestData, failures, GetExpectedRows());
            var initialIssues = GetExpectedIssues();
            await WaitForIssues(listener, failures, initialIssues);

            // One check operator per lane, each its own scope
            var checkIds = listener.CheckIds();
            Assert.Equal(2, checkIds.Count);
            Assert.Contains(checkIds, x => x.StartsWith("substream_0/", StringComparison.Ordinal));
            Assert.Contains(checkIds, x => x.StartsWith("substream_1/", StringComparison.Ordinal));
            Assert.Equal(2, listener.StreamNames().Count);
            Assert.Equal(2, listener.ResetCount);
            Assert.Equal(new[] { "Order {orderkey} is too large" }, listener.CheckNames());

            // Each lane reports the status of its own part
            await WaitForStatus(status, failures, initialIssues.Count);
            Assert.Equal(checkIds.OrderBy(x => x, StringComparer.Ordinal), status.Latest().Keys.Select(x => x.CheckId).OrderBy(x => x, StringComparer.Ordinal));
            Assert.All(status.Statuses(), x => Assert.Equal("Order {orderkey} is too large", x.CheckName));
            Assert.All(status.Latest().Values, x => Assert.True(x.ActiveIssues > 0));

            // Deleted orders and deleted users resolve issues
            var deletedOrders = failingOrders.Skip(50).Take(20).ToList();
            var deletedUsers = _generator.Users.Take(50).ToList();
            Assert.Contains(failingOrders, o => !deletedOrders.Contains(o) && deletedUsers.Any(u => u.UserKey == o.UserKey));
            foreach (var order in deletedOrders)
            {
                _generator.DeleteOrder(order);
            }
            foreach (var user in deletedUsers)
            {
                _generator.DeleteUser(user);
            }

            await WaitForSinkData(latestData, failures, GetExpectedRows());
            var remainingIssues = GetExpectedIssues();
            await WaitForIssues(listener, failures, remainingIssues);
            await WaitForStatus(status, failures, remainingIssues.Count);

            Assert.Equal(initialIssues.Count, listener.RaisedCount);
            Assert.Equal(initialIssues.Count - remainingIssues.Count, listener.ResolvedCount);
            Assert.Equal(2, listener.ResetCount);
            Assert.Empty(failures);
        }

        private List<UserKeyRow> GetExpectedRows()
        {
            return _generator.Orders
                .Join(_generator.Users, o => o.UserKey, u => u.UserKey, (o, u) => new UserKeyRow(u.UserKey))
                .ToList();
        }

        private List<string> GetExpectedIssues()
        {
            return _generator.Orders
                .Join(_generator.Users, o => o.UserKey, u => u.UserKey, (o, u) => o)
                .Where(x => x.OrderKey >= 900)
                .Select(x => $"Order {x.OrderKey} is too large")
                .OrderBy(x => x, StringComparer.Ordinal)
                .ToList();
        }

        private static Order WithUserKey(Order order, int userKey)
        {
            return new Order()
            {
                OrderKey = order.OrderKey,
                UserKey = userKey,
                Orderdate = order.Orderdate,
                GuidVal = order.GuidVal,
                Money = order.Money
            };
        }

        private record UserKeyRow(long UserKey);

        private static async Task WaitForIssues(
            CheckIssueListener listener,
            ConcurrentBag<(string Substream, Exception? Exception)> failures,
            List<string> expected)
        {
            var stopwatch = Stopwatch.StartNew();
            while (stopwatch.Elapsed < TimeSpan.FromSeconds(60) && !expected.SequenceEqual(listener.ActiveIssues()))
            {
                ThrowOnFailure(failures);
                await Task.Delay(10);
            }
            Assert.Equal(expected, listener.ActiveIssues());
            Assert.Empty(listener.Violations());
        }

        /// <summary>
        /// Waits until the latest status of both lanes adds up to the expected issues, one failing row each.
        /// </summary>
        private static async Task WaitForStatus(
            CheckStatusListener status,
            ConcurrentBag<(string Substream, Exception? Exception)> failures,
            int expectedIssues)
        {
            var stopwatch = Stopwatch.StartNew();
            while (stopwatch.Elapsed < TimeSpan.FromSeconds(60) && !StatusMatches(status, expectedIssues))
            {
                ThrowOnFailure(failures);
                await Task.Delay(10);
            }
            var latest = status.Latest().Values.ToList();
            Assert.Equal(2, latest.Count);
            Assert.Equal(expectedIssues, latest.Sum(x => x.ActiveIssues));
            Assert.Equal(expectedIssues, latest.Sum(x => x.FailingRows));
            Assert.Empty(status.Violations());
        }

        private static bool StatusMatches(CheckStatusListener status, int expectedIssues)
        {
            var latest = status.Latest().Values.ToList();
            return latest.Count == 2 && latest.Sum(x => x.ActiveIssues) == expectedIssues && latest.Sum(x => x.FailingRows) == expectedIssues;
        }

        private static async Task WaitForSinkData(
            ConcurrentDictionary<string, EventBatchData> latestData,
            ConcurrentBag<(string Substream, Exception? Exception)> failures,
            List<UserKeyRow> expected)
        {
            var expectedBatch = BatchConverter.ConvertToBatchSorted(expected, GlobalMemoryManager.Instance);
            var stopwatch = Stopwatch.StartNew();
            while (true)
            {
                ThrowOnFailure(failures);
                if (latestData.TryGetValue("substream_0", out var actual))
                {
                    try
                    {
                        EventBatchAssertion.Equal(expectedBatch, actual);
                        return;
                    }
                    catch when (stopwatch.Elapsed < TimeSpan.FromSeconds(60))
                    {
                        // Data has not caught up yet
                    }
                }
                else if (stopwatch.Elapsed >= TimeSpan.FromSeconds(60))
                {
                    Assert.Fail("The sink substream never produced any output data.");
                }
                await Task.Delay(10);
            }
        }

        private static void ThrowOnFailure(ConcurrentBag<(string Substream, Exception? Exception)> failures)
        {
            var failure = failures.FirstOrDefault(x => x.Exception != null);
            if (failure.Exception != null)
            {
                throw new Exception($"Substream {failure.Substream} failed", failure.Exception);
            }
        }
    }
}
