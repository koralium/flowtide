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
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.LocalDisk;
using System.Diagnostics;
using Xunit.Abstractions;

namespace FlowtideDotNet.AcceptanceTests
{
    [Trait("Category", "Chaos")]
    public class CheckpointChaosTests
    {
        private const string Query = @"
            INSERT INTO output
            SELECT o.userkey, count(*), sum(o.orderkey)
            FROM orders o
            INNER JOIN users u ON o.userkey = u.userkey
            GROUP BY o.userkey";

        private static readonly TimeSpan ConvergeTimeout = TimeSpan.FromSeconds(90);

        private readonly ITestOutputHelper _output;

        public CheckpointChaosTests(ITestOutputHelper output)
        {
            _output = output;
            FastEngineTimings.Apply();
        }

        /// <summary>
        /// Small files and a real disk, so walks roll files and a restart reads what an aborted epoch left behind.
        /// </summary>
        private sealed class DiskReservoirTestStream : FlowtideTestStream
        {
            private readonly string _directory;

            public DiskReservoirTestStream(string testName, string directory) : base(testName)
            {
                _directory = directory;
                // Small enough that eviction, the commit walk and fetches overlap.
                CachePageCount = 128;
                MinCachePageCount = 32;
            }

            protected override IPersistentStorage CreatePersistentStorage(string testName, bool ignoreSameDataCheck)
            {
                return new ReservoirPersistentStorage(new ReservoirStorageOptions()
                {
                    FileProvider = new LocalDiskProvider(_directory),
                    MaxFileSize = 256 * 1024,
                    SnapshotCheckpointInterval = 5
                });
            }
        }

        private enum ChaosAction
        {
            None,
            CrashNow,
            CrashAfterDelay,
            CrashAfterCheckpoint,
            RestartFromDisk
        }

        /// <summary>
        /// Every change to the source tables, replayed onto a fresh stream so a restart reads the same change log.
        /// </summary>
        private sealed class Workload
        {
            private readonly Random _random;
            private readonly List<Action<FlowtideTestStream>> _log = new List<Action<FlowtideTestStream>>();
            private readonly List<int> _deletedUsers = new List<int>();
            private readonly List<(int UserKey, string Text)> _roundTrace = new List<(int UserKey, string Text)>();
            private readonly HashSet<int> _arrivedOrders = new HashSet<int>();
            private readonly HashSet<int> _arrivedAtUsers = new HashSet<int>();

            /// <summary>
            /// A group created and emptied inside one round comes out of the aggregate as a count 0 row, with or
            /// without faults. That is an engine finding of its own, FLOWTIDE_CHAOS_SAME_ROUND_EMPTY=1 brings it back.
            /// </summary>
            private readonly bool _allowSameRoundEmpty = Environment.GetEnvironmentVariable("FLOWTIDE_CHAOS_SAME_ROUND_EMPTY") == "1";
            private int _nextUserKey = 1;
            private int _nextOrderKey = 1;

            public Workload(Random random)
            {
                _random = random;
            }

            public void Replay(FlowtideTestStream stream)
            {
                foreach (var operation in _log)
                {
                    operation(stream);
                }
            }

            public void Round(FlowtideTestStream stream)
            {
                _roundTrace.Clear();
                _arrivedOrders.Clear();
                _arrivedAtUsers.Clear();
                var users = _random.Next(30, 90);
                for (int i = 0; i < users; i++)
                {
                    _roundTrace.Add((_nextUserKey, "user added"));
                    Apply(stream, NewUser(_nextUserKey++));
                }
                if (_deletedUsers.Count > 0 && _random.Next(3) == 0)
                {
                    // Back again, the join must pick its surviving orders up.
                    var index = _random.Next(_deletedUsers.Count);
                    _roundTrace.Add((_deletedUsers[index], "user added back"));
                    Apply(stream, NewUser(_deletedUsers[index]));
                    _deletedUsers.RemoveAt(index);
                }

                var orders = _random.Next(300, 1200);
                for (int i = 0; i < orders; i++)
                {
                    var order = NewOrder(_nextOrderKey++, RandomUserKey(stream));
                    _roundTrace.Add((order.UserKey, $"order {order.OrderKey} added"));
                    Arrived(order);
                    Apply(stream, order);
                }

                var moves = _random.Next(50, 200);
                for (int i = 0; i < moves && stream.Orders.Count > 0; i++)
                {
                    var order = stream.Orders[_random.Next(stream.Orders.Count)];
                    var moved = NewOrder(order.OrderKey, RandomUserKey(stream));
                    if (!_allowSameRoundEmpty && _arrivedOrders.Contains(order.OrderKey))
                    {
                        continue;
                    }
                    Arrived(moved);
                    _roundTrace.Add((order.UserKey, $"order {order.OrderKey} moved away to user {moved.UserKey}"));
                    _roundTrace.Add((moved.UserKey, $"order {order.OrderKey} moved in from user {order.UserKey}"));
                    Apply(stream, moved);
                }

                var deletes = _random.Next(20, 100);
                for (int i = 0; i < deletes && stream.Orders.Count > 0; i++)
                {
                    var order = stream.Orders[_random.Next(stream.Orders.Count)];
                    if (!_allowSameRoundEmpty && _arrivedOrders.Contains(order.OrderKey))
                    {
                        continue;
                    }
                    _roundTrace.Add((order.UserKey, $"order {order.OrderKey} deleted"));
                    Apply(stream, s => s.DeleteOrder(order));
                }

                if (stream.Users.Count > 10 && _random.Next(2) == 0)
                {
                    var user = stream.Users[_random.Next(stream.Users.Count)];
                    if (_allowSameRoundEmpty || !_arrivedAtUsers.Contains(user.UserKey))
                    {
                        _deletedUsers.Add(user.UserKey);
                        _roundTrace.Add((user.UserKey, "user deleted"));
                        Apply(stream, s => s.DeleteUser(user));
                    }
                }
            }

            private void Arrived(Order order)
            {
                _arrivedOrders.Add(order.OrderKey);
                _arrivedAtUsers.Add(order.UserKey);
            }

            /// <summary>
            /// What this round did to one user, in order.
            /// </summary>
            public string Trace(string userKey)
            {
                return string.Join("; ", _roundTrace.Where(entry => entry.UserKey.ToString() == userKey).Select(entry => entry.Text));
            }

            /// <summary>
            /// One more order, the checkpoint it causes shows where the numbering continued.
            /// </summary>
            public void Probe(FlowtideTestStream stream)
            {
                if (stream.Users.Count == 0)
                {
                    Apply(stream, NewUser(_nextUserKey++));
                }
                var order = NewOrder(_nextOrderKey++, stream.Users[_random.Next(stream.Users.Count)].UserKey);
                _roundTrace.Add((order.UserKey, $"order {order.OrderKey} added after the action"));
                Apply(stream, order);
            }

            private int RandomUserKey(FlowtideTestStream stream)
            {
                // A few orders point at users that do not exist, the inner join drops them.
                if (stream.Users.Count == 0 || _random.Next(20) == 0)
                {
                    return _nextUserKey + 1000;
                }
                return stream.Users[_random.Next(stream.Users.Count)].UserKey;
            }

            private void Apply(FlowtideTestStream stream, User user)
            {
                Apply(stream, s => s.AddOrUpdateUser(user));
            }

            private void Apply(FlowtideTestStream stream, Order order)
            {
                Apply(stream, s => s.AddOrUpdateOrder(order));
            }

            private void Apply(FlowtideTestStream stream, Action<FlowtideTestStream> operation)
            {
                _log.Add(operation);
                operation(stream);
            }

            private static User NewUser(int userKey)
            {
                return new User()
                {
                    UserKey = userKey,
                    FirstName = $"first{userKey}",
                    LastName = $"last{userKey}",
                    CompanyId = "1",
                    Active = true
                };
            }

            private Order NewOrder(int orderKey, int userKey)
            {
                // A new object per change, the change log keeps the reference.
                var guid = new byte[16];
                _random.NextBytes(guid);
                return new Order()
                {
                    OrderKey = orderKey,
                    UserKey = userKey,
                    Orderdate = new DateTime(2024, 1, 1).AddDays(orderKey % 365),
                    GuidVal = new Guid(guid),
                    Money = orderKey
                };
            }
        }

        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        public async Task RandomCrashesAndRestartsConvergeToTheReference(int defaultSeed)
        {
            var seed = int.TryParse(Environment.GetEnvironmentVariable("FLOWTIDE_CHAOS_SEED"), out var envSeed) ? envSeed + defaultSeed : defaultSeed;
            var rounds = int.TryParse(Environment.GetEnvironmentVariable("FLOWTIDE_CHAOS_ROUNDS"), out var envRounds) ? envRounds : 16;
            var random = new Random(seed);
            var workload = new Workload(random);
            var testName = $"CheckpointChaos/seed{seed}";
            var directory = Path.Combine("./data/chaos", $"seed{seed}_{Guid.NewGuid():N}");
            _output.WriteLine($"seed {seed}, {rounds} rounds, storage {directory}");

            var stream = new DiskReservoirTestStream(testName, directory);
            try
            {
                workload.Round(stream);
                await stream.StartStream(Query, pageSize: 64, ignoreSameDataCheck: true);
                await AssertConverges(stream, workload, "initial load");

                for (int round = 1; round <= rounds; round++)
                {
                    workload.Round(stream);
                    // Two quiet rounds first, the checkpoint version must be well past a fresh stream's before it is compared.
                    var action = round <= 2 ? ChaosAction.None : (ChaosAction)random.Next(Enum.GetValues<ChaosAction>().Length);
                    if (Environment.GetEnvironmentVariable("FLOWTIDE_CHAOS_NO_FAULTS") == "1")
                    {
                        // The same draws and the same data, without any crash or restart.
                        action = ChaosAction.None;
                    }
                    var delay = random.Next(1, 40);
                    _output.WriteLine($"round {round}: {action}, users {stream.Users.Count}, orders {stream.Orders.Count}, checkpoint {stream.SinkLastCheckpointDoneVersion}");

                    if (action == ChaosAction.CrashAfterCheckpoint)
                    {
                        // The version, not the harness update counter, this test never drains that one.
                        await WaitForCheckpointPast(stream, stream.SinkLastCheckpointDoneVersion, $"round {round}");
                    }
                    else if (action == ChaosAction.CrashAfterDelay || action == ChaosAction.RestartFromDisk)
                    {
                        await Task.Delay(delay);
                    }

                    // Taken right before the action, a checkpoint after it must be numbered past this.
                    var versionBefore = stream.SinkLastCheckpointDoneVersion;
                    if (action == ChaosAction.RestartFromDisk)
                    {
                        // No stop checkpoint, the new stream finds whatever the aborted epoch wrote.
                        await stream.DisposeAsync();
                        stream = new DiskReservoirTestStream(testName, directory);
                        workload.Replay(stream);
                        await stream.StartStream(Query, pageSize: 64, ignoreSameDataCheck: true);
                    }
                    else if (action != ChaosAction.None)
                    {
                        await stream.Crash();
                    }

                    workload.Probe(stream);
                    await AssertConverges(stream, workload, $"round {round} after {action}");
                    await AssertContinuedFromCheckpoint(stream, versionBefore, $"round {round} after {action}");
                }
            }
            finally
            {
                await stream.DisposeAsync();
                try
                {
                    Directory.Delete(directory, recursive: true);
                }
                catch (IOException)
                {
                }
                catch (UnauthorizedAccessException)
                {
                }
            }
        }

        private async Task WaitForCheckpointPast(FlowtideTestStream stream, long version, string phase)
        {
            var watch = Stopwatch.StartNew();
            while (stream.SinkLastCheckpointDoneVersion <= version)
            {
                if (watch.Elapsed > ConvergeTimeout)
                {
                    throw new Xunit.Sdk.XunitException($"No checkpoint completed past version {version} within {ConvergeTimeout} ({phase}).");
                }
                await stream.SchedulerTick();
                await Task.Delay(5);
            }
        }

        /// <summary>
        /// A stream that lost its state recomputes the same output, only its checkpoint numbering starts over.
        /// </summary>
        private async Task AssertContinuedFromCheckpoint(FlowtideTestStream stream, long versionBefore, string phase)
        {
            var watch = Stopwatch.StartNew();
            while (stream.SinkLastCheckpointDoneVersion <= versionBefore)
            {
                if (watch.Elapsed > ConvergeTimeout)
                {
                    throw new Xunit.Sdk.XunitException($"The stream did not continue from its checkpoint ({phase}): it is at version {stream.SinkLastCheckpointDoneVersion}, it was at {versionBefore} before.");
                }
                await stream.SchedulerTick();
                await Task.Delay(20);
            }
            _output.WriteLine($"  continued at checkpoint {stream.SinkLastCheckpointDoneVersion}");
        }

        private async Task AssertConverges(FlowtideTestStream stream, Workload workload, string phase)
        {
            var watch = Stopwatch.StartNew();
            Exception? lastMismatch = null;
            while (watch.Elapsed < ConvergeTimeout)
            {
                // A stream failure that is not an injected crash surfaces here, not as a mismatch.
                await stream.SchedulerTick();
                try
                {
                    var expected = stream.Orders
                        .Join(stream.Users, order => order.UserKey, user => user.UserKey, (order, user) => order)
                        .GroupBy(order => order.UserKey)
                        .Select(group => new { UserKey = group.Key, Count = group.Count(), Sum = group.Sum(order => (long)order.OrderKey) });
                    stream.AssertCurrentDataEqual(expected);
                    _output.WriteLine($"  converged after {watch.ElapsedMilliseconds} ms ({phase})");
                    return;
                }
                catch (Exception mismatch)
                {
                    lastMismatch = mismatch;
                }
                await Task.Delay(20);
            }
            throw new Xunit.Sdk.XunitException($"The output did not reach the reference within {ConvergeTimeout} ({phase}): {lastMismatch?.Message}{Environment.NewLine}{DescribeDifference(stream, workload)}");
        }

        /// <summary>
        /// Names the rows that differ, a count alone does not say whether a row is stale, doubled or missing.
        /// </summary>
        private static string DescribeDifference(FlowtideTestStream stream, Workload workload)
        {
            var expected = stream.Orders
                .Join(stream.Users, order => order.UserKey, user => user.UserKey, (order, user) => order)
                .GroupBy(order => order.UserKey)
                .ToDictionary(group => group.Key.ToString(), group => $"count {group.Count()}, sum {group.Sum(order => (long)order.OrderKey)}");

            var actual = stream.GetActualRowsAsVectors();
            var actualRows = new List<(string Key, string Value)>();
            for (int i = 0; i < actual.Count; i++)
            {
                actualRows.Add((actual.Columns[0].GetValueAt(i, default).ToString()!, $"count {actual.Columns[1].GetValueAt(i, default)}, sum {actual.Columns[2].GetValueAt(i, default)}"));
            }

            var lines = new List<string>();
            foreach (var doubled in actualRows.GroupBy(row => row.Key).Where(group => group.Count() > 1))
            {
                lines.Add($"user {doubled.Key} has {doubled.Count()} output rows: {string.Join(" / ", doubled.Select(row => row.Value))}, expected {(expected.TryGetValue(doubled.Key, out var value) ? value : "no row")}");
            }
            foreach (var row in actualRows.GroupBy(row => row.Key).Where(group => group.Count() == 1).Select(group => group.First()))
            {
                if (!expected.TryGetValue(row.Key, out var value))
                {
                    lines.Add($"user {row.Key} has an output row ({row.Value}) the reference does not have, user exists: {stream.Users.Any(user => user.UserKey.ToString() == row.Key)}, orders: {stream.Orders.Count(order => order.UserKey.ToString() == row.Key)}");
                }
                else if (value != row.Value)
                {
                    lines.Add($"user {row.Key}: output {row.Value}, reference {value}");
                }
            }
            var actualKeys = actualRows.Select(row => row.Key).ToHashSet();
            foreach (var missing in expected.Where(pair => !actualKeys.Contains(pair.Key)))
            {
                lines.Add($"user {missing.Key} is missing from the output, reference {missing.Value}");
            }
            var keys = actualRows.Select(row => row.Key).Concat(expected.Keys).Distinct()
                .Where(key => lines.Any(line => line.StartsWith($"user {key} ") || line.StartsWith($"user {key}:")));
            foreach (var key in keys.Take(5))
            {
                lines.Add($"this round, user {key}: {workload.Trace(key)}");
            }
            return string.Join(Environment.NewLine, lines.Take(25));
        }
    }
}
