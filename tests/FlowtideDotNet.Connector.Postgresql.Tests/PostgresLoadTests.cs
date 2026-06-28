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

using FlowtideDotNet.Connector.PostgreSQL;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Substrait.Sql;
using System.Diagnostics;

namespace FlowtideDotNet.Connector.PostgreSQL.Tests
{
    public partial class PostgresSourceTests
    {
        // Ticks the scheduler until the sink reflects the expected row count (or fails). Generous budget for load.
        private async Task WaitForRowCountLoad(PostgresTestStream stream, int expected, int maxTicks = 3000)
        {
            for (int i = 0; i < maxTicks; i++)
            {
                await stream.SchedulerTick();
                try
                {
                    if (stream.GetActualRowsAsVectors().Count == expected)
                    {
                        return;
                    }
                }
                catch
                {
                    // No data produced yet.
                }
                await Task.Delay(15);
            }
            var actual = -1;
            try { actual = stream.GetActualRowsAsVectors().Count; } catch { }
            Assert.Fail($"Timed out: expected {expected} rows, last saw {actual} after {maxTicks} ticks.");
        }

        [Fact]
        public async Task Load_LargeSnapshotAndMixedDelta()
        {
            const int n = 20000;
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.load_t");
            await _fixture.ExecuteAsync("CREATE TABLE public.load_t (id int PRIMARY KEY, val text NOT NULL)");
            await _fixture.ExecuteAsync($"INSERT INTO public.load_t SELECT g, 'v' || g FROM generate_series(1, {n}) g");

            await using var stream = CreateStream(nameof(Load_LargeSnapshotAndMixedDelta), PostgresReplicationMode.PerTable);

            var sw = Stopwatch.StartNew();
            await stream.StartStream("INSERT INTO output SELECT id, val FROM public.load_t");
            await WaitForRowCountLoad(stream, n);
            _output.WriteLine($"Snapshot: {n} rows in {sw.ElapsedMilliseconds} ms ({n * 1000L / Math.Max(1, sw.ElapsedMilliseconds)} rows/s)");

            // A large mixed delta: update all rows, insert another n, delete the first quarter.
            int quarter = n / 4;
            sw.Restart();
            await _fixture.ExecuteAsync("UPDATE public.load_t SET val = 'u' || id");
            await _fixture.ExecuteAsync($"INSERT INTO public.load_t SELECT g, 'v' || g FROM generate_series({n + 1}, {2 * n}) g");
            await _fixture.ExecuteAsync($"DELETE FROM public.load_t WHERE id <= {quarter}");

            int expectedCount = (n - quarter) + n;
            await WaitForRowCountLoad(stream, expectedCount);
            int deltaOps = n + n + quarter;
            _output.WriteLine($"Delta: {deltaOps} ops -> {expectedCount} rows in {sw.ElapsedMilliseconds} ms ({deltaOps * 1000L / Math.Max(1, sw.ElapsedMilliseconds)} ops/s)");

            // Verify the exact reconciled state.
            var expected = Enumerable.Range(quarter + 1, n - quarter).Select(i => new { id = (long)i, val = "u" + i })
                .Concat(Enumerable.Range(n + 1, n).Select(i => new { id = (long)i, val = "v" + i }));
            stream.AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task Load_BackpressureWithSmallChannel()
        {
            const int n = 5000;
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.bp_t");
            await _fixture.ExecuteAsync("CREATE TABLE public.bp_t (id int PRIMARY KEY, val text NOT NULL)");
            await _fixture.ExecuteAsync("INSERT INTO public.bp_t VALUES (1, 'seed')");

            // A tiny channel forces the replication loop to block on a full channel and resume as the operator drains -
            // exercising back-pressure. Nothing should be lost or deadlock.
            var stream = new PostgresTestStream(nameof(Load_BackpressureWithSmallChannel), new PostgresSourceOptions
            {
                ConnectionStringFunc = () => _fixture.ConnectionString,
                ReplicationMode = PostgresReplicationMode.PerTable,
                ChannelCapacity = 16,
                DeltaLoadInterval = TimeSpan.FromMilliseconds(50)
            });
            await using var _ = stream;
            stream.RegisterTableProviders(b => b.AddPostgreSqlProvider(() => _fixture.ConnectionString));
            await stream.StartStream("INSERT INTO output SELECT id, val FROM public.bp_t");
            await WaitForRowCountLoad(stream, 1);

            await _fixture.ExecuteAsync($"INSERT INTO public.bp_t SELECT g, 'v' || g FROM generate_series(2, {n + 1}) g");
            await WaitForRowCountLoad(stream, n + 1);

            var expected = new[] { new { id = 1L, val = "seed" } }
                .Concat(Enumerable.Range(2, n).Select(i => new { id = (long)i, val = "v" + i }));
            stream.AssertCurrentDataEqual(expected);
        }

        [Fact]
        public async Task Load_PersistentCrashUnderLoad()
        {
            const int n = 4000;
            await DropPersistentLogicalSlots();
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.loadcrash_t");
            await _fixture.ExecuteAsync("CREATE TABLE public.loadcrash_t (id int PRIMARY KEY, val text NOT NULL)");
            await _fixture.ExecuteAsync($"INSERT INTO public.loadcrash_t SELECT g, 'v' || g FROM generate_series(1, {n}) g");

            var fileProvider = new MemoryFileProvider();
            try
            {
                await using var stream = new PostgresTestStream(nameof(Load_PersistentCrashUnderLoad), new PostgresSourceOptions
                {
                    ConnectionStringFunc = () => _fixture.ConnectionString,
                    ReplicationMode = PostgresReplicationMode.PerTable,
                    SlotDurability = PostgresSlotDurability.Persistent,
                    DeltaLoadInterval = TimeSpan.FromMilliseconds(50)
                }, fileProvider);
                // Crash several checkpoints while a large snapshot + delta is in flight; must reconcile to the right state.
                stream.EgressCrashOnCheckpoint(3);
                stream.RegisterTableProviders(b => b.AddPostgreSqlProvider(() => _fixture.ConnectionString));
                await stream.StartStream("INSERT INTO output SELECT id, val FROM public.loadcrash_t");

                await _fixture.ExecuteAsync($"UPDATE public.loadcrash_t SET val = 'u' || id WHERE id <= {n / 2}");
                await _fixture.ExecuteAsync($"INSERT INTO public.loadcrash_t SELECT g, 'v' || g FROM generate_series({n + 1}, {n + n / 2}) g");

                int expectedCount = n + n / 2;
                await WaitForRowCountLoad(stream, expectedCount);

                var expected = Enumerable.Range(1, n / 2).Select(i => new { id = (long)i, val = "u" + i })
                    .Concat(Enumerable.Range(n / 2 + 1, n / 2).Select(i => new { id = (long)i, val = "v" + i }))
                    .Concat(Enumerable.Range(n + 1, n / 2).Select(i => new { id = (long)i, val = "v" + i }));
                stream.AssertCurrentDataEqual(expected);
            }
            finally
            {
                await DropPersistentLogicalSlots();
            }
        }
    }
}
