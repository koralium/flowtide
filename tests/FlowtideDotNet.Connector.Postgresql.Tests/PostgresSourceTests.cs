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
using FlowtideDotNet.Substrait.Sql;

namespace FlowtideDotNet.Connector.PostgreSQL.Tests
{
    public class PostgresSourceTests : IClassFixture<PostgresFixture>
    {
        private readonly PostgresFixture _fixture;

        public PostgresSourceTests(PostgresFixture fixture)
        {
            _fixture = fixture;
        }

        private PostgresTestStream CreateStream(string testName, PostgresReplicationMode mode)
        {
            var stream = new PostgresTestStream(testName, new PostgresSourceOptions
            {
                ConnectionStringFunc = () => _fixture.ConnectionString,
                ReplicationMode = mode,
                DeltaLoadInterval = TimeSpan.FromMilliseconds(200)
            });
            stream.RegisterTableProviders(builder => builder.AddPostgreSqlProvider(() => _fixture.ConnectionString));
            return stream;
        }

        private static async Task WaitForRowCount(PostgresTestStream stream, int expectedCount)
        {
            for (int i = 0; i < 150; i++)
            {
                await stream.SchedulerTick();
                await Task.Delay(200);
                try
                {
                    if (stream.GetActualRowsAsVectors().Count == expectedCount)
                    {
                        return;
                    }
                }
                catch
                {
                    // _actualData not produced yet.
                }
            }
            Assert.Fail($"Timed out waiting for {expectedCount} rows.");
        }

        private static async Task WaitAndAssert<T>(PostgresTestStream stream, IEnumerable<T> expected)
        {
            for (int i = 0; i < 150; i++)
            {
                await stream.SchedulerTick();
                await Task.Delay(200);
                try
                {
                    stream.AssertCurrentDataEqual(expected);
                    return;
                }
                catch
                {
                    // Not yet reflecting the expected data.
                }
            }
            // Final attempt - throws with a detailed assertion message.
            stream.AssertCurrentDataEqual(expected);
        }

        [Theory]
        [InlineData(PostgresReplicationMode.PerTable)]
        [InlineData(PostgresReplicationMode.Shared)]
        public async Task SnapshotThenInsertUpdateDelete(PostgresReplicationMode mode)
        {
            var table = $"users_{mode}".ToLowerInvariant();
            await _fixture.ExecuteAsync($"DROP TABLE IF EXISTS public.{table}");
            await _fixture.ExecuteAsync($"CREATE TABLE public.{table} (id int PRIMARY KEY, name text NOT NULL)");
            await _fixture.ExecuteAsync($"INSERT INTO public.{table} (id, name) VALUES (1, 'alice'), (2, 'bob')");

            await using var stream = CreateStream(nameof(SnapshotThenInsertUpdateDelete) + mode, mode);
            await stream.StartStream($"INSERT INTO output SELECT id, name FROM public.{table}");

            // Snapshot
            await WaitForRowCount(stream, 2);
            stream.AssertCurrentDataEqual(new[]
            {
                new { id = 1L, name = "alice" },
                new { id = 2L, name = "bob" }
            });

            // Insert
            await _fixture.ExecuteAsync($"INSERT INTO public.{table} (id, name) VALUES (3, 'carol')");
            await WaitForRowCount(stream, 3);

            // Update
            await _fixture.ExecuteAsync($"UPDATE public.{table} SET name = 'robert' WHERE id = 2");
            await WaitForRowCount(stream, 3);

            // Delete
            await _fixture.ExecuteAsync($"DELETE FROM public.{table} WHERE id = 1");
            await WaitForRowCount(stream, 2);

            stream.AssertCurrentDataEqual(new[]
            {
                new { id = 2L, name = "robert" },
                new { id = 3L, name = "carol" }
            });
        }

        [Fact]
        public async Task SharedModeReadsMultipleTablesOverOneSlot()
        {
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.shared_a");
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.shared_b");
            await _fixture.ExecuteAsync("CREATE TABLE public.shared_a (id int PRIMARY KEY, name text NOT NULL)");
            await _fixture.ExecuteAsync("CREATE TABLE public.shared_b (id int PRIMARY KEY, name text NOT NULL)");
            await _fixture.ExecuteAsync("INSERT INTO public.shared_a (id, name) VALUES (1, 'a1')");
            await _fixture.ExecuteAsync("INSERT INTO public.shared_b (id, name) VALUES (1, 'b1'), (2, 'b2')");

            await using var stream = CreateStream(nameof(SharedModeReadsMultipleTablesOverOneSlot), PostgresReplicationMode.Shared);
            await stream.StartStream(@"
                INSERT INTO output
                SELECT id, name FROM public.shared_a
                UNION ALL
                SELECT id, name FROM public.shared_b");

            await WaitForRowCount(stream, 3);

            await _fixture.ExecuteAsync("INSERT INTO public.shared_a (id, name) VALUES (2, 'a2')");
            await WaitForRowCount(stream, 4);

            stream.AssertCurrentDataEqual(new[]
            {
                new { id = 1L, name = "a1" },
                new { id = 2L, name = "a2" },
                new { id = 1L, name = "b1" },
                new { id = 2L, name = "b2" }
            });
        }

        [Fact]
        public async Task UnchangedToastColumnIsBackfilledOnUpdate()
        {
            var big = new string('x', 5000);
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.toast_t");
            await _fixture.ExecuteAsync("CREATE TABLE public.toast_t (id int PRIMARY KEY, n int NOT NULL, big text NOT NULL)");
            // EXTERNAL storage forces the large value out-of-line (TOAST) and disables compression, so an UPDATE that
            // leaves it unchanged emits an "unchanged TOAST" marker on the replication stream.
            await _fixture.ExecuteAsync("ALTER TABLE public.toast_t ALTER COLUMN big SET STORAGE EXTERNAL");
            await _fixture.ExecuteAsync($"INSERT INTO public.toast_t (id, n, big) VALUES (1, 1, '{big}')");

            await using var stream = CreateStream(nameof(UnchangedToastColumnIsBackfilledOnUpdate), PostgresReplicationMode.PerTable);
            await stream.StartStream("INSERT INTO output SELECT id, n, big FROM public.toast_t");

            await WaitAndAssert(stream, new[] { new { id = 1L, n = 1L, big } });

            // Update only n; big is unchanged and TOASTed. The connector must backfill big from the previous row.
            await _fixture.ExecuteAsync("UPDATE public.toast_t SET n = 42 WHERE id = 1");

            await WaitAndAssert(stream, new[] { new { id = 1L, n = 42L, big } });
        }

        [Fact]
        public async Task TruncateRetractsAllRows()
        {
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.trunc_t");
            await _fixture.ExecuteAsync("CREATE TABLE public.trunc_t (id int PRIMARY KEY, name text NOT NULL)");
            await _fixture.ExecuteAsync("INSERT INTO public.trunc_t VALUES (1, 'a'), (2, 'b'), (3, 'c')");

            await using var stream = CreateStream(nameof(TruncateRetractsAllRows), PostgresReplicationMode.PerTable);
            await stream.StartStream("INSERT INTO output SELECT id, name FROM public.trunc_t");

            await WaitAndAssert(stream, new[]
            {
                new { id = 1L, name = "a" },
                new { id = 2L, name = "b" },
                new { id = 3L, name = "c" }
            });

            await _fixture.ExecuteAsync("TRUNCATE public.trunc_t");

            // Known-shape empty set so the assertion compares against a 0-row, same-schema batch.
            var none = new[] { new { id = 0L, name = "" } }.Take(0);
            await WaitAndAssert(stream, none);
        }
    }
}
