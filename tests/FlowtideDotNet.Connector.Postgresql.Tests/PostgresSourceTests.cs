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
    }
}
