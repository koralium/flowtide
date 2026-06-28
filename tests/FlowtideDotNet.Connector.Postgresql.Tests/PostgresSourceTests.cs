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
using Xunit.Abstractions;

namespace FlowtideDotNet.Connector.PostgreSQL.Tests
{
    public partial class PostgresSourceTests : IClassFixture<PostgresFixture>
    {
        private readonly PostgresFixture _fixture;
        private readonly ITestOutputHelper _output;

        public PostgresSourceTests(PostgresFixture fixture, ITestOutputHelper output)
        {
            _fixture = fixture;
            _output = output;
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

        [Fact]
        public async Task ResumesAndReconcilesAfterRestart()
        {
            const string testName = nameof(ResumesAndReconcilesAfterRestart);
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.restart_t");
            await _fixture.ExecuteAsync("CREATE TABLE public.restart_t (id int PRIMARY KEY, name text NOT NULL)");
            await _fixture.ExecuteAsync("INSERT INTO public.restart_t VALUES (1, 'a'), (2, 'b')");

            // A shared file provider lets the second stream restore from the first stream's checkpoint.
            var fileProvider = new MemoryFileProvider();

            PostgresSourceOptions Options() => new()
            {
                ConnectionStringFunc = () => _fixture.ConnectionString,
                ReplicationMode = PostgresReplicationMode.PerTable,
                DeltaLoadInterval = TimeSpan.FromMilliseconds(200)
            };

            await using (var first = new PostgresTestStream(testName, Options(), fileProvider))
            {
                first.RegisterTableProviders(b => b.AddPostgreSqlProvider(() => _fixture.ConnectionString));
                await first.StartStream("INSERT INTO output SELECT id, name FROM public.restart_t");
                await WaitAndAssert(first, new[]
                {
                    new { id = 1L, name = "a" },
                    new { id = 2L, name = "b" }
                });
            }

            // Changes made while the stream is down. A temporary slot cannot replay these, so they must be picked up
            // by the re-snapshot that runs on restore.
            await _fixture.ExecuteAsync("INSERT INTO public.restart_t VALUES (3, 'c')");
            await _fixture.ExecuteAsync("UPDATE public.restart_t SET name = 'a2' WHERE id = 1");
            await _fixture.ExecuteAsync("DELETE FROM public.restart_t WHERE id = 2");

            await using var second = new PostgresTestStream(testName, Options(), fileProvider);
            second.RegisterTableProviders(b => b.AddPostgreSqlProvider(() => _fixture.ConnectionString));
            await second.StartStream("INSERT INTO output SELECT id, name FROM public.restart_t");

            // Reconciled state after restore: id=1 updated, id=2 deleted, id=3 inserted.
            await WaitAndAssert(second, new[]
            {
                new { id = 1L, name = "a2" },
                new { id = 3L, name = "c" }
            });

            // And live streaming resumed: a change after restart flows through.
            await _fixture.ExecuteAsync("INSERT INTO public.restart_t VALUES (4, 'd')");
            await WaitAndAssert(second, new[]
            {
                new { id = 1L, name = "a2" },
                new { id = 3L, name = "c" },
                new { id = 4L, name = "d" }
            });
        }

        [Fact]
        public async Task RoundTripsManyDataTypes()
        {
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.types_t");
            await _fixture.ExecuteAsync(@"CREATE TABLE public.types_t (
                id int PRIMARY KEY, n int NOT NULL, b boolean, i2 smallint, i4 integer, i8 bigint,
                f4 real, f8 double precision, num numeric, t text, u uuid, bin bytea, ts timestamptz, j json)");
            await _fixture.ExecuteAsync(@"INSERT INTO public.types_t VALUES
                (1, 1, true, 100, 70000, 5000000000, 1.5, 2.25, 123.45, 'hello',
                 '11111111-1111-1111-1111-111111111111', '\x010203', '2024-01-02 03:04:05+00', '{""k"": 1}')");

            await using var stream = CreateStream(nameof(RoundTripsManyDataTypes), PostgresReplicationMode.PerTable);
            await stream.StartStream("INSERT INTO output SELECT id, n, b, i2, i4, i8, f4, f8, num, t, u, bin, ts, j FROM public.types_t");

            var ts = new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc);

            // Snapshot (typed values from the reader).
            await WaitAndAssert(stream, new[]
            {
                new { id = 1L, n = 1L, b = true, i2 = 100L, i4 = 70000L, i8 = 5000000000L, f4 = 1.5, f8 = 2.25,
                      num = 123.45m, t = "hello", u = "11111111-1111-1111-1111-111111111111", bin = new byte[] { 1, 2, 3 }, ts, j = "{\"k\": 1}" }
            });

            // Updating only n re-sends every column in text form on the wire, exercising the text decode path for all types.
            await _fixture.ExecuteAsync("UPDATE public.types_t SET n = 2 WHERE id = 1");
            await WaitAndAssert(stream, new[]
            {
                new { id = 1L, n = 2L, b = true, i2 = 100L, i4 = 70000L, i8 = 5000000000L, f4 = 1.5, f8 = 2.25,
                      num = 123.45m, t = "hello", u = "11111111-1111-1111-1111-111111111111", bin = new byte[] { 1, 2, 3 }, ts, j = "{\"k\": 1}" }
            });
        }

        [Fact]
        public async Task CompositePrimaryKeyUpdateAndDelete()
        {
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.comp_t");
            await _fixture.ExecuteAsync("CREATE TABLE public.comp_t (a int, b int, val text NOT NULL, PRIMARY KEY (a, b))");
            await _fixture.ExecuteAsync("INSERT INTO public.comp_t VALUES (1, 1, 'x'), (1, 2, 'y')");

            await using var stream = CreateStream(nameof(CompositePrimaryKeyUpdateAndDelete), PostgresReplicationMode.PerTable);
            await stream.StartStream("INSERT INTO output SELECT a, b, val FROM public.comp_t");

            await WaitAndAssert(stream, new[] { new { a = 1L, b = 1L, val = "x" }, new { a = 1L, b = 2L, val = "y" } });

            await _fixture.ExecuteAsync("UPDATE public.comp_t SET val = 'z' WHERE a = 1 AND b = 2");
            await _fixture.ExecuteAsync("DELETE FROM public.comp_t WHERE a = 1 AND b = 1");

            await WaitAndAssert(stream, new[] { new { a = 1L, b = 2L, val = "z" } });
        }

        [Fact]
        public async Task KeyChangingUpdate()
        {
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.kc_t");
            await _fixture.ExecuteAsync("CREATE TABLE public.kc_t (id int PRIMARY KEY, name text NOT NULL)");
            await _fixture.ExecuteAsync("INSERT INTO public.kc_t VALUES (1, 'a')");

            await using var stream = CreateStream(nameof(KeyChangingUpdate), PostgresReplicationMode.PerTable);
            await stream.StartStream("INSERT INTO output SELECT id, name FROM public.kc_t");

            await WaitAndAssert(stream, new[] { new { id = 1L, name = "a" } });

            // Changing the primary key value: pgoutput sends the old key, so the old row must be retracted.
            await _fixture.ExecuteAsync("UPDATE public.kc_t SET id = 2 WHERE id = 1");

            await WaitAndAssert(stream, new[] { new { id = 2L, name = "a" } });
        }

        [Fact]
        public async Task NullValuesRoundTrip()
        {
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.null_t");
            await _fixture.ExecuteAsync("CREATE TABLE public.null_t (id int PRIMARY KEY, name text)");
            await _fixture.ExecuteAsync("INSERT INTO public.null_t (id, name) VALUES (1, NULL)");

            await using var stream = CreateStream(nameof(NullValuesRoundTrip), PostgresReplicationMode.PerTable);
            await stream.StartStream("INSERT INTO output SELECT id, name FROM public.null_t");

            await WaitAndAssert(stream, new[] { new { id = 1L, name = (string?)null } });

            await _fixture.ExecuteAsync("UPDATE public.null_t SET name = 'x' WHERE id = 1");
            await WaitAndAssert(stream, new[] { new { id = 1L, name = (string?)"x" } });

            await _fixture.ExecuteAsync("UPDATE public.null_t SET name = NULL WHERE id = 1");
            await WaitAndAssert(stream, new[] { new { id = 1L, name = (string?)null } });
        }

        [Fact]
        public async Task NoPrimaryKeyThrows()
        {
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.nopk_t");
            await _fixture.ExecuteAsync("CREATE TABLE public.nopk_t (id int, name text)");

            await using var stream = CreateStream(nameof(NoPrimaryKeyThrows), PostgresReplicationMode.PerTable);

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(
                () => stream.StartStream("INSERT INTO output SELECT id, name FROM public.nopk_t"));
            Assert.Contains("primary key", exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public async Task RoundTripsTemporalAndTextTypes()
        {
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.temporal_t");
            await _fixture.ExecuteAsync(@"CREATE TABLE public.temporal_t (
                id int PRIMARY KEY, n int NOT NULL, d date, ts timestamp, tm time, v varchar(20))");
            await _fixture.ExecuteAsync(@"INSERT INTO public.temporal_t VALUES
                (1, 1, '2024-03-04', '2024-03-04 05:06:07', '03:04:05', 'hi')");

            await using var stream = CreateStream(nameof(RoundTripsTemporalAndTextTypes), PostgresReplicationMode.PerTable);
            await stream.StartStream("INSERT INTO output SELECT id, n, d, ts, tm, v FROM public.temporal_t");

            var d = new DateTime(2024, 3, 4, 0, 0, 0, DateTimeKind.Utc);
            var ts = new DateTime(2024, 3, 4, 5, 6, 7, DateTimeKind.Utc);
            var tm = new TimeOnly(3, 4, 5).Ticks;

            await WaitAndAssert(stream, new[] { new { id = 1L, n = 1L, d, ts, tm, v = "hi" } });

            await _fixture.ExecuteAsync("UPDATE public.temporal_t SET n = 2 WHERE id = 1");
            await WaitAndAssert(stream, new[] { new { id = 1L, n = 2L, d, ts, tm, v = "hi" } });
        }

        [Fact]
        public async Task RoundTripsArrayAndJsonbAsText()
        {
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.arr_t");
            await _fixture.ExecuteAsync(@"CREATE TABLE public.arr_t (
                id int PRIMARY KEY, n int NOT NULL, ints int[], strs text[], jb jsonb)");
            await _fixture.ExecuteAsync(@"INSERT INTO public.arr_t VALUES (1, 1, '{1,2,3}', '{a,b}', '{""k"": 1}')");

            await using var stream = CreateStream(nameof(RoundTripsArrayAndJsonbAsText), PostgresReplicationMode.PerTable);
            await stream.StartStream("INSERT INTO output SELECT id, n, ints, strs, jb FROM public.arr_t");

            // Arrays have no typed converter, so they round-trip as their PostgreSQL text form on both paths.
            await WaitAndAssert(stream, new[] { new { id = 1L, n = 1L, ints = "{1,2,3}", strs = "{a,b}", jb = "{\"k\": 1}" } });

            await _fixture.ExecuteAsync("UPDATE public.arr_t SET n = 2 WHERE id = 1");
            await WaitAndAssert(stream, new[] { new { id = 1L, n = 2L, ints = "{1,2,3}", strs = "{a,b}", jb = "{\"k\": 1}" } });
        }

        [Fact]
        public async Task FilterPushdownAcrossBoundary()
        {
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.filt_t");
            await _fixture.ExecuteAsync("CREATE TABLE public.filt_t (id int PRIMARY KEY, age int NOT NULL)");
            await _fixture.ExecuteAsync("INSERT INTO public.filt_t VALUES (1, 40), (2, 20), (3, 35)");

            await using var stream = CreateStream(nameof(FilterPushdownAcrossBoundary), PostgresReplicationMode.PerTable);
            await stream.StartStream("INSERT INTO output SELECT id, age FROM public.filt_t WHERE age >= 30");

            await WaitAndAssert(stream, new[] { new { id = 1L, age = 40L }, new { id = 3L, age = 35L } });

            // One insert below the threshold (excluded) and one above (included).
            await _fixture.ExecuteAsync("INSERT INTO public.filt_t VALUES (4, 25), (5, 50)");
            await WaitAndAssert(stream, new[] { new { id = 1L, age = 40L }, new { id = 3L, age = 35L }, new { id = 5L, age = 50L } });

            // Update a row to cross into the filter, and another to cross out of it.
            await _fixture.ExecuteAsync("UPDATE public.filt_t SET age = 60 WHERE id = 2");
            await _fixture.ExecuteAsync("UPDATE public.filt_t SET age = 10 WHERE id = 1");
            await WaitAndAssert(stream, new[] { new { id = 3L, age = 35L }, new { id = 5L, age = 50L }, new { id = 2L, age = 60L } });
        }

        private async Task<long> CountPersistentLogicalSlots()
        {
            var result = await _fixture.ExecuteScalarAsync(
                "SELECT count(*) FROM pg_replication_slots WHERE slot_type = 'logical' AND temporary = false");
            return Convert.ToInt64(result);
        }

        private async Task DropPersistentLogicalSlots()
        {
            await _fixture.ExecuteAsync(
                "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_type = 'logical' AND temporary = false AND active = false");
        }

        [Fact]
        public async Task PersistentSlotResumesWithoutResnapshot()
        {
            const string testName = nameof(PersistentSlotResumesWithoutResnapshot);
            await DropPersistentLogicalSlots();
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.persist_t");
            await _fixture.ExecuteAsync("CREATE TABLE public.persist_t (id int PRIMARY KEY, name text NOT NULL)");
            await _fixture.ExecuteAsync("INSERT INTO public.persist_t VALUES (1, 'a'), (2, 'b')");

            var fileProvider = new MemoryFileProvider();
            PostgresSourceOptions Options() => new()
            {
                ConnectionStringFunc = () => _fixture.ConnectionString,
                ReplicationMode = PostgresReplicationMode.PerTable,
                SlotDurability = PostgresSlotDurability.Persistent,
                DeltaLoadInterval = TimeSpan.FromMilliseconds(200)
            };

            await using (var first = new PostgresTestStream(testName, Options(), fileProvider))
            {
                first.RegisterTableProviders(b => b.AddPostgreSqlProvider(() => _fixture.ConnectionString));
                await first.StartStream("INSERT INTO output SELECT id, name FROM public.persist_t");
                await WaitAndAssert(first, new[] { new { id = 1L, name = "a" }, new { id = 2L, name = "b" } });
            }

            // Unlike a temporary slot, a persistent slot survives the stream being torn down.
            Assert.True(await CountPersistentLogicalSlots() >= 1, "Expected a persistent logical slot to remain after disposal.");

            // A change during downtime is retained in WAL by the persistent slot and delivered on resume (not via a snapshot).
            await _fixture.ExecuteAsync("INSERT INTO public.persist_t VALUES (3, 'c')");

            var second = new PostgresTestStream(testName, Options(), fileProvider);
            try
            {
                second.RegisterTableProviders(b => b.AddPostgreSqlProvider(() => _fixture.ConnectionString));
                await second.StartStream("INSERT INTO output SELECT id, name FROM public.persist_t");
                await WaitAndAssert(second, new[]
                {
                    new { id = 1L, name = "a" },
                    new { id = 2L, name = "b" },
                    new { id = 3L, name = "c" }
                });
            }
            finally
            {
                await second.DisposeAsync();
                await DropPersistentLogicalSlots();
            }
        }

        [Fact]
        public async Task ReplicationConnectionDropSurfacesAsFaultWithoutDeadlock()
        {
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.drop_t");
            await _fixture.ExecuteAsync("CREATE TABLE public.drop_t (id int PRIMARY KEY, name text NOT NULL)");
            await _fixture.ExecuteAsync("INSERT INTO public.drop_t VALUES (1, 'a')");

            var stream = CreateStream(nameof(ReplicationConnectionDropSurfacesAsFaultWithoutDeadlock), PostgresReplicationMode.PerTable);
            try
            {
                await stream.StartStream("INSERT INTO output SELECT id, name FROM public.drop_t");
                await WaitAndAssert(stream, new[] { new { id = 1L, name = "a" } });

                // Kill the replication (walsender) connection out from under the running stream.
                await _fixture.ExecuteAsync("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE backend_type = 'walsender'");

                // The fault must surface as a thrown error within a bounded number of ticks. If the loop drove the
                // rollback itself it would deadlock (rollback disposes the source, which waits for the loop), and this
                // loop would never return - the test would time out instead of catching.
                Exception? caught = null;
                for (int i = 0; i < 100 && caught == null; i++)
                {
                    try
                    {
                        await stream.SchedulerTick();
                        await Task.Delay(200);
                    }
                    catch (Exception ex)
                    {
                        caught = ex;
                    }
                }

                Assert.NotNull(caught);
            }
            finally
            {
                try
                {
                    await stream.DisposeAsync();
                }
                catch
                {
                    // The stream is in a failed state; disposal errors are not the subject of this test.
                }
            }
        }

        [Fact]
        public async Task AdversarialTextValuesRoundTrip()
        {
            var nasty = "line1\nline2\ttab 'single' \"double\" \\back %like_ ünïcödé 漢字 🎉 end";
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.text_t");
            await _fixture.ExecuteAsync("CREATE TABLE public.text_t (id int PRIMARY KEY, txt text NOT NULL)");
            await _fixture.ExecuteAsync("INSERT INTO public.text_t (id, txt) VALUES (1, @txt)", ("txt", nasty));

            await using var stream = CreateStream(nameof(AdversarialTextValuesRoundTrip), PostgresReplicationMode.PerTable);
            await stream.StartStream("INSERT INTO output SELECT id, txt FROM public.text_t");
            await WaitAndAssert(stream, new[] { new { id = 1L, txt = nasty } });

            // Same value via the replication/text path on an update. (PostgreSQL text cannot store a NUL byte, so the
            // connector will never receive one - not included here.)
            var nasty2 = nasty + " UPDATED \r\n end";
            await _fixture.ExecuteAsync("UPDATE public.text_t SET txt = @txt WHERE id = 1", ("txt", nasty2));
            await WaitAndAssert(stream, new[] { new { id = 1L, txt = nasty2 } });
        }

        [Fact]
        public async Task EmptyStringIsDistinctFromNull()
        {
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.empty_t");
            await _fixture.ExecuteAsync("CREATE TABLE public.empty_t (id int PRIMARY KEY, a text, b text)");
            await _fixture.ExecuteAsync("INSERT INTO public.empty_t (id, a, b) VALUES (1, '', NULL)");

            await using var stream = CreateStream(nameof(EmptyStringIsDistinctFromNull), PostgresReplicationMode.PerTable);
            await stream.StartStream("INSERT INTO output SELECT id, a, b FROM public.empty_t");
            await WaitAndAssert(stream, new[] { new { id = 1L, a = (string?)"", b = (string?)null } });

            // Swap them via the delta path: a -> NULL, b -> ''.
            await _fixture.ExecuteAsync("UPDATE public.empty_t SET a = NULL, b = '' WHERE id = 1");
            await WaitAndAssert(stream, new[] { new { id = 1L, a = (string?)null, b = (string?)"" } });
        }

        [Fact]
        public async Task NumericExtremes()
        {
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.num_t");
            await _fixture.ExecuteAsync("CREATE TABLE public.num_t (id bigint PRIMARY KEY, big bigint NOT NULL, dec numeric(30,10) NOT NULL)");
            await _fixture.ExecuteAsync($"INSERT INTO public.num_t VALUES ({long.MinValue}, {long.MaxValue}, -12345678901234.5678901234)");

            await using var stream = CreateStream(nameof(NumericExtremes), PostgresReplicationMode.PerTable);
            await stream.StartStream("INSERT INTO output SELECT id, big, dec FROM public.num_t");
            await WaitAndAssert(stream, new[] { new { id = long.MinValue, big = long.MaxValue, dec = -12345678901234.5678901234m } });

            await _fixture.ExecuteAsync($"INSERT INTO public.num_t VALUES (0, -1, 0.0000000001)");
            await WaitAndAssert(stream, new[]
            {
                new { id = long.MinValue, big = long.MaxValue, dec = -12345678901234.5678901234m },
                new { id = 0L, big = -1L, dec = 0.0000000001m }
            });
        }

        [Fact]
        public async Task PerTableModeMultipleTablesIndependentSlots()
        {
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.pt_a");
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.pt_b");
            await _fixture.ExecuteAsync("CREATE TABLE public.pt_a (id int PRIMARY KEY, name text NOT NULL)");
            await _fixture.ExecuteAsync("CREATE TABLE public.pt_b (id int PRIMARY KEY, name text NOT NULL)");
            await _fixture.ExecuteAsync("INSERT INTO public.pt_a VALUES (1, 'a1')");
            await _fixture.ExecuteAsync("INSERT INTO public.pt_b VALUES (1, 'b1')");

            await using var stream = CreateStream(nameof(PerTableModeMultipleTablesIndependentSlots), PostgresReplicationMode.PerTable);
            await stream.StartStream(@"INSERT INTO output
                SELECT id, name FROM public.pt_a UNION ALL SELECT id, name FROM public.pt_b");
            await WaitAndAssert(stream, new[] { new { id = 1L, name = "a1" }, new { id = 1L, name = "b1" } });

            await _fixture.ExecuteAsync("UPDATE public.pt_a SET name = 'a1x' WHERE id = 1");
            await _fixture.ExecuteAsync("INSERT INTO public.pt_b VALUES (2, 'b2')");
            await WaitAndAssert(stream, new[]
            {
                new { id = 1L, name = "a1x" }, new { id = 1L, name = "b1" }, new { id = 2L, name = "b2" }
            });
        }

        [Fact]
        public async Task UserManagedPublication()
        {
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.usrpub_t");
            await _fixture.ExecuteAsync("CREATE TABLE public.usrpub_t (id int PRIMARY KEY, name text NOT NULL)");
            await _fixture.ExecuteAsync("DROP PUBLICATION IF EXISTS my_managed_pub");
            await _fixture.ExecuteAsync("CREATE PUBLICATION my_managed_pub FOR TABLE public.usrpub_t");
            await _fixture.ExecuteAsync("INSERT INTO public.usrpub_t VALUES (1, 'a')");

            var stream = new PostgresTestStream(nameof(UserManagedPublication), new PostgresSourceOptions
            {
                ConnectionStringFunc = () => _fixture.ConnectionString,
                ReplicationMode = PostgresReplicationMode.PerTable,
                PublicationName = "my_managed_pub",
                DeltaLoadInterval = TimeSpan.FromMilliseconds(200)
            });
            await using var _ = stream;
            stream.RegisterTableProviders(b => b.AddPostgreSqlProvider(() => _fixture.ConnectionString));
            await stream.StartStream("INSERT INTO output SELECT id, name FROM public.usrpub_t");
            await WaitAndAssert(stream, new[] { new { id = 1L, name = "a" } });

            await _fixture.ExecuteAsync("UPDATE public.usrpub_t SET name = 'b' WHERE id = 1");
            await WaitAndAssert(stream, new[] { new { id = 1L, name = "b" } });

            // The connector must NOT have created its own publication for this table when one was supplied.
            var autoPubsForTable = Convert.ToInt64(await _fixture.ExecuteScalarAsync(
                "SELECT count(*) FROM pg_publication_tables WHERE tablename = 'usrpub_t' AND pubname LIKE 'flowtide%'"));
            Assert.Equal(0, autoPubsForTable);

            await _fixture.ExecuteAsync("DROP PUBLICATION IF EXISTS my_managed_pub");
        }

        [Fact]
        public async Task CompositeKeyChangingUpdate()
        {
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.ck_t");
            await _fixture.ExecuteAsync("CREATE TABLE public.ck_t (a int, b int, val text NOT NULL, PRIMARY KEY (a, b))");
            await _fixture.ExecuteAsync("INSERT INTO public.ck_t VALUES (1, 1, 'x'), (1, 2, 'y')");

            await using var stream = CreateStream(nameof(CompositeKeyChangingUpdate), PostgresReplicationMode.PerTable);
            await stream.StartStream("INSERT INTO output SELECT a, b, val FROM public.ck_t");
            await WaitAndAssert(stream, new[] { new { a = 1L, b = 1L, val = "x" }, new { a = 1L, b = 2L, val = "y" } });

            // Change one component of the composite key: the old (1,1) must be retracted and (2,1) inserted.
            await _fixture.ExecuteAsync("UPDATE public.ck_t SET a = 2 WHERE a = 1 AND b = 1");
            await WaitAndAssert(stream, new[] { new { a = 2L, b = 1L, val = "x" }, new { a = 1L, b = 2L, val = "y" } });
        }

        [Fact]
        public async Task MixedOperationsInSingleTransaction()
        {
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.txn_t");
            await _fixture.ExecuteAsync("CREATE TABLE public.txn_t (id int PRIMARY KEY, name text NOT NULL)");
            await _fixture.ExecuteAsync("INSERT INTO public.txn_t VALUES (1, 'a'), (2, 'b')");

            await using var stream = CreateStream(nameof(MixedOperationsInSingleTransaction), PostgresReplicationMode.PerTable);
            await stream.StartStream("INSERT INTO output SELECT id, name FROM public.txn_t");
            await WaitAndAssert(stream, new[] { new { id = 1L, name = "a" }, new { id = 2L, name = "b" } });

            // Insert, update and delete of different keys, committed atomically in one transaction.
            await _fixture.ExecuteAsync(@"BEGIN;
                INSERT INTO public.txn_t VALUES (3, 'c');
                UPDATE public.txn_t SET name = 'b2' WHERE id = 2;
                DELETE FROM public.txn_t WHERE id = 1;
                COMMIT;");
            await WaitAndAssert(stream, new[] { new { id = 2L, name = "b2" }, new { id = 3L, name = "c" } });
        }

        [Fact]
        public async Task TimestampTzWithNonUtcServerTimezone()
        {
            var dbName = Convert.ToString(await _fixture.ExecuteScalarAsync("SELECT current_database()"))!;
            await _fixture.ExecuteAsync($"ALTER DATABASE \"{dbName}\" SET timezone = 'America/New_York'");
            try
            {
                await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.tz_t");
                await _fixture.ExecuteAsync("CREATE TABLE public.tz_t (id int PRIMARY KEY, ts timestamptz NOT NULL)");
                await _fixture.ExecuteAsync("INSERT INTO public.tz_t VALUES (1, '2024-01-02 03:04:05+00')");

                await using var stream = CreateStream(nameof(TimestampTzWithNonUtcServerTimezone), PostgresReplicationMode.PerTable);
                await stream.StartStream("INSERT INTO output SELECT id, ts FROM public.tz_t");
                var expected1 = new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc);
                await WaitAndAssert(stream, new[] { new { id = 1L, ts = expected1 } });

                // This row arrives via replication, whose walsender renders timestamptz in the (now non-UTC) server
                // timezone, e.g. '...-04'. The connector must still decode it to the same UTC instant.
                await _fixture.ExecuteAsync("INSERT INTO public.tz_t VALUES (2, '2024-06-15 18:30:00+00')");
                var expected2 = new DateTime(2024, 6, 15, 18, 30, 0, DateTimeKind.Utc);
                await WaitAndAssert(stream, new[] { new { id = 1L, ts = expected1 }, new { id = 2L, ts = expected2 } });
            }
            finally
            {
                await _fixture.ExecuteAsync($"ALTER DATABASE \"{dbName}\" SET timezone = 'UTC'");
            }
        }

        [Fact]
        public async Task UuidPrimaryKeyUpdateAndDelete()
        {
            const string id1 = "11111111-1111-1111-1111-111111111111";
            const string id2 = "22222222-2222-2222-2222-222222222222";
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.uuid_t");
            await _fixture.ExecuteAsync("CREATE TABLE public.uuid_t (id uuid PRIMARY KEY, name text NOT NULL)");
            await _fixture.ExecuteAsync($"INSERT INTO public.uuid_t VALUES ('{id1}', 'a')");

            await using var stream = CreateStream(nameof(UuidPrimaryKeyUpdateAndDelete), PostgresReplicationMode.PerTable);
            await stream.StartStream("INSERT INTO output SELECT id, name FROM public.uuid_t");
            await WaitAndAssert(stream, new[] { new { id = id1, name = "a" } });

            // Update (non-key) requires the uuid key to match in the reconciling tree.
            await _fixture.ExecuteAsync($"UPDATE public.uuid_t SET name = 'b' WHERE id = '{id1}'");
            await WaitAndAssert(stream, new[] { new { id = id1, name = "b" } });

            // Delete requires the uuid key to match; then a different key is inserted.
            await _fixture.ExecuteAsync($"DELETE FROM public.uuid_t WHERE id = '{id1}'");
            await _fixture.ExecuteAsync($"INSERT INTO public.uuid_t VALUES ('{id2}', 'c')");
            await WaitAndAssert(stream, new[] { new { id = id2, name = "c" } });
        }

        [Fact]
        public async Task LargeSnapshotAndDeltaCrossBatchBoundaries()
        {
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.big_t");
            await _fixture.ExecuteAsync("CREATE TABLE public.big_t (id int PRIMARY KEY, name text NOT NULL)");
            await _fixture.ExecuteAsync("INSERT INTO public.big_t SELECT g, 'v' || g FROM generate_series(1, 250) g");

            await using var stream = CreateStream(nameof(LargeSnapshotAndDeltaCrossBatchBoundaries), PostgresReplicationMode.PerTable);
            await stream.StartStream("INSERT INTO output SELECT id, name FROM public.big_t");

            await WaitAndAssert(stream, Enumerable.Range(1, 250).Select(i => new { id = (long)i, name = "v" + i }));

            // A delta larger than the 100-row batch size, in one transaction.
            await _fixture.ExecuteAsync("INSERT INTO public.big_t SELECT g, 'v' || g FROM generate_series(251, 500) g");
            await WaitAndAssert(stream, Enumerable.Range(1, 500).Select(i => new { id = (long)i, name = "v" + i }));
        }

        [Fact]
        public async Task NoOpUpdateAndRepeatedAndReinsert()
        {
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.churn_t");
            await _fixture.ExecuteAsync("CREATE TABLE public.churn_t (id int PRIMARY KEY, name text NOT NULL)");
            await _fixture.ExecuteAsync("INSERT INTO public.churn_t VALUES (1, 'a'), (2, 'keep')");

            await using var stream = CreateStream(nameof(NoOpUpdateAndRepeatedAndReinsert), PostgresReplicationMode.PerTable);
            await stream.StartStream("INSERT INTO output SELECT id, name FROM public.churn_t");
            await WaitAndAssert(stream, new[] { new { id = 1L, name = "a" }, new { id = 2L, name = "keep" } });

            // No-op update (value unchanged), several rapid updates, and a delete+reinsert of the same key.
            await _fixture.ExecuteAsync("UPDATE public.churn_t SET name = 'keep' WHERE id = 2");
            await _fixture.ExecuteAsync("UPDATE public.churn_t SET name = 'b' WHERE id = 1");
            await _fixture.ExecuteAsync("UPDATE public.churn_t SET name = 'c' WHERE id = 1");
            await _fixture.ExecuteAsync("DELETE FROM public.churn_t WHERE id = 1");
            await _fixture.ExecuteAsync("INSERT INTO public.churn_t VALUES (1, 'reborn')");

            await WaitAndAssert(stream, new[] { new { id = 1L, name = "reborn" }, new { id = 2L, name = "keep" } });
        }

        [Fact]
        public async Task NonPublicSchema()
        {
            await _fixture.ExecuteAsync("DROP SCHEMA IF EXISTS myschema CASCADE");
            await _fixture.ExecuteAsync("CREATE SCHEMA myschema");
            await _fixture.ExecuteAsync("CREATE TABLE myschema.t (id int PRIMARY KEY, name text NOT NULL)");
            await _fixture.ExecuteAsync("INSERT INTO myschema.t VALUES (1, 'a')");

            await using var stream = CreateStream(nameof(NonPublicSchema), PostgresReplicationMode.PerTable);
            await stream.StartStream("INSERT INTO output SELECT id, name FROM myschema.t");
            await WaitAndAssert(stream, new[] { new { id = 1L, name = "a" } });

            await _fixture.ExecuteAsync("INSERT INTO myschema.t VALUES (2, 'b')");
            await _fixture.ExecuteAsync("UPDATE myschema.t SET name = 'a2' WHERE id = 1");
            await WaitAndAssert(stream, new[] { new { id = 1L, name = "a2" }, new { id = 2L, name = "b" } });
        }

        [Fact]
        public async Task SharedTemporaryMultipleTablesRecoverFromCheckpointCrash()
        {
            foreach (var t in new[] { "stc_a", "stc_b" })
            {
                await _fixture.ExecuteAsync($"DROP TABLE IF EXISTS public.{t}");
                await _fixture.ExecuteAsync($"CREATE TABLE public.{t} (id int PRIMARY KEY, name text NOT NULL)");
            }
            await _fixture.ExecuteAsync("INSERT INTO public.stc_a VALUES (1, 'a1')");
            await _fixture.ExecuteAsync("INSERT INTO public.stc_b VALUES (1, 'b1')");

            const string sql = @"INSERT INTO output
                SELECT id, name FROM public.stc_a UNION ALL SELECT id, name FROM public.stc_b";

            // Temporary (default) shared slot: a checkpoint crash rolls back all operators, which must rebuild the
            // shared reader+slot+snapshot together rather than re-importing the (now-invalid) exported snapshot.
            var stream = new PostgresTestStream(nameof(SharedTemporaryMultipleTablesRecoverFromCheckpointCrash), new PostgresSourceOptions
            {
                ConnectionStringFunc = () => _fixture.ConnectionString,
                ReplicationMode = PostgresReplicationMode.Shared,
                SlotDurability = PostgresSlotDurability.Temporary,
                DeltaLoadInterval = TimeSpan.FromMilliseconds(200)
            });
            await using var _ = stream;
            stream.EgressCrashOnCheckpoint(3);
            stream.RegisterTableProviders(b => b.AddPostgreSqlProvider(() => _fixture.ConnectionString));
            await stream.StartStream(sql);

            await WaitAndAssert(stream, new[] { new { id = 1L, name = "a1" }, new { id = 1L, name = "b1" } });

            await _fixture.ExecuteAsync("INSERT INTO public.stc_a VALUES (2, 'a2')");
            await _fixture.ExecuteAsync("INSERT INTO public.stc_b VALUES (2, 'b2')");
            await WaitAndAssert(stream, new[]
            {
                new { id = 1L, name = "a1" }, new { id = 2L, name = "a2" },
                new { id = 1L, name = "b1" }, new { id = 2L, name = "b2" }
            });
        }

        [Fact]
        public async Task SharedReplicationDropSurfacesWithoutDeadlock()
        {
            await DropPersistentLogicalSlots();
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.sdrop_a");
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.sdrop_b");
            await _fixture.ExecuteAsync("CREATE TABLE public.sdrop_a (id int PRIMARY KEY, name text NOT NULL)");
            await _fixture.ExecuteAsync("CREATE TABLE public.sdrop_b (id int PRIMARY KEY, name text NOT NULL)");
            await _fixture.ExecuteAsync("INSERT INTO public.sdrop_a VALUES (1, 'a')");
            await _fixture.ExecuteAsync("INSERT INTO public.sdrop_b VALUES (1, 'b')");

            var stream = CreateStream(nameof(SharedReplicationDropSurfacesWithoutDeadlock), PostgresReplicationMode.Shared);
            try
            {
                await stream.StartStream(@"INSERT INTO output
                    SELECT id, name FROM public.sdrop_a UNION ALL SELECT id, name FROM public.sdrop_b");
                await WaitAndAssert(stream, new[] { new { id = 1L, name = "a" }, new { id = 1L, name = "b" } });

                // Kill the single shared walsender. Both tables' operators must surface the fault and roll back without
                // the shared reader deadlocking or stalling (the dead reader is removed so a re-init gets a fresh one).
                await _fixture.ExecuteAsync("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE backend_type = 'walsender'");

                Exception? caught = null;
                for (int i = 0; i < 100 && caught == null; i++)
                {
                    try
                    {
                        await stream.SchedulerTick();
                        await Task.Delay(200);
                    }
                    catch (Exception ex)
                    {
                        caught = ex;
                    }
                }

                Assert.NotNull(caught);
            }
            finally
            {
                try { await stream.DisposeAsync(); } catch { }
            }
        }

        [Fact]
        public async Task PersistentSlotRecoversFromCheckpointCrashThenRestart()
        {
            const string testName = nameof(PersistentSlotRecoversFromCheckpointCrashThenRestart);
            await DropPersistentLogicalSlots();
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.crash_t");
            await _fixture.ExecuteAsync("CREATE TABLE public.crash_t (id int PRIMARY KEY, name text NOT NULL)");
            await _fixture.ExecuteAsync("INSERT INTO public.crash_t VALUES (1, 'a')");

            var fileProvider = new MemoryFileProvider();
            PostgresSourceOptions Options() => new()
            {
                ConnectionStringFunc = () => _fixture.ConnectionString,
                ReplicationMode = PostgresReplicationMode.PerTable,
                SlotDurability = PostgresSlotDurability.Persistent,
                DeltaLoadInterval = TimeSpan.FromMilliseconds(200)
            };

            try
            {
                await using (var first = new PostgresTestStream(testName, Options(), fileProvider))
                {
                    // Make the first few checkpoints fail: the source prepares (and confirms one-behind) but the global
                    // checkpoint aborts and rolls back. The stream must recover without losing or duplicating data.
                    first.EgressCrashOnCheckpoint(3);
                    first.RegisterTableProviders(b => b.AddPostgreSqlProvider(() => _fixture.ConnectionString));
                    await first.StartStream("INSERT INTO output SELECT id, name FROM public.crash_t");
                    await WaitAndAssert(first, new[] { new { id = 1L, name = "a" } });

                    await _fixture.ExecuteAsync("INSERT INTO public.crash_t VALUES (2, 'b')");
                    await WaitAndAssert(first, new[] { new { id = 1L, name = "a" }, new { id = 2L, name = "b" } });
                }

                // A change while down, then restart: resume must include everything (the failed checkpoints never
                // advanced confirmed_flush past durable state, so nothing was skipped).
                await _fixture.ExecuteAsync("INSERT INTO public.crash_t VALUES (3, 'c')");

                var second = new PostgresTestStream(testName, Options(), fileProvider);
                try
                {
                    second.RegisterTableProviders(b => b.AddPostgreSqlProvider(() => _fixture.ConnectionString));
                    await second.StartStream("INSERT INTO output SELECT id, name FROM public.crash_t");
                    await WaitAndAssert(second, new[]
                    {
                        new { id = 1L, name = "a" },
                        new { id = 2L, name = "b" },
                        new { id = 3L, name = "c" }
                    });
                }
                finally
                {
                    await second.DisposeAsync();
                }
            }
            finally
            {
                await DropPersistentLogicalSlots();
            }
        }

        [Fact]
        public async Task SharedPersistentMultipleTablesRecoverFromCheckpointCrash()
        {
            const string testName = nameof(SharedPersistentMultipleTablesRecoverFromCheckpointCrash);
            await DropPersistentLogicalSlots();
            foreach (var t in new[] { "mt_a", "mt_b", "mt_c" })
            {
                await _fixture.ExecuteAsync($"DROP TABLE IF EXISTS public.{t}");
                await _fixture.ExecuteAsync($"CREATE TABLE public.{t} (id int PRIMARY KEY, name text NOT NULL)");
            }
            await _fixture.ExecuteAsync("INSERT INTO public.mt_a VALUES (1, 'a1')");
            await _fixture.ExecuteAsync("INSERT INTO public.mt_b VALUES (1, 'b1')");
            await _fixture.ExecuteAsync("INSERT INTO public.mt_c VALUES (1, 'c1')");

            var fileProvider = new MemoryFileProvider();
            PostgresSourceOptions Options() => new()
            {
                ConnectionStringFunc = () => _fixture.ConnectionString,
                ReplicationMode = PostgresReplicationMode.Shared,
                SlotDurability = PostgresSlotDurability.Persistent,
                DeltaLoadInterval = TimeSpan.FromMilliseconds(200)
            };

            const string sql = @"INSERT INTO output
                SELECT id, name FROM public.mt_a
                UNION ALL SELECT id, name FROM public.mt_b
                UNION ALL SELECT id, name FROM public.mt_c";

            try
            {
                await using (var first = new PostgresTestStream(testName, Options(), fileProvider))
                {
                    // Three sources on one shared slot, with several global checkpoints failing and rolling back.
                    first.EgressCrashOnCheckpoint(3);
                    first.RegisterTableProviders(b => b.AddPostgreSqlProvider(() => _fixture.ConnectionString));
                    await first.StartStream(sql);
                    await WaitAndAssert(first, new[]
                    {
                        new { id = 1L, name = "a1" }, new { id = 1L, name = "b1" }, new { id = 1L, name = "c1" }
                    });

                    // Changes to every table while checkpoints are still recovering.
                    await _fixture.ExecuteAsync("INSERT INTO public.mt_a VALUES (2, 'a2')");
                    await _fixture.ExecuteAsync("INSERT INTO public.mt_b VALUES (2, 'b2')");
                    await _fixture.ExecuteAsync("INSERT INTO public.mt_c VALUES (2, 'c2')");
                    await WaitAndAssert(first, new[]
                    {
                        new { id = 1L, name = "a1" }, new { id = 2L, name = "a2" },
                        new { id = 1L, name = "b1" }, new { id = 2L, name = "b2" },
                        new { id = 1L, name = "c1" }, new { id = 2L, name = "c2" }
                    });
                }

                // Downtime changes to every table, then restart: the single shared slot must resume all three without
                // skipping any (confirmed_flush is the min across tables, never ahead of any table's durable state).
                await _fixture.ExecuteAsync("INSERT INTO public.mt_a VALUES (3, 'a3')");
                await _fixture.ExecuteAsync("INSERT INTO public.mt_b VALUES (3, 'b3')");
                await _fixture.ExecuteAsync("INSERT INTO public.mt_c VALUES (3, 'c3')");

                var second = new PostgresTestStream(testName, Options(), fileProvider);
                try
                {
                    second.RegisterTableProviders(b => b.AddPostgreSqlProvider(() => _fixture.ConnectionString));
                    await second.StartStream(sql);
                    await WaitAndAssert(second, new[]
                    {
                        new { id = 1L, name = "a1" }, new { id = 2L, name = "a2" }, new { id = 3L, name = "a3" },
                        new { id = 1L, name = "b1" }, new { id = 2L, name = "b2" }, new { id = 3L, name = "b3" },
                        new { id = 1L, name = "c1" }, new { id = 2L, name = "c2" }, new { id = 3L, name = "c3" }
                    });
                }
                finally
                {
                    await second.DisposeAsync();
                }
            }
            finally
            {
                await DropPersistentLogicalSlots();
            }
        }

        [Fact]
        public async Task SharedPersistentSlotResumesMultipleTables()
        {
            const string testName = nameof(SharedPersistentSlotResumesMultipleTables);
            await DropPersistentLogicalSlots();
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.sp_a");
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.sp_b");
            await _fixture.ExecuteAsync("CREATE TABLE public.sp_a (id int PRIMARY KEY, name text NOT NULL)");
            await _fixture.ExecuteAsync("CREATE TABLE public.sp_b (id int PRIMARY KEY, name text NOT NULL)");
            await _fixture.ExecuteAsync("INSERT INTO public.sp_a VALUES (1, 'a1')");
            await _fixture.ExecuteAsync("INSERT INTO public.sp_b VALUES (1, 'b1')");

            var fileProvider = new MemoryFileProvider();
            PostgresSourceOptions Options() => new()
            {
                ConnectionStringFunc = () => _fixture.ConnectionString,
                ReplicationMode = PostgresReplicationMode.Shared,
                SlotDurability = PostgresSlotDurability.Persistent,
                DeltaLoadInterval = TimeSpan.FromMilliseconds(200)
            };

            const string sql = @"INSERT INTO output
                SELECT id, name FROM public.sp_a
                UNION ALL
                SELECT id, name FROM public.sp_b";

            await using (var first = new PostgresTestStream(testName, Options(), fileProvider))
            {
                first.RegisterTableProviders(b => b.AddPostgreSqlProvider(() => _fixture.ConnectionString));
                await first.StartStream(sql);
                await WaitAndAssert(first, new[] { new { id = 1L, name = "a1" }, new { id = 1L, name = "b1" } });
            }

            // A single persistent slot serves both tables and survives teardown.
            Assert.Equal(1, await CountPersistentLogicalSlots());

            // Downtime changes to both tables must be delivered on resume.
            await _fixture.ExecuteAsync("INSERT INTO public.sp_a VALUES (2, 'a2')");
            await _fixture.ExecuteAsync("INSERT INTO public.sp_b VALUES (2, 'b2')");

            var second = new PostgresTestStream(testName, Options(), fileProvider);
            try
            {
                second.RegisterTableProviders(b => b.AddPostgreSqlProvider(() => _fixture.ConnectionString));
                await second.StartStream(sql);
                await WaitAndAssert(second, new[]
                {
                    new { id = 1L, name = "a1" },
                    new { id = 2L, name = "a2" },
                    new { id = 1L, name = "b1" },
                    new { id = 2L, name = "b2" }
                });
            }
            finally
            {
                await second.DisposeAsync();
                await DropPersistentLogicalSlots();
            }
        }

        [Fact]
        public async Task ColumnProjectionIgnoresUnselectedColumnChanges()
        {
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.proj_t");
            await _fixture.ExecuteAsync("CREATE TABLE public.proj_t (id int PRIMARY KEY, a int NOT NULL, b int NOT NULL)");
            await _fixture.ExecuteAsync("INSERT INTO public.proj_t VALUES (1, 10, 100), (2, 20, 200)");

            await using var stream = CreateStream(nameof(ColumnProjectionIgnoresUnselectedColumnChanges), PostgresReplicationMode.PerTable);
            // Only id and b are selected; column a is not part of the output.
            await stream.StartStream("INSERT INTO output SELECT id, b FROM public.proj_t");

            await WaitAndAssert(stream, new[] { new { id = 1L, b = 100L }, new { id = 2L, b = 200L } });

            // Changing only the unselected column must not change the projected output.
            await _fixture.ExecuteAsync("UPDATE public.proj_t SET a = 999 WHERE id = 1");
            await WaitAndAssert(stream, new[] { new { id = 1L, b = 100L }, new { id = 2L, b = 200L } });

            // Changing a selected column is reflected.
            await _fixture.ExecuteAsync("UPDATE public.proj_t SET b = 111 WHERE id = 1");
            await WaitAndAssert(stream, new[] { new { id = 1L, b = 111L }, new { id = 2L, b = 200L } });
        }
    }
}
