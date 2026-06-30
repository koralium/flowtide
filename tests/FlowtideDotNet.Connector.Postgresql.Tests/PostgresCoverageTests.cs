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

namespace FlowtideDotNet.Connector.PostgreSQL.Tests
{
    public partial class PostgresSourceTests
    {
        // REPLICA IDENTITY FULL makes UPDATE/DELETE carry the full old-row image (FullUpdateMessage / FullDeleteMessage),
        // a decode path the default (PK) replica identity never exercises.
        [Fact]
        public async Task ReplicaIdentityFullUpdateDeleteAndKeyChange()
        {
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.rifull_t");
            await _fixture.ExecuteAsync("CREATE TABLE public.rifull_t (id int PRIMARY KEY, name text NOT NULL)");
            await _fixture.ExecuteAsync("ALTER TABLE public.rifull_t REPLICA IDENTITY FULL");
            await _fixture.ExecuteAsync("INSERT INTO public.rifull_t VALUES (1, 'a'), (2, 'b')");

            await using var stream = CreateStream(nameof(ReplicaIdentityFullUpdateDeleteAndKeyChange), PostgresReplicationMode.PerTable);
            await stream.StartStream("INSERT INTO output SELECT id, name FROM public.rifull_t");
            await WaitAndAssert(stream, new[] { new { id = 1L, name = "a" }, new { id = 2L, name = "b" } });

            await _fixture.ExecuteAsync("UPDATE public.rifull_t SET name = 'a2' WHERE id = 1");
            await _fixture.ExecuteAsync("DELETE FROM public.rifull_t WHERE id = 2");
            await WaitAndAssert(stream, new[] { new { id = 1L, name = "a2" } });

            // Key-changing update with a full old image: the old key must be extracted from the full tuple to retract.
            await _fixture.ExecuteAsync("UPDATE public.rifull_t SET id = 10 WHERE id = 1");
            await WaitAndAssert(stream, new[] { new { id = 10L, name = "a2" } });
        }

        // REPLICA IDENTITY FULL combined with an unchanged TOAST column. The new tuple still carries the TOAST value as
        // an "unchanged" marker (replica identity governs the OLD tuple, not the new), so the backfill-from-previous-row
        // path must work under FULL exactly as it does under the default identity.
        [Fact]
        public async Task ReplicaIdentityFullWithUnchangedToast()
        {
            var big = new string('x', 5000);
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.fulltoast_t");
            await _fixture.ExecuteAsync("CREATE TABLE public.fulltoast_t (id int PRIMARY KEY, n int NOT NULL, big text NOT NULL)");
            await _fixture.ExecuteAsync("ALTER TABLE public.fulltoast_t ALTER COLUMN big SET STORAGE EXTERNAL");
            await _fixture.ExecuteAsync("ALTER TABLE public.fulltoast_t REPLICA IDENTITY FULL");
            await _fixture.ExecuteAsync($"INSERT INTO public.fulltoast_t (id, n, big) VALUES (1, 1, '{big}')");

            await using var stream = CreateStream(nameof(ReplicaIdentityFullWithUnchangedToast), PostgresReplicationMode.PerTable);
            await stream.StartStream("INSERT INTO output SELECT id, n, big FROM public.fulltoast_t");
            await WaitAndAssert(stream, new[] { new { id = 1L, n = 1L, big } });

            // Non-key update, TOAST unchanged -> backfilled from the previous row (keyed by the unchanged key).
            await _fixture.ExecuteAsync("UPDATE public.fulltoast_t SET n = 42 WHERE id = 1");
            await WaitAndAssert(stream, new[] { new { id = 1L, n = 42L, big } });

            // Key-changing update with the TOAST still unchanged: the backfill lookup is by the NEW key (not yet in the
            // tree), so the connector falls back to a reconciling reload - which must still land on the right state.
            await _fixture.ExecuteAsync("UPDATE public.fulltoast_t SET id = 2 WHERE id = 1");
            await WaitAndAssert(stream, new[] { new { id = 2L, n = 42L, big } });
        }

        // A CDC source feeding a stateful, watermark-driven aggregation - retract+re-aggregate must stay correct.
        [Fact]
        public async Task AggregationOverCdcSource()
        {
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.agg_t");
            await _fixture.ExecuteAsync("CREATE TABLE public.agg_t (id int PRIMARY KEY, grp text NOT NULL, amount int NOT NULL)");
            await _fixture.ExecuteAsync("INSERT INTO public.agg_t VALUES (1, 'x', 10), (2, 'x', 20), (3, 'y', 5)");

            await using var stream = CreateStream(nameof(AggregationOverCdcSource), PostgresReplicationMode.PerTable);
            await stream.StartStream("INSERT INTO output SELECT grp, SUM(amount) AS total FROM public.agg_t GROUP BY grp");
            await WaitAndAssert(stream, new[] { new { grp = "x", total = 30L }, new { grp = "y", total = 5L } });

            await _fixture.ExecuteAsync("UPDATE public.agg_t SET amount = 100 WHERE id = 1"); // x: 30 -> 120
            await _fixture.ExecuteAsync("INSERT INTO public.agg_t VALUES (4, 'y', 7)");        // y: 5 -> 12
            await _fixture.ExecuteAsync("DELETE FROM public.agg_t WHERE id = 3");              // y: 12 -> 7
            await WaitAndAssert(stream, new[] { new { grp = "x", total = 120L }, new { grp = "y", total = 7L } });
        }

        // Two DIFFERENT CDC tables joined - the engine must coordinate two distinct source watermarks.
        [Fact]
        public async Task JoinOfTwoDifferentCdcTables()
        {
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.jt_orders");
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.jt_users");
            await _fixture.ExecuteAsync("CREATE TABLE public.jt_users (uid int PRIMARY KEY, uname text NOT NULL)");
            await _fixture.ExecuteAsync("CREATE TABLE public.jt_orders (oid int PRIMARY KEY, uid int NOT NULL, item text NOT NULL)");
            await _fixture.ExecuteAsync("INSERT INTO public.jt_users VALUES (1, 'alice'), (2, 'bob')");
            await _fixture.ExecuteAsync("INSERT INTO public.jt_orders VALUES (10, 1, 'book')");

            await using var stream = CreateStream(nameof(JoinOfTwoDifferentCdcTables), PostgresReplicationMode.Shared);
            await stream.StartStream(@"INSERT INTO output
                SELECT o.oid, u.uname, o.item FROM public.jt_orders o JOIN public.jt_users u ON o.uid = u.uid");
            await WaitAndAssert(stream, new[] { new { oid = 10L, uname = "alice", item = "book" } });

            await _fixture.ExecuteAsync("UPDATE public.jt_users SET uname = 'alice2' WHERE uid = 1"); // right side
            await _fixture.ExecuteAsync("INSERT INTO public.jt_orders VALUES (11, 2, 'pen')");         // left side
            await WaitAndAssert(stream, new[]
            {
                new { oid = 10L, uname = "alice2", item = "book" },
                new { oid = 11L, uname = "bob", item = "pen" }
            });
        }

        // float8 +/-Infinity arrives as text "Infinity"/"-Infinity" on the replication path; the converter must not throw.
        [Fact]
        public async Task FloatInfinityRoundTrips()
        {
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.inf_t");
            await _fixture.ExecuteAsync("CREATE TABLE public.inf_t (id int PRIMARY KEY, d float8 NOT NULL)");
            await _fixture.ExecuteAsync("INSERT INTO public.inf_t VALUES (1, 'Infinity'), (2, '-Infinity'), (3, 1.5)");

            await using var stream = CreateStream(nameof(FloatInfinityRoundTrips), PostgresReplicationMode.PerTable);
            await stream.StartStream("INSERT INTO output SELECT id, d FROM public.inf_t");
            await WaitAndAssert(stream, new[]
            {
                new { id = 1L, d = double.PositiveInfinity },
                new { id = 2L, d = double.NegativeInfinity },
                new { id = 3L, d = 1.5 }
            });

            // Same values via the replication (text) path.
            await _fixture.ExecuteAsync("INSERT INTO public.inf_t VALUES (4, 'Infinity'), (5, '-Infinity')");
            await WaitAndAssert(stream, new[]
            {
                new { id = 1L, d = double.PositiveInfinity },
                new { id = 2L, d = double.NegativeInfinity },
                new { id = 3L, d = 1.5 },
                new { id = 4L, d = double.PositiveInfinity },
                new { id = 5L, d = double.NegativeInfinity }
            });
        }

        // A table whose only column is its primary key: NonKeyColumnIndexes is empty, exercising the no-other-columns path.
        [Fact]
        public async Task KeyOnlyTable()
        {
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.keyonly_t");
            await _fixture.ExecuteAsync("CREATE TABLE public.keyonly_t (id int PRIMARY KEY)");
            await _fixture.ExecuteAsync("INSERT INTO public.keyonly_t VALUES (1), (2)");

            await using var stream = CreateStream(nameof(KeyOnlyTable), PostgresReplicationMode.PerTable);
            await stream.StartStream("INSERT INTO output SELECT id FROM public.keyonly_t");
            await WaitAndAssert(stream, new[] { new { id = 1L }, new { id = 2L } });

            await _fixture.ExecuteAsync("INSERT INTO public.keyonly_t VALUES (3)");
            await _fixture.ExecuteAsync("DELETE FROM public.keyonly_t WHERE id = 1");
            await WaitAndAssert(stream, new[] { new { id = 2L }, new { id = 3L } });
        }

        // Probe: timestamptz 'infinity'/'-infinity'. The snapshot reads it typed; the replication path gets the text
        // "infinity", which DateTime.Parse rejects. Surfaces whether infinite timestamps are supported.
        [Fact]
        public async Task TimestampInfinityRoundTrips()
        {
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.tsinf_t");
            await _fixture.ExecuteAsync("CREATE TABLE public.tsinf_t (id int PRIMARY KEY, ts timestamptz NOT NULL)");
            await _fixture.ExecuteAsync("INSERT INTO public.tsinf_t VALUES (1, 'infinity'), (2, '2024-01-01 00:00:00+00')");

            await using var stream = CreateStream(nameof(TimestampInfinityRoundTrips), PostgresReplicationMode.PerTable);
            await stream.StartStream("INSERT INTO output SELECT id, ts FROM public.tsinf_t");
            await WaitAndAssert(stream, new[]
            {
                new { id = 1L, ts = DateTime.MaxValue },
                new { id = 2L, ts = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc) }
            });

            await _fixture.ExecuteAsync("INSERT INTO public.tsinf_t VALUES (3, '-infinity')");
            await WaitAndAssert(stream, new[]
            {
                new { id = 1L, ts = DateTime.MaxValue },
                new { id = 2L, ts = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc) },
                new { id = 3L, ts = DateTime.MinValue }
            });
        }

        // Snapshot of a table that is empty at start, then receives changes - the full load must complete with zero rows.
        [Fact]
        public async Task EmptyTableSnapshotThenInserts()
        {
            await _fixture.ExecuteAsync("DROP TABLE IF EXISTS public.emptysnap_t");
            await _fixture.ExecuteAsync("CREATE TABLE public.emptysnap_t (id int PRIMARY KEY, name text NOT NULL)");

            await using var stream = CreateStream(nameof(EmptyTableSnapshotThenInserts), PostgresReplicationMode.PerTable);
            await stream.StartStream("INSERT INTO output SELECT id, name FROM public.emptysnap_t");

            await _fixture.ExecuteAsync("INSERT INTO public.emptysnap_t VALUES (1, 'a')");
            await WaitAndAssert(stream, new[] { new { id = 1L, name = "a" } });
        }
    }
}
