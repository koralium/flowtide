---
sidebar_position: 11
---

# PostgreSQL Connector

The PostgreSQL connector reads data from PostgreSQL tables using **logical replication**. It is built around
two design goals:

* **Nothing is ever left pinned on the server.** The connector uses *temporary* replication slots, which PostgreSQL
  drops automatically when the connection closes. A crashed or stopped Flowtide stream can never leave a slot behind
  that retains WAL and fills the disk.
* **Just point it at a table.** Publications and slots are created and managed for you; you only write SQL against the
  table.

The connector has two parts:

* **Source** – snapshots a table and then streams inserts, updates and deletes from the WAL.
* **Table Provider** – provides table information to the SQL plan builder.

## Prerequisites

Because temporary slots cannot be resumed after a reconnect, the connector treats a restart as a **re-snapshot**,
which is reconciled against existing state by the read operator, so no rows are duplicated or lost.

The PostgreSQL server and role must satisfy:

* `wal_level = logical` (server configuration; requires a restart).
* The connecting role has the `REPLICATION` attribute (or is a superuser), and `pg_hba.conf` permits a replication
  connection from the Flowtide host.
* Every source table has a **primary key** (or `REPLICA IDENTITY FULL`), so that updates and deletes carry the key
  needed to emit retractions.

The source validates `wal_level` and the replication permission when it starts and throws a clear error if they are
not met.

## Replication modes

The connector can consume replication either per table or shared across a database, controlled by
`PostgresSourceOptions.ReplicationMode`:

| Mode | Slots | Behaviour |
| --- | --- | --- |
| `PerTable` | One temporary slot per table | Most isolated: a slow or failing table does not affect the others. PostgreSQL decodes the WAL once per slot, so this scales poorly with many tables. |
| `Shared` (default) | One temporary slot per (stream, database) | PostgreSQL decodes the WAL only once and the changes are demultiplexed to each table. Trades a small amount of head-of-line blocking for far less load on the source database. |

Both modes use the same snapshot-then-stream logic; only where the slot lives differs. In a multi-node deployment the
shared reader is scoped per (stream, database, node), so each node decodes only the tables it hosts.

## Slot durability

`PostgresSourceOptions.SlotDurability` controls whether the slot is temporary or persistent:

| Durability | Restart behaviour | Cleanup |
| --- | --- | --- |
| `Temporary` (default) | The slot is dropped when the connection closes, so a restart **re-snapshots** the whole table and reconciles it against the checkpointed state. Cost grows with table size. | None — nothing is ever left on the server. |
| `Persistent` | A named slot survives restarts, so the stream **resumes** from the last durably-checkpointed position instead of re-snapshotting. The confirmed position is advanced only after a checkpoint, so resuming never loses or (thanks to the reconciling apply) duplicates rows. If the slot has been invalidated (`wal_status = 'lost'`) it falls back to a re-snapshot. | The slot is **not** dropped automatically. You must `pg_drop_replication_slot(...)` it when the stream is permanently retired, otherwise it retains WAL on the server. Bound the retention with `max_slot_wal_keep_size`. |

`Persistent` is the right choice for large tables where a full re-snapshot on every restart is too expensive. It works
in both `PerTable` and `Shared` mode. In `Shared` mode the single slot's confirmed position only advances to the
**minimum** position all of its tables have durably checkpointed, so a table that is ahead simply re-processes the gap
on resume (which is idempotent).

## Publications

By default Flowtide creates and manages a publication automatically (one publication per table in `PerTable` mode,
one publication for all tables in `Shared` mode). If you prefer to manage the publication yourself — for example
because the Flowtide role should not have `CREATE PUBLICATION` rights — set `PostgresSourceOptions.PublicationName` to
an existing publication that already contains every table the stream reads.

## Source

Add the source to the connector manager:

```csharp
connectorManager.AddPostgresSource(() => "Host=localhost;Username=postgres;Password=...;Database=mydb");
```

Or with options:

```csharp
connectorManager.AddPostgresSource(new PostgresSourceOptions
{
    ConnectionStringFunc = () => connectionString,
    ReplicationMode = PostgresReplicationMode.Shared,
    // Use an existing, user-managed publication instead of auto-managing one:
    // PublicationName = "my_publication",
});
```

Register the table provider with the SQL plan builder so the planner knows the table schema:

```csharp
sqlPlanBuilder.AddPostgreSqlProvider(() => connectionString);
```

Tables are referenced as `schema.table` (or just `table`, which defaults to the `public` schema):

```sql
INSERT INTO my_sink
SELECT id, name FROM public.users
```

## Options

| Option | Default | Description |
| --- | --- | --- |
| `ConnectionStringFunc` | required | Connection string for both snapshot reads and the replication connection. |
| `ReplicationMode` | `Shared` | `PerTable` or `Shared`. |
| `SlotDurability` | `Temporary` | `Temporary` (re-snapshot on restart) or `Persistent` (resume from checkpoint). |
| `PublicationName` | `null` | Use an existing publication instead of auto-managing one. |
| `TableNameTransform` | `null` | Map a read relation to the actual schema/table parts. |
| `SlotPrefix` | `flowtide` | Prefix for generated replication slot names. |
| `PublicationPrefix` | `flowtide` | Prefix for auto-managed publication names. |
| `StatusUpdateInterval` | 10s | How often a standby status update (LSN feedback) is sent. |
| `DeltaLoadInterval` | 1s | How often the stream is polled for buffered changes. |
| `ChannelCapacity` | 1024 | Per-table buffer size in `Shared` mode (back-pressure bound). |
| `SnapshotBatchSize` | 10000 | Rows read per batch during the initial snapshot. |

## Supported data types

PostgreSQL types are mapped to Flowtide types as follows. Unmapped types (arrays, ranges, network types, enums, …)
are read as their PostgreSQL text representation — consistently in both the snapshot and the replication stream (the
snapshot reads them with a `::text` cast so the two paths agree).

| PostgreSQL | Flowtide |
| --- | --- |
| `bool` | Boolean |
| `int2`, `int4`, `int8`, `oid` | Int64 |
| `float4`, `float8` | Double |
| `numeric`, `decimal`, `money` | Decimal |
| `text`, `varchar`, `char`, `name`, `uuid`, `json`, `jsonb`, `xml` | String |
| `bytea` | Binary |
| `date`, `timestamp`, `timestamptz` | Timestamp |
| `time`, `timetz` | Int64 (ticks) |

## Observability and resilience

* The source exposes an `events` counter and a `postgres_applied_lsn` gauge (the last WAL position applied to the
  stream) per operator.
* Snapshot connections are opened through the configured `ResiliencePipeline` (a Polly pipeline; the default retries
  with backoff), so transient connection failures during a snapshot are retried before escalating.

## Notes and limitations

* **Re-snapshot on restart (temporary slots).** With the default `Temporary` durability a restart re-reads the table
  and reconciles it against existing state. For very large tables this can be costly — use `SlotDurability = Persistent`
  to resume from the last checkpoint instead.
* **TOAST.** When an `UPDATE` omits an unchanged out-of-line (TOAST) column, the value is backfilled from the previous
  row, so the row is emitted with its full, correct value. (If the previous row is unexpectedly missing, the table
  falls back to a reconciling reload.)
* **TRUNCATE** is reflected by reconciling against the now-empty table, which retracts all of its rows.
* **Replica identity.** Tables need a primary key and a replica identity that carries the key in WAL (the default
  identity, which is the primary key, or `FULL`). `REPLICA IDENTITY NOTHING` is rejected at startup.
* **Type-fidelity caveats.** `bytea` assumes the default `bytea_output = hex`. `numeric`/`decimal` values of `NaN`
  are not supported. `timestamptz`/`timetz` are decoded from the replication text using the server's `TimeZone`;
  standard whole-hour offsets are handled, but exotic sub-hour zones may need the server session in UTC. `money` is
  surfaced as its text representation rather than a decimal (its text form is locale-dependent).
