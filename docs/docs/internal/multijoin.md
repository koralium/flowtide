---
sidebar_position: 4
---


# Multi Join

A query that joins several tables normally runs as a chain of joins. Each join in the chain stores its two inputs, and for every join after the first, one of those inputs is the joined rows of the joins before it. The more tables a query joins, the more copies of the same data the stream keeps in its state.

With multi join, which is on by default, a chain of joins runs as one operator. It stores each table once and no joined rows in between. The result of the query is the same.

## When to use it

Use it when a stream joins three or more tables and its state is large. In our measurements with one matching row per join, a multi join held between 60% and 80% of the state of the chain at three tables. At eleven tables it held between 32% and 49% with the default of five tables per multi join, and between 16% and 43% with all eleven in one. The wider the rows, the larger the saving.

In our measurements a multi join was also faster than the chain: between 1.5 and 7 times on joins of five and nine tables. The gain is largest when many small changes arrive close together. The multi join then takes the changes that wait for it together, where a chain takes them one at a time. These measurements used tables that fit in memory. We have not measured state that is larger than the cache on real storage.

When changes arrive one at a time with pauses between them, there is nothing to take together. For changes of a few rows on three tables, and on five tables with inner joins, a multi join then measured slower than the chain, at 0.7 to 0.9 times its speed. We have not measured why.

A multi join is one operator. With parallelization on, the joins it replaces are no longer split into partitions, only its inputs are. We have not measured a multi join against a chain that is split.

It does not change streams with two-table joins only.

## How to turn it off

It is on by default. Turn it off on the optimizer settings of the stream:

```csharp
builder.Services.AddFlowtideStream("stream")
    .AddSqlFileAsPlan("query.sql")
    .SetOptimizerSettings(new PlanOptimizerSettings()
    {
        MultiJoin = false
    })
    // connectors, storage and the rest as before
```

Or when you optimize a plan yourself:

```csharp
var optimized = PlanOptimizer.Optimize(plan, new PlanOptimizerSettings()
{
    MultiJoin = false
});
flowtideBuilder.AddPlan(optimized, optimize: false);
```

## Settings

| Setting | Default | What it does |
| --- | --- | --- |
| `MultiJoin` | `true` | Runs chains of joins as multi joins. |
| `MultiJoinMaxInputs` | `5` | The most tables one multi join takes. A longer chain is split into several multi joins in a row. The lowest value is 3. |
| `MultiJoinPairConditions` | `true` | Takes joins that compare their two sides next to the key, for example a date between a start and an end date. |

A higher `MultiJoinMaxInputs` saves more state, since every split stores the joined rows that pass from one multi join to the next. How a higher value affects speed depends on the query, so measure before you raise it.

## Which joins it takes

Inner, left, right and full outer joins on equal keys, and the joins that `IN` and `EXISTS` turn into.

What a join can have next to its equal keys depends on its type:

| Join | Extra conditions |
| --- | --- |
| Inner | Any. |
| Left, right | Any, when the side that can be filled with nulls is one table. |
| Full outer | None. |
| `IN`, `EXISTS` | Each condition reads columns from the subquery only. |

A left or right join can compare its two sides, for example:

```sql
FROM orders o
LEFT JOIN prices p ON o.product = p.product AND o.date >= p.startDate AND o.date < p.endDate
```

With `MultiJoinPairConditions` off, such a join stays an ordinary join, and so does any join that compares its two sides with `<`, `<=`, `>` or `>=`.

These joins stay ordinary joins, also with the settings on:

- A full outer join with an extra condition.
- A left or right join that compares its two sides when the side that can be filled with nulls is itself a join, for example a subquery or a view with a join in it.
- A join whose key or condition reads a value inside a struct, a map or a list of the rows joined before it. Reading inside a column of a table that is joined directly is fine.
- A join whose key takes columns from more than one of the tables joined before it, for example `ON c.x = a.x AND c.y = b.y`.
- Joins inside a recursive query.
- All joins of a plan that is split for [distributed mode](../distributed/index.md).

A multi join also ends where something other than a join sits between two joins. The joins below that point and the joins above it can each become a multi join of their own, when three or more tables are left on that side. This happens for:

- A join on the result of an expression over the joins before it.
- A left or right join with a condition that computes a value from the tables joined before it, for example `LEFT JOIN c ON b.k = c.k AND b.active = 1` after `a JOIN b`. A condition that is only a boolean column, `AND b.active`, does not end the multi join.
- An inner or right join with an extra condition on columns that a left, right or full outer join before it can fill with nulls, for example `a LEFT JOIN b ON a.k = b.k JOIN c ON b.j = c.j AND b.active = 1`.

A query can mix both. The joins that qualify become multi joins and the rest run as before.

Multi join needs the column store, which is on by default. A stream that has turned the column store off fails to build when its plan has a multi join.

## Changing the setting on an existing stream

Turning `MultiJoin` or `MultiJoinPairConditions` on or off, or changing `MultiJoinMaxInputs`, changes how the joins store their rows. A stream cannot continue on the state it had before.

- If the stream takes its version from the plan (`AddVersioningFromPlanHash()`) or has no version set, and keeps its state in Reservoir storage on disk or in a blob store, it gets a new version and reads its sources again from the start. Reservoir storage keeps the state of every version apart.
- If the stream has a version you set yourself (`AddVersioningFromString("v1")`), it refuses to start and reports that the stored plan hash is different. Give the stream a new version to start it again.
- If the storage does not keep versions apart, the stream refuses to start in the same way whatever its version is. Remove its state, or point it to an empty storage, to start it again.

Plan the change as you would plan a change of the query itself.

## How it works

A multi join is one operator with one input per table. Every table is stored once, in a tree ordered by the columns it is joined on. A table that is joined on different columns in different joins also gets a small second tree for the other columns. No join results are stored.

When a batch arrives on one input, the operator stores its rows in the tree of that input and looks up the matching rows in the other trees, one join after the other. The rows it finds are the output.

Joins that keep unmatched rows (left, right and full outer joins) also keep a small tree per join with the number of matches per key. It tells the operator when a row has to be filled with nulls and when that row has to be taken back.

### Rounds

The operator is a `MultipleInputRoundVertex`. All inputs put their batches in one queue. When the operator takes a batch and more batches already wait behind it, it takes them together as a round.

- Batches of the same input are put together into one batch.
- Each batch of the round runs on its own task. It stores its rows and looks up in the other trees.
- Every tree has a queue with the batches of the round in order. A batch uses a tree when the batches before it are done with that tree, so the result is the same as taking the batches one after the other.
- Different trees are used by different batches at the same time. That is where a round is faster.

Plans with joins that keep unmatched rows take the batches of a round one after the other.

When no batch waits, a batch is taken on its own.

### Lookups that wait for storage

When a lookup has to wait for a page to be read, the lookups of the following joins are started beside it instead of after it. This only happens when a lookup really waits, a page that is in the cache is read directly.

## What to watch

Each multi join reports these metrics, next to the ones every operator has:

| Metric | What it tells you |
| --- | --- |
| `flowtide_lookup_ms` | Time spent finding the matching rows for a batch. |
| `flowtide_insert_ms` | Time spent storing a batch. |
| `flowtide_pad_correction_ms` | Time spent adding and removing the rows that left, right and full outer joins fill with nulls. |
| `flowtide_deferred_rows` | Rows received but not stored or joined yet. The operator holds back scattered inserts and takes them together at the next watermark or checkpoint. |
| `flowtide_round_batches` | Batches that were taken together with other batches that waited. It grows when changes arrive faster than they are joined. |
| `flowtide_parallel_lookups` | Lookups that were started while another lookup waited for storage. It grows when the state does not fit in the cache, and a little after every checkpoint. |

The three time metrics are histograms, `flowtide_deferred_rows` is a gauge and the last two are counters. They are exported through OpenTelemetry and are not shown in the built in stream view yet.

When a stream changes from ordinary joins to multi joins, compare the state size and the time between checkpoints with what it had before.
