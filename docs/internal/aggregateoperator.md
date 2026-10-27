---
sidebar_position: 2
---

# Aggregate Operator Internals

This page describes how the columnar aggregate operator is implemented. For the
user-facing description (supported features, metrics) see the
[Aggregate Operator](../operators/aggregate.md) page.

The operator implements grouped and global aggregation for a Substrait
*Aggregate Relation*. Updated aggregate values are emitted on watermark updates —
this batches downstream traffic and lets each measure's state be updated in place
to reduce allocations. The exception is retractions for groups that are *fully
removed* within a batch: those are emitted during ingestion (see the ingest path
below), because a removed group never reaches the next watermark.

## Two B+ trees

The operator keeps two B+ trees:

* **Persisted aggregate tree** (`grouping_set_1_v1`) — key is the group-key
  columns, value is a `ColumnAggregateValueContainer` holding, per measure, the
  inline state plus the bookkeeping needed for incremental emission (the group
  weight, a `previousValueSent` flag, and the previously emitted value per
  measure).
* **Temporary "modified keys" tree** (`grouping_set_1_v1_temp`) — the set of
  group keys touched since the last watermark. It drives incremental emission
  and is cleared at the end of every watermark.

Some measures keep additional trees of their own (see below).

## Measure taxonomy

Measures fall into three kinds depending on where their state lives and how the
value is produced:

| Kind | Measures | State | Value produced |
| ---- | -------- | ----- | -------------- |
| Stateless | `sum`, `sum0`, `count`, `avg` | Inline in the persisted tree value | In `Compute`, folding over the group's rows |
| Shared-tree | `min`, `max`, `count_distinct`, `list_agg`, `string_agg` | A shared `(group…, value)` B+ tree (`sharedtree_{i}`) | Searched from the tree at emission time |
| Own-tree | `min_by`, `max_by`, `list_union_distinct_agg` | A dedicated B+ tree (`minbytree` / `maxbytree` / `listuniondistinctaggtree`) | Searched from its own tree at emission time |

**Stateless** measures store a single value per group inline. `sum` and `avg`
store a `{ sum, count }` struct rather than a bare number so they can revert to
`null` once the last non-null contributor is retracted (a bare running total
cannot distinguish "no contributors" from "contributors that sum to zero").
`sum0` emits a zero typed to the aggregate's declared return type for an
empty/all-null group.

**Shared-tree** measures are keyed by `(group…, value)`. One shared tree is
created per distinct `(value expression, filter, ignore-nulls)` key, so
`min(x)`, `max(x)` and `count_distinct(x)` over the same column share a single
tree. Nulls are filtered on insert. For most of them the value for a group is
derived by searching the tree on the partial (group-only) key — `min` reads the
leftmost matching entry, `max` the rightmost, and `list_agg`/`string_agg`
concatenate the matched range. `count_distinct` is the exception: it does not
search at emission. Instead it maintains the distinct count incrementally as
values enter and leave the shared tree (via the insert mutation hook) and stores
that count inline in the persisted state, read back like a stateless measure.

**Own-tree** measures cannot share a tree and each manage their own. `min_by` and
`max_by` are keyed by `(group…, orderBy)` and store the value column (null
order-by rows are filtered). `list_union_distinct_agg` flattens its list argument
and keys by `(group…, element)` so the tree itself deduplicates the union; it
opts out of sharing (`SupportsSharedTree => false`) because that flattening needs
its own insert path.

## Ingest path (`OnRecieve`)

1. Sort the incoming batch by group key.
2. Build a *compute range* per unique group (a contiguous slice of the sorted
   indices).
3. For each measure:
   * Stateless measures run `Compute` over the group's rows and update their
     inline state in place. If the measure has a `FILTER`, the mutator first
     compacts the group's rows down to those matching the predicate, so the
     filtered rows are the only ones folded in.
   * Shared-tree and own-tree measures store their `(group, value)` rows into
     their trees via a bulk insert.
4. Group existence is tracked by the group's **total** weight (unfiltered). When
   it reaches zero the group is deleted from the persisted tree and a retraction
   of its last emitted value is emitted immediately, during ingestion — a deleted
   key is netted out of the temporary tree (see below), so it is never revisited
   at the next watermark and must be retracted here.
5. The temporary tree is updated with the touched group keys (`+1`) and deletes
   (`-1`). Deletes net out, which preserves the **temp-tree invariant**: every
   key in the temporary tree still exists in the persisted tree, so the
   watermark lookup never sees a not-found result for a temp key.

## Emission (`OnWatermark`)

There are two paths:

* **Initial data** — on the first watermark the whole persisted tree is
  iterated and emitted.
* **Incremental** — afterwards, only the temporary tree (the modified keys) is
  iterated.

Incremental emission diffs against what was last sent. For each modified group
it computes the new value, looks the group up in the persisted tree, and:

* if a value was previously emitted (`previousValueSent`), emits a **retraction**
  of the old value (read from the stored `previousValueCol`) with weight `-1`,
  plus the new value with weight `+1`;
* updates `previousValueCol` to the new value.

A group that was touched but whose value did not actually change still emits a
retract/insert pair of the same value — a net-zero in the change stream.

Two bulk B+ tree searches are involved, and both must handle a group's keys
straddling leaf-page boundaries by carrying unmatched keys to the adjacent leaf:

* the **persisted-tree group lookup** (`AggregateSearchComparer`) carries
  forward (`SeekNextPageForValue`);
* the **shared / own-tree value searches** carry forward when reading the
  leftmost matching entry (`min`, `min_by`, `list_agg`, `string_agg`) and
  backward (`SeekPreviousPageForValue`) when reading the rightmost (`max`,
  `max_by`). (`count_distinct` does no emission-time search — see above.)

This carry logic is subtle and was historically the source of several "wrong
value on a multi-leaf tree" bugs, so it is covered heavily by tests that force
many groups across many small pages.

## Checkpointing

On checkpoint the persisted tree, the temporary tree, and every measure's own
trees are committed together as one atomic snapshot.

Committing the temporary tree is deliberate. A checkpoint can fall between
watermarks, with keys that were modified but not yet emitted; committing the
temp tree means those survive a restore and are emitted at the next watermark.
In the common case the temp tree is empty at checkpoint time, because a
watermark just drained and cleared it.

## Semantics notes

* `sum` over an all-null group is `null` (SQL semantics); `sum0` is the
  always-zero variant and emits a column-typed zero.
* `FILTER` is honoured by every measure kind.
* `DISTINCT` and `ORDER BY` are rejected at plan time for the aggregates that
  cannot honour them (e.g. `sum(DISTINCT …)`, `list_agg(… ORDER BY …)`), rather
  than being silently ignored. `count(DISTINCT …)` is supported, and
  `DISTINCT` on `min`/`max` is a no-op.
