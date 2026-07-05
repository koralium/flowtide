# SQL Substream Statements

> [!WARNING]
> Distributed mode is still experimental.

Substream statements give manual control over which SQL statements run in which substream. For most cases [automatic distribution](automaticdistribution.md) is simpler, manual substreams are useful when the placement of specific pipelines matters.

A `SUBSTREAM` statement assigns every statement after it to the named substream:

```sql
SUBSTREAM sub1;

CREATE VIEW read_users WITH (DISTRIBUTED = true, SCATTER_BY = userkey, PARTITION_COUNT = 2) AS
SELECT userkey FROM users;

INSERT INTO output SELECT userkey FROM read_users WITH (PARTITION_ID = 0);

SUBSTREAM sub2;

INSERT INTO output SELECT userkey FROM read_users WITH (PARTITION_ID = 1);
```

## Distributed views

A view created with `DISTRIBUTED = true` becomes an exchange point whose output other substreams can consume:

* `SCATTER_BY` names the column whose hash decides the partition a row belongs to. It is required when the view is consumed from another substream.
* `PARTITION_COUNT` sets how many partitions the view output is split into.
* A consumer can pick specific partitions with `WITH (PARTITION_ID = n)`, without the hint it receives all partitions.

## Rules

* Once `SUBSTREAM` is used, **every** `INSERT` must be inside a substream. A top level insert would run in every substream and duplicate its output, so mixed plans are rejected when the plan is built.
* A distributed view consumed from another substream must declare `SCATTER_BY`. Without it the view would have to be broadcast across substreams, which is not supported, and the plan build fails with an error saying so.
* Recursive queries cannot span substreams.
