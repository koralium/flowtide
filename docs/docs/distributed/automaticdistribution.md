---
sidebar_position: 2
---

# Automatic Distribution

> [!WARNING]
> Distributed mode is still experimental.

Automatic distribution takes a completely normal plan, without any distribution hints, and splits it into a chosen number of substreams. This is the recommended way to use distributed mode.

```sql
INSERT INTO output
SELECT u.userkey FROM users u
INNER JOIN orders o ON u.userkey = o.userkey;
```

Run with a substream count of two, the join above runs in both substreams. Users and orders rows are scattered between the substreams by hashing on *userkey*, each substream joins its own share, and the results are gathered back to the substream that runs the sink.

The split happens when the plan is optimized. The [in-process host](inprocess.md) and the Orleans *substreamCount* option do this automatically. It can also be done manually through the plan optimizer:

```csharp
var distributedPlan = PlanOptimizer.Optimize(plan, new PlanOptimizerSettings
{
    DistributedPlanOptions = new DistributedPlanOptions
    {
        SubstreamCount = 2
    }
});
```

## What is distributed

* **Joins** are partitioned by hashing on the join keys that are compared with equality. A join that mixes equality and other conditions, for example `ON a.id = b.id AND a.start < b.ts`, is partitioned on the equality keys only.
* **Aggregates** with group by are partitioned on the grouping columns.
* **Window functions** are partitioned on their partition columns.

## Pushdown

Operators above a distributed operator are pushed down into the partitions when they can run per partition:

* Filters and projections are always pushed down, they work row by row.
* Aggregates and window functions are pushed down when they group on columns the data is already partitioned on. For instance a *GROUP BY* on the join key runs both the join and the aggregate inside every substream, only the aggregated results cross to the sink substream.

Filters that are pushed down run before the data is shuffled between substreams, which reduces how much data crosses the exchange.

## What stays in one substream

* Recursive queries, the whole pipeline that contains one stays together.
* Joins without any equality condition, they can not be co-partitioned.
* Aggregates using the surrogate key function.
