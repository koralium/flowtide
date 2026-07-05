# Automatic Distribution

> [!WARNING]
> Distributed mode is still experimental.

Automatic distribution takes a completely normal plan, without any distribution hints, and splits it into a chosen number of substreams. It is the recommended way to use distributed mode.

```sql
INSERT INTO output
SELECT u.userkey FROM users u
INNER JOIN orders o ON u.userkey = o.userkey;
```

Run with a substream count of two, the join above runs with one partition copy in each substream. Users and orders rows are scattered between the substreams by hashing on `userkey`, each substream joins its share, and the results are gathered back to the substream that runs the sink.

How the plan is split is decided when the plan is optimized. With the [in-process host](inprocess.md) and the Orleans `substreamCount` option this happens automatically. It can also be applied manually through the plan optimizer:

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

* **Joins** get one partition copy per substream, partitioned by hashing the join keys that are compared with equality. A join that mixes equality and inequality conditions, for example `ON a.id = b.id AND a.start < b.ts`, is partitioned on the equality keys only.
* **Aggregates** with groupings get one partition copy per substream, partitioned on the grouping columns.
* **Window functions** are partitioned on their partition columns.

## Lane pushdown

Operators above a distributed operator are pushed down into the partition lanes when they are safe to run per partition:

* Filters and projections are always pushed down, they operate row by row.
* Aggregates and window functions are pushed down when their grouping or partition columns cover the columns the lane is already partitioned on. `GROUP BY` on the join key over a join, for example, runs the whole join-then-aggregate pipeline inside every substream, and only the aggregated results cross to the sink substream.

Pushing filters down means they run before data is shuffled between substreams, which reduces the amount of data crossing the exchange.

## What stays in one substream

* Recursive queries (iterations). The whole pipeline containing an iteration stays together.
* Joins where no key is compared with equality, they cannot be co-partitioned.
* Aggregates using the surrogate key function.
