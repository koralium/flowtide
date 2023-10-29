---
sidebar_position: 3
---

# Join Operators

Join in Flowtide is implemented with two different operators, a *block-nested join operator*, and a *merge-join operator*.
Both operators can be in use, by using the generic [substrait join relation](https://substrait.io/relations/logical_relations/#join-operation).
The plan optimizer will select which operator should be in use based on the join condition. It is also possible to use the [merge-equijoin-operator](https://substrait.io/relations/physical_relations/#merge-equijoin-operator) defined in substrait, and the merge join operator will be in use without any optmization.

At this point, the *merge-join operator* will only be used if the condition contains a condition that defines an equality between the two inputs. Example:

* **left.col1 = right.col2** - Will result in a merge-join.
* **left.col1 = right.col2 AND left.col2 < right.col3** - Will result in a merge-join.
* **left.col1 = right.col2 OR left.col2 < right.col3** - Will result in a block-nested join.
* **left.col2 < right.col3** - Will result in a block-nested join.

The *merge-join* is highly efficient if compared to the *block-nested join*, so it is advicable to always try to have a join condition with an equality expression.

## Merge-Join Operator

The *merge-join operator* is a stateful operator that is implemented by two different B+ trees, one for each input source.
The trees are sorted based on the keys used in the equality condition.

### Metrics

| Metric Name   | Type      | Description                                           |
| ------------- | --------- | ----------------------------------------------------- |
| busy          | Gauge     | Value 0-1 on how busy the operator is.                |
| backpressure  | Gauge     | Value 0-1 on how much backpressure the operator has.  |
| health        | Gauge     | Value 0 or 1, if the operator is healthy or not.      |
| events        | Counter   | How many events that pass through the operator.       |

:::info

At this point, a merge-join operator will never be unhealthy.

:::

## Block-Nested Join Operator

The *block-nested join operator* is a stateful operator that is implemented using 2 persistent B+ trees, and two temporary B+ trees.
The temporary trees fill up with data until a watermark is recieved in which they it performs the join operations.
It does this to reduce the amount of I/O that has to be made when reading through the entire persisted dataset.

### Metrics

| Metric Name   | Type      | Description                                           |
| ------------- | --------- | ----------------------------------------------------- |
| busy          | Gauge     | Value 0-1 on how busy the operator is.                |
| backpressure  | Gauge     | Value 0-1 on how much backpressure the operator has.  |
| health        | Gauge     | Value 0 or 1, if the operator is healthy or not.      |

:::info

At this point, a block-nested join operator will never be unhealthy.
If there is a failure against the state, the stream will instead restart.

:::