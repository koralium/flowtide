---
sidebar_position: 6
---

# Aggregate Operator

The *Aggregate Operator* implements the *Substrait* [Aggregate Relation](https://substrait.io/relations/logical_relations/#aggregate-operation).
This handles computation of *aggregate functions* and groupings.

At this time not all features on the *aggregate relation* is supported, and some features are limited.

* Measures does not support filters yet, such as SUM(c) FILTER(WHERE...)
* When doing groupings, such as 'GROUP BY', only one grouping set is supported (you can group multiple columns in a single grouping set).

The operator is stateful and uses one persistent B+ tree to store all the measures state values, and one temporary B+ tree to keep track of
which keys have been modified.

New values are only sent out on watermark updates, this is done to reduce the amount of traffic on the stream, and allow the state of each measure
to be updated in-place, to reduce memory allocation.


## Metrics

The *Aggregate Operator* has the following metrics:

| Metric Name   | Type      | Description                                           |
| ------------- | --------- | ----------------------------------------------------- |
| busy          | Gauge     | Value 0-1 on how busy the operator is.                |
| backpressure  | Gauge     | Value 0-1 on how much backpressure the operator has.  |
| health        | Gauge     | Value 0 or 1, if the operator is healthy or not.      |

:::info

At this point, an aggregate operator will never be unhealthy.
If there is a failure against the state, the stream will instead restart.

:::