---
sidebar_position: 8
---

# Top N Operator

The *Top N Operator* implements the *Top N Relation* defined in [substrait](https://substrait.io/relations/physical_relations/#top-n-operation).
It returns the top N rows in a query based on user provided sort fields.
It stores all events in a B+ tree based on the giving ordering. For each event, it also has to check if the event is in the top, and if so
create negation event if another event is no longer in the top.

## Metrics

The *Top N Operator* has the following metrics:

| Metric Name   | Type      | Description                                           |
| ------------- | --------- | ----------------------------------------------------- |
| busy          | Gauge     | Value 0-1 on how busy the operator is.                |
| backpressure  | Gauge     | Value 0-1 on how much backpressure the operator has.  |
| health        | Gauge     | Value 0 or 1, if the operator is healthy or not.      |
| events        | Counter   | How many events the operator outputs.                 |

