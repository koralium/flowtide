---
sidebar_position: 5
---

# Set Operator

The *Set Operator* can take in 2->N inputs and will perform a set operation on the inputs. 
It is a stateful operator for all operations except *UnionAll*, for the others one B+ tree is used to store
the weights from the different inputs.

The following operations are supported:

* UnionAll
* UnionDistinct
* MinusPrimary (EXCEPT DISTINCT)
* MinusPrimaryAll (EXCEPT ALL)
* IntersectionMultiset (INTERSECT DISTINCT)
* IntersectionMultisetAll (INTERSECT ALL)

## Metrics

The *Set Operator* has the following metrics:

| Metric Name               | Type      | Description                                           |
| ------------------------- | --------- | ----------------------------------------------------- |
| busy                      | Gauge     | Value 0-1 on how busy the operator is.                |
| backpressure              | Gauge     | Value 0-1 on how much backpressure the operator has.  |
| health                    | Gauge     | Value 0 or 1, if the operator is healthy or not.      |
| events                    | Counter   | How many events that the operator outputs.            |
| events_processed          | Counter   | How many events that the operator processes.          |

:::info

At this point, a set operator will never be unhealthy.
If there is a failure against the state, the stream will instead restart.

:::