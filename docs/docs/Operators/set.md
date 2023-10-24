---
sidebar_position: 5
---

# Set Operator

The *Set Operator* can take in 2->N inputs and will perform a set operation on the inputs. At this point only *UNION ALL* is supported by the set operator.
It is a stateful operator and has one B+ tree for each input.

## Metrics

The *Set Operator* has the following metrics:

| Metric Name   | Type      | Description                                           |
| ------------- | --------- | ----------------------------------------------------- |
| busy          | Gauge     | Value 0-1 on how busy the operator is.                |
| backpressure  | Gauge     | Value 0-1 on how much backpressure the operator has.  |
| health        | Gauge     | Value 0 or 1, if the operator is healthy or not.      |
| events        | Counter   | How many events that pass through the operator.       |

:::info

At this point, a set operator will never be unhealthy.
If there is a failure against the state, the stream will instead restart.

:::