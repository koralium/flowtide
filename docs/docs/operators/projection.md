---
sidebar_position: 1
---

# Projection Operator

The *Projection Operator* implements the *Project relation* defined in [substrait](https://substrait.io/relations/logical_relations/#project-operation). The *Projection operator* is stateless and does not need to store any state.

As defined in substrait, it takes in two properties, *input* which is the operator that feeds the projection, and an array of expressions which defines
how data should be projected. It also implements the common *Emit* field which allows a user to reduce the amount of data outputted by the operator.
All expressions defined in the array will have an emit index offset by the input length.

## Metrics

The *Projection Operator* has the following metrics:

| Metric Name   | Type      | Description                                           |
| ------------- | --------- | ----------------------------------------------------- |
| busy          | Gauge     | Value 0-1 on how busy the operator is.                |
| backpressure  | Gauge     | Value 0-1 on how much backpressure the operator has.  |
| health        | Gauge     | Value 0 or 1, if the operator is healthy or not.      |
| events        | Counter   | How many events that pass through the operator.       |

:::info

At this point, a projection operator will never be unhealthy.

:::