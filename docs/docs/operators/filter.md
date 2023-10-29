---
sidebar_position: 2
---

# Filter Operator

The *Filter Operator* implements the *Filter relation* defined in [substrait](https://substrait.io/relations/logical_relations/#filter-operation). The *Filter operator* is stateless and does not need to store any state.

As defined in substrait, it takes in two properties, *input* which is the operator that feeds the filter with data, and a filter expression that will filter events 
out from the stream. It also implements the common *Emit* field which allows a user to reduce the amount of data outputted by the operator.

## Metrics

The *Projection Operator* has the following metrics:

| Metric Name   | Type      | Description                                           |
| ------------- | --------- | ----------------------------------------------------- |
| busy          | Gauge     | Value 0-1 on how busy the operator is.                |
| backpressure  | Gauge     | Value 0-1 on how much backpressure the operator has.  |
| health        | Gauge     | Value 0 or 1, if the operator is healthy or not.      |

:::info

At this point, a filter operator will never be unhealthy.

:::