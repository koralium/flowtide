---
sidebar_position: 4
---

# Normalization Operator

The *Normalization Operator* is used mostly as a helper operator for ingestion of data.
It takes in a list of primary keys in which it will output always one row for. An input to the normalization operator does not need
to keep track of the differential data, but can instead send in updated values to a row, and the normalization operator will
handle the creation of the different delta operations needed to be sent out on the stream.

The properties it can take are:

* KeyIndex - an array of indices on which columns are for the primary key.
* Filter - a filter that reduces the output of the normalization operator.
* Input - the input operator that sends in data to this operator.

It is a stateful operator and uses a single B+ tree to perform its operations.

## Metrics

The *Normalization Operator* has the following metrics:

| Metric Name   | Type      | Description                                           |
| ------------- | --------- | ----------------------------------------------------- |
| busy          | Gauge     | Value 0-1 on how busy the operator is.                |
| backpressure  | Gauge     | Value 0-1 on how much backpressure the operator has.  |
| health        | Gauge     | Value 0 or 1, if the operator is healthy or not.      |
| events        | Counter   | How many events that pass through the operator.       |

:::info

At this point, a normalization operator will never be unhealthy.
If there is a failure against the state, the stream will instead restart.

:::