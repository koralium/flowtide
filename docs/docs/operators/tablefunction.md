---
sidebar_position: 9
---

# Table Function Operator

The table function operator(s) execute different table functions.
It actually consists of two different operators.

One that is used with an input relation, which is used in cases when the function has an argument that is dependent of data.
The other operator is an ingress operator when the table function is used in a 'FROM' statement or similar in SQL.
When used as an ingress, the data returned is static and wont be changed over time.

Both operators are stateless, so they do not affect disk storage, and are usually low on memory usage.

To read more how to use table functions, see: [Table Functions](/docs/expressions/tablefunctions).

## Metrics

The *Table Function Operators* have the following metrics:

| Metric Name       | Type      | Description                                           |
| ----------------- | --------- | ----------------------------------------------------- |
| busy              | Gauge     | Value 0-1 on how busy the operator is.                |
| backpressure      | Gauge     | Value 0-1 on how much backpressure the operator has.  |
| health            | Gauge     | Value 0 or 1, if the operator is healthy or not.      |
| events            | Counter   | How many events that the operator outputs.            |
| events_processed  | Counter   | How many events the operator processes.               |

:::info

At this point, a table function operator will never be unhealthy.

:::