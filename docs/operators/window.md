---
sidebar_position: 9
---

# Window Operator

The **Window Operator** implements the *Consistent Partition Window* relation defined in [Substrait](https://substrait.io/relations/physical_relations/#consistent-partition-window-operation).

This operator supports **only one window function per instance**. To apply multiple window functions, chain multiple Window Operators sequentially.

The operator is **stateful** and relies on two B+ trees:

- **Persistent Tree** – Stores all rows in the dataset, ordered by partition columns followed by order-by columns.
- **Temporary Partition Tree** – Tracks which partitions have been updated (not persisted).

## Behavior

When events are received:

- Rows are inserted into the Persistent Tree.
- If a row is deleted (i.e., an event with negative weight), that row's negative output is sent downstream immediately.
- The partition key is calculated and recorded in the Temporary Partition Tree.

For **upsert operations**, the row is held until a watermark is received, at which point the final calculation is performed.

When a **watermark event** is received, the operator:

- Iterates over all changed partitions.
- Applies the configured window function.

The logic for how values are calculated within a partition is delegated to the specific window function implementation.

## Metrics

The Window Operator exposes the following metrics:

| Metric Name       | Type      | Description                                           |
| ----------------- | --------- | ----------------------------------------------------- |
| busy              | Gauge     | Value 0-1 on how busy the operator is.                |
| backpressure      | Gauge     | Value 0-1 on how much backpressure the operator has.  |
| health            | Gauge     | Value 0 or 1, if the operator is healthy or not.      |
| events            | Counter   | How many events the operator outputs.                 |
| events_processed  | Counter   | How many events the operator recieves.                |

