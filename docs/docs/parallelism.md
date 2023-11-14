---
sidebar_position: 3
---


# Parallelism

:::warning

Parallelism support is still experimental.

:::

Some operators support running in parallel. This is done by partitioning the input into an operator. For instance in an aggregate with a grouping.
It can partition the data on the grouping values.

This can help reduce bottlenecks on certain operators that do complex operations.

To run a stream with parallelism enabled, add the following to the *FlowtideBuilder*:

```csharp
flowtideBuilder
    // Set an integer number here
    .SetParallelism(parallelism)
```