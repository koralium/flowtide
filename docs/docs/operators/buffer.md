---
sidebar_position: 7
---

# Buffer Operator

The buffer operator collects all events that happens in between watermarks and stores them in a temporary tree.
This is useful if a row is updated where no values are changed and you do not wish it to be propogated to the rest of the stream.

The buffer operator has no properties.

## Metrics

The *Buffer Operator* has the following metrics:

| Metric Name   | Type      | Description                                           |
| ------------- | --------- | ----------------------------------------------------- |
| busy          | Gauge     | Value 0-1 on how busy the operator is.                |
| backpressure  | Gauge     | Value 0-1 on how much backpressure the operator has.  |
| health        | Gauge     | Value 0 or 1, if the operator is healthy or not.      |
| events        | Counter   | How many events that the operator outputs.            |