---
sidebar_position: 0
---

# General Metrics

This section describes different general metrics that are exposed by Flowtide.
For operator specific metrics, please check under [operators](/docs/category/operators).

All metrics in flowtide are prefixed with `flowtide_`.

## Standard labels

| Label Name    | Scope     | Description                                                                 |
| ------------- | --------- | --------------------------------------------------------------------------- |
| stream        | Stream    | Name of the stream, exist on all metrics for a stream.                      |
| operator      | Operator  | Id of an operator, exist on all metrics that are specific for an operator   |

## `flowtide_state` {#flowtide_state}

* Scope: Stream
* Type: Gauge

Contains the current state of the stream, only contains the stream label, the value is an integer that follows the
stream state enum.

Value table:

| Value  | Name           |  Description                                                                      |
| ------ | -------------- | --------------------------------------------------------------------------------- |
| 0      | NotStarted     | The stream has either never started or has been stopped.                          |
| 1      | Starting       | The stream is starting up, occurs from not started or from a failure state.       |
| 2      | Running        | The stream is running normally.                                                   |
| 3      | Failure        | A failure has happened and the stream will try and recover.                       |
| 4      | Deleting       | The stream is currently deleting itself.                                          |
| 5      | Deleted        | The stream has finished deleting itself.                                          |
| 6      | Stopping       | The stream is currently being stopped.                                            |

## `flowtide_wanted_state`

* Scope: Stream
* Type: Gauge

Contains the wanted state of the stream, as an example, when one stops a stream, the wanted state is `NotStarted`,
but the current state might be `Stopping`. Uses the same value table as described in [flowtide_state](#flowtide_state).

## `flowtide_health`

* Scope: Stream and Operator
* Type: Gauge

The health of both the stream and operators, if no operator label exist, it is the overal stream health.
The value can be '0' (Unhealthy), '0.5' (Degraded) and '1' (Healthy).

## `flowtide_metadata`

* Scope: Operator
* Type: Gauge

The metadata metric only exposes labels and the value can be ignored. It exists on all operators.

| Label Name    | Example        |  Description                                                                |
| ------------- | -------------- | --------------------------------------------------------------------------- |
| links         | [1, 2]         | JSON array of operator ids that this operator sends it output to.           |
| title         | Merge Join     | Title/Display name of the operator.                                         |

## `flowtide_link`

* Scope: Operator
* Type: Gauge

Describes an output flow from one operator to another, each output in an operator has its own metric series.
Same as in metadata, the value is not used, and this is only described by its labels.

| Label Name     | Example        |  Description                                                                          |
| -------------- | -------------- | ------------------------------------------------------------------------------------- |
| source         | 1              | Id of the operator that the link originates from (same as 'operator' label)           |
| target         | 2              | Id of the operator data flows to.                                                     |
| id             | 1-2            | Unique id of a link, to more easily identify new links if required.                   |

## `flowtide_memory_allocated_bytes`

* Scope: Operator
* Type: Counter

Contains a value on how much unmanaged memory an operator has allocated. This value will never decrease, to calculate the current usage
it must be subtracted by `flowtide_memory_freed_bytes`.

## `flowtide_memory_freed_bytes`

* Scope: Operator
* Type: Counter

Contains a value on how much unmanaged memory an operator has freed. The value will never decrease. Is usually used together
with `flowtide_memory_allocated_bytes` to calculate current memory usage.

## `flowtide_memory_allocation_count`

* Scope: Operator
* Type: Counter

A counter that contains how many allocation operations has been done by an operator. Can be used together with 
`flowtide_memory_allocated_bytes` to calculate average size allocations as an example.

## `flowtide_memory_free_count`

* Scope: Operator
* Type: Counter

A counter that contains how many free operations has been done by an operator. Can be used together with 
`flowtide_memory_allocation_count` to get how many current allocations exist for an operator.

## `flowtide_lru_table_cache_tries`

* Scope: Stream
* Type: Counter

A counter that contains how many get operations have occured against the LRU cache. Used with `flowtide_lru_table_cache_hits`
and `flowtide_lru_table_cache_misses` to calculate hit and miss percentages.

## `flowtide_lru_table_cache_misses`

* Scope: Stream
* Type: Counter

Contains a value on how many cache misses have occured against the LRU cache.

## `flowtide_lru_table_cache_hits`

* Scope: Stream
* Type: Counter

Contains a value on how many cache hits have occured against the LRU cache.

## `flowtide_lru_table_max_size`

* Scope: Stream
* Type: Gauge

Contains the value of the maximum amount of pages that can exist in the LRU cache, if this value is reached, the stream will
halt until pages have been offloaded to disk.

## `flowtide_lru_table_size`

* Scope: Stream
* Type: Gauge

Contains the current amount of pages in the LRU cache.

## `flowtide_lru_table_cleanup_start`

* Scope: Stream
* Type: Gauge

Contains the value where offloading to disk will occur. Before this value no pages will be written to disk.

## `flowtide_temporary_write_ms`

* Scope: Operator
* Type: Histogram

A histogram that describes how long it takes to write to temporary storage from LRU cache.

## `flowtide_temporary_read_ms`

* Scope: Operator
* Type: Histogram

A histogram that describes how long it takes to read from temporary storage into LRU cache.

## `flowtide_persistence_read_ms`

* Scope: Operator
* Type: Histogram

A histogram that describes how long it takes to read from persistent storage into LRU cache.

## `flowtide_latency`

* Scope: Operator
* Type: Histogram

Histogram that describes how long time it takes for a watermark to traverse the stream. Each egress point
has a histogram per source operator that produces watermarks. This allows a developer to gain
understanding how quickly events can be resolved in the stream.
