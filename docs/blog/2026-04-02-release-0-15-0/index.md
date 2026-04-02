---
slug: release-0-15-0
title: Release 0.15.0
tags: [release]
---

## Major changes

### Reservoir — New Storage Backend

A brand-new storage backend called **Reservoir** has been introduced and is now the **recommended** storage solution for all deployments.
Reservoir is designed for cloud-native workloads and provides a multi-tier architecture:

* **In-Flight Buffer** — volatile RAM layer for Read-Your-Own-Writes consistency.
* **Local Disk Cache** — immutable, CRC-verified cache of 64 MB blobs fetched on-demand from remote storage.
* **Persistent Storage** — authoritative state in cloud object storage (Azure Blob Storage, local disk, etc.).

Key design highlights include page packing into large immutable files to minimize cloud API calls, CRC32/CRC64 checksums at page and file level for data integrity, snapshot checkpoints for fast recovery, and automatic compaction of fragmented files.

Legacy storage backends (FasterKV, temporary file cache, SQL Server) remain available but are no longer recommended for new deployments. FasterKV has been moved to its own NuGet package (`FlowtideDotNet.Storage.FasterKV`) and is no longer included by default.

To learn more, visit [State Persistence docs](https://koralium.github.io/flowtide/docs/statepersistence).

### StarRocks Connector

A new connector for [StarRocks](https://www.starrocks.io/) has been added, starting with a **sink operator**. The StarRocks sink writes data using the built-in Stream Load mechanism and requires the target table to have a primary key defined for upsert and delete support.

Two execution modes are available:
* **OnCheckpoint** — data is committed transactionally at checkpoint boundaries.
* **OnWatermark** — data is flushed on each watermark for lower latency (at-least-once semantics).

To learn more, visit [StarRocks Connector docs](https://koralium.github.io/flowtide/docs/connectors/starrocks).

### OpenLineage Support

Flowtide now has built-in support for reporting data lineage events to an [OpenLineage](https://openlineage.io/)-compatible endpoint over HTTP. When enabled, the reporter automatically sends lineage events as the stream transitions between states (starting, running, completed, or failed). Schema information can optionally be included in the events.

To learn more, visit [OpenLineage docs](https://koralium.github.io/flowtide/docs/monitoring/openlineage).

### INSERT OVERWRITE for Delta Lake

The Delta Lake connector now supports `INSERT OVERWRITE {tableName}` as an alternative to `INSERT INTO`. This replaces all content in the delta table with the new data from the stream on the initial load. After the initial overwrite, subsequent inserts, updates, and deletes behave normally. The existing data is committed as `remove` operations in the delta log, so the full table history is preserved.

Additionally, **column mapping** is now supported when writing to delta tables.

### SpiceDB `materialize_permission` Table Function

The SpiceDB connector has been refactored and now exposes a `materialize_permission` SQL table function for easily denormalizing permissions directly in your queries. The function automatically fetches the schema from SpiceDB at plan-build time, removing the need for manual schema loading.

```sql
SELECT subject_type, subject_id, relation, resource_type, resource_id
FROM spicedb.materialize_permission('document', 'view')
```

To learn more, visit [SpiceDB Connector docs](https://koralium.github.io/flowtide/docs/connectors/spicedb).

### .NET 10 Support

Flowtide now targets **.NET 10** in addition to .NET 8, allowing projects on the latest runtime to benefit from the newest performance improvements and language features.

## Minor changes

### New SQL Functions

Several new scalar and aggregate functions have been added:

* **`datediff`** — calculates the difference between two dates in a specified date part.
* **`round_calendar`** — rounds a timestamp to a calendar boundary (e.g., start of hour, day, month).
* **`xxhash64`** — computes an xxHash64 hash of input values.
* **`log10`** — computes the base-10 logarithm.
* **`count_distinct`** — aggregate function that counts distinct values.
* **`avg` (window function)** — average is now available as a window function. A bug with using window functions inside recursive CTEs has also been fixed.

### Generic Data Sink: Fetch Existing Data

The generic data sink now allows fetching existing data from the target on initial startup. This is used to compare against incoming data to avoid duplicates and to produce deletes for data that is no longer present in the stream result.

### SharePoint Source Refactored to Column-Based Data

The SharePoint source has been rebuilt to use column-based data and now supports resync operations.

### JsonElement Support in C# Object Converter

The C# object converter now supports `JsonElement` as a type, allowing direct mapping of JSON data when converting objects to columns.

### Precompiled Frontend

The ASP.NET Core monitoring UI now ships with a precompiled frontend. **Node.js is no longer required** to build the project.

### Kafka Improvements

* Tombstone events are now skipped during initial load when loading existing data from Kafka.
* The Kafka sink can once again handle deleted rows during initial batch processing.

### Delta Lake Source Fixes

* The delta lake source can now read tables that only have a checkpoint as the first entry.
* A fix ensures correct difference detection in column batch reads.

### Memory Optimization for Window Functions

Memory allocation on the managed heap has been reduced for `MIN`/`MAX` window functions, decreasing GC pressure.

### Bug Fixes

* **Union column insert range** — fixed incorrect count calculation when null values are present in the range.
* **C# Object Sink** — fixed an exception when removing a row with a non-nullable field.
* **SQL case sensitivity** — single identifiers in SQL are no longer case sensitive.
* **Surrogate key / upper metadata** — `upper` and surrogate key `int64` now return correct types for metadata.
* **Plan serialization** — `list` and `map` types now serialize correctly to JSON when serializing the plan.
* **Delta load task reset** — fixed an issue where the delta load task was not reset when a full load followed a delta load.
