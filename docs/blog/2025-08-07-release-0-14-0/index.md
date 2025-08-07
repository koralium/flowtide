---
slug: release-0-14-0
title: Release 0.14.0
tags: [release]
---

## Major changes

### Window functions support

Flowtide now support SQL window functions such as `ROW_NUMBER`, `SUM`, `LEAD`, `LAG`.
This is a quite new feature and any feedback would be highly appriciated.

To learn which functions are supported please visit [docs](https://koralium.github.io/flowtide/docs/expressions/windowfunctions).

### Qdrant Connector

A connector for Qdrant has been added to make it easy to integrate data to be used by AI or other tools that rely on a vector database. This connector allows easy near real-time synchronization of data used by Qdrant.

To learn more how to use the Qdrant connector please visit [docs](https://koralium.github.io/flowtide/docs/connectors/qdrant).

### New test framework

A new framework has been released called `FlowtideDotNet.TestFramework` to help simplify the process of doing integration testing of streams.

To learn more please visit [docs](https://koralium.github.io/flowtide/docs/testing).

## Minor changes

### SQL Server Connector Changes

The SQL Server connector has been rebuilt to support tables without change tracking (including views). This works by polling all data, so it should not be used in large tables, but can give a quick start to experiment with a tables data before enabling change tracking.

Optimizations has been added for partitioned tables, to give quick selects from large tables where each partition is fetched individually to reduce the cost of cross-partition queries. 