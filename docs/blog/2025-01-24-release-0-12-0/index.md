---
slug: release-0-12-0
title: Release 0.12.0
tags: [release]
---

## Major changes

### All Processing Operators Updated to Column-Based Events

All processing operators now use the column-based event format, leading to better performance. 
However, some sources and sinks for connectors still use the row-based event format. 
Additionally, a few functions continue to rely on the row-based event format.

### MongoDB Source Support

This release adds support to read data from MongoDB, this includes using 
MongoDBs change stream to directly react on data changes.

### SQL Server Support for Stream State Persistence

You can now store the stream state in SQL Server. For setup instructions, refer to the documentation:
https://koralium.github.io/flowtide/docs/statepersistence#sql-server-storage

### Timestamp with Time Zone Data Type

A new data type for timestamps has been added. 
This ensures that connectors can correctly use the appropriate data type, especially when writing. 
For example, writing to MongoDB now uses the BSON Date type.


## Minor Changes

### Virtual Table Support

Static data selection is now supported. Example usage:

```
INSERT INTO output 
SELECT * FROM 
(
  VALUES 
  (1, 'a'),
  (2, 'b'),
  (3, 'c')
)
```
