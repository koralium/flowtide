---
slug: release-0-13-0
title: Release 0.13.0
tags: [release]
---

## Major changes

### New serializer to improve serialization speed
A new custom serializer has been implemented that follows the Apache Arrow serialization while minimizing extra allocations and memory copies.
Instead of using `Stream` it uses `IBufferWriter` to directly write to properly memory-aligned memory
to write to files.

Additionally, the default compression method was also changed from using ZLib to Zstd. 
This change was also made to improve serialization performance.

### Support for pause & resume

A new feature has been added to allow pausing and resuming data streams, making it easier to conduct maintenance or temporarily halt processing without losing state.

For more information, visit https://koralium.github.io/flowtide/docs/deployment/pauseresume.

### Integer column changed from 64 bits to dynamic size

The integer column was changed to now instead select the bit size based on the data inside of the column.
This change reduces memory usage for columns with smaller integer values. Bit size is determined on a per-page basis, so pages with larger values will only use higher bit sizes when necessary.

### Delta Lake Support

This version adds support to both read and write to the Delta Lake format. This allows easy integration
to data lake storage. To learn more about delta lake support, please visit: https://koralium.github.io/flowtide/docs/connectors/deltalake

### Custom data source & sink changed to use column based events

Both the custom data source and sink have now been changed to use column based events.
This improves connector performance by eliminating the need to convert data between column-based and row-based formats during streaming.

## Minor changes

### Elasticsearch connector change from Nest to Elastic.Clients.Elasticsearch

The Elasticsearch connector has been updated from the deprecated `Nest` package to `Elastic.Clients.Elasticsearch`. This change requires stream configurations to be adjusted for the new connection settings.

Additionally, connection settings are now provided via a function, enabling dynamic credential management, such as rolling passwords.

### Add support for custom stream listeners

Applications can now listen to stream events like checkpoints, state changes, and failures, allowing for custom exit strategies or monitoring logic.


Example:

```csharp
.AddCustomOptions(s =>
{
    s.WithExitProcessOnFailure();
});
```

### Cache lookup table for state clients

An internal optimization adds a small lookup table for state client page access, reducing contention on the global LRU cache. This change has shown a 10–12% performance improvement in benchmarks.