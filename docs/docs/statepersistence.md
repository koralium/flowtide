---
sidebar_position: 2
---

# State Persistence

:::info

The settings in this page does not cover when setting up Flowtide using Dependency Injection.
These settings can be applied when setting up a stream using *FlowtideBuilder* class.

:::

All Flowtide streams require a persistent storage solution to function.
It is responsible for persisting the data at checkpoint intervals to ensure the stream can continue to operate in case of a failure.

At this time there are two different implementations for the persistent storage solution, FasterKV backed, and a temporary file cache solution.

## FasterKV storage

FasterKV is persistent key value store built by Microsoft. It is the only storage solution available for Flowtide that will persist data between runs.
FasterKV is highly configurable, and how you configure it will affect the performance of your stream.

To configure your stream to use FasterKV storage, add the following to the builder:

```csharp
builder
.WithStateOptions(() => new StateManagerOptions()
{
    PersistentStorage = new FasterKvPersistentStorage(new FasterKVSettings<long, SpanByte>()
    {
        // Set the fasterKV configuration here
        ...
    })
});
```

### Useful configuration options

| Property          | Description                                           |
| ----------------- | ----------------------------------------------------- |
| LogDevice         | The log device that will write to storage             |
| MemorySize        | How much memory FasterKV can use                      |
| PageSize          | how large a page is                                   |
| CheckpointDir     | Where checkpoints should be stored                    |
| CheckpointManager | Checkpoint manager, useful if using Azure Storage.    |

### Storing to disk

This is an example of a configuration to store to a disk.

```csharp
var baseDirectory = "/persistence/"
builder.WithStateOptions(() => new StateManagerOptions()
{
    // Set cache page count to reduce the memory usage
    CachePageCount = 10000,
    PersistentStorage = new FasterKvPersistentStorage(new FasterKVSettings<long, SpanByte>()
    {
        // Checkpoint directory
        CheckpointDir = $"{baseDirectory}/checkpoints",
        // A local file log device
        LogDevice = Devices.CreateLogDevice($"{baseDirectory}/log"),
        // Redice memory usage of fasterKV, to limit memory usage
        MemorySize =  1024L * 1024L * 64,
        // Page size
        PageSize = 1024 * 1024 * 16,
    }),
    TemporaryStorageOptions = new FileCacheOptions()
    {
        // Path where the temporary cache is stored
        DirectoryPath = $"./temp"
    }
})
```

### Storing to Azure Storage

Storing the data in an Azure Storage requires a bit more configuration, especially a checkpoint manager.

```csharp
// Create azure storage device
var log = new AzureStorageDevice(STORAGE_STRING, BASE_CONTAINER, "", "hlog.log");

// Create azure storage backed checkpoint manager
var checkpointManager = new DeviceLogCommitCheckpointManager(
                new AzureStorageNamedDeviceFactory(STORAGE_STRING),
                new DefaultCheckpointNamingScheme($"{BASE_CONTAINER}/checkpoints/"));

builder.WithStateOptions(() => new StateManagerOptions()
{
    // Set cache page count to reduce the memory usage
    CachePageCount = 10000,
    PersistentStorage = new FasterKvPersistentStorage(new FasterKVSettings<long, SpanByte>()
    {
        CheckpointManager = checkpointManager,
        LogDevice = log,
        // Redice memory usage of fasterKV, to limit memory usage
        MemorySize =  1024L * 1024L * 64,
        // Page size
        PageSize = 1024 * 1024 * 16,
    }),
    TemporaryStorageOptions = new FileCacheOptions()
    {
        // Path where the temporary cache is stored
        DirectoryPath = $"./temp"
    }
})
```

## Temporary file cache storage

This storage solution is useful when developing or running unit tests on a stream.
All data will be cleared between each run, but it will be persisted to local disk to reduce RAM usage and allow you to run streams with alot of data.

The implementation of this is using the same solution as the intermediate file cache solution where modified pages are stored between checkpoints.

To configure your stream to use this storage solution, add the following to the stream builder:

```csharp
builder
.WithStateOptions(() => new StateManagerOptions()
{
    // This is non persistent storage, use FasterKV persistence storage instead if you want persistent storage
    PersistentStorage = new FileCachePersistentStorage(new FlowtideDotNet.Storage.FileCacheOptions()
    {
        DirectoryPath = "./tmp"
    })
});
```

### Configuration

| Property      | Default value         | Description                         | 
| ------------- | --------------------- | ----------------------------------- |
| DirectoryPath | ./data/tempFiles      | Path where the files will be stored |



## Storage solution

The stream storage is built on a three tier architecture, there is the in memory cache, the local disk modified page cache, and the persistent data.

A data page is fetched using the following logic:

```kroki type=blockdiag
  blockdiag {
    IsInMemory [label = "Page is in memory", shape = diamond, width = 200]
    FetchFromMemory [label = "Fetch from memory"]
    IsInModifiedCache [label = "Page in modified cache", shape = diamond, width = 250]
    FetchFromModifiedCache [label = "Fetch from modified cache", width = 200]
    FetchFrompersistentStorage [label = "Fetch from persistent storage", width = 200]
    IsInMemory -> FetchFromMemory [label = "Yes"]
    IsInMemory -> IsInModifiedCache [label = "No"]
    IsInModifiedCache -> FetchFromModifiedCache [label = "Yes"]
    IsInModifiedCache -> FetchFrompersistentStorage [label = "No"]
  }
```

## Compression

It is possible to compress pages in the state.
This is done by providing two functions to state serialize options, a compress function and a decompress function.

Example using ZLib compression:

```csharp
builder.WithStateOptions(() => new StateManagerOptions()
{
    ...
    SerializeOptions = new StateSerializeOptions()
    {
        CompressFunc = (stream) =>
        {
            return new System.IO.Compression.ZLibStream(stream, CompressionMode.Compress);
        },
        DecompressFunc = (stream) =>
        {
            return new System.IO.Compression.ZLibStream(stream, CompressionMode.Decompress);
        }
    }
})
```