---
sidebar_position: 2
---

# State Persistence

All Flowtide streams require a persistent storage solution to function.
It is responsible for persisting the data at checkpoint intervals to ensure the stream can continue to operate in case of a failure.

The recommended storage backend is the **Reservoir** storage system. It is designed for cloud-native deployments and provides a multi-tier architecture with an in-flight buffer, local disk cache, and persistent object storage (such as Azure Blob Storage). For details on the internal architecture, see [Reservoir Storage System](./internal/reservoir).

Legacy storage backends (FasterKV, temporary file cache, SQL Server) are still available but are not recommended for new deployments.

## Reservoir Storage

Reservoir storage packs pages into large immutable files (default 64 MB), uses CRC32/CRC64 checksums for data integrity, and maintains a local disk cache for near-SSD read latency while the authoritative state lives in cloud object storage.

### Azure Blob Storage

Store persistent state to Azure Blob Storage. This is the recommended configuration for production deployments.

Install the `FlowtideDotNet.Storage.AzureBlobs` NuGet package and use one of the `AddAzureBlobStorage` extension methods:

```csharp
builder.Services.AddFlowtideStream("yourstream")
    // ...
    .AddStorage(s =>
    {
        // Using a connection string
        s.AddAzureBlobStorage(
            connectionString: "DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net",
            containerName: "flowtide-state",
            directoryPath: "mystream",
            localCacheDirectory: "/cache/flowtide"
        );
    });
```

Other authentication methods are also supported:

```csharp
// Using a TokenCredential (e.g., DefaultAzureCredential for managed identity)
s.AddAzureBlobStorage(
    storageUri: new Uri("https://myaccount.blob.core.windows.net"),
    containerName: "flowtide-state",
    tokenCredential: new DefaultAzureCredential(),
    directoryPath: "mystream"
);

// Using a SAS credential
s.AddAzureBlobStorage(
    storageUri: new Uri("https://myaccount.blob.core.windows.net"),
    containerName: "flowtide-state",
    sasCredential: new AzureSasCredential("sv=..."),
    directoryPath: "mystream"
);

// Using an options delegate for full control
s.AddAzureBlobStorage(opt =>
{
    opt.ConnectionString = "...";
    opt.ContainerName = "flowtide-state";
    opt.DirectoryPath = "mystream";
    opt.LocalCacheDirectory = "/cache/flowtide";
    opt.MaxFileSize = 64 * 1024 * 1024;
    opt.SnapshotCheckpointInterval = 20;
    opt.CompactionFileSizeRatioThreshold = 0.33f;
    opt.MaxCacheSizeBytes = 10L * 1000 * 1000 * 1000;
});
```

#### Configuration

| Property                          | Default           | Description |
| --------------------------------- | ----------------- | ----------- |
| `ConnectionString`                | —                 | Azure Storage connection string. |
| `ContainerName`                   | —                 | Name of the blob container. |
| `DirectoryPath`                   | `null`            | Sub-directory within the container. |
| `LocalCacheDirectory`             | System temp folder | Local directory for the disk cache. |
| `MaxFileSize`                     | 64 MB             | Maximum size of each data file. |
| `SnapshotCheckpointInterval`      | 20                | Number of checkpoints between full snapshots. Lower values speed up recovery at the cost of larger checkpoint writes. |
| `CompactionFileSizeRatioThreshold`| 0.33              | When active data in a file drops below this ratio of max file size, the file is eligible for compaction. |
| `MaxCacheSizeBytes`               | 10 GB             | Maximum size of the local disk cache. Least recently used files are evicted when the cache is full. |

### Local File Storage

Store persistent state to a local directory. Useful for single-node deployments or when cloud storage is not needed.

```csharp
builder.Services.AddFlowtideStream("yourstream")
    // ...
    .AddStorage(s =>
    {
        var reservoirBuilder = s.AddFileStorage("/persistence/flowtide");

        // Optionally configure the reservoir
        reservoirBuilder.SetSnapshotCheckpointInterval(10);
        reservoirBuilder.SetMaxDataFileSize(32 * 1024 * 1024);
    });
```

The `AddFileStorage` method returns an `IReservoirBuilder` that allows further configuration:

| Method                            | Default           | Description |
| --------------------------------- | ----------------- | ----------- |
| `SetSnapshotCheckpointInterval`   | 20                | Number of checkpoints between full snapshots. |
| `SetMaxDataFileSize`              | 64 MB             | Maximum data file size in bytes. |
| `SetCacheSize`                    | 10 GB             | Maximum local cache size in bytes. |
| `SetCache`                        | Same directory     | Set a custom cache storage provider. |
| `DisableCache`                    | —                 | Disable the local cache entirely. |
| `OldStreamVersionsRetention`      | -1 (keep all)     | Number of previous stream versions to retain. Only applies when versioning is explicitly configured on the stream builder via `AddVersioningFromPlanHash()`, `AddVersioningFromString()`, or `AddVersioningFromAssembly()`. Without versioning, the stream uses a single default version and this setting has no effect. The current version is always preserved; this setting controls how many *previous* versions are kept alongside it. A value of 0 deletes all old versions immediately, 1 keeps one previous version, and -1 retains all versions indefinitely. |

### Temporary Development Storage

For development and testing, use temporary storage that is automatically cleaned up when the application exits.

```csharp
builder.Services.AddFlowtideStream("yourstream")
    // ...
    .AddStorage(s =>
    {
        s.AddTemporaryStorage();

        // Or specify a custom directory
        // s.AddTemporaryStorage("/tmp/flowtide-dev");
    });
```

:::warning

Temporary storage is not suitable for production. All data is deleted when the application exits.

:::

### Using FlowtideBuilder (without Dependency Injection)

When configuring a stream directly with `FlowtideBuilder`, create a `ReservoirPersistentStorage` instance and pass it through `StateManagerOptions`:

```csharp
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.LocalDisk;

var reservoirBuilder = new ReservoirBuilder();
reservoirBuilder.SetStorage(new LocalDiskProvider("/persistence/flowtide"));
// Optionally set cache, snapshot interval, max file size, etc.
// reservoirBuilder.SetSnapshotCheckpointInterval(10);

builder.WithStateOptions(new StateManagerOptions()
{
    PersistentStorage = new ReservoirPersistentStorage(reservoirBuilder),
    TemporaryStorageOptions = new FileCacheOptions()
    {
        DirectoryPath = "./temp"
    }
});
```

---

## FasterKV storage (Legacy)

:::warning

FasterKV storage is a legacy option. Consider using [Reservoir Storage](#reservoir-storage) for new deployments.

:::

FasterKV is a persistent key value store built by Microsoft.
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

## Temporary file cache storage (Legacy)

:::warning

This is a legacy option. Consider using [Temporary Development Storage](#temporary-development-storage) for new projects.

:::

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


## SQL Server storage (Experimental)

:::warning

SQL Server storage support is still experimental.

:::

Store persistent data to sql server. 

Before using this storage solution you must manually create required tables using this creation script: [Sql tables creation script](https://github.com/koralium/flowtide/blob/main/src/FlowtideDotNet.Storage.SqlServer/Script/create_tables.sql).

The sql user running the system requires the following specific permissions:
* `SELECT`
* `INSERT`
* `DELETE`
* `UPDATE`

```csharp
builder.Services.AddFlowtideStream("yourstream")
    [...]
    .AddStorage(s =>
    {
        // register sql server storage using default settings
        s.AddSqlServerStorage("[connectionstring]");
        // register sql server storge using function to retrieve the connection string.
        s.AddSqlServerStorage(() => "[connectionstring]");
        // or use the overload to specify more settings
        s.AddSqlServerStorage(new SqlServerPersistentStorageSettings()
        {
            ConnectionStringFunc = () => builder.Configuration.GetConnectionString("[connectionstring]"),
            // if you created the tables on a non default schema (or with another name) you can specify the full name for the tables used here.
            // it's also possible to specify the database name as part of table name.
            StreamTableName = "[MySchema].[Streams]",
            StreamPageTableName = "[MyDatabase].[MySchema].[StreamPages2]"
        });
    });
```

## Storage Architecture

When using **FasterKV** or **file cache** storage, the stream storage is built on a three-tier architecture: the in-memory cache, the local disk modified page cache, and the persistent data.

When using **Reservoir** storage, the architecture expands to five tiers: RAM pages (LRU), disk spillover, an in-flight buffer, a local disk cache, and persistent cloud storage. See [Reservoir Storage System](./internal/reservoir) for details.

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
The option that exist today is to compress pages with Zstd. Most storage backends add zstd compression by default to save on network throughput and storage size.

To set compression, it is set under add storage:

```csharp
builder.AddStorage(b => {
    ...
    
    // Use zstd page compression
    b.ZstdPageCompression();
    
    // Use no compression even if the storage medium added compression
    b.NoCompression();
});
```