---
sidebar_position: 2
---

# Delta Lake Connector

The Delta Lake connector allows a stream to read existing data & changes from a delta lake table.

The nuget package for the connector is:

* FlowtideDotNet.Connector.DeltaLake

The easiest way to get started with both the source and the sink is to use the `AddDeltaLakeCatalog` method that registers them both under a catalog name.

```csharp
connectorManager.AddDeltaLakeCatalog("my_catalog", new DeltaLakeOptions() {
    // Connect to local disk, azure, AWS etc here, add the directory beneath the actual table you want to query (table name and folders are selected in the query)
    StorageLocation = Files.Of.LocalDisk("./testdata")
});
```

This then allows you to both read and write in sql:

```sql
INSERT INTO my_catalog.{optional_directory_name}.my_table_name
SELECT ... FROM my_catalog.{optional_directory_name}.my_table_name
```

As with all other connectors you can ofcourse read and write from and to any other connector.

## Options

These are the options that can be configured when adding the delta lake connector:

| Name                             | Description                                                                                                                  | Default    | Required |
| -------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- | ---------- | -------- |
| StorageLocation                  | Connection where the tables are located                                                                                      | Null       | Yes      |
| OneVersionPerCheckpoint          | Only used for reading, starts from the first version and sends each commit once per checkpoint                               | False      | No       |
| StartFromFirstVersion            | Only used for reading, the first start reads the oldest version of the table instead of the latest, then every later version | False      | No       |
| IncludeChangeHistory             | Only used for reading, adds `__weight`, `__modificationDate` and `__version` and sends every change as an insert, see [Change history](#change-history) | False | No |
| DeltaCheckInterval               | Only used for reading, how often it should be checked if a new commit exists                                                 | 10 seconds | No       |
| WriteChangeDataOnNewTables       | If new tables created by the connector should enable change feed                                                             | False      | No       |
| EnableDeletionVectorsOnNewTables | If new tables created by the connector should enable deletion vectors                                                        | True       | No       |
| EnableColumnMappingOnNewTables   | If new tables created by the connector should use column mapping by name                                                     | True       | No       |
| CheckpointInterval               | Only used for writing, how many versions there are between delta checkpoints, 0 turns it off | 20 | No |
| MaxFileSizeBytes                 | Only used for writing, the size at which the sink starts a new data file                                                     | 100 MB     | No       |
| CompressionCodec                 | Only used for writing, the codec of written Parquet files, see [Compression](#compression)                                   | Null       | No       |
| StatsPruningColumns              | Only used for writing, how many written columns the sink keeps file statistics for to skip files when deleting rows         | 8          | No       |
| PruningMemoryBytes               | Only used for writing, memory all Delta Lake sinks in the process share for those statistics, the first sink fixes the value | 1 GB       | No       |
| IgnoreUnmatchedDeletes           | Only used for writing, skips and counts a deleted row that no file holds instead of failing the checkpoint, see [Deleting rows](#deleting-rows) | False | No |
| CheckpointMaxAge                 | Only used for writing, writes a delta checkpoint if a version has waited this long, null turns it off | 24 hours | No |
| MaxConcurrentMaintenanceJobs     | Only used for writing, how many checkpoint and cleanup jobs can run at the same time in the process | 2 | No |
| MaintenanceTempDirectory         | Only used for writing, local folder used when building checkpoints | ./data/tempFiles/deltalake | No |
| MaintenanceRequestsPerSecond     | Only used for writing, max storage requests per second for all background maintenance in the process | 200 | No |
| MaintenanceBytesPerSecond        | Only used for writing, max bytes per second for all background maintenance in the process | 64 MiB | No |
| AssumeCompleteStorageListing     | Only used for writing, enables the cleanup on your own storage wrapper, see [Table maintenance](#table-maintenance) | False | No |
| RebuildLegacyCheckpoints         | Only used for writing, builds delta checkpoints from the full log instead of continuing from checkpoints written by other tools | False | No |
| HistoryRetention                 | Only used for writing, how long old versions are kept before they are deleted, see [History retention](#history-retention) | Null | No |
| OrphanFileSweepInterval          | Only used for writing, how often files that no version of the table uses are deleted, needs `HistoryRetention`, see [Orphan files](#orphan-files) | Null | No |

The options given to `AddDeltaLakeCatalog` apply to every table the catalog reads or writes.


## Delta Lake Source

The delta lake source allows ingesting data from a delta lake table. To connect to a delta lake table, use `AddDeltaLake` method on the connector manager.

```csharp
connectorManager.AddDeltaLakeSource(new DeltaLakeOptions() {
    // Connect to local disk, azure, AWS etc here, add the directory beneath the actual table you want to query (table name and folders are selected in the query)
    StorageLocation = Files.Of.LocalDisk("./testdata")
});
```

The delta lake connector uses [Stowage](https://github.com/aloneguid/stowage) to allow connection to different cloud storage solutions, please visit that link to
check possible connections.

Important to note is that the directory of storage location should be beneath the actual table you want to query. The actual table is selected in the query. Example:

```sql
INSERT INTO output
SELECT * FROM my_delta_lake_table
```

In combination with the connector manager addition above, this will use the path `./testdata/my_delta_lake_table`.

If you instead would write:

```sql
INSERT INTO output
SELECT * FROM my_folder.my_delta_lake_table
```

It becomes: `./testdata/my_fylder/mmy_delta_lake_table`.

The delta lake source can calculate all the changes from the table, so no additional state is stored in the stream of the data to correctly calculate changes.


**Supported features**

* Calculate change data from add/remove actions
* Use cdc files if they exist for change data
* Deletion vectors
* Partitioned data
* Column mapping

Commits that only rearrange files, where every action has `dataChange` set to false such as an `OPTIMIZE`, are skipped.

### When the source stops reading

After its first load the source reads the table's log one version at a time, so it needs every commit after its own position. If those are removed
before the source reads them, the source stops reading the table instead of skipping data:

* The next commit is gone while later versions exist, for example after log retention, such as the sink's
  [History retention](#history-retention), removed it while the stream was stopped.
* A data or change data file of a version the source has not read yet was removed, for example by `VACUUM`.

The source then logs an error, reports itself unhealthy (`flowtide_health` is 0) and keeps its position, the rest of the stream keeps running.
To read the table again, reset the stream state so the source starts from the latest snapshot, this needs a table whose latest version can still be read.
The stream then sends every row of the table again, so sinks that append, such as a Delta Lake sink with `INSERT INTO`, need their tables emptied first.
A file missing during the first load fails the stream instead, which then retries from the latest snapshot.
Keep the table's log and file retention longer than the longest time a stream can be stopped to avoid this.

### Replaying delta changes

One feature of the delta lake source is the possibility to replay the log where each commit to the delta table is sent separately once per checkpoint.
This can be useful in unit tests, but also if one wants to send historic data to a destination system such as a time-series database and not skip over a commit.

To use the replay functionality use the following setting in options:


```csharp
connectorManager.AddDeltaLakeSource(new DeltaLakeOptions() {
    StorageLocation = Files.Of.LocalDisk("./testdata"),
    OneVersionPerCheckpoint = true
});
```

`StartFromFirstVersion` also starts from the oldest version but does not wait for a checkpoint between versions, so a long log loads faster.
Without [Change history](#change-history) the versions read together cancel each other out, the result is the same as starting from the latest version.

### Change history

With `IncludeChangeHistory` the source sends the table as a change log, nothing it sends is ever retracted. Three columns are added to every table:

| Column               | Type      | Value                                                                                         |
| -------------------- | --------- | --------------------------------------------------------------------------------------------- |
| `__weight`           | long      | 1 for an inserted row, -1 for a deleted row, the row itself is sent as an insert                |
| `__modificationDate` | timestamp | The time of the version the change comes from                                                 |
| `__version`          | long      | The version the change comes from, the first load uses the version it read                    |

An update is a deleted row and an inserted row in the same version, an update that does not change the selected columns sends nothing.
The time of a version is the first that exists of `inCommitTimestamp` and `timestamp` in its `commitInfo`, the latest `modificationTime` of an added file
and the latest `deletionTimestamp` of a removed file, otherwise it is null.

Combine it with `StartFromFirstVersion` to read the whole history of a table:

```csharp
connectorManager.AddDeltaLakeSource(new DeltaLakeOptions() {
    StorageLocation = Files.Of.LocalDisk("./testdata"),
    IncludeChangeHistory = true,
    StartFromFirstVersion = true
});
```

The table as it was at version 10 is then:

```sql
INSERT INTO output
SELECT id, name FROM (
    SELECT id, name, SUM(__weight) AS total FROM my_delta_lake_table
    WHERE __version <= 10
    GROUP BY id, name
)
WHERE total > 0
```

Things to keep in mind:

* Order changes by `__version`, `__modificationDate` comes from the writers' clocks and can go back between versions.
* The history starts at the oldest version the log still has. After log cleanup that is the oldest checkpoint, earlier changes are gone. With
  [History retention](#history-retention) on a Flowtide table it is the oldest version the sink kept, which `flowtide_delta_retention_oldest_version` shows.
* State that keeps the history, such as the query above, grows with every version.
* A table that already has a column with one of these names can not be read with `IncludeChangeHistory`.

### Sample

There is an example in the samples folder that uses the Delta Lake Source to read data from an azure blob storage.
To run the example, start the `AspireSamples` project and select one of:

* DeltaLake-Source
* DeltaLake-Source, Replay history

After the project has started, inspect the console log of the application to see a log output of the rows.

### Azure Blob Storage Configuration Example

When using Azure Blob Storage you would configure the storage example like this example:

```csharp
connectors.AddDeltaLakeSource(new DeltaLakeOptions()
{
    StorageLocation = Files.Of.AzureBlobStorage(accountName, accountKey)
});
```

This connects the source to the root of the blob storage, to then query a table you must include the container name and all subfolders to the table location:

```sql
SELECT * FROM my_container.my_optional_parent_folder.my_table
```

## Delta Lake Sink

The delta lake sink allows to materialize/denormalize queries as a delta lake table. The connector will continuously update the table on each checkpoint.
It does not write on each watermark update, this is done to reduce the amount of files that are written and try to only write consistent updates.
This means that the write frequency can be modified by changing how often a checkpoint is taken.

To add the delta lake sink, write the following to the connector manager:

```csharp
connectors.AddDeltaLakeSink(new DeltaLakeOptions()
{
    StorageLocation = Files.Of.LocalDisk("./testdata")
});
```

Delta Lake Features:

* Deletion vectors
* Statistics output
* Data file skipping
* Change data files

> [!WARNING]
> The delta lake sink does not yet support partitioned tables.

A stream checkpoint that changes no rows writes no new version of the table, unless it creates the table, starts an `INSERT OVERWRITE`, or
[History retention](#history-retention) needs one.

### Delta checkpoints

The sink writes a delta checkpoint every `CheckpointInterval` versions (default 20), so readers do not have to read the entire delta log.
The checkpoints are built in the background and never slow down or fail the stream.

If the table already has checkpoints written by another tool, the sink continues from them. If you want the sink to build its own checkpoints
from the full log instead, set `RebuildLegacyCheckpoints` to true. This only works if every commit from version 0 still exists.

Some tables cannot get checkpoints, for instance tables using delta features the sink does not know. Writing still works, but readers have to
read more of the log, and old history cannot be deleted.

With the default settings, background maintenance can use around 1.5 GB of memory. Lower `MaxConcurrentMaintenanceJobs` to use less.
On S3 it is also a good idea to add an `AbortIncompleteMultipartUpload` lifecycle rule, which removes the parts of failed checkpoint uploads.

### Table maintenance

The sink deletes files it wrote that no version of the table uses, for instance files from a write that failed. This runs in the background
on Azure Blob Storage, S3 and local disk. Files from a write that was interrupted, for instance by a crash, can be left behind. The
[orphan file sweep](#orphan-files) can delete them.

If you use your own storage wrapper you can set `AssumeCompleteStorageListing` to true to enable the cleanup. Only do this if the storage
always lists every file, otherwise files that are still in use can be deleted.

### History retention

By default the sink never deletes old versions, which means the delta log and old data files keep growing. To delete old versions, set
`HistoryRetention`:

```csharp
connectors.AddDeltaLakeSink(new DeltaLakeOptions()
{
    StorageLocation = Files.Of.LocalDisk("./testdata"),
    HistoryRetention = TimeSpan.FromDays(7)
});
```

Once a day the sink then deletes the versions that are older than the retention, together with the data files that only those versions used.
All versions inside the retention can still be read, also with time travel. The value must be at least one day and in whole hours, and it
only works on Azure Blob Storage, S3 and local disk. The sink also sets the table properties `delta.logRetentionDuration` and
`delta.deletedFileRetentionDuration` to the same value.

History is usually kept up to around two days longer than the retention, since the cleanup runs once a day and counts from midnight UTC.

Things to keep in mind:

* The sink should be the only writer to the table. Each commit from another writer delays the cleanup by about one retention period.
* Make sure no reader needs versions older than the retention. This includes a stream reading the table that has been stopped for a while.
* Do not edit or delete the `.flowtide-cleanup` files in `_delta_log`, they keep track of the cleanup.
* Do not go back to an older version of Flowtide once the cleanup has run on a table.

#### Large tables and long histories

In our tests, cleaning one day of history on a table with 200 000 files took between 15 and 40 minutes, and one process kept up with
300 tables that commit every minute, but not with 300 tables that commit every 10 seconds.

The first cleanup of a table with a long history has to read all of it. On Azure Blob Storage and local disk you can make this faster by
raising `MaintenanceRequestsPerSecond` (default 200). On S3 the log is read one file at a time, so a table with a very long history, roughly
700 000 versions or more, may never finish its first cleanup. Turn on retention early if you use S3.

#### Turning it off

Remove `HistoryRetention` and `OrphanFileSweepInterval` and restart the stream. A cleanup that has started is paused and continues if you turn retention on again.

To stop all background deletes at once, also those of table maintenance, set this switch at startup:

```csharp
AppContext.SetSwitch("FlowtideDotNet.DeltaLake.DisableMaintenanceDeletes", true);
```

#### If the cleanup is suspended

If `flowtide_delta_retention_state` is 6 the cleanup is suspended, and a warning with the reason is logged. Writing to the table still works.

* `DeleteFailed`: deleting files failed, check that the credentials are allowed to delete.
* Another writer committed during the cleanup: it tries again the next day.
* `ManifestSuspended`: a `.flowtide-cleanup` file is missing or was changed. Stop the streams that write the table, restore the file from a
  backup and start them again. If there is no backup, turn off `HistoryRetention`. Nothing more is deleted on the table until this is fixed.

### Orphan files

Files in the table folder that no version of the table uses are orphan files, for instance files left behind when a crash interrupts a
write. It is possible to let the sink delete them in the background by setting `OrphanFileSweepInterval`. Once every interval a
sweep deletes the files older than `HistoryRetention` plus one hour that no kept version of the table uses. The interval must be at
least one day. The sweep needs `HistoryRetention` and only works on Azure Blob Storage, S3 and local disk, not on your own storage
wrapper:

```csharp
connectors.AddDeltaLakeSink(new DeltaLakeOptions()
{
    StorageLocation = Files.Of.LocalDisk("./testdata"),
    HistoryRetention = TimeSpan.FromDays(7),
    OrphanFileSweepInterval = TimeSpan.FromDays(7)
});
```

Things to keep in mind:

* The sweep can delete any unused file in the table folder, also files that Flowtide did not write. It skips Delta tables inside the
  table folder, and files and folders whose names start with `.` or `_`, except the `_change_data` folder at the top of the table.
  Keep any other files out of the table folder.
* A sweep keeps the names of all files in the table folder in memory while it checks them. Plan for about 2.5 GB for one million
  files on the host with the largest table.
* One sweep deletes at most 100 000 files, the next sweeps delete the rest.
* A sweep is skipped, and tried again later, if the table gets a new version while the sweep lists its files. Listing a million
  files can take a minute or more, so on a large table that gets a new version every minute the sweeps may never finish.
* Each commit from another writer or from an older version of Flowtide pauses the sweeps, usually for about one retention period.

#### Running a sweep now

Sweeps run on a schedule, not when the stream starts. It is possible to run one now with `DeltaLakeMaintenance.RequestOrphanSweep`.
Pass the same storage object you gave as `StorageLocation` and the table name without the catalog, for example `my_folder/my_table`
for `my_catalog.my_folder.my_table`. This also works without `OrphanFileSweepInterval`.

```csharp
var storage = Files.Of.LocalDisk("./testdata");
connectorManager.AddDeltaLakeCatalog("my_catalog", new DeltaLakeOptions()
{
    StorageLocation = storage,
    HistoryRetention = TimeSpan.FromDays(7)
});

// Later, while the stream is running
var result = DeltaLakeMaintenance.RequestOrphanSweep(storage, "my_table");
```

| Result | Description |
| ------ | ----------- |
| `Scheduled` | A sweep runs in the background. A restart before it has finished drops the request. |
| `AlreadyPending` | An earlier request has not finished yet, this one joins it. |
| `Unavailable` | No running stream in this process writes the table with `HistoryRetention` on this storage, or the storage does not support the sweep. |

### Monitoring maintenance

Warnings are logged as `Delta table {table} cleanup: {reason}` or `Delta table {table} checkpoint maintenance: {reason}`. After a
start, the first orphan sweep that lists the table's files logs a warning with how many it found and about how much memory they take,
to help you size the host.
These are the most useful gauges:

| Gauge | Description |
| ----- | ----------- |
| `flowtide_delta_checkpoint_lag_versions` | Versions since the last delta checkpoint |
| `flowtide_delta_checkpoint_consecutive_failures` | Failed checkpoint attempts in a row |
| `flowtide_delta_retention_state` | 0 off or not started, 1 idle, 2 preparing, 3 cleaning up, 4 paused, 5 checking another writer's commits, 6 suspended, 7 preparing failed and retried after 24 hours |
| `flowtide_delta_retention_oldest_version` | The oldest version that is kept |
| `flowtide_delta_retention_backlog_seconds` | How far the cleanup is behind the retention |

### Compression

Data and change data files are compressed with zstd by default. The codec is chosen in this order:

1. The `CompressionCodec` option, it applies to every table the sink writes.
2. The table property `delta.parquet.compression.codec` (`uncompressed`, `snappy`, `gzip`, `lz4`, `lz4_raw` or `zstd`).
3. zstd. An unknown value of the table property also gives zstd and logs a warning.

The file names carry the codec, for example `part-00000-{guid}.zstd.parquet` or `part-00000-{guid}.snappy.parquet`, uncompressed files end in `.parquet`.
Earlier versions of the connector wrote uncompressed files named `.snappy.parquet`. Readers take the codec from the file itself, so those files stay readable.

### Deleting rows

To find the files that can hold a deleted row, the sink keeps the statistics of the first `StatsPruningColumns` written columns of every file in memory,
up to `PruningMemoryBytes` shared by all Delta Lake sinks in the process. A table's first column has priority over other tables' extra columns: when it does
not fit, the tables holding extra columns give them up at their next checkpoint. The table that did not fit keeps its statistics in the stream's state,
which can spill to disk, until the stream restarts.

If the stream deletes a row that none of the table's files hold, for instance because another writer removed it, the checkpoint fails with
`Could not find any data file that contains the row ...` and the stream restarts until the row can be found. It is possible to skip such deletes instead
by setting `IgnoreUnmatchedDeletes` to true. The sink then logs a warning, counts the skipped rows in `flowtide_delta_delete_unmatched_rows`, and the table
keeps any copy of the row it still has.

Things to keep in mind:
* Files another engine rewrote, for instance with `OPTIMIZE`, are matched by column name, also when the timestamps are stored as microseconds, nanoseconds or INT96.
* A file whose surviving rows hold timestamps finer than a microsecond can only be deleted from with deletion vectors enabled.
* A map with the same key more than once is only found with its entries in the same order as Flowtide wrote them.

### Other writers

The sink expects to be the only writer of its table. Commits that another writer adds between two of the sink's commits, such as an `OPTIMIZE`,
are picked up and kept.

If another writer commits the version the sink is publishing, the sink stops publishing instead of overwriting it. It then logs an error, reports itself
unhealthy (`flowtide_health` is 0) and keeps every row it receives in the stream's state. It tries again at every later checkpoint, which needs new data
or a restart of the stream, and continues once the conflicting `_delta_log/{version}.json` file has been removed.
Remove it only while the other writer has not committed a later version. When it retries after stopping, and when it publishes after a restart, the sink
first lists the log and stays stopped if the log holds a later version or a checkpoint at that version.
Resetting the stream state also recovers, the stream then writes every row again. With `INSERT OVERWRITE` that replaces the table, with `INSERT INTO`
the rows are appended, so empty or recreate the table first.

With `HistoryRetention`, commits from another writer also delay the cleanup, see [History retention](#history-retention).


### Change data feed

It is possible to enable flowtide to write change data files which can help speed up change data readers.
This is done by setting the flag `WriteChangeDataOnNewTables` to `true` in the options when configuring the delta lake connector.
If it is an existing table, the writerFeatures in protocol must contain `changeDataFeed` and the metadata configuration must contain `delta.enableChangeDataFeed`
set to `true`.

This creates new files in a `_change_data` folder and adds the corresponding actions in the delta log.
Change data files are only added if there are deletes on existing rows, since the `add` action gives the same performance
to read change data if there are no deletes. This also saves on storage space.

To be able to reduce memory usage, Flowtide writes the change data files as it creates a commit. When the commit has no deletes they
are left out of it and deleted as described in [Table maintenance](#table-maintenance).

Since this connector writes the exact result set in the query, it does not have the concept of primary keys and rows can be duplicated if that is the result
of the query. This means that the change data feed only contains `delete` and `insert` operations.


### Supported data types

Flowtide can write the following data types to a delta lake table:

* Array
* Binary
* Boolean
* Date
* Decimal
* Float
* Double
* Byte
* Short
* Integer
* Long
* String
* Timestamp
* Struct
* Map

### Creating a new table

It is possible to create a new delta lake table if it does not exist. This is done by using the `CREATE TABLE` syntax to
clearly set the data types for each column.

Not all delta lake data types are supported in Flowtide SQL, so for full type support please create an empty table manually.
The data types that are supported with `CREATE TABLE` and how they are mapped are:

| Flowtide SQL name | Flowtide Internal Type | Delta Lake name | Comment                                                                                            |
| ----------------- | ---------------------- | --------------- | -------------------------------------------------------------------------------------------------- |
| int               | Integer                | long            | In flowtide integers are treated as the same type, the size is dynamically increased.              |
| binary            | binary                 | binary          |                                                                                                    |
| boolean           | boolean                | boolean         |                                                                                                    |
| date              | timestamp              | date            |                                                                                                    |
| decimal           | decimal                | decimal         | Flowtide uses C# decimal format, this then gets converted into a decimal with precision and scale. |
| double            | double                 | double          | Flowtide only manages double precision floating point numbers.                                     |
| timestamp         | timestamp              | timestamp       |                                                                                                    |
| struct            | map                    | struct          | Map should be used to create struct values at this point.                                          |
| map               | map                    | map             |                                                                                                    |
| list              | list                   | array           |                                                                                                    |


#### Sql example

```sql
CREATE TABLE test (
  intval INT,
  binval BINARY,
  boolval BOOLEAN,
  dateval DATE,
  decimalVal DECIMAL(19, 3),
  doubleVal DOUBLE,
  timeVal TIMESTAMP, 
  structVal STRUCT<firstName STRING, lastName STRING>,
  mapVal MAP<STRING, STRING>,
  listVal LIST<STRING>
);

INSERT INTO test
SELECT 
  intval, 
  binval, 
  boolval, 
  dateval, 
  decimalVal, 
  doubleVal, 
  timeVal, 
  MAP('firstName', firstName, 'lastName', lastName) as structVal,
  MAP('firstName', firstName, 'lastName', lastName) as mapVal,
  list(firstName, lastName) as listVal
FROM my_source_table;
```

### Insert overwrite

The delta lake connector supports `INSERT OVERWRITE {tableName}` instead of `INSERT INTO`, which replaces 
all content in the delta table with the new data from the stream.
This occurs on the initial load, after which subsequent inserts, updates, and deletes behave normally on the table.

This allows easy full reload of a table with an integration. All the existing data is commited as `remove` 
operations in the commit log, which means it is still possible to look at history of the table.