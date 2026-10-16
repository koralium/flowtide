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
| CheckpointInterval               | Only used for writing, a checkpoint is built in the background once this many versions follow the latest one, 0 turns checkpoints off, see [Checkpoints](#checkpoints) | 20 | No |
| MaxFileSizeBytes                 | Only used for writing, the size at which the sink starts a new data file                                                     | 100 MB     | No       |
| CompressionCodec                 | Only used for writing, the codec of written Parquet files, see [Compression](#compression)                                   | Null       | No       |
| StatsPruningColumns              | Only used for writing, how many written columns the sink keeps file statistics for to skip files when deleting rows         | 8          | No       |
| PruningMemoryBytes               | Only used for writing, memory all Delta Lake sinks in the process share for those statistics, the first sink fixes the value | 1 GB       | No       |
| CheckpointMaxAge                 | Only used for writing, how long published versions may wait for a checkpoint before one is built regardless of the interval, null turns the age trigger off | 24 hours | No |
| MaxConcurrentMaintenanceJobs     | Only used for writing, how many checkpoint builds run at once in the process, the first sink fixes the value, also one with a CheckpointInterval of 0; cleanup jobs run in slots of their own beside them, see [Limits](#limits) | 2 | No |
| MaintenanceTempDirectory         | Only used for writing, the local folder for checkpoint builds, the first sink fixes the value, also one with a CheckpointInterval of 0 | ./data/tempFiles/deltalake | No |
| AssumeCompleteStorageListing     | Only used for writing, vouches for a storage the connector does not recognize, such as a custom wrapper, as a whole stack: listings are complete and in order, Stat returns null only for a missing object, a read or delete reaches only the object of exactly the given name (no short-name or trimmed spelling reaches another object), and a delete removes exactly the named object. Maintenance then treats the store as eligible, which also lets it rebuild a torn checkpoint without the full history. It cannot make a known failed store, such as Google Cloud Storage, eligible | False | No |
| RebuildLegacyCheckpoints         | Only used for writing, when every commit since version 0 is still there, rebuild from the whole log instead of building on a checkpoint the sink did not write (or wrote on top of one), and instead of declining when no starting checkpoint can be used | False | No |

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

* The next commit is gone while later versions exist, for example after log retention removed it while the stream was stopped.
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
* The history starts at the oldest version the log still has. After log cleanup that is the oldest checkpoint, earlier changes are gone.
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

A checkpoint that changes no rows writes no new version of the table, unless it creates the table or starts an `INSERT OVERWRITE`.

### Checkpoints

A Delta checkpoint lets readers start from one file instead of replaying every commit. The sink builds checkpoints in the background, outside
the commit path: publishing a version only hands its number to the table's maintenance worker, so a large table never delays a commit.

A checkpoint is built at the newest published version when `CheckpointInterval` versions follow the latest checkpoint, or when a published version
has waited `CheckpointMaxAge`. That wait is counted from when the running stream first saw the version, so a restart starts it again. A build starts from the newest checkpoint below it that it can read within its limits, and replays the commits after it.
The first checkpoint of a table replays the log from version 0. The output is written in the local temp folder, read back and checked in full, and only
then published as `{version}.checkpoint.parquet`, after which `_last_checkpoint` is updated. The hint is never moved to an older version.

Checkpoints are classic, single-file checkpoints with `add`, `remove`, `protocol`, `metaData`, `txn` and `domainMetadata` rows. Removes keep no
statistics or tags, and tombstones older than `delta.deletedFileRetentionDuration` (one week by default) are dropped.

#### Limits

All Delta Lake sinks in the process share the maintenance budget:

- `MaxConcurrentMaintenanceJobs` checkpoint builds run at once. Beside them, up to two cleanup jobs (discard drains and staged commit sweeps,
  see [Table maintenance](#table-maintenance)) run in slots of their own, which no option changes. A table runs one job at a time, and a
  table waiting for its own earlier job holds no slot.
- Each job, build or cleanup, may use 84 MiB of managed memory and 256 MiB of native Parquet memory. A build keeps up to 64 MiB of it for
  the commits after its starting checkpoint and sorts a larger tail through files in the temp folder. These figures exclude the
  `PruningMemoryBytes` the sinks keep for deleting rows.
- Downloads share a 128 MiB allowance, admitted by listed size, since the storage clients read whole objects; a larger object runs alone.
- Files in the temp folder share a 4 GiB disk cap. Each write reserves its space first, and a file's space is released only after it is deleted.
- An action line over 4 MiB, or with more than 65,536 entries, declines the build before any of it is buffered. An oversized `commitInfo` line is skipped.

With the defaults this allows about 340 MiB per job and about 1.45 GiB for maintenance in the process (two builds, two cleanup jobs and
the download allowance), or about 2.45 GiB for the connector together with `PruningMemoryBytes`. Setting `MaxConcurrentMaintenanceJobs` to 1
saves about 340 MiB. These are allocation allowances, not limits on the process's resident memory: runtime memory, memory the allocator
keeps, the sinks' own file catalogs and the storage listing come on top. A single object larger than the download allowance adds its own size while it downloads.

A build keeps its starting checkpoint and its output in the temp folder together, and on local disk the output and its published copy, so a
checkpoint larger than about 2 GiB is not built: the build declines on the disk cap.

A build that cannot work within these limits declines; the checkpoint is optional, so a decline never fails the stream. It is logged once per reason
at warning level, shown in the gauges below, and retried with a backoff that starts at one minute and doubles up to 30 minutes. After a failure,
or a decline that is not held (see below), the retry also waits at least three times as long as the attempt took, so a table whose builds keep
failing does not keep a build slot busy.

#### Starting checkpoints written by others

A checkpoint the sink did not write, or one it wrote on top of such a checkpoint, may lack tombstones, transactions or domains that its writer dropped.
The new checkpoint records this gap in its file metadata and in the `delta_checkpoint_inherited_gap` gauge. With `RebuildLegacyCheckpoints` the sink
instead rebuilds from the whole log, when every commit since version 0 is still there, and builds on its own complete checkpoint after that.

A starting checkpoint is skipped, and an older readable one used instead when there is one, when its file is outside the limits above (its row
groups, pages or metadata are too large), when it uses sidecar files, or when it cannot be decoded. The last two are found while it is read, so
the attempt that finds them fails and the next one skips it. With `RebuildLegacyCheckpoints`, when no starting checkpoint is left and every
commit since version 0 is still there, the build replays the log from version 0. A torn checkpoint, one whose file was cut short, is rebuilt in place.

The build declines when its starting checkpoint:

- holds only parsed statistics (`stats_parsed` without JSON `stats`), unless they are row counts only, which are converted;
- holds rows that break the Delta specification, or a non-null column the sink does not know.

It also declines when the table names a reader or writer version, or a feature, that is not in the table below.

A declined build is not retried while nothing it depends on changes: the same starting checkpoints, the commits that hold the protocol, or the
oversized commit. On storages whose listings report a modification time or an MD5, as local disk does, a checkpoint replaced at the same
version is picked up at the next attempt. On storages without them it is picked up within 6 hours. A decline that depends on the output itself, such
as a deletion vector count, is retried at each backoff; its starting checkpoint is then read from the local cache, the commits after it from the
table again.

| Feature | Why a classic checkpoint is correct |
| ------- | ----------------------------------- |
| appendOnly, invariants, checkConstraints, generatedColumns, identityColumns, allowColumnDefaults | They constrain data writes; the checkpoint copies `metaData` unchanged |
| changeDataFeed | The specification excludes `cdc` actions from checkpoints |
| columnMapping, timestampNtz, typeWidening, typeWidening-preview, variantType, variantType-preview | Schema and type rules live in `metaData`; statistics and partition values are copied as written |
| deletionVectors | Deletion vector descriptors are copied; `numRecords` is required and checked |
| rowTracking | `baseRowId` and `defaultRowCommitVersion` are copied, and so is the `delta.rowTracking` domain |
| domainMetadata, clustering | Domains are written; removed domains are not |
| v2Checkpoint | Writers may always create a classic checkpoint |
| inCommitTimestamp | It lives in `commitInfo` and `metaData.configuration`; `commitInfo` is not checkpointed |
| icebergCompatV1, icebergCompatV2, materializePartitionColumns | They constrain data writes only |
| vacuumProtocolCheck | It restricts cleanup, not checkpoints |

Tables with `checkpointProtection`, `catalogManaged`, `catalogOwned-preview` or `coordinatedCommits-preview` get no checkpoints from the sink:
the first forbids checkpoints that do not support every protocol feature, and with the others commits may not be in `_delta_log`.

#### Storage

- Put `MaintenanceTempDirectory` on the same volume as tables on local disk, so publishing a checkpoint is a rename. Across volumes it is a copy
  that is flushed before the rename.
- Each process locks its own folder under the temp folder, and cleans up folders of processes that are gone only behind their lock. Folders that are
  or hold a link are never cleaned. Where file locks do not work, for example with `DOTNET_SYSTEM_IO_DISABLEFILELOCKING`, cleanup is skipped and logged.
- An upload that fails is abandoned. On S3, add an `AbortIncompleteMultipartUpload` lifecycle rule; Azure removes uncommitted blocks after 7 days.
- Where neither the object's metadata nor the listing reports its size, the size check after an upload reads the checkpoint back to count it.
- On a storage that wraps local disk the checkpoint is uploaded, not renamed, so a crash can leave a partial file. The next build finds it torn and
  rebuilds it.
- The storage must allow concurrent calls, since builds of different tables run at the same time as the sinks.
- When `_last_checkpoint` names a version without a complete checkpoint at or after it, which some readers fail on, the sink builds a checkpoint at
  once to repair it, whatever the interval.

#### Gauges

Per table: `delta_checkpoint_version`, `delta_checkpoint_lag_versions`, `delta_checkpoint_oldest_uncheckpointed_seconds`,
`delta_checkpoint_consecutive_failures` (failed or declined attempts in a row), `delta_checkpoint_skipped` (the latest attempt declined), `delta_checkpoint_dangling_hint`,
`delta_checkpoint_last_duration_ms`, `delta_checkpoint_last_validation_ms`, `delta_checkpoint_last_spill_bytes`, `delta_checkpoint_last_transfer_bytes`,
`delta_checkpoint_last_mode` (1 when the tail was sorted through files), `delta_checkpoint_inherited_gap` and `delta_checkpoint_detached_jobs`.

For the process, on the `FlowtideDotNet.Connector.DeltaLake` meter: `flowtide_delta_maintenance_running_jobs`, `_queued_jobs`,
`_oldest_wait_seconds`, `_transfer_bytes`, `_disk_bytes`, `_pending_deletion_bytes`, `_detached_jobs`, `_tick_ms` and `_init_failed` (1 once a
sink in this process failed to start its maintenance worker; invalid maintenance settings only log a warning).

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

### Other writers

The sink expects to be the only writer of its table. Before each commit it lists the table's log once, commits that another writer added between two
checkpoints, such as an `OPTIMIZE`, are read and kept.

If another writer commits the version the sink is publishing, the sink stops publishing instead of overwriting it. It then logs an error, reports itself
unhealthy (`flowtide_health` is 0) and keeps every row it receives in the stream's state. It tries again at every later checkpoint, which needs new data
or a restart of the stream, and continues once the conflicting `_delta_log/{version}.json` file has been removed.
Remove it only while the other writer has not committed a later version. When it retries after stopping, and when it publishes after a restart, the sink
first lists the log and stays stopped if the log holds a later version or a checkpoint at that version.
Resetting the stream state also recovers, the stream then writes every row again. With `INSERT OVERWRITE` that replaces the table, with `INSERT INTO`
the rows are appended, so empty or recreate the table first.


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