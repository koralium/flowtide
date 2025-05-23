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

| Name                             | Description                                                                            | Default    | Required |
| -------------------------------- | -------------------------------------------------------------------------------------- | ---------- | -------- |
| StorageLocation                  | Connection where the tables are located                                                | Null       | Yes      |
| OneVersionPerCheckpoint          | Only used for reading, it then makes sure that each commit is sent once per checkpoint | False      | No       |
| DeltaCheckInterval               | Only used for reading, how often it should be checked if a new commit exists           | 10 seconds | No       |
| WriteChangeDataOnNewTables       | If new tables created by the connector should enable change feed                       | False      | No       |
| EnableDeletionVectorsOnNewTables | If new tables created by the connector should enable deletion vectors                  | True       | No       |


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

:::warning

The delta lake sink does not yet support partitioned tables.

:::

### Change data feed

It is possible to enable flowtide to write change data files which can help speed up change data readers.
This is done by setting the flag `WriteChangeDataOnNewTables` to `true` in the options when configuring the delta lake connector.
If it is an existing table, the writerFeatures in protocol must contain `changeDataFeed` and the metadata configuration must contain `delta.enableChangeDataFeed`
set to `true`.

This creates new files in a `_change_data` folder and adds the corresponding actions in the delta log.
Change data files are only added if there are deletes on existing rows, since the `add` action gives the same performance
to read change data if there are no deletes. This also saves on storage space.

To be able to reduce memory usage, Flowtide writes the change data files as it creates a commit, but they can then be deleted
before completing the commit.

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