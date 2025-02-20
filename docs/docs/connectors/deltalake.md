---
sidebar_position: 2
---

# Delta Lake Connector

The Delta Lake connector allows a stream to read existing data & changes from a delta lake table.

The nuget package for the connector is:

* FlowtideDotNet.Connector.DeltaLake

## Delta Lake Source

The delta lake source allows ingesting data from a delta lake table. To connect to a delta lake table, use `AddDeltaLake` method on the connector manager.

```csharp
connectorManager.AddDeltaLake(new DeltaLakeOptions() {
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
connectorManager.AddDeltaLake(new DeltaLakeOptions() {
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
connectors.AddDeltaLake(new DeltaLakeOptions()
{
    StorageLocation = Files.Of.AzureBlobStorage(accountName, accountKey)
});
```

This connects the source to the root of the blob storage, to then query a table you must include the container name and all subfolders to the table location:

```sql
SELECT * FROM my_container.my_optional_parent_folder.my_table
```