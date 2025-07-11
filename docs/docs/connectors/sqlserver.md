---
sidebar_position: 1
---

# SQL Server Connector

The SQL Server connector has three different parts:

* **Source** - Reads data from a SQL Server table.
* **Sink** - Writes results from a stream into a SQL Server table.
* **Table Provider** - Provides table information to the SQL plan builder.

## Supported Data Types

The *SQL Server connector* supports reading and writing the following data types:

* Int
* BigInt
* Binary
* Bit
* Char
* Date
* Datetime
* Datetime2
* Decimal
* Float
* Image
* Money
* Nchar
* Ntext
* Numeric
* Nvarchar
* Real
* Smalldatetime
* Smallint
* Text
* Time
* Tinyint
* Uniqueidentifier
* Varbinary
* Varchar
* Xml

## Source

The SQL Server Source allows Flowtide to fetch rows and upates from a SQL Server table.

:::info

It's strongly recommended that change tracking is enabled on the targeted tables. And must be enabled to allow near-realtime streaming.

:::

The source uses the following logic to fetch data into the stream:

```kroki type=blockdiag
  blockdiag {
    GetLatestChangeVersion [label = "Get latest change version", width = 200]
    LoadExisting [label = "Load 10 000 existing"]
    HasMoreExisting [label = "Has more", shape = diamond]
    FetchChanges [label = "Fetch changes"]
    StoreChangeVersion [label = "Store change version"]

    GetLatestChangeVersion -> LoadExisting
    LoadExisting -> HasMoreExisting
    HasMoreExisting -> LoadExisting [label = "Yes"]
    HasMoreExisting -> FetchChanges
    FetchChanges -> StoreChangeVersion
    StoreChangeVersion -> FetchChanges 
  }
```

### Reading with change tracking
To configure a source for a table with change tracking:
```csharp
connectors.AddSqlServerSource(() => "connectionstring");

connectors.AddSqlServerSource(new SqlServerSourceOptions
{
    ConnectionStringFunc = () => "",
});
```

By default changes are fetched once per second and can be modified using the `DeltaLoadInterval` option.

### Reading without change tracking
Flowtide can read data from sources that do not have change tracking enabled. For these sources data are all data is fetched on an interval, specified with the `FullReloadInterval` option on the source.

:::warning

Note that changes are not directly caught when change tracking is disabled.

::: 

#### Reading from views
To allow  from sql server views the following options must be set:

```csharp
connectors.AddSqlServerSource(new SqlServerSourceOptions
{
    ConnectionStringFunc = () => "",
    EnableFullReload = true,
    FullReloadInterval = TimeSpan.FromHours(24),
});
```

This will read all data from the view on an interval specified. Delta loading data is not enabled when targeting a view.

#### Reading from tables
When targeting a table that does not have change tracking enabled an additional option must be provided `AllowFullReloadOnTablesWithoutChangeTracking`.

```csharp
connectors.AddSqlServerSource(new SqlServerSourceOptions
{
    ConnectionStringFunc = () => "",
    EnableFullReload = true,
    FullReloadInterval = TimeSpan.FromHours(24),
    AllowFullReloadOnTablesWithoutChangeTracking = true
});
```

This will read all data from the table on an interval specified in `FullReloadInterval`. Delta loading data is not enabled when targeting a table without change tracking.

:::info

If the table supports change tracking, delta loading will still be used even if these options are provided. But a full load will occur on the provided interval.

:::

#### Reading from large views or tables without change tracking

When targeting a large view or table it's possible to control the allowed size (number of rows) with the `FullLoadMaxRowCount` option. This default value is 1 000 000 rows.

### Retry strategy (reading from SQL Server)

By default the SQL Server source has a default retry strategy that will retry up to 10 times with increasing intervals, totaling a period of ~16 minutes. 
If no successful connection could be made during this period the stream will be restarted.

A custom pipeline can be specified on the source by setting the `ResiliencePipeline` property.
```csharp
connectors.AddSqlServerSource(new SqlServerSourceOptions
{
    ResiliencePipeline = myPipeline
});
```
Flowtide uses `polly` to handle retries, documentation and examples can be found here: [Polly](https://github.com/App-vNext/Polly).

## Sink

The *SQL Server Sink* implements the *grouped write operator*. This means that all rows are grouped by a primary key, thus all
sink tables must have a primary key defined.

:::info

All SQL Server Sink tables must have a primary key defined. The primary key must also be in the query that fills the table.

:::

Its implementation waits fully until the stream has reached a steady state at a time T until it writes data to the database.
This means that its table output can always be traced back to a state from the source systems.

To use the *SQL Server Sink* add the following line to the *ConnectorManager*:

```csharp
connectorManager.AddSqlServerSink(() => connectionString);
```

As with the *SQL Server Source*, the connection string is returned by a function to enable dynamic connection strings.

The sink inserts data into *SQL Server* by creating a temporary table, which follows the table structure of the destination with an added operation metadata column.
The data is inserted into the temporary table using *Bulk Copy*. This allows for fast binary insertion into the temporary table.

After data has been inserted into the temporary table, a merge into statement is run that merges data into the destination table.
After all data has been merged, the temporary table is cleared of all data.

:::warning

If there are multiple rows in the result with the same primary key, only the latest seen row will be inserted into the destination table.

:::

### Custom Primary Keys

In some scenarios you may want to override the table's primary keys or the table might not have a primary key configured.
In this scenario you can provide column names for the columns you want Flowtide to use as primary keys.

Ex:

```csharp
connectorManager.AddSqlServerSink("your regexp on table names", new SqlServerSinkOptions() {
  ConnectionStringFunc = () => connectionString,
  CustomPrimaryKeys = new List<string>() { "my_column1", "my_column2" }
});
```

## SQL Table Provider

The SQL table provider is added to the *SQL plan builder* which will try and look after used tables in its configured *SQL Server*.
It provides metadata information about what the column names are in the table.

To use the *table provider* add the following line to the *Sql plan builder*:

```csharp
sqlBuilder.AddSqlServerProvider(() => connectionString);
```

If you are starting Flowtide with dependency injection, a table provider is added automatically, so this step is not required.