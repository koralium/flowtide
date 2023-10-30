---
sidebar_position: 1
---

# SQL Server Connector

The SQL Server connector has three different parts:

* **Source** - Reads data from a SQL Server table.
* **Sink** - Writes results from a stream into a SQL Server table.
* **Table Provider** - Provides table information to the SQL plan builder.

## Source

The SQL Server Source allows Flowtide to fetch rows and updates from a SQL Server table.
There is one prerequisite for this connector to work:


:::info

Change tracking must be enabled on the table.

:::

Without change tracking, Flowtide wont be able to find updates on the table.
There are plans to allow the source to run in batch mode where it computes the delta inside of the connector, but
that is not yet available.

The SQL Server Source can be added to the 'ReadWriteFactory' with the following line:

```csharp
factory.AddSqlServerSource("your regexp on table names", () => connectionString);
```

The connection string must be set as a function, since the idea is that the connection string might change, from say a system such as
*Hashicorp Vault*.

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

The source will retry fetching data in-case of a SQL Server error, as long as it can reconnect to the database.
It will mark the operator as unhealthy, but it will not trigger a stream restart.

If the operator cannot reconnect to the SQL Server, it will trigger a full stream restart.

## Sink

The *SQL Server Sink* implements the *grouped write operator*. This means that all rows are grouped by a primary key, thus all
sink tables must have a primary key defined.

:::info

All SQL Server Sink tables must have a primary key defined. The primary key must also be in the query that fills the table.

:::

Its implementation waits fully until the stream has reached a steady state at a time T until it writes data to the database.
This means that its table output can always be traced back to a state from the source systems.

To use the *SQL Server Sink* add the following line to the *ReadWriteFactory*:

```csharp
factory.AddSqlServerSink("your regexp on table names", () => connectionString);
```

As with the *SQL Server Source*, the connection string is returned by a function to enable dynamic connection strings.

The sink inserts data into *SQL Server* by creating a temporary table, which follows the table structure of the destination with an added operation metadata column.
The data is inserted into the temporary table using *Bulk Copy*. This allows for fast binary insertion into the temporary table.

After data has been inserted into the temporary table, a merge into statement is run that merges data into the destination table.
After all data has been merged, the temporary table is cleared of all data.

:::warning

If there are multiple rows in the result with the same primary key, only the latest seen row will be inserted into the destination table.

:::

## SQL Table Provider

The SQL table provider is added to the *SQL plan builder* which will try and look after used tables in its configured *SQL Server*.
It provides metadata information about what the column names are in the table.

To use the *table provider* add the following line to the *Sql plan builder*:

```csharp
sqlBuilder.AddSqlServerProvider(() => connectionString);
```