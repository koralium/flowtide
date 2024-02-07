
<br />
<p align="center">
  <h1 align="center">Flowtide.NET</h1>

  <p align="center">
    Streaming integration engine
  <br />
    <a href="https://koralium.github.io/flowtide/docs/intro"><strong>Explore the docs »</strong></a>
  </p>
</p>

**Flowtide** is a data streaming and integration engine designed for seamless and efficient real-time data processing. It stands out by offering differential data computation, versatile connector support, SQL language support for data streams and integrations, and high-performance operations among other features. Flowtide is tailored for developers and companies looking to efficiently handle complex data transformations and integrations.

## Key Features

- **Differential Data Computation**: Processes data changes differentially, enabling the construction of the current state at any given time.
- **Versatile Connector Support**: Ready-built connectors are available for databases such as SQL Server, MongoDB, CosmosDB, Elasticsearch, etc., with the possibility to build your own connectors.
- **SQL Language Support**: Write your data streams and integrations using SQL, making complex data handling accessible and manageable.
- **High-Performance Operations**: Supports various operations such as projection, filtering, joining, aggregation, and recursion, enabling complex data transformations.
- **Integrated Checkpointing System**: Features a custom storage solution with a built-in checkpointing system to ensure data integrity and support full rollback capabilities on failure.
- **Larger than RAM Support**: Writes data to temporary storage on disk to manage data streams larger than the system's memory.
- **Materialize Data**: Easily materialize data from one or multiple sources into a destination, enabling architectures such as CQRS.
- **Vector Clock Watermark System**: Notifies when rows from the source system have been processed by the stream, informing users when the data has been transferred.

## Getting started

To get started with Flowtide, the easiest way is to create a new C# project. This guide will show an example with SQL Server, but you can change
to another connector as well.

Create a minimal API AspNetCore application and install the following nuget package:

* FlowtideDotNet.AspNetCore
* FlowtideDotNet.SqlServer

### Creating a plan

The first step is to create an execution plan, this can be be done with any substrait plan creator.
But it is also possible to do it with SQL inside flowtide. This tutorial will only show how to create a plan with SQL.

Add the following to your _Program.cs_:

```csharp

var sqlBuilder = new SqlPlanBuilder();

sqlBuilder.Sql(@"
CREATE TABLE {sqlserver database name}.{schema name}.{tablename} (
  val any
);

CREATE TABLE {sqlserver database name}.{schema name}.{othertablename} (
  val any
);

INSERT INTO {sqlserver database name}.{schema name}.{destinationname}
SELECT t.val FROM {sqlserver database name}.{schema name}.{tablename} t
LEFT JOIN {sqlserver database name}.{schema name}.{othertablename} o
ON t.val = o.val
WHERE t.val = 123;
");

var plan = sqlBuilder.GetPlan();
```

Replace all values with that are between { } with your own table names in your SQL Server.

### Setting up a read and write factory

Each stream requires a factory that provides it with source and sink operators. These provide the actual implementation when talking with other sources.

Some examples of sinks and sources are:

* MS SQL
* Kafka
* MongoDB
* Elasticsearch
* And more

This example will add a connection for SQL Server:

```csharp
var factory = new ReadWriteFactory();
// Wildcard that all sources should use the following configuration
factory.AddSqlServerSource("*", () => "Server={your server};Database={your database};Trusted_Connection=True;");
// Wildcard that all sinks will use this configuration
factory.AddSqlServerSink("*", () => "Server={your server};Database={your database};Trusted_Connection=True;");
```

### Running the stream

Finally to run the stream we add the following code:

```csharp
builder.Services.AddFlowtideStream(b =>
{
    b.AddPlan(plan)
    .AddReadWriteFactory(factory)
    .WithStateOptions(new StateManagerOptions()
    {
        // This is non persistent storage, use FasterKV persistence storage instead if you want persistent storage
        PersistentStorage = new FileCachePersistentStorage(new FlowtideDotNet.Storage.FileCacheOptions()
        {
        })
    });
});
```

#### Persistent storage

The previous example does not use persistent storage, to use persistent storage, you can instead use the FasterKV storage:

```csharp
PersistentStorage = new FasterKvPersistentStorage(new FasterKVSettings<long, SpanByte>()
{
    RemoveOutdatedCheckpoints = true,
    MemorySize = 1024 * 1024 * 128,
    PageSize = 1024 * 1024 * 16,
    LogDevice = Devices.CreateLogDevice("./data/persistent/log"),
    CheckpointDir = "./data/checkpoints"
})
```

The stream will then be persistent between checkpoints.

### Adding the UI

If you want to add the UI to visualize the progress of the stream, add the following code after "var app = builder.Build();".

```
app.UseFlowtideUI("/stream");
```

### Full example

Here is the full code example to get started:

```csharp

var builder = WebApplication.CreateBuilder(args);

var sqlBuilder = new SqlPlanBuilder();

sqlBuilder.Sql(@"
CREATE TABLE {sqlserver database name}.{schema name}.{tablename} (
  val any
);

CREATE TABLE {sqlserver database name}.{schema name}.{othertablename} (
  val any
);

INSERT INTO {sqlserver database name}.{schema name}.{destinationname}
SELECT t.val FROM {sqlserver database name}.{schema name}.{tablename} t
LEFT JOIN {sqlserver database name}.{schema name}.{othertablename} o
ON t.val = o.val
WHERE t.val = 123;
");

var plan = sqlBuilder.GetPlan();

var factory = new ReadWriteFactory();
// Wildcard that all sources should use the following configuration
factory.AddSqlServerSource("*", () => "Server={your server};Database={your database};Trusted_Connection=True;");
// Wildcard that all sinks will use this configuration
factory.AddSqlServerSink("*", () => "Server={your server};Database={your database};Trusted_Connection=True;");

builder.Services.AddFlowtideStream(b =>
{
    b.AddPlan(plan)
    .AddReadWriteFactory(factory)
    .WithStateOptions(new StateManagerOptions()
    {
        // This is non persistent storage, use FasterKV persistence storage instead if you want persistent storage
        PersistentStorage = new FileCachePersistentStorage(new FlowtideDotNet.Storage.FileCacheOptions()
        {
        })
    });
});

var app = builder.Build();
app.UseFlowtideUI("/stream");

app.Run();
```
