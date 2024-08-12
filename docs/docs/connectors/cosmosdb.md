---
sidebar_position: 2
---

# CosmosDB Connector

The CosmosDB connector allows you to insert data into a CosmosDB container.
At this time only a sink is implemented, there is no support yet to have CosmosDB as a source.

## Sink

The CosmosDB sink allows the insertion of data into a CosmosDB container.

:::info

All CosmosDB insertions must contain a column called 'id' and also a column that matches the configured partition key on the CosmosDB container.

:::

Its implementation waits fully until the stream has reached a steady state at a time T until it writes data to the container.
This means that its table output can always be traced back to a state from the source systems.

To use the *CosmosDB Sink* add the following line to the *IConnectorManager*:

```csharp
connectorManager.AddCosmosDbSink("your regexp on table names", connectionString, databaseName, containerName);
```

### Example

Having a column named 'id' and also a column matching the configured primary key is required for the sink to function.

```csharp
sqlBuilder.Sql(@"
    INSERT into cosmos
    SELECT userKey as id, companyId as pk, firstName, lastName 
    FROM users
");

connectorManager.AddCosmosDbSink("cosmos", connectionString, databaseName, containerName);

...
```