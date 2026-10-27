---
sidebar_position: 0
---

# Catalogs

It is possible to add connectors under catalogs, this can be useful if one wants to read data from data sources that may have the same logical name. Say two SQL Servers that have the same database names (or even schema and table names).

Example reading data from a catalog when using SQL Server:

```sql
... FROM {catalogName}.{databaseName}.{schema}.{tableName}
```

A catalog is added on the connector manager as follows:

```csharp
connectorManager.AddCatalog("catalogName", (connectors) => {
    // Add connectors here as normal
});
```

It works the same with dependency injection:

```csharp
services.AddFlowtideStream("my_stream_name")
  ...
  .AddConnectors(connectorManager => {
    connectorManager.AddCatalog("catalogName", (connectors) => {
        // Add connectors here as normal
    });
  });
```
