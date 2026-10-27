---
sidebar_position: 1
---

# Table Provider

It is possible to add table providers to the *SqlPlanBuilder*. These providers are called each time the compiler finds the usage of a table.
It asks each provider if it contains information about that specific table.

A table provider is defined by the following interface:

```csharp
public interface ITableProvider
{
    bool TryGetTableInformation(string tableName, [NotNullWhen(true)] out TableMetadata? tableMetadata);
}
```

Creating a provider can be beneficial if you want to skip writing sql code to create table definitions for each used table.

## Registering a table provider

You register a table provider on the *SqlPlanBuilder* by calling the AddTableProvider method:

```csharp
var sqlBuilder = new SqlPlanBuilder();
sqlBuilder.AddTableProvider(new MyCustomProvider());
```

Some connectors, such as *SQL Server connector* might already come with a provider, a connector usually add an extension method to add its provider.
Example on how *SQL Server connector* provider is added:

```csharp
sqlBuilder.AddSqlServerProvider(() => connectionString);
```