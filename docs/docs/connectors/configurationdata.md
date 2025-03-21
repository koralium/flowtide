---
sidebar_position: 1
---

# Configuration Data

It is possible to use data from IConfiguration as a data source, this can be useful if one wants to use configuration inside the stream, for instance to
filter out rows that do not exist in the configuration.

## Options Data Source

The options data source uses `IOptionsMonitor<TOptions>` to listen to changes and to get the configuration settings.

The Options data source is built-in as a default connector when installing FlowtideDotNet, so no extra nuget package is required.
Options are added with the `AddOptionsSource<TOptions>` method on the connector manager.
One of the main benefits of this connector comes if you have a configuration provider that allows reloading, then data can be updated
without restarting the stream.

Example: 
```csharp
internal class TestOptions
{
    public string? Name { get; set; }
}

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOptions<TestConfig>()
    .Bind(builder.Configuration.GetSection("config"));

builder.Services.AddFlowtideStream("test")
    .AddConnectors((connectorManager) =>
    {
        connectorManager.AddOptionsSource<TestConfig>("config");

        ...
})

...
```

Configuration settings:

```json
{
    "config": {
        "name": "hello"
    }
}
```

SQL statement:

```sql
INSERT INTO output
SELECT name FROM config
```

This stream would result in the following data:

| name  |
| ----- |
| hello |

### Example using options data source as an exclude filter

One scenario where this might be useful is to handle exclusions, say a specific row should not be sent to a destination.

You could then have an options class similar to:

```csharp
internal class TestOptions
{
    public List<string>? ExcludedIds { get; set; }
}
```

And an sql that looks the following:

```sql
CREATE VIEW excluded_ids WITH (BUFFERED = true) AS
select excludedId FROM config c
INNER JOIN UNNEST(c.excludedIds) excludedId;

INSERT INTO my_destination
SELECT m.id, m.other FROM my_table m
LEFT JOIN excluded_ids e ON m.id = e.excludedId
WHERE e.excludedId is null;
```

The buffered view helps with performance where it buffers the changes from the `UNNEST` statement so only changing rows are returned.
We can then do a left join to match with the exluded ids but only return rows where there was no match.