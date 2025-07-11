---
sidebar_position: 6
---

# Custom Data Source

It is possible to fetch data from a custom source such as an API, other database, file system, etc.

There are multiple ways:

* **Implement GenericDataSource(Async)** - Simplified source which should return C# objects. This is the recomended way to start implementing a source.
* **Implement ReadBaseOperator** - This allows the creation of a low-level read operator, where serialization, state storage, watermarks and checkpointing must be handled.

## Generic Data Source

The generic data source allows easy implementation against custom sources that returns C# objects.

It allows:

* Full batch reloads, where all the data is imported again and delta is computed.
* Delta loads, where delta should be returned.
* Custom watermark provided by the source.
* Scheduling of full batch and delta reloads.

There are two classes that can be implemented for the generic data source:

* **GenericDataSourceAsync** - Data is returned by an IAsyncEnumerable, this should be used with remote sources.
* **GenericDataSource** - Data is returned by an IEnumerable, which should be used in cases where data is already in memory.

When implementing a generic data source, it is important to think about memory usage, for instance, do not
fetch all rows and store them in memory and then return them, this can cause huge memory spikes or out of memory.
Instead yield return values and fetch the data in batches. The operator stores the data in B+ trees that will be
temporarily stored on disk if the memory usage is too high.

### Implementation example

```csharp
public class ExampleDataSource : GenericDataSourceAsync<User>
{
    private readonly IUserRepository _userRepository;

    public ExampleDataSource(IUserRepository userRepository) 
    {
        _userRepository = userRepository;
    }

    // Fetch delta every 1 second
    public override TimeSpan? DeltaLoadInterval => TimeSpan.FromSeconds(1);

    // Reload all data every 1 hours, this is not required, but can be useful.
    // If for instance deletes cant be found in deltas from the source,
    // a full reload would find all deleted rows.
    public override TimeSpan? FullLoadInterval => TimeSpan.FromHours(1);

    protected override IEnumerable<FlowtideGenericObject<User>> DeltaLoadAsync(long lastWatermark)
    {
        var changes = _userRepository.GetChangesFromWatermarkAsync(lastWatermark);

        await foreach(var change in changes) {
            yield return new FlowtideGenericObject<User>(change.Id, change, change.Timestamp);
        }
    }

    protected override IEnumerable<FlowtideGenericObject<User>> FullLoadAsync()
    {
        var data = _userRepository.GetAllDataAsync(lastWatermark);
        
        await foreach(var row in data) {
            yield return new FlowtideGenericObject<User>(row.Id, row, row.Timestamp);
        }
    }
}
```

To use your data source, add the following to the *ConnectorManager*:

```csharp
connectorManager.AddCustomSource(
    "{the table name}", 
    (readRelation) => new ExampleDataSource(userRepository));
```

All rows must return an identifier that is used when calculating changes of the data. This key can be accessed when querying with the special name of `__key`.

### Trigger data reloads programatically

The generic data source also registers triggers that allows the user to notify the stream when a reload should happen.

The following triggers are registered:

* **full_load** - Does a full load on all running generic data sources
* **delta_load** - Does a delta load on all running generic data sources
* **full_load_\{tableName\}** - Full load for a specific source
* **delta_load_\{tableName\}** - Delta load for a specific source

Example on calling a trigger:

```csharp
await stream.CallTrigger("delta_load", default);
```

Calling the triggers programatically can be useful if having an interval would cause too much latency for the data.

### SQL Table Provider

There is a table provider as well for the generic data source, that can be used to easily import table metadata from a class.

Example:

```csharp
sqlPlanBuilder.AddGenericDataTable<User>("users");
```

If you are starting Flowtide with dependency injection, a table provider is added automatically, so this step is not required.

## Generic data sink

The generic data sink allow the implementation of a sink that reads the rows as C# classes.
This limits the stream to only send those specific columns, but it can be useful im cases such as API integrations where there is a strict schema.

### Implementation example

Create a class that inherits from *GenericDataSink*.

```csharp
internal class TestDataSink : GenericDataSink<User>
{
    public override Task<List<string>> GetPrimaryKeyNames()
    {
        return Task.FromResult(new List<string> { "{primaryKeyColumnName}" });
    }

    public override async Task OnChanges(IAsyncEnumerable<FlowtideGenericWriteObject<User>> changes, Watermark watermark, bool isInitialData, CancellationToken cancellationToken)
    {
        await foreach(var userChange in changes)
        {
            if (!userChange.IsDeleted)
            {
                // Do upsert to destination
            }
            else
            {
                // Do delete against destination
            }
        }
    }
}
```

Add the generic data sink to the *ConnectorManager*:

```csharp
connectorManager.AddCustomSink("{tableName}", (rel) => new testDataSink());
```

## Adding custom converters

It is possible to register custom converters to convert .NET objects into the column format, and back. This is done by overriding the `GetCustomConverters` method in both
generic sources and sinks.

One example is for instance to use strings for enums instead of integers. Example:

```csharp
internal class MyDataSource : GenericDataSource<MyClass>
{
    ...

    public override IEnumerable<IObjectColumnResolver> GetCustomConverters()
    {
        // Returns a new converter resolver for enums that will use strings instead of integers
        yield return new EnumResolver(enumAsStrings: true);
    }

    ...
}
```