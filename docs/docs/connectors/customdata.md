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

To use your data source, add the following to the *ReadWriteFactory*:

```csharp
factory.AddGenericDataSource(
    "{regex for the table name}", 
    (readRelation) => new ExampleDataSource(userRepository));
```

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
