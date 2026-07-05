# In-Process Distributed Hosting

> [!WARNING]
> Distributed mode is still experimental.

The in-process host runs every substream inside one process, with the exchange between them passing events by reference. It behaves exactly like a real distributed deployment, including coordinated checkpoints, stop drain and failure recovery, which makes it useful for testing plans and connectors before deploying them, and for verifying how a plan distributes.

```csharp
var stream = new DistributedStreamBuilder("my_stream")
    .AddPlan(() =>
    {
        var sqlPlanBuilder = new SqlPlanBuilder();
        sqlPlanBuilder.Sql(@"
        INSERT INTO output
        SELECT u.userkey FROM users u
        INNER JOIN orders o ON u.userkey = o.userkey;
        ");
        return sqlPlanBuilder.GetPlan();
    })
    .WithStateOptionsFactory(substreamName => new StateManagerOptions
    {
        // Every substream needs its OWN state storage
        PersistentStorage = CreateStorageFor(substreamName)
    })
    .ConfigureSubstream((substreamName, flowtideBuilder) =>
    {
        var connectorManager = new ConnectorManager();
        connectorManager.AddSource(...);
        connectorManager.AddSink(...);
        flowtideBuilder.AddConnectorManager(connectorManager);
    })
    .DistributeAutomatically(substreamCount: 2)
    .Build();

await stream.StartAsync();
// ...
await stream.StopAsync();
await stream.DisposeAsync();
```

## Notes

* `AddPlan` takes a plan **factory**, not a plan instance. Building a stream modifies the plan in place through connector hooks, so every substream must build from its own fresh plan.
* `WithStateOptionsFactory` is called once per substream. Substreams must not share persistent storage, each one owns its own checkpoints.
* `ConfigureSubstream` runs for every substream and configures its connectors and any per substream settings on the underlying `FlowtideBuilder`.
* `DistributeAutomatically(count)` applies [automatic distribution](automaticdistribution.md). Plans that already use [SQL substream statements](sqlsubstreams.md) are built as written instead.
* `StopAsync` performs the coordinated stop: the substreams run stop checkpoint cycles until the data they exchanged has been drained on both sides, bounded by the stop drain timeout (default 30 seconds, configurable per substream with `SetStopDrainTimeout` on the `FlowtideBuilder`).
* `DeleteAsync` deletes the state of every substream and completes when the deletion has fully finished.
* The built stream exposes the individual substreams through the `Substreams` dictionary, keyed by substream name.
