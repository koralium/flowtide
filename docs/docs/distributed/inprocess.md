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
    .AddConnectorManager(substreamName =>
    {
        // Called once per substream, each substream gets its own connector manager
        var connectorManager = new ConnectorManager();
        connectorManager.AddSource(...);
        connectorManager.AddSink(...);
        return connectorManager;
    })
    .DistributeAutomatically(substreamCount: 2)
    .Build();

await stream.StartAsync();
// ...
await stream.StopAsync();
await stream.DisposeAsync();
```

## Notes

* `StartAsync` also drives the recurring connector triggers of all substreams, sources that poll for changes work without calling `RunAsync` on the individual substreams. Failures inside a substream, for example a connector that cannot initialize, do not fail the start call: the substream retries them in the background, observe them through `WithFailureListener` on the substream builders and through `Health`.
* `AddPlan` takes a plan **factory**, not a plan instance. Building a stream modifies the plan in place through connector hooks, so every substream must build from its own fresh plan.
* `WithStateOptionsFactory` is called once per substream. Substreams must not share persistent storage, each one owns its own checkpoints.
* `AddConnectorManager` takes a factory called once per substream, so connector factories holding per stream state are never shared between substreams.
* `ConfigureSubstream` is the hook for any other per substream settings on the underlying `FlowtideBuilder`, for example failure listeners or custom schedulers.
* `Health` reports the worst health across the substreams, `Pause` and `Resume` pause and resume all substreams together.
* `DistributeAutomatically(count)` applies [automatic distribution](automaticdistribution.md). Plans that already use [SQL substream statements](sqlsubstreams.md) are built as written instead.
* `StopAsync` performs the coordinated stop: the substreams run stop checkpoint cycles until the data they exchanged has been drained on both sides, bounded by the stop drain timeout (default 30 seconds, configurable per substream with `SetStopDrainTimeout` on the `FlowtideBuilder`).
* `DeleteAsync` deletes the state of every substream and completes when the deletion has fully finished.
* The built stream exposes the individual substreams through the `Substreams` dictionary, keyed by substream name.
