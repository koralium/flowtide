---
sidebar_position: 4
---

# In-Process Hosting

> [!WARNING]
> Distributed mode is still experimental.

The in-process host runs every substream inside one process. It behaves the same as a real distributed deployment, including coordinated checkpoints, stop drain and failure recovery. This makes it useful for testing plans and connectors before deploying them, and for verifying how a plan distributes.

To create a distributed stream, use the *DistributedStreamBuilder*:

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
        // Every substream needs its own state storage
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

*AddPlan* takes a plan **factory** and not a plan instance. Building a stream modifies the plan in place, so every substream must build from its own fresh plan.

*DistributeAutomatically* applies [automatic distribution](automaticdistribution.md). Plans that already use [SQL substream statements](sqlsubstreams.md) are built as written instead.

## Starting

*StartAsync* starts all substreams and also drives their recurring connector triggers, there is no need to call *RunAsync* on the individual substreams.

Failures inside a substream, for example a connector that can not initialize, do not fail the start call. The substream retries them in the background. Failures can be observed with *WithFailureListener* on the substream builders and through the *Health* property.

## Stopping

*StopAsync* performs the coordinated stop. The substreams run stop checkpoint cycles until the data they exchanged has been drained on both sides. The drain is bounded by the stop drain timeout, default 30 seconds, which can be changed with *SetStopDrainTimeout* on the substream builders.

*DeleteAsync* deletes the state of every substream and completes when the deletion has finished.

## Other members

* *Health* reports the worst health across the substreams.
* *Pause* and *Resume* pause and resume all substreams together.
* *ConfigureSubstream* is a hook for any other per substream settings on the underlying *FlowtideBuilder*, for example failure listeners.
* *Substreams* exposes the individual substreams, keyed by substream name.
