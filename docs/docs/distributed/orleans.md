---
sidebar_position: 5
---

# Orleans Hosting

> [!WARNING]
> Distributed mode is still experimental.

The Orleans host runs every substream as a grain. The substreams spread out across the silo cluster, Orleans decides the placement, and streams recover automatically when a silo fails. Substreams on the same silo pass data to each other by reference, serialization only happens when the data crosses to another silo.

The Orleans host is in the *FlowtideDotNet.Cluster.Orleans* package.

## Setup

Add Flowtide to the silo with *AddFlowtideOrleans*:

```csharp
builder.Services.AddOrleans(b =>
{
    b.UseLocalhostClustering();
    // Grain state for the stream and substream bookkeeping, use a persistent
    // provider in production
    b.AddMemoryGrainStorage("stream_metadata");
    // Reminders keep the streams alive across silo failures, use a persistent
    // reminder service in production
    b.UseInMemoryReminderService();

    b.Services.AddFlowtideOrleans(connectors =>
    {
        connectors.AddSource(...);
        connectors.AddSink(...);
    }, (streamName, substreamName, storage) =>
    {
        // Called once per substream, every substream needs its own storage
        storage.AddTemporaryDevelopmentStorage(o =>
        {
            o.DirectoryPath = $"./temp/{streamName}/{substreamName}";
        });
    });
});
```

Two things must be registered on the silo:

* Grain storage with the name **stream_metadata**. The stream grain stores which substreams it has started there, so a stop always reaches all of them.
* **Reminders**. Every running substream registers a keep alive reminder that restarts the stream if its grain was lost with a silo. With in-memory reminders the streams survive silo failures, with persistent reminders they also survive a full cluster restart.

## Starting and stopping streams

Streams are started and stopped through the stream grain, keyed by the stream name. The stream grain prepares the plan once and sends it to every substream grain.

```csharp
var streamGrain = grainFactory.GetGrain<IStreamGrain>("my_stream");

// substreamCount applies automatic distribution to a normal plan
await streamGrain.StartStreamAsync(new StartStreamRequest(sqlText, substreamCount: 4));

// The coordinated stop drains the data exchanged between the substreams
await streamGrain.StopStreamAsync();
```

Plans that use [SQL substream statements](sqlsubstreams.md) run one grain per declared substream, *substreamCount* can then be left out.

A stream can also be started from a plan built in code:

```csharp
// The plan is serialized into the request and prepared by the stream grain,
// by default it runs through the optimizer and is split into the given substream count.
await streamGrain.StartStreamAsync(StartStreamRequest.FromPlan(plan, substreamCount: 4));

// A plan that is already optimized (or already distributed into substream roots)
// runs exactly as given.
await streamGrain.StartStreamAsync(StartStreamRequest.FromPlan(plan, optimizePlan: false));
```

A started stream keeps running the plan it was started with. Starting the same stream again with the same request does nothing, starting it with a different plan or substream count throws an exception. To deploy a new plan, stop the stream and start it again with the new one.

## Status

The start call returns before the streams have started, they start in the background. Use *GetStatusAsync* to see the substreams become healthy:

```csharp
var status = await streamGrain.GetStatusAsync();
foreach (var substream in status.Substreams)
{
    Console.WriteLine($"{substream.SubstreamName}: {substream.State} {substream.Health} {substream.LastFailure}");
}
```

A stream that can not start, for example when a connector can not initialize, retries in the background and reports the reason in *LastFailure*.

## Deleting a stream

*DeleteStreamAsync* stops all substreams and deletes their state, and completes when the deletion has finished. Only substreams that are recorded as started are reached, so to delete the state of a stopped stream, start it again first and then delete it.

```csharp
await streamGrain.DeleteStreamAsync();
```

## Options

The stop drain timeout (default 30 seconds) and other stream settings can be changed per substream:

```csharp
services.AddFlowtideOrleans(connectors => { ... }, (streamName, substreamName, storage) => { ... },
    options =>
    {
        options.ConfigureBuilder = (streamName, substreamName, flowtideBuilder) =>
        {
            flowtideBuilder.SetStopDrainTimeout(TimeSpan.FromSeconds(10));
        };
    });
```

There is also an *AddFlowtideOrleans* overload where the connectors callback gets the stream name, so different streams can use different connectors. The callback must return the same connectors for the same stream name.

## Silo failures

Substream grains on a lost silo are restarted on the surviving silos by their reminders. They roll back together with the substreams they exchange data with and catch up by replaying from the sources.

## Migration and rebalancing

A substream grain can be moved to another silo without a recovery, for example to rebalance load:

```csharp
var substreamGrain = grainFactory.GetGrain<ISubStreamGrain>(
    SubStreamGrainKey.Create("my_stream", "substream_0"));
await substreamGrain.MigrateAsync();
```

The grain drains the data exchanged with the other substreams, stops at a final checkpoint and migrates. The new activation restores that checkpoint and reconnects, the other substreams keep running and nothing is replayed. This requires state storage that is reachable from every silo. With silo-local storage the reconnect is refused and the stream falls back to normal recovery instead.

A migration that can not complete cleanly, for example when a peer is unreachable, falls back to the same recovery as a silo failure. Orleans can also decide to skip a requested migration, the keep alive reminder then restarts the stream in place.

## Metrics

*app.StartFlowtideMetrics("/stream")* exposes the Flowtide metrics endpoints without the UI, which fits silo hosts. The monitoring described under [Monitoring](../monitoring/generalmetrics.md) applies per substream.

A runnable example is available in the repository under *samples/OrleansSample*.
