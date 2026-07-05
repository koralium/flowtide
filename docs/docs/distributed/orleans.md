---
sidebar_position: 5
---

# Orleans Hosting

> [!WARNING]
> Distributed mode is still experimental.

The Orleans host runs every substream as a grain. The substreams of a stream spread across the silo cluster, Orleans handles the placement, and streams recover automatically when a silo fails.

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

* Grain storage with the name **stream_metadata**. The stream grain persists which substreams it started there, so a stop always reaches every started substream.
* **Reminders**. Every running substream registers a keep alive reminder that restarts the stream when its grain was lost with a silo. With in-memory reminders the streams survive individual silo failures, persistent reminders also survive full cluster restarts.

## Starting and stopping streams

Streams are started and stopped through the stream grain. The grain is keyed by the stream name and only carries the SQL text, every substream grain builds its own plan from it.

```csharp
var streamGrain = grainFactory.GetGrain<IStreamGrain>("my_stream");

// substreamCount applies automatic distribution to a normal plan
await streamGrain.StartStreamAsync(new StartStreamRequest(sqlText, substreamCount: 4));

// The coordinated stop drains the data exchanged between the substreams
await streamGrain.StopStreamAsync();
```

Plans that use [SQL substream statements](sqlsubstreams.md) run one grain per declared substream instead, *substreamCount* can then be omitted.

A started stream keeps running the plan it was started with. Starting the same stream again with the identical request does nothing, starting it with a different SQL text or substream count throws an exception. To deploy a new plan, stop the stream and start it with the new SQL.

## Status

The start call returns before the streams have started, they start in the background. Use *GetStatusAsync* to see the substreams becoming healthy:

```csharp
var status = await streamGrain.GetStatusAsync();
foreach (var substream in status.Substreams)
{
    Console.WriteLine($"{substream.SubstreamName}: {substream.State} {substream.Health} {substream.LastFailure}");
}
```

A stream that cannot start, for example because a connector cannot initialize, retries in the background and reports the reason in *LastFailure*.

## Deleting a stream

*DeleteStreamAsync* stops all substreams and deletes their state, and completes when the deletion has finished. Only substreams recorded as started are reached, to delete the state of a stopped stream, start it again first and then delete.

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

There is also an *AddFlowtideOrleans* overload where the connectors callback receives the stream name, which allows different streams to use different connectors. The callback must return the same connectors for the same stream name, every substream grain builds its plan from them.

## Silo failures

Substream grains on a lost silo are reactivated on the surviving silos by their reminders. They roll back together with the substreams they exchange data with and catch up by replaying from the sources. Fetches from abandoned stream instances are refused so they can not steal data from the recovered streams.

## Metrics

*app.StartFlowtideMetrics("/stream")* exposes the Flowtide metrics endpoints without the UI, which fits silo hosts. The monitoring described under [Monitoring](../monitoring/generalmetrics.md) applies per substream.

A runnable example is available in the repository under *samples/OrleansSample*.
