# Orleans Distributed Hosting

> [!WARNING]
> Distributed mode is still experimental.

The Orleans host runs every substream as a grain, so the substreams of a stream spread across the silo cluster with Orleans handling placement, and streams recover automatically when a silo fails.

## Setup

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
        // Called once per substream, every substream needs its OWN storage
        storage.AddTemporaryDevelopmentStorage(o =>
        {
            o.DirectoryPath = $"./temp/{streamName}/{substreamName}";
        });
    });
});
```

Streams are started and stopped through the stream grain. The grains are keyed by stream name and only carry the SQL text, every substream grain builds its own plan from it:

```csharp
var streamGrain = grainFactory.GetGrain<IStreamGrain>("my_stream");

// substreamCount applies automatic distribution to a normal plan
await streamGrain.StartStreamAsync(new StartStreamRequest(sqlText, substreamCount: 4));

// Starting returns before the streams have started in the background,
// poll the status to observe the substreams becoming healthy
var status = await streamGrain.GetStatusAsync();
foreach (var substream in status.Substreams)
{
    Console.WriteLine($"{substream.SubstreamName}: {substream.State} {substream.Health} {substream.StartFailure}");
}

// The grain remembers which substreams it started, the coordinated stop
// drains the data exchanged between them
await streamGrain.StopStreamAsync();
```

Plans that use [SQL substream statements](sqlsubstreams.md) run one grain per declared substream instead, `substreamCount` can then be omitted.

A started stream keeps running the plan it was started with: starting the same stream again with the identical request is a no-op, starting it with a different SQL text or substream count throws. To deploy a new plan version, stop the stream and start it with the new SQL.

`GetStatusAsync` is also how background start failures surface. The start call returns success once the substream grains accepted the start, the streams themselves start asynchronously — a stream that cannot start, for example because its storage is unreachable, reports the failure in `StartFailure` and is retried by the keep alive watchdog.

## Requirements

* **Grain storage named `stream_metadata`** must be registered. The stream grain persists which substreams it started there, which is what lets the argument free stop reach every started substream, and the substream grains persist their start records there.
* **Reminders** must be registered. Every running substream registers a keep alive reminder that restarts the stream when its grain was lost with a silo, and recreates a grain whose stream is stuck. With in-memory reminders the streams survive individual silo failures as long as the cluster itself lives, persistent reminders survive full restarts.

## Operational behavior

* **Silo failure**: substream grains on the lost silo are reactivated on surviving silos by their reminders, roll back together with the substreams they exchange data with, and catch up by replay. Fetches from abandoned stream instances are fenced by fetch epochs so they cannot steal data from the recovered streams.
* **Stopping** uses the same coordinated drain as the in-process host, bounded by the stop drain timeout (default 30 seconds). It can be changed through the options:

```csharp
services.AddFlowtideOrleans(connectors => { ... }, (streamName, substreamName, storage) => { ... },
    options =>
    {
        options.ConfigureBuilder = flowtideBuilder =>
        {
            flowtideBuilder.SetStopDrainTimeout(TimeSpan.FromSeconds(10));
        };
    });
```

* **Status**: `IStreamGrain.GetStatusAsync` reports every started substream with its stream state, health and any background start failure — use it for readiness checks and to verify a deployment actually came up.
* **Metrics**: `app.StartFlowtideMetrics("/stream")` exposes the Flowtide metrics endpoints without the UI, which fits silo hosts. The regular Flowtide monitoring described under [Monitoring](../monitoring/generalmetrics.md) applies per substream.

A runnable example is available in the repository under `samples/OrleansSample`.
