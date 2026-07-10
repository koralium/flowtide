using FlowtideDotNet.Cluster.Orleans.Interfaces;
using OpenTelemetry.Metrics;
using OrleansSample;
using FlowtideDotNet.AspNetCore.Extensions;
using SqlSampleWithUI;
using FlowtideDotNet.DependencyInjection;
using FlowtideDotNet.Core.Sinks;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOrleans(b =>
{
    b.UseLocalhostClustering();
    b.AddMemoryGrainStorage("stream_metadata");
    // Backs the substream keep alive reminder, use a persistent reminder
    // service in production so streams restart after a full cluster restart.
    b.UseInMemoryReminderService();
    b.Services.AddFlowtideOrleans(c =>
    {
        c.AddSource(new DummyReadFactory("*"));
        c.AddSink(new DummyWriteFactory("*"));
    }, (streamName, substreamName, storage) =>
    {
        storage.MaxPageCount = 1_000_000;
        storage.AddFileStorage($"./temp/{streamName}/{substreamName}");
    });
});


var app = builder.Build();

app.StartFlowtideMetrics("/stream");

var grainFactory = app.Services.GetRequiredService<IGrainFactory>();
var streamGrain = grainFactory.GetGrain<IStreamGrain>("stream");

await app.StartAsync();

await streamGrain.StartStreamAsync(new FlowtideDotNet.Cluster.Orleans.Messages.StartStreamRequest(@"
CREATE TABLE table1 (val any);
CREATE TABLE table2 (val any);

INSERT INTO output
SELECT
    a.val
FROM table1 a
LEFT JOIN table2 b
ON a.val = b.val;
", substreamCount: 8));

await app.WaitForShutdownAsync();