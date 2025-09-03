using FlowtideDotNet.Orleans.Interfaces;
using OpenTelemetry.Metrics;
using OrleansSample;
using FlowtideDotNet.AspNetCore.Extensions;
using SqlSampleWithUI;
using FlowtideDotNet.DependencyInjection;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenTelemetry()
    .WithMetrics(builder =>
    {
        builder.AddPrometheusExporter(o =>
        {

        });
        builder.AddMeter("flowtide.*");
    });

builder.Services.AddOrleans(b =>
{
    b.UseLocalhostClustering();
    b.AddMemoryGrainStorage("stream_metadata");
    b.Services.AddFlowtideOrleans(c =>
    {
        c.AddSource(new DummyReadFactory("*"));
        c.AddSink(new DummyWriteFactory("*"));
    }, (streamName, substreamName, storage) =>
    {
        storage.MaxPageCount = 100_000;
        storage.AddTemporaryDevelopmentStorage(b =>
        {
            b.DirectoryPath = $"./temp/{streamName}/{substreamName}";
        });
    });
});
// Add services to the container.


var app = builder.Build();

app.StartFlowtideMetrics("/stream");

var grainFactory = app.Services.GetRequiredService<IGrainFactory>();
var streamGrain = grainFactory.GetGrain<IStreamGrain>("stream");

app.UseOpenTelemetryPrometheusScrapingEndpoint();

await app.StartAsync();

await streamGrain.StartStreamAsync(new FlowtideDotNet.Orleans.Messages.StartStreamRequest(@"
CREATE TABLE table1 (val any);
CREATE TABLE table2 (val any);

SUBSTREAM sub1;

CREATE VIEW read_table_1_stream1 WITH (DISTRIBUTED = true, SCATTER_BY = val, PARTITION_COUNT = 2) AS
SELECT val FROM table1;

SUBSTREAM sub2;

CREATE VIEW read_table_2_stream2 WITH (DISTRIBUTED = true, SCATTER_BY = val, PARTITION_COUNT = 2) AS
SELECT val FROM table2;

SUBSTREAM sub1;

INSERT INTO output
SELECT 
    a.val 
FROM read_table_1_stream1 a WITH (PARTITION_ID = 0)
LEFT JOIN read_table_2_stream2 b WITH (PARTITION_ID = 0)
ON a.val = b.val;

SUBSTREAM sub2;

INSERT INTO output
SELECT 
    a.val 
FROM read_table_1_stream1 a WITH (PARTITION_ID = 1)
LEFT JOIN read_table_2_stream2 b WITH (PARTITION_ID = 1)
ON a.val = b.val;
"));

await app.WaitForShutdownAsync();