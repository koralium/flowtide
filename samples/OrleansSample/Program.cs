using FASTER.core;
using FlowtideDotNet.Core;
using FlowtideDotNet.Orleans.Interfaces;
using FlowtideDotNet.Orleans.Internal;
using FlowtideDotNet.Substrait.Sql;
using Orleans.Serialization;
using Orleans.Serialization.Cloning;
using Orleans.Serialization.Serializers;
using static SqlParser.Ast.DataType;
using FlowtideDotNet.Core.Sinks;
using OpenTelemetry.Metrics;
using static SqlParser.Ast.Action;
using System.Net.Security;
using OrleansSample;
using FlowtideDotNet.AspNetCore.Extensions;
using System.Net.Sockets;
using FlowtideDotNet.Core.Optimizer;

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
    
    b.Services.AddSingleton<OrleansPlanSerializer>();
    b.Services.AddSingleton<IGeneralizedCodec, OrleansPlanSerializer>();
    b.Services.AddSingleton<IGeneralizedCopier, OrleansPlanSerializer>();
    b.Services.AddSingleton<ITypeFilter, OrleansPlanSerializer>();

    var connMgr = new ConnectorManager();
    connMgr.AddSource(new DummyReadFactory("*"));
    //connMgr.AddConsoleSink("*");
    connMgr.AddBlackholeSink("*");
    b.Services.AddSingleton<IConnectorManager>(connMgr);
    //b.Services.AddSerializer(s =>
    //{
    //    s.Sys
    //})
});
// Add services to the container.

var app = builder.Build();

app.StartFlowtideMetrics("/stream");

var grainFactory = app.Services.GetRequiredService<IGrainFactory>();
var grain = grainFactory.GetGrain<IStreamGrain>("sub1");
var grain2 = grainFactory.GetGrain<IStreamGrain>("sub2");
//var grain3 = grainFactory.GetGrain<IStreamGrain>("stream3");
//var grain4 = grainFactory.GetGrain<IStreamGrain>("stream4");
//var grain5 = grainFactory.GetGrain<IStreamGrain>("stream5");
//var grain6 = grainFactory.GetGrain<IStreamGrain>("stream6");
//var grain7 = grainFactory.GetGrain<IStreamGrain>("stream7");
//var grain8 = grainFactory.GetGrain<IStreamGrain>("stream8");
// Configure the HTTP request pipeline.

app.UseOpenTelemetryPrometheusScrapingEndpoint();

await app.StartAsync();
//CREATE TABLE table1 (
//    id any
//);

//SUBSTREAM sub1;

//CREATE VIEW partitioned_data WITH(DISTRIBUTED = true) AS
//SELECT id FROM table1;

//SUBSTREAM sub2;

//INSERT INTO console
//SELECT * FROM partitioned_data;

SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
sqlPlanBuilder.Sql(@"
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
");

var plan = sqlPlanBuilder.GetPlan();

plan = PlanOptimizer.Optimize(plan, new PlanOptimizerSettings()
{
    Parallelization = 1,
    SimplifyProjection = true
});

await grain.StartStreamAsync(new FlowtideDotNet.Orleans.Messages.StartStreamMessage("stream", plan, "sub1"));
await grain2.StartStreamAsync(new FlowtideDotNet.Orleans.Messages.StartStreamMessage("stream2", plan, "sub2"));
//await grain3.StartStreamAsync(new FlowtideDotNet.Orleans.Messages.StartStreamMessage("stream3", plan, "sub2"));
//await grain4.StartStreamAsync(new FlowtideDotNet.Orleans.Messages.StartStreamMessage("stream4", plan, "sub2"));
//await grain5.StartStreamAsync(new FlowtideDotNet.Orleans.Messages.StartStreamMessage("stream5", plan, "sub2"));
//await grain6.StartStreamAsync(new FlowtideDotNet.Orleans.Messages.StartStreamMessage("stream6", plan, "sub2"));
//await grain7.StartStreamAsync(new FlowtideDotNet.Orleans.Messages.StartStreamMessage("stream7", plan, "sub2"));
//await grain8.StartStreamAsync(new FlowtideDotNet.Orleans.Messages.StartStreamMessage("stream8", plan, "sub2"));

while (true)
{
    await Task.Delay(TimeSpan.FromSeconds(10));
    GC.Collect(GC.MaxGeneration, GCCollectionMode.Aggressive, true, true);
}
await app.WaitForShutdownAsync();