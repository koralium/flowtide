using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.AspNetCore.Extensions;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using OpenTelemetry.Metrics;
using MonitoringAzureMonitor;
using Azure.Monitor.OpenTelemetry.Exporter;

var builder = WebApplication.CreateBuilder(args);


builder.Services.AddOpenTelemetry()
    .WithMetrics(builder =>
    {
        builder.AddAzureMonitorMetricExporter(o =>
        {
            o.ConnectionString = "{your connection string}";
        });
        builder.AddMeter("flowtide.*");
    });

builder.Services.AddHealthChecks()
    .AddFlowtideCheck()
    .AddApplicationInsightsPublisher("{your connection string}");

var sqlBuilder = new SqlPlanBuilder();

sqlBuilder.Sql(@"
CREATE TABLE testtable (
  val any
);

CREATE TABLE other (
  val any
);

INSERT INTO output
SELECT t.val FROM testtable t
LEFT JOIN other o
ON t.val = o.val
WHERE t.val = 123;
");

var plan = sqlBuilder.GetPlan();

var factory = new ReadWriteFactory();
// Add connections here to your real data sources, such as SQL Server, Kafka or similar.
factory.AddReadResolver((readRel, opt) =>
{
    return new ReadOperatorInfo(new DummyReadOperator(opt));
});
factory.AddWriteResolver((writeRel, opt) =>
{
    return new DummyWriteOperator(opt);
});

builder.Services.AddFlowtideStream(b =>
{
    b.AddPlan(plan)
    .AddReadWriteFactory(factory)
    .WithStateOptions(() => new StateManagerOptions()
    {
        // This is non persistent storage, use FasterKV persistence storage instead if you want persistent storage
        PersistentStorage = new FileCachePersistentStorage(new FlowtideDotNet.Storage.FileCacheOptions()
        {
        })
    });
});

var app = builder.Build();

app.UseFlowtideUI("/stream");

app.Run();