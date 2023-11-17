using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Substrait.Sql;
using MonitoringPrometheus;
using FlowtideDotNet.AspNetCore.Extensions;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using OpenTelemetry.Metrics;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenTelemetry()
    .WithMetrics(builder =>
    {
        builder.AddPrometheusExporter(o =>
        {
            
        });
        builder.AddView((instrument) =>
        {
            return new MetricStreamConfiguration()
            {
                Name = $"{instrument.Meter.Name}.{instrument.Name}"
            };
        });
        builder.AddMeter("flowtide.*");
    });

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

// Configure the HTTP request pipeline.
app.UseOpenTelemetryPrometheusScrapingEndpoint();
app.UseFlowtideUI("/stream");

app.Run();

internal record WeatherForecast(DateOnly Date, int TemperatureC, string? Summary)
{
    public int TemperatureF => 32 + (int)(TemperatureC / 0.5556);
}
