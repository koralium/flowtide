using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Substrait.Sql;
using MonitoringPrometheus;
using FlowtideDotNet.AspNetCore.Extensions;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using OpenTelemetry.Metrics;
using FlowtideDotNet.Core;
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

var sqlText = @"
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
";

builder.Services.AddFlowtideStream("PrometheusSample")
    .AddSqlTextAsPlan(sqlText)
    .AddConnectors(connectorManager =>
    {
        connectorManager.AddSource(new DummyReadFactory("*"));
        connectorManager.AddSink(new DummyWriteFactory("*"));
    })
    .AddStorage(storage =>
    {
        storage.AddTemporaryDevelopmentStorage();
    });

builder.Services.AddCors(o =>
{
    o.AddPolicy("AllowAll", builder =>
    {
        builder.AllowAnyOrigin()
            .AllowAnyMethod()
            .AllowAnyHeader();
    });
});

var app = builder.Build();

app.UseCors("AllowAll");
// Configure the HTTP request pipeline.
app.UseOpenTelemetryPrometheusScrapingEndpoint();
app.UseFlowtideUI("/stream");

app.Run();