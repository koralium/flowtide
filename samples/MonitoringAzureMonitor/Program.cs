using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.AspNetCore.Extensions;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using MonitoringAzureMonitor;
using Azure.Monitor.OpenTelemetry.Exporter;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Core;
using FlowtideDotNet.DependencyInjection;
using FlowtideDotNet.Core.Sources.Generic;

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

builder.Services.AddFlowtideStream("AzureMonitorSample")
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

var app = builder.Build();

app.UseFlowtideUI("/stream");

app.Run();