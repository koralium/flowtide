---
sidebar_position: 3
---

# Azure Monitor

This section describes how to export monitoring data to *Azure Monitor*. It will show both how to export metrics, and also health check data.

## Metrics export

To export metrics information, you need to install the following nuget packages:

* OpenTelemetry.Extensions.Hosting
* Azure.Monitor.OpenTelemetry.Exporter

Next in your *Program.cs* add the following code:

```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(builder =>
    {
        builder.AddAzureMonitorMetricExporter(o =>
        {
            o.ConnectionString = "{your connection string}";
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
```

Replace *{your connection string}* with your application insights connection string. This will then start uploading custom metrics to your Application Insights in Azure Monitor.

## Health check export

If you want to publish/export health check information, install the following nuget package:

* AspNetCore.HealthChecks.Publisher.ApplicationInsights

Add the following to your *Program.cs*:

```csharp
builder.Services.AddHealthChecks()
    .AddFlowtideCheck()
    .AddApplicationInsightsPublisher("{your connection string}");
```

Replace *{your connection string}* with your application insights connection string. You should now see custom events being published to Application Insights
with health check information.

## Sample

A sample application exist for both setups in [github](https://github.com/koralium/flowtide/tree/main/samples/MonitoringAzureMonitor).