---
sidebar_position: 2
---

# Prometheus

It is possible to export the stream metrics to *Prometheus* using *OpenTelemetry*.
The prometheus exporter is right now in preview, but it can be used to set up an exporter if you ran an .ASPNetCore project.

To setup the exporter, install the following nuget packages:

* OpenTelemetry.Exporter.Prometheus.AspNetCore
* OpenTelemetry.Extensions.Hosting

Add the following code to your *Program.cs* if you run minimal API:

```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(builder =>
    {
        builder.AddPrometheusExporter();

        // Configure a view to get unique names for each metric
        builder.AddView((instrument) =>
        {
            return new MetricStreamConfiguration()
            {
                Name = $"{instrument.Meter.Name}.{instrument.Name}"
            };
        });
        // Add all flowtide metrics
        builder.AddMeter("flowtide.*");
    });

...

// Adds the scraping endpoint
app.UseOpenTelemetryPrometheusScrapingEndpoint();
```

If you dont add the view, you will get multiple instruments with the same name in the export.
Now if you visit '/metrics' when you run your app, you should see something similar to this:

```
# TYPE flowtide_stream_health gauge
flowtide_stream_health 1 1698504668714

# TYPE flowtide_stream_operator_3_busy gauge
flowtide_stream_operator_3_busy 0.9800000190734863 1698504668714

# TYPE flowtide_stream_operator_3_backpressure gauge
flowtide_stream_operator_3_backpressure 0 1698504668714

# TYPE flowtide_stream_operator_3_health gauge
flowtide_stream_operator_3_health 1 1698504668714

# TYPE flowtide_stream_operator_3_events_total counter
flowtide_stream_operator_3_events_total 691000 1698504668714

# TYPE flowtide_stream_operator_2_busy gauge
flowtide_stream_operator_2_busy 0 1698504668714

# TYPE flowtide_stream_operator_2_backpressure gauge
flowtide_stream_operator_2_backpressure 0 1698504668714

# TYPE flowtide_stream_operator_2_health gauge
flowtide_stream_operator_2_health 1 1698504668714

# TYPE flowtide_stream_operator_1_events_total counter
flowtide_stream_operator_1_events_total 691000 1698504668714

# TYPE flowtide_stream_operator_1_busy gauge
flowtide_stream_operator_1_busy 0 1698504668714

# TYPE flowtide_stream_operator_1_backpressure gauge
flowtide_stream_operator_1_backpressure 0 1698504668714

# TYPE flowtide_stream_operator_1_health gauge
flowtide_stream_operator_1_health 1 1698504668714

# TYPE flowtide_stream_operator_0_busy gauge
flowtide_stream_operator_0_busy 0 1698504668714

# TYPE flowtide_stream_operator_0_InputQueue gauge
flowtide_stream_operator_0_InputQueue 0 1698504668714

# TYPE flowtide_stream_operator_0_health gauge
flowtide_stream_operator_0_health 1 1698504668714

# TYPE flowtide_stream_operator_4_backpressure gauge
flowtide_stream_operator_4_backpressure 1 1698504668714

# TYPE flowtide_stream_operator_4_busy gauge
flowtide_stream_operator_4_busy 0 1698504668714

# TYPE flowtide_stream_operator_4_health gauge
flowtide_stream_operator_4_health 1 1698504668714

# TYPE flowtide_stream_operator_5_backpressure gauge
flowtide_stream_operator_5_backpressure 1 1698504668714

# TYPE flowtide_stream_operator_5_busy gauge
flowtide_stream_operator_5_busy 0 1698504668714

# TYPE flowtide_stream_operator_5_health gauge
flowtide_stream_operator_5_health 1 1698504668714

# EOF
```

## Sample

You can find a sample in [github](https://github.com/koralium/flowtide/tree/main/samples/MonitoringPrometheus) to see how it can be setup.