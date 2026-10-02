---
sidebar_position: 4
---

# OpenLineage

Flowtide has built-in support for reporting data lineage events to an [OpenLineage](https://openlineage.io/)-compatible endpoint over HTTP.
Events can also be sent to a Kafka topic with the `FlowtideDotNet.OpenLineage.Kafka` package, see [Kafka](#kafka).
When enabled, the reporter automatically sends lineage events as the stream transitions between states (starting, running, completed, or failed).

## Setup with Dependency Injection

If you have added your stream using `AddFlowtideStream`, you can enable OpenLineage HTTP reporting with the `AddOpenLineageHttp` extension method.

Install the following NuGet packages:

* FlowtideDotNet.DependencyInjection

Add the following code to your *Program.cs*:

```csharp
builder.Services.AddFlowtideStream("mystream")
    .AddOpenLineageHttp(opt =>
    {
        opt.Url = "http://localhost:5000/api/v1/lineage";
    });
```

## Setup with FlowtideBuilder

If you are using `FlowtideBuilder` directly, call `WithOpenLineageHttp`:

```csharp
var builder = new FlowtideBuilder("mystream")
    .AddPlan(plan)
    .WithOpenLineageHttp(new OpenLineageHttpOptions
    {
        Url = "http://localhost:5000/api/v1/lineage"
    });
```

## Configuration Options

The following options are available on `OpenLineageHttpOptions`:

| Option          | Type                              | Required | Description                                                                                     |
| --------------- | --------------------------------- | -------- | ----------------------------------------------------------------------------------------------- |
| Url             | `string?`                         | Yes      | The URL of the OpenLineage HTTP endpoint to send lineage events to.                             |
| IncludeSchema   | `bool`                            | No       | Whether to include schema information in the lineage events. Defaults to `false`.               |
| RunId           | `Guid?`                           | No       | A custom run identifier. If not set, a new `Guid` is generated automatically for each stream.   |
| OnRequest       | `Action<HttpRequestMessage>?`     | No       | A callback invoked on each outgoing HTTP request before it is sent.                             |

## Adding Authentication Headers

Use the `OnRequest` callback to modify outgoing HTTP requests, for example to add an authorization header:

```csharp
builder.Services.AddFlowtideStream("mystream")
    .AddOpenLineageHttp(opt =>
    {
        opt.Url = "http://localhost:5000/api/v1/lineage";
        opt.OnRequest = message =>
        {
            message.Headers.Authorization =
                new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", "my-token");
        };
    });
```

## Including Schema Information

Set `IncludeSchema` to `true` to include dataset schema (column-level) information in the lineage events:

```csharp
builder.Services.AddFlowtideStream("mystream")
    .AddOpenLineageHttp(opt =>
    {
        opt.Url = "http://localhost:5000/api/v1/lineage";
        opt.IncludeSchema = true;
    });
```

## Kafka

The `FlowtideDotNet.OpenLineage.Kafka` package sends the events to a Kafka topic, following the format of the OpenLineage Kafka transport:

* Each event is written as one JSON message to the configured topic.
* The message key is `run:{job namespace}/{job name}`, for a stream named `mystream` this is `run:flowtide/mystream`. Set `MessageKey` to use another key.

Install the following NuGet package:

* FlowtideDotNet.OpenLineage.Kafka

### Setup with Dependency Injection

```csharp
builder.Services.AddFlowtideStream("mystream")
    .AddOpenLineageKafka(opt =>
    {
        opt.ProducerConfig = new ProducerConfig
        {
            BootstrapServers = "localhost:9092"
        };
        opt.TopicName = "openlineage.events";
    });
```

### Setup with FlowtideBuilder

```csharp
var builder = new FlowtideBuilder("mystream")
    .AddPlan(plan)
    .WithOpenLineageKafka(new OpenLineageKafkaOptions
    {
        ProducerConfig = new ProducerConfig
        {
            BootstrapServers = "localhost:9092"
        },
        TopicName = "openlineage.events"
    });
```

### Kafka Configuration Options

The following options are available on `OpenLineageKafkaOptions`:

| Option          | Type              | Required | Description                                                                                   |
| --------------- | ----------------- | -------- | --------------------------------------------------------------------------------------------- |
| ProducerConfig  | `ProducerConfig?` | Yes      | The Kafka producer configuration, such as bootstrap servers and authentication settings.      |
| TopicName       | `string?`         | Yes      | The topic the lineage events are written to.                                                  |
| MessageKey      | `string?`         | No       | The key for every message. Defaults to `run:{job namespace}/{job name}`.                      |
| IncludeSchema   | `bool`            | No       | Whether to include schema information in the lineage events. Defaults to `false`.             |
| RunId           | `Guid?`           | No       | A custom run identifier. If not set, a new `Guid` is generated automatically for each stream. |

## Custom Transports

To send the events somewhere else, implement `IOpenLineageTransport` and register it with `WithOpenLineage`:

```csharp
var builder = new FlowtideBuilder("mystream")
    .AddPlan(plan)
    .WithOpenLineage(new OpenLineageOptions(), () => new MyTransport());
```

A transport receives each event as JSON together with the job namespace and name. If `EmitAsync` throws, the event is retried.

## Reported Events

The reporter listens to stream state changes and sends OpenLineage events accordingly:

| Stream State | OpenLineage Event Type | Description                                          |
| ------------ | ---------------------- | ---------------------------------------------------- |
| Starting     | START                  | The stream is starting up.                           |
| Running      | RUNNING                | The stream is running normally.                      |
| Stopped      | COMPLETE               | The stream has been stopped after a stopping request. |
| Failure      | FAIL                   | The stream has encountered an error.                 |