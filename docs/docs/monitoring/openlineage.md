---
sidebar_position: 4
---

# OpenLineage

Flowtide has built-in support for reporting data lineage events to an [OpenLineage](https://openlineage.io/)-compatible endpoint over HTTP.
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

## Reported Events

The reporter listens to stream state changes and sends OpenLineage events accordingly:

| Stream State | OpenLineage Event Type | Description                                          |
| ------------ | ---------------------- | ---------------------------------------------------- |
| Starting     | START                  | The stream is starting up.                           |
| Running      | RUNNING                | The stream is running normally.                      |
| Stopped      | COMPLETE               | The stream has been stopped after a stopping request. |
| Failure      | FAIL                   | The stream has encountered an error.                 |