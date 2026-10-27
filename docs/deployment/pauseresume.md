---
sidebar_position: 1
---

# Pause And Resume

It is possible to pause and resume a flowtide stream, this is useful when there might be maintainance work on a source system, or as a panic button to stop
the execution.

A pause does not stop the stream, but stops the traversal of all events inside of the stream. This means that resuming a paused stream is quick and does
not need to download any state from persistent storage.

## Pause and stop using IConfiguration

One of the easier ways to add pause and resume is to utilize `IConfiguration` in .NET. Flowtide uses an `IOptionsMonitor<FlowtidePauseOptions>` to check
for changes on the configuration and pauses and resumes the stream based on the value.

The class looks as follows:

```
public class FlowtidePauseOptions
{
    public bool IsPaused { get; set; }
}
```

So to utilize this when using `FlowtideDotNet.AspNetCore` or `FlowtideDotNet.DependencyInjection` you add the following:

```
var builder = WebApplication.CreateBuilder(args);

...

// Map flowtide pause options to enable pausing and resuming from configuration
builder.Services.AddOptions<FlowtidePauseOptions>()
    .Bind(builder.Configuration.GetSection("your_section"));

...

// Add the stream as normal
builder.Services.AddFlowtideStream("stream_name")
...
```

This is best fitted with an `IConfiguration` provider that supports loading changes dynamically such as Hashicorp Vault or Azure Key Vault.
Pausing and resuming using `IConfiguration`provider is dependent on the update frequency of the provider, so to utilize this fully this interval should be
kept quite low. 

## Pause and stop using API endpoint

When using `FlowtideDotNet.AspNetCore` you can also map API endpoints to allow for pause and resume.

Example:

```
var builder = WebApplication.CreateBuilder(args);

...

// Add the stream as normal
builder.Services.AddFlowtideStream("stream_name")

...

var app = builder.Build();

...

// Map pause and resume endpoints
app.UseFlowtidePauseResumeEndpoints("base_path");
```

Two endpoints are registered under the base path, `/pause` and `/resume`.