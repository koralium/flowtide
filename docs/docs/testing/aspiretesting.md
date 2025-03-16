---
sidebar_position: 2
---

# .NET Aspire Testing

If you run your stream with the help of .NET Aspire it may be beneficial to run the tests using the Aspire test framework.
This page describes how `FlowtideDotNet.TestFramework` can help you test your stream more easily.

Testing an aspire project requires using HTTP calls to check the stream status, instead of injecting other type of monitors
to the stream to check the status. Since this endpoint can contain exception information, this means that the test endpoint could cause a security issue and should only be mapped during tests.

## Add the test information HTTP endpoint

To be able to get information about the stream status such as checkpoint information, you must map
a helper endpoint in your stream application. This is done by calling `MapFlowtideTestInformation`.

Example:

```csharp
if (builder.Configuration.GetValue<bool>("TEST_MODE"))
{
    // If we are in test mode, map the test endpoint
    app.MapFlowtideTestInformation();
}
```

## Enable the test mode configuration in the Aspire configuration

The next step is to add in .NET Aspire configuration that the test mode configuration is enabled:

```csharp
var project = builder.AddProject<MyStreamProject>("stream");

...

if (builder.Configuration.GetValue<bool>("test_mode"))
{
    project = project.WithEnvironment("TEST_MODE", "true");
}
```

This enables turning on test mode from the integration tests.

## Wait for checkpoints in the integration test

The final step is to start checking for checkpoints in the integration tests.
Since a checkpoint happens after a data change and is considered a stable state, it is best
to run assertions first after a checkpoint has happened.

```csharp
[Fact]
public async Task MyTest()
{
    var appHost = await DistributedApplicationTestingBuilder.CreateAsync<Projects.MyAspire>([
        "test_mode=true"
    ]);

    await using var app = await appHost.BuildAsync();
    var resourceNotificationService = app.Services.GetRequiredService<ResourceNotificationService>();
    await app.StartAsync();

    var httpClient = app.CreateHttpClient("my_project_resource_name");

    await resourceNotificationService.WaitForResourceAsync("my_project_resource_name", KnownResourceStates.Running).WaitAsync(TimeSpan.FromSeconds(30));

    // Create the stream http monitor
    var streamMonitor = new StreamTestHttpMonitor(httpClient, "my_stream_name");

    // Wait for a checkpoint to occur
    await streamMonitor.WaitForCheckpoint();

    // Assert here against your destination
}
```

If you want to test modifying data while the stream is running, it may be benifical to pause and resume it.
To do that, also add `UseFlowtidePauseResumeEndpoints` to the exposed endpoints when testing. Please read [Pause And Resume](../deployment/pauseresume.md#pause-and-stop-using-api-endpoint) for more information.