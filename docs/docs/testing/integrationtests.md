---
sidebar_position: 1
---

# Integration testing

This section assumes that the Flowtide stream is implemented as a webapplication with minimal API.

Given a stream that is configured in a similar matter to this:

```csharp
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddFlowtideStream("stream")
    .AddSqlFileAsPlan("stream.sql")
    .AddConnectors(c =>
    {
        // Add empty sql server sinks that will be overridden by the tests
        c.AddSqlServerSource(() => "");
        c.AddSqlServerSink(() => "");
    })
    .AddStorage(storage =>
    {
        storage.AddFasterKVAzureStorage("", "", "");
    });

var app = builder.Build();

app.Run();

public partial class Program { }
```

Create an XUnit test project and install the following nuget packages:

* FlowtideDotNet.TestFramework
* Microsoft.AspNetCore.Mvc.Testing

## Configure WebApplicationFactory

Create a class for your integration tests and configure a WebApplicationFactory.

With the WebApplicationFactory it is possible to override connectors and storage to allow
using different connectors for the test.

```csharp
public class IntegrationTests : IDisposable
{
    private readonly WebApplicationFactory<Program> _factory;
    private readonly StreamTestMonitor _inProcessMonitor;

    public IntegrationTests() 
    {
        _factory = new WebApplicationFactory<Program>().WithWebHostBuilder(b =>
        {
            b.ConfigureTestServices(services =>
            {
                services.AddFlowtideStream("stream")
                // Add to override connectors
                .AddConnectors(c =>
                {
                    // Add new connectors here
                })
                .AddStorage(storage =>
                {
                    // Change to temporary storage for testing
                    storage.AddTemporaryDevelopmentStorage();
                })
                // Add a test monitor that can be used to check for checkpoints (which ensures data updates)
                .AddStreamTestMonitor(_inProcessMonitor);
            });
        });
    }

    public void Dispose()
    {
        _factory.Dispose();
    }
}
```

## Add a test table as a source

It is possible to create a test table where you can create mock data used for the tests.

Example:

```csharp
public class IntegrationTests : IDisposable
{
    private TestDataTable _source;
    ...

    public IntegrationTests()
    {
        _source = TestDataTable.Create(new[]
        {
            new { val = 0 },
            new { val = 1 },
            new { val = 2 },
            new { val = 3 },
            new { val = 4 }
        });

        _factory = new WebApplicationFactory<Program>().WithWebHostBuilder(b =>
        {
            b.ConfigureTestServices(services =>
            {
                services.AddFlowtideStream("stream")
                // Add to override connectors
                .AddConnectors(c =>
                {
                    c.AddTestDataTable("testtable", _source);
                })
                ...
            });
        });
    }
}
```

A test data table can be added under `AddConnectors` with the specific table name it should be registered under.

## Add a test data sink

It may be useful to add a test data sink which allows evaluating the output of the stream.
This can be used to evaluate that the query plan actually results with the expected data.

```csharp
public class IntegrationTests : IDisposable
{
    private TestDataSink _sink;
    ...

    public IntegrationTests()
    {
        _sink = new TestDataSink();

        _factory = new WebApplicationFactory<Program>().WithWebHostBuilder(b =>
        {
            b.ConfigureTestServices(services =>
            {
                services.AddFlowtideStream("stream")
                // Add to override connectors
                .AddConnectors(c =>
                {
                    ...
                    // Test data sink is added using a regexp expression that matches destination names
                    c.AddTestDataSink(".*", _sink);
                })
                ...
            });
        });
    }
}
```

## Creating a test

The next step is to create the actual test case to test the stream.

```csharp
[Fact]
public async Task TestStreamOutput()
{
    _factory.CreateClient(); //Create a client to start the stream

    // Wait for the stream to checkpoint before asserting the resultig data
    await _inProcessMonitor.WaitForCheckpoint();

    Assert.True(_sink.IsCurrentDataEqual(new[] 
    { 
        new { val = 0 },
        new { val = 1 },
        new { val = 2 },
        new { val = 3 },
        new { val = 4 }
    }));
}
```

## Full example

Here is the full example of the test class:

```csharp
public class IntegrationTests : IDisposable
{
    private readonly WebApplicationFactory<Program> _factory;
    private TestDataSink _sink;
    private TestDataTable _source;
    private StreamTestMonitor _inProcessMonitor;

    public IntegrationTests()
    {
        _source = TestDataTable.Create(new[]
        {
            new { val = 0 },
            new { val = 1 },
            new { val = 2 },
            new { val = 3 },
            new { val = 4 }
        });

        _sink = new TestDataSink();
        _inProcessMonitor = new StreamTestMonitor();
        _factory = new WebApplicationFactory<Program>().WithWebHostBuilder(b =>
        {
            b.ConfigureTestServices(services =>
            {
                services.AddFlowtideStream("stream")
                .AddConnectors(c =>
                {
                    // Override connectors
                    c.AddTestDataTable("testtable", _source);
                    c.AddTestDataSink(".*", _sink);
                })
                .AddStorage(storage =>
                {
                    // Change to temporary storage for unit tests
                    storage.AddTemporaryDevelopmentStorage();
                })
                .AddStreamTestMonitor(_inProcessMonitor);
            });
        });
    }

    public void Dispose()
    {
        _factory.Dispose();
    }

    [Fact]
    public async Task TestStreamOutput()
    {
        _factory.CreateClient(); //Create a client to start the stream

        await _inProcessMonitor.WaitForCheckpoint();

        Assert.True(_sink.IsCurrentDataEqual(new[] 
        { 
            new { val = 0 },
            new { val = 1 },
            new { val = 2 },
            new { val = 3 },
            new { val = 4 }
        }));

        // Add a new row
        _source.AddRows(new { val = 5 });

        // Remove a row
        _source.RemoveRows(new { val = 3 });

        await _inProcessMonitor.WaitForCheckpoint();

        Assert.True(_sink.IsCurrentDataEqual(new[]
        {
            new { val = 0 },
            new { val = 1 },
            new { val = 2 },
            new { val = 4 },
            new { val = 5 }
        }));
    }
}
```
