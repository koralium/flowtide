using CustomSinkSample;
using FlowtideDotNet.AspNetCore.Extensions;
using FlowtideDotNet.Core.Sources.Generic;
using FlowtideDotNet.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;


HostApplicationBuilder builder = Host.CreateApplicationBuilder(args);

List<SinkModel> existingData = new List<SinkModel>();

Console.WriteLine("Enter rows that should already exist in the destination (sink). This demonstrates how Flowtide fetches existing data during sink initialization.");
Console.WriteLine("Enter rows in the format '<id> <name>'. Type 'C' and press Enter to continue.");

while (true)
{
    var data = Console.ReadLine();
    if (data != null)
    {
        if (data.Equals("C", StringComparison.OrdinalIgnoreCase))
        {
            break;
        }
        var split = data.Split(' ');
        if (split.Length == 2)
        {
            existingData.Add(new SinkModel()
            {
                Id = split[0],
                Name = split[1]
            });
        }
    }
}

List<InputModel> initialDataToSend = new List<InputModel>();

Console.WriteLine("Enter initial rows to be sent from the source. (Note: Any row previously entered in the sink that is NOT entered here will sent to sink as deleted to sync the two stores).");
Console.WriteLine("Enter rows in the format '<id> <name>'. Type 'C' and press Enter to continue.");

while (true)
{
    var data = Console.ReadLine();
    if (data != null)
    {
        if (data.Equals("C", StringComparison.OrdinalIgnoreCase))
        {
            break;
        }
        var split = data.Split(' ');
        if (split.Length == 2)
        {
            initialDataToSend.Add(new InputModel()
            {
                Id = split[0],
                Name = split[1],
                IsDeleted = false
            });
        }
    }
}

// Add services to the container.
builder.Services.AddSingleton(new ConsoleInputSource(initialDataToSend));

builder.Services.AddSingleton((provider) => new CustomSink(existingData, provider.GetRequiredService<ILogger<CustomSink>>()));

builder.Services.AddFlowtideStream("test")
    .AddSqlFileAsPlan("query.sql")
    .AddVersioningFromAssembly()
    .AddConnectors((connectorManager) =>
    {
        connectorManager.AddCustomSource("custom_source", (readRel) => connectorManager.ServiceProvider.GetRequiredService<ConsoleInputSource>());
        connectorManager.AddCustomSink("custom_sink", (writeRel) => connectorManager.ServiceProvider.GetRequiredService<CustomSink>());
    })
    .AddStorage(b =>
    {
        b.AddTemporaryStorage();
    });

var app = builder.Build();

var consoleInputSource = app.Services.GetRequiredService<ConsoleInputSource>();

await app.StartAsync();

while (true)
{
    Console.WriteLine("Enter row updates. Use the format '<id> <name>' for updates/inserts, or 'D <id>' to delete. To simulate a stream crash, type 'crash'. To exit, type 'exit'.");

    var data = Console.ReadLine();

    if (data != null)
    {
        var split = data.Split(' ');

        if (split.Length == 2)
        {
            bool isDeleted = false;

            if (split[0].Equals("D", StringComparison.OrdinalIgnoreCase))
            {
                isDeleted = true;
            }

            if (isDeleted)
            {
                await consoleInputSource.EnqueueData(new InputModel()
                {
                    Id = split[1],
                    IsDeleted = isDeleted
                });
            }
            else
            {
                await consoleInputSource.EnqueueData(new InputModel()
                {
                    Id = split[0],
                    Name = split[1],
                    IsDeleted = isDeleted
                });
            }
        }
        else if (split.Length == 1 && split[0].Equals("crash", StringComparison.OrdinalIgnoreCase))
        {
            await consoleInputSource.EnqueueData(new InputModel()
            {
                Crash = true
            });
        }
        else if (split.Length == 1 && split[0].Equals("exit", StringComparison.OrdinalIgnoreCase))
        {
            break;
        }
        else
        {
            Console.WriteLine("Invalid input. Use format '<id> <name>' for update or 'D <id>' for deletes.");
        }
    }
}
