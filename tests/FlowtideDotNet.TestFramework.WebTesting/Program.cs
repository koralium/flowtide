using FlowtideDotNet.AspNetCore.Extensions;
using FlowtideDotNet.Core;
using FlowtideDotNet.DependencyInjection;

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

app.MapFlowtideTestInformation();

app.Run();

public partial class Program { }