using FlowtideDotNet.AspNetCore.Extensions;
using FlowtideDotNet.Core;
using FlowtideDotNet.DependencyInjection;
using FlowtideDotNet.Lineage.DataHub;

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
        storage.AddTemporaryStorage();
    })
    .AddDataHubLineage();

var app = builder.Build();

app.MapFlowtideTestInformation();
app.MapFlowtideDataHubLineage();

app.Run();

public partial class Program { }