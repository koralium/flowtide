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
        storage.AddTemporaryStorage();
    })
    .AddDbtManifest();

// Test connectors report the excluded namespace "test".
builder.Services.AddFlowtideDbtManifest(o => o.ExcludedNamespaces.Remove("test"));

var app = builder.Build();

app.MapFlowtideTestInformation();
app.MapFlowtideDbtManifest();

app.Run();

public partial class Program { }