// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//  
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using FlowtideDotNet.AspNetCore.Extensions;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Sinks;
using FlowtideDotNet.DependencyInjection;
using OpenTelemetry.Logs;
using Stowage;

var builder = WebApplication.CreateBuilder(args);

var azureLakeBlobStorageConnStr = builder.Configuration.GetConnectionString("lake");
var azurePersistenceBlobStorage = builder.Configuration.GetConnectionString("persistence");

var shouldReplay = builder.Configuration.GetValue<bool>("replay");

// Add logging export to otlp for aspire
builder.Logging.AddOpenTelemetry(log =>
{
    log.AddOtlpExporter();
});

// Add health checks
builder.Services.AddHealthChecks()
    .AddFlowtideCheck();

// Create the stream
builder.Services.AddFlowtideStream("my_stream")
    .AddSqlFileAsPlan("stream.sql")
    .AddConnectors(connectors =>
    {
        var (endpoint, accountName, accountKey)= GetUriAndSharedKeyFromBlobConnectionString(azureLakeBlobStorageConnStr!);
        connectors.AddDeltaLake(new FlowtideDotNet.Connector.DeltaLake.DeltaLakeOptions()
        {
            StorageLocation = Files.Of.AzureBlobStorage(endpoint, accountName, accountKey),
            OneVersionPerCheckpoint = shouldReplay,
            DeltaCheckInterval = TimeSpan.FromSeconds(1)
        });
        connectors.AddConsoleSink(".*");
    })
    .AddStorage(storage =>
    {
        // Set min page count to reduce ram usage, or increase it for lower latency
        storage.MinPageCount = 0;

        // Set max process memory to reduce ram usage, or increase it for lower latency
        // This is set to 2GB
        storage.MaxProcessMemory = 2L * 1024 * 1024 * 1024;

        // Use azure storage for persistence
        storage.AddFasterKVAzureStorage(azurePersistenceBlobStorage!, "mystream", "mydirname");
    });

var app = builder.Build();

// Map health check
app.MapHealthChecks("/health");

app.UseFlowtideUI("/stream");

app.Run();

static (Uri endpoint, string accountName, string sharedkey) GetUriAndSharedKeyFromBlobConnectionString(string connectionString)
{
    var parts = connectionString.Split(';');
    var endpoint = parts.First(p => p.StartsWith("BlobEndpoint=")).Substring("BlobEndpoint=".Length);
    var sharedkey = parts.First(p => p.StartsWith("AccountKey=")).Substring("AccountKey=".Length);
    var accountName = parts.First(p => p.StartsWith("AccountName=")).Substring("AccountName=".Length);
    return (new Uri(endpoint), accountName, sharedkey);
}