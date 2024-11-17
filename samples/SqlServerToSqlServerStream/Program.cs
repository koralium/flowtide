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
using FlowtideDotNet.DependencyInjection;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;

var builder = WebApplication.CreateBuilder(args);

var sqlDb1ConnStr = builder.Configuration.GetConnectionString("sqldb1");
var sqlDb2ConnStr = builder.Configuration.GetConnectionString("sqldb2");

if (sqlDb1ConnStr == null)
{
    throw new InvalidOperationException("Connection string for sqldb1 is not found, start this sample from the AspireSample project.");
}
if (sqlDb2ConnStr == null)
{
    throw new InvalidOperationException("Connection string for sqldb2 is not found, start this sample from the AspireSample project.");
}

// Add logging export to otlp for aspire
builder.Logging.AddOpenTelemetry(log =>
{
    log.AddOtlpExporter();
});

// Export metrics
builder.Services.AddOpenTelemetry()
    .WithMetrics(builder =>
    {
        builder.AddMeter("flowtide.*");
        builder.AddOtlpExporter();
    });

// Add health checks
builder.Services.AddHealthChecks()
    .AddFlowtideCheck();

// Create the stream
builder.Services.AddFlowtideStream("my_stream")
    .AddSqlFileAsPlan("stream.sql")
    .AddConnectors(connectors =>
    {
        connectors.AddSqlServerAsCatalog("db1", () => sqlDb1ConnStr);
        connectors.AddSqlServerAsCatalog("db2", () => sqlDb2ConnStr);
    })
    .AddStorage(storage =>
    {
        // Set min page count to reduce ram usage, or increase it for lower latency
        storage.MinPageCount = 0;

        // Set max process memory to reduce ram usage, or increase it for lower latency
        // This is set to 2GB
        storage.MaxProcessMemory = 2L * 1024 * 1024 * 1024;

        // Use azure storage for persistence
        storage.AddFasterKVAzureStorage(builder.Configuration.GetConnectionString("blobs")!, "mystream", "mydirname");
    });

var app = builder.Build();

// Map health check
app.MapHealthChecks("/health");

// Map the stream UI
app.UseFlowtideUI("/stream");

app.Run();