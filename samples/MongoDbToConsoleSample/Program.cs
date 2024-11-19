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

// Add logging export to otlp for aspire
builder.Logging.AddOpenTelemetry(log =>
{
    log.AddOtlpExporter();
});

// Add health checks
builder.Services.AddHealthChecks()
    .AddFlowtideCheck();

builder.Services.AddFlowtideStream("stream")
    .AddSqlFileAsPlan("stream.sql")
    .AddConnectors(c =>
    {
        c.AddMongoDbSource(builder.Configuration.GetConnectionString("source")!);
    })
    .AddStorage(c =>
    {
        c.AddTemporaryDevelopmentStorage();
    });
// Add services to the container.

var app = builder.Build();

app.UseHealthChecks("/health");
app.UseFlowtideUI("/stream");

app.Run();