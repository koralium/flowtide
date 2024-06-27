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
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Sql;
using SqlSampleWithUI;
using FlowtideDotNet.DependencyInjection;
using FlowtideDotNet.Core.Sources.Generic;
using FlowtideDotNet.Core.ColumnStore.Memory;
using OpenTelemetry.Metrics;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenTelemetry()
    .WithMetrics(builder =>
    {
        builder.AddPrometheusExporter(o =>
        {

        });
        builder.AddMeter("flowtide.*");
    });

var sqlText = @"
CREATE TABLE testtable (
  val any
);

CREATE TABLE other (
  val any
);

INSERT INTO output
SELECT t.val FROM testtable t
LEFT JOIN other o
ON t.val = o.val;
";

builder.Services.AddFlowtideStream("test")
.AddSqlTextAsPlan(sqlText)
.AddConnectors((connectorManager) =>
{
    connectorManager.AddSource(new DummyReadFactory("*"));
    connectorManager.AddSink(new DummyWriteFactory("*"));
})
.AddStorage(b =>
{
    b.AddTemporaryDevelopmentStorage();
    b.MaxProcessMemory = 6L * 1024 * 1024 * 1024;
    b.MinPageCount = 0;
});

builder.Services.AddCors();

builder.Services.AddHealthChecks()
    .AddFlowtideCheck();

var app = builder.Build();
app.UseCors(b =>
{
    b.AllowAnyOrigin().AllowAnyMethod().AllowAnyHeader();
});

app.UseHealthChecks("/health");
app.UseOpenTelemetryPrometheusScrapingEndpoint();
app.UseFlowtideUI("/");


app.Run();
//await app.StartAsync();

//while (true)
//{
//    var allocated = BatchMemoryManager.AllocatedMemory;

//    long size = 0;
//    foreach(var kv in allocated)
//    {
//        size += kv.Value.memory.Memory.Length;
//    }

//    await Task.Delay(1000);
//}