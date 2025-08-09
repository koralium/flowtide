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

using Azure.Monitor.OpenTelemetry.Exporter;
using FlowtideDotNet.AspNetCore.Extensions;
using FlowtideDotNet.DependencyInjection;
using MonitoringAzureMonitor;

var builder = WebApplication.CreateBuilder(args);


builder.Services.AddOpenTelemetry()
    .WithMetrics(builder =>
    {
        builder.AddAzureMonitorMetricExporter(o =>
        {
            o.ConnectionString = "{your connection string}";
        });
        builder.AddMeter("flowtide.*");
    });

builder.Services.AddHealthChecks()
    .AddFlowtideCheck()
    .AddApplicationInsightsPublisher("{your connection string}");

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
ON t.val = o.val
WHERE t.val = 123;
";

builder.Services.AddFlowtideStream("AzureMonitorSample")
    .AddSqlTextAsPlan(sqlText)
    .AddConnectors(connectorManager =>
    {
        connectorManager.AddSource(new DummyReadFactory("*"));
        connectorManager.AddSink(new DummyWriteFactory("*"));
    })
    .AddStorage(storage =>
    {
        storage.AddTemporaryDevelopmentStorage();
    });

var app = builder.Build();

app.UseFlowtideUI("/stream");

app.Run();