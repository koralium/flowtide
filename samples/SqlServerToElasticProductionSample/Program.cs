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

using FASTER.core;
using FASTER.devices;
using FlowtideDotNet.AspNetCore.Extensions;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Storage.DeviceFactories;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;
using Microsoft.Extensions.Logging.Console;
using Nest;
using OpenTelemetry.Metrics;
using System.IO.Compression;
using FlowtideDotNet.DependencyInjection;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenTelemetry()
    .WithMetrics(builder =>
    {
        builder.AddPrometheusExporter();
        builder.AddMeter("flowtide.*");
    });

// Stream version is used to create a unique stream, and create new elasticsearch indices if required.
var streamVersion = builder.Configuration.GetValue<string>("StreamVersion") ?? throw new InvalidOperationException("StreamVersion not found");
var azureStorageString = builder.Configuration.GetConnectionString("azureStorage") ?? throw new InvalidOperationException("AzureStorage connection string not found");

builder.Services.AddFlowtideStream("sqlservertoelastic")
    .AddSqlFileAsPlan("query.sql")
    .AddConnectors(connectorManager =>
    {
        connectorManager.AddSqlServerSource(() => builder.Configuration.GetConnectionString("SqlServer") ?? throw new InvalidOperationException("SqlServer connection string not found"));
        connectorManager.AddElasticsearchSink("*", new FlowtideDotNet.Connector.ElasticSearch.FlowtideElasticsearchOptions()
        {
            ConnectionSettings = new ConnectionSettings(new Uri(builder.Configuration.GetValue<string>("ElasticsearchUrl") ?? throw new InvalidOperationException("ElasticsearchUrl not found"))),
            GetIndexNameFunc = (writeRel) =>
            {
                return $"{writeRel.NamedObject.DotSeperated}_{streamVersion}";
            },
            OnInitialDataSent = async (client, writeRel, indexName) =>
            {
                var aliasName = writeRel.NamedObject.DotSeperated;
                var oldIndices = await client.GetIndicesPointingToAliasAsync(aliasName);
                var putAliasResponse = await client.Indices.PutAliasAsync(indexName, writeRel.NamedObject.DotSeperated);

                if (putAliasResponse.IsValid)
                {
                    foreach (var oldIndex in oldIndices)
                    {
                        if (oldIndex != indexName)
                        {
                            await client.Indices.DeleteAsync(oldIndex);
                        }
                    }
                }
                else
                {
                    throw new InvalidOperationException(putAliasResponse.ServerError.Error.StackTrace);
                }
            }
        });
    })
    .AddStorage(storage =>
    {
        storage.AddFasterKVAzureStorage(azureStorageString, "sqlservertoelastic", streamVersion);
    });

var app = builder.Build();

app.UseFlowtideUI("/stream");
app.UseOpenTelemetryPrometheusScrapingEndpoint();

app.Run();
