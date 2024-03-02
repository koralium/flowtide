using FASTER.core;
using FASTER.devices;
using FlowtideDotNet.AspNetCore.Extensions;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Storage.DeviceFactories;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;
using Microsoft.Extensions.Logging.Console;
using Nest;
using OpenTelemetry.Metrics;
using System.IO.Compression;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenTelemetry()
    .WithMetrics(builder =>
    {
        builder.AddPrometheusExporter();
        builder.AddMeter("flowtide.*");
    });

// Stream version is used to create a unique stream, and create new elasticsearch indices if required.
var streamVersion = builder.Configuration.GetValue<string>("StreamVersion") ?? throw new InvalidOperationException("StreamVersion not found");

var query = File.ReadAllText("query.sql");

SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
sqlPlanBuilder.AddSqlServerProvider(() => builder.Configuration.GetConnectionString("SqlServer") ?? throw new InvalidOperationException("SqlServer connection string not found"));
sqlPlanBuilder.Sql(query);
var plan = sqlPlanBuilder.GetPlan();

var readWriteFactory = new ReadWriteFactory();
readWriteFactory.AddSqlServerSource("*", () => builder.Configuration.GetConnectionString("SqlServer") ?? throw new InvalidOperationException("SqlServer connection string not found"));
readWriteFactory.AddElasticsearchSink("*", new FlowtideDotNet.Connector.ElasticSearch.FlowtideElasticsearchOptions()
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

var azureStorageString = builder.Configuration.GetConnectionString("azureStorage") ?? throw new InvalidOperationException("AzureStorage connection string not found");

// Create a logger factory to get logs from the storage devices
var storageLoggerFactory = LoggerFactory.Create(b =>
{
    b.AddConsole();
});
var log = new AzureStorageDevice(azureStorageString, "sqlservertoelastic", streamVersion, "hlog.log", logger: storageLoggerFactory.CreateLogger("azurestorage"));

// Create azure storage backed checkpoint manager
var checkpointManager = new DeviceLogCommitCheckpointManager(
                new AzureStorageNamedDeviceFactory(azureStorageString, logger: storageLoggerFactory.CreateLogger("azurestoragefactory")),
                new DefaultCheckpointNamingScheme($"sqlservertoelastic/{streamVersion}/checkpoints/"), logger: storageLoggerFactory.CreateLogger("checkpointmanager"));

builder.Services.AddFlowtideStream(x =>
{
    x.AddPlan(plan)
    .AddReadWriteFactory(readWriteFactory)
    .WithStateOptions(new StateManagerOptions()
    {
        // Read cache for reduced latency, minimizes calls to azure storage, but increases memory and local disk usage
        UseReadCache = true,
        // Use a maximum of 8 gb of memory
        MaxProcessMemory = 1024 * 1024 * 1024 * 8L,
        PersistentStorage = new FlowtideDotNet.Storage.Persistence.FasterStorage.FasterKvPersistentStorage(new FASTER.core.FasterKVSettings<long, FASTER.core.SpanByte>()
        {
            // 32 mb memory
            MemorySize = 1024 * 1024 * 32,
            PageSize = 1024 * 1024 * 16,
            CheckpointManager = checkpointManager,
            LogDevice = log
        }),
        SerializeOptions = new FlowtideDotNet.Storage.StateSerializeOptions()
        {
            CompressFunc = (stream) =>
            {
                return new System.IO.Compression.ZLibStream(stream, CompressionMode.Compress);
            },
            DecompressFunc = (stream) =>
            {
                return new System.IO.Compression.ZLibStream(stream, CompressionMode.Decompress);
            }
        }
    });
}, "sqlservertoelastic");

var app = builder.Build();

app.UseFlowtideUI("/stream");
app.UseOpenTelemetryPrometheusScrapingEndpoint();

app.Run();
