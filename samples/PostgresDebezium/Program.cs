using Confluent.Kafka;
using FlowtideDotNet.AspNetCore.Extensions;
using FlowtideDotNet.Connector.Kafka;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Sql;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
var query = File.ReadAllText("query.sql");
SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
sqlPlanBuilder.Sql(query);

var plan = sqlPlanBuilder.GetPlan();

ReadWriteFactory readWriteFactory = new ReadWriteFactory()
    .AddKafkaSource("*", new FlowtideKafkaSourceOptions
    {
        ConsumerConfig = new ConsumerConfig
        {
            BootstrapServers = "localhost:9093",
            GroupId = "flowtide"
        },
        KeyDeserializer = new FlowtideDebeziumKeyDeserializer(),
        ValueDeserializer = new FlowtideDebeziumValueDeserializer()
    })
    .AddConsoleSink(".*");

builder.Services.AddFlowtideStream(x =>
{
    x.AddPlan(plan)
    .AddReadWriteFactory(readWriteFactory)
    .WithStateOptions(new StateManagerOptions()
    {
        // This is non persistent storage, use FasterKV persistence storage instead if you want persistent storage
        PersistentStorage = new FileCachePersistentStorage(new FlowtideDotNet.Storage.FileCacheOptions()
        {
        })
    });
});

var app = builder.Build();

// Configure the HTTP request pipeline.
app.UseFlowtideUI("/stream");

app.Run();