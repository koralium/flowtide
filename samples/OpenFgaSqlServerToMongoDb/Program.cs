using FlowtideDotNet.Connector.OpenFGA;
using OpenFga.Sdk.Client.Model;
using OpenFga.Sdk.Client;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Connector.OpenFGA.Extensions;
using FlowtideDotNet.AspNetCore.Extensions;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Connector.MongoDB.Extensions;

var builder = WebApplication.CreateBuilder(args);

var openFgaConfig = new OpenFga.Sdk.Client.ClientConfiguration()
{
    ApiUrl = builder.Configuration.GetValue<string>("openfga_url")!,
};

var openFgaClient = new OpenFgaClient(openFgaConfig);
var stores = await openFgaClient.ListStores();
var store = stores.Stores.Find(x => x.Name == "testrbac");

if (store == null)
{
    throw new InvalidOperationException("Could not find store 'testrbac'");
}

var authModel = await openFgaClient.ReadLatestAuthorizationModel(new ClientListRelationsOptions()
{
    StoreId = store.Id
});

if (authModel == null || authModel.AuthorizationModel == null)
{
    throw new InvalidOperationException("Could not find authorization model for store 'testrbac'");
}

openFgaConfig.StoreId = store.Id;
openFgaConfig.AuthorizationModelId = authModel.AuthorizationModel.Id;

var permissionViewPlan = OpenFgaToFlowtide.Convert(authModel.AuthorizationModel, "doc", "can_view", "openfga");

var query = File.ReadAllText("query.sql");
SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
sqlPlanBuilder.AddSqlServerProvider(() => builder.Configuration.GetConnectionString("SqlServer")!);
sqlPlanBuilder.AddPlanAsView("permissionview", permissionViewPlan);
sqlPlanBuilder.Sql(query);

var plan = sqlPlanBuilder.GetPlan();

ReadWriteFactory readWriteFactory = new ReadWriteFactory()
    .AddOpenFGASource("openfga", new OpenFgaSourceOptions()
    {
        ClientConfiguration = openFgaConfig
    })
    .AddSqlServerSource("demo.*", () => builder.Configuration.GetConnectionString("SqlServer")!)
    .AddMongoDbSink("*", new FlowtideDotNet.Connector.MongoDB.FlowtideMongoDBSinkOptions()
    {
        Collection = "demo",
        ConnectionString = builder.Configuration.GetConnectionString("mongodb")!,
        Database = "demo",
        PrimaryKeys = new List<string>() { "docid" }
    });

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