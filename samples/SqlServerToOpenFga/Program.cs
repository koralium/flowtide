using FlowtideDotNet.Connector.OpenFGA.Extensions;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Substrait.Sql;
using OpenFga.Sdk.Client;
using OpenFga.Sdk.Client.Model;
using System.Text.Json;
using FlowtideDotNet.AspNetCore.Extensions;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Core;

var builder = WebApplication.CreateBuilder(args);

var openFgaConfig = new OpenFga.Sdk.Client.ClientConfiguration()
{
    ApiUrl = builder.Configuration.GetValue<string>("openfga_url")!,
};

var openFgaClient = new OpenFgaClient(openFgaConfig);
var stores = await openFgaClient.ListStores();
var store = stores.Stores.Find(x => x.Name == "demo");

if (store == null)
{
    throw new InvalidOperationException("Could not find store 'demo'");
}

var authModel = await openFgaClient.ReadLatestAuthorizationModel(new ClientListRelationsOptions()
{
    StoreId = store.Id
});

if (authModel == null || authModel.AuthorizationModel == null)
{
    throw new InvalidOperationException("Could not find authorization model for store 'demo'");
}

var query = File.ReadAllText("query.sql");
SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
sqlPlanBuilder.AddSqlServerProvider(() => builder.Configuration.GetConnectionString("SqlServer")!);
sqlPlanBuilder.Sql(query);
var plan = sqlPlanBuilder.GetPlan();

openFgaConfig.StoreId = store.Id;
openFgaConfig.AuthorizationModelId = authModel.AuthorizationModel.Id;

IConnectorManager connectorManager = new ConnectorManager()
    .AddSqlServerSource(() => builder.Configuration.GetConnectionString("SqlServer")!)
    .AddOpenFGASink("*", new FlowtideDotNet.Connector.OpenFGA.OpenFgaSinkOptions()
    {
        ClientConfiguration = openFgaConfig
    });

builder.Services.AddFlowtideStream(x =>
{
    x.AddPlan(plan)
    .AddConnectorManager(connectorManager)
    .WithStateOptions(new StateManagerOptions()
    {
        // This is non persistent storage, use FasterKV persistence storage instead if you want persistent storage
        PersistentStorage = new FileCachePersistentStorage(new FlowtideDotNet.Storage.FileCacheOptions()
        {
        })
    });
});

var app = builder.Build();
app.UseFlowtideUI("/stream");
app.Run();