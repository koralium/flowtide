using FlowtideDotNet.Connector.OpenFGA.Extensions;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Substrait.Sql;
using OpenFga.Sdk.Client;
using OpenFga.Sdk.Client.Model;
using System.Text.Json;
using FlowtideDotNet.AspNetCore.Extensions;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.StateManager;

var builder = WebApplication.CreateBuilder(args);

var query = File.ReadAllText("query.sql");
SqlPlanBuilder sqlPlanBuilder = new SqlPlanBuilder();
sqlPlanBuilder.AddSqlServerProvider(() => builder.Configuration.GetConnectionString("SqlServer")!);
sqlPlanBuilder.Sql(query);
var plan = sqlPlanBuilder.GetPlan();

var openFgaConfig = new OpenFga.Sdk.Client.ClientConfiguration()
{
    ApiUrl = builder.Configuration.GetValue<string>("openfga_url")!,
};

var openFgaClient = new OpenFgaClient(openFgaConfig);

var createStoreResponse = await openFgaClient.CreateStore(new OpenFga.Sdk.Client.Model.ClientCreateStoreRequest()
{
    Name = "demo"
});

var createModelRequest = JsonSerializer.Deserialize<ClientWriteAuthorizationModelRequest>(@"
{
  ""schema_version"": ""1.1"",
  ""type_definitions"": [
    {
      ""type"": ""user"",
      ""relations"": {},
      ""metadata"": null
    },
    {
      ""type"": ""group"",
      ""relations"": {
        ""member"": {
          ""this"": {}
        }
      },
      ""metadata"": {
        ""relations"": {
          ""member"": {
            ""directly_related_user_types"": [
              {
                ""type"": ""user""
              }
            ]
          }
        }
      }
    }
  ]
}");

var createModelResponse = await openFgaClient.WriteAuthorizationModel(createModelRequest!, new ClientWriteOptions()
{
    StoreId = createStoreResponse.Id
});

openFgaConfig.StoreId = createStoreResponse.Id;
openFgaConfig.AuthorizationModelId = createModelResponse.AuthorizationModelId;

ReadWriteFactory readWriteFactory = new ReadWriteFactory()
    .AddSqlServerSource("demo.*", () => builder.Configuration.GetConnectionString("SqlServer")!)
    .AddOpenFGASink("*", new FlowtideDotNet.Connector.OpenFGA.OpenFgaSinkOptions()
    {
        ClientConfiguration = openFgaConfig
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
app.UseFlowtideUI("/stream");
app.Run();