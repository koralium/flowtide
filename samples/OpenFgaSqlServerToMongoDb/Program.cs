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
using FlowtideDotNet.Connector.MongoDB.Extensions;
using FlowtideDotNet.Connector.OpenFGA;
using FlowtideDotNet.Connector.OpenFGA.Extensions;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.DependencyInjection;
using OpenFga.Sdk.Client;
using OpenFga.Sdk.Client.Model;

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

builder.Services.AddFlowtideStream("stream")
    .AddSqlPlan(sqlBuilder =>
    {
        sqlBuilder.AddPlanAsView("permissionview", permissionViewPlan);
        sqlBuilder.Sql(query);
    })
    .AddConnectors(connectorManager =>
    {
        connectorManager.AddOpenFGASource("openfga", new OpenFgaSourceOptions
        {
            ClientConfiguration = openFgaConfig
        });
        connectorManager.AddSqlServerSource(() => builder.Configuration.GetConnectionString("SqlServer")!);
        connectorManager.AddMongoDbSink("*", new FlowtideDotNet.Connector.MongoDB.FlowtideMongoDBSinkOptions()
        {
            Collection = "demo",
            ConnectionString = builder.Configuration.GetConnectionString("mongodb")!,
            Database = "demo",
            PrimaryKeys = new List<string>() { "docid" }
        });
    })
    .AddStorage(storage =>
    {
        storage.AddTemporaryDevelopmentStorage();
    });

var app = builder.Build();

// Configure the HTTP request pipeline.
app.UseFlowtideUI("/stream");

app.Run();