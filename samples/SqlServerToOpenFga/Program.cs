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

using FlowtideDotNet.Connector.OpenFGA.Extensions;
using FlowtideDotNet.Core.Engine;
using OpenFga.Sdk.Client;
using OpenFga.Sdk.Client.Model;
using FlowtideDotNet.AspNetCore.Extensions;
using FlowtideDotNet.Core;
using FlowtideDotNet.DependencyInjection;

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

openFgaConfig.StoreId = store.Id;
openFgaConfig.AuthorizationModelId = authModel.AuthorizationModel.Id;

builder.Services.AddFlowtideStream("stream")
    .AddSqlFileAsPlan("query.sql")
    .AddConnectors(connectorManager =>
    {
        connectorManager.AddSqlServerSource(() => builder.Configuration.GetConnectionString("SqlServer")!);
        connectorManager.AddOpenFGASink("*", new FlowtideDotNet.Connector.OpenFGA.OpenFgaSinkOptions()
        {
            ClientConfiguration = openFgaConfig
        });
    })
    .AddStorage(storage =>
    {
        storage.AddTemporaryDevelopmentStorage();
    });

var app = builder.Build();
app.UseFlowtideUI("/stream");
app.Run();