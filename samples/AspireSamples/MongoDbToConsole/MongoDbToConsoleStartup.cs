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

using Amazon.Runtime.Internal.Util;
using AspireSamples.DataMigration;
using AspireSamples.Entities;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;
using Projects;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static System.Reflection.Metadata.BlobBuilder;

namespace AspireSamples.MongoDbToConsole
{
    internal static class MongoDbToConsoleStartup
    {
        public static void RunSample(IDistributedApplicationBuilder builder)
        {
            var source = builder.AddResource(new MongoDbResource("mongo"))
                .WithImage("mongo", "8.0")
                .WithEndpoint(27017, 27017, scheme: "mongodb", name: "tcp")
                .WithArgs("--replSet", "rs0", "--bind_ip_all", "--port", "27017");

            var mongoInitialize = builder.AddContainer("mongo-initialize", "mongo", "8.0")
                .WithArgs("/bin/bash", "-c", $"mongosh mongodb://{source.Resource.Name}:27017 --eval \"rs.initiate({{_id:'rs0',members:[{{_id:0,host:'127.0.0.1:27017'}}]}})\"")
                .WaitFor(source);

            // Update to finished state when the resource exits
            builder.Eventing.Subscribe<ResourceReadyEvent>(mongoInitialize.Resource, async (ev, token) =>
            {
                var resourceNotification = ev.Services.GetRequiredService<ResourceNotificationService>();
                
                await resourceNotification.WaitForResourceAsync(mongoInitialize.Resource.Name, "Exited");

                // Small wait to ensure the resource is ready to be updated
                // If done immediately, the container might overwrite the state
                await Task.Delay(100);
                await resourceNotification.PublishUpdateAsync(mongoInitialize.Resource, s => s with
                {
                    ResourceType = "Container",
                    State = "Finished"
                });
            });

            DataGenerator dataGenerator = new DataGenerator();

            var dataInsert = DataInsertResource.AddDataInsert(builder, "data-insert",
                async (logger, statusUpdate, token) =>
                {
                    //var endPointDetails = source.Resource.GetEndpoint("mongo");
                    var connStr = await source.Resource.ConnectionStringExpression.GetValueAsync(token);
                    MongoClient mongoClient = new MongoClient(connStr);

                    var database = mongoClient.GetDatabase("test");
                    var userCollection = database.GetCollection<User>("users");
                    var orderCollection = database.GetCollection<Order>("orders");

                    var users = dataGenerator.GenerateUsers(100_000);
                    var orders = dataGenerator.GenerateOrders(100_000);

                    await userCollection.InsertManyAsync(users);
                    await orderCollection.InsertManyAsync(orders);
                },
                (logger, token) =>
                {
                    return Task.CompletedTask;
                })
                .WaitFor(mongoInitialize);

            var stream = builder.AddProject<MongoDbToConsoleSample>("stream")
                .WithHttpHealthCheck("/health")
                .WithReference(source)
                .WaitFor(dataInsert);

            builder.Build().Run();
        }
    }
}
