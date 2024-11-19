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

using AspireSamples.DataMigration;
using AspireSamples.Entities;
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
            var source = builder.AddMongoDB("source");

            DataGenerator dataGenerator = new DataGenerator();

            var dataInsert = DataInsertResource.AddDataInsert(builder, "data-insert",
                async (logger, statusUpdate, token) =>
                {
                    MongoClient mongoClient = new MongoClient(await source.Resource.ConnectionStringExpression.GetValueAsync(token));

                    var database = mongoClient.GetDatabase("test");
                    var userCollection = database.GetCollection<User>("users");
                    var orderCollection = database.GetCollection<Order>("orders");

                    var users = dataGenerator.GenerateUsers(1000);
                    var orders = dataGenerator.GenerateOrders(1000);

                    await userCollection.InsertManyAsync(users);
                    await orderCollection.InsertManyAsync(orders);
                },
                (logger, token) =>
                {
                    return Task.CompletedTask;
                })
                .WaitFor(source);

            var stream = builder.AddProject<MongoDbToConsoleSample>("stream")
                .WithHttpHealthCheck("/health")
                .WithReference(source)
                .WaitFor(source)
                .WaitFor(dataInsert);

            builder.Build().Run();
        }
    }
}
