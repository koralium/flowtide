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

using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Engine;
using Microsoft.Azure.Cosmos;

namespace FlowtideDotNet.Connector.CosmosDB.Tests
{
    internal class CosmosDbTestStream : FlowtideTestStream
    {
        private string testConnString = "AccountEndpoint=https://localhost:8081/;AccountKey=C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==;";
        private readonly string testName;

        public CosmosDbTestStream(string testName) : base(testName)
        {
            this.testName = testName;

            InitializeCosmosDb().GetAwaiter().GetResult();
            
        }

        private async Task InitializeCosmosDb()
        {
            var cosmosClient = new CosmosClient(testConnString);
            
            var dbProps = await cosmosClient.CreateDatabaseIfNotExistsAsync(testName);
            await dbProps.Database.CreateContainerIfNotExistsAsync(testName, "/pk");
        }

        protected override void AddWriteResolvers(IConnectorManager factory)
        {
            factory.AddCosmosDbSink("*", new FlowtideCosmosOptions()
            {
                ConnectionString = testConnString,
                ContainerName = testName,
                DatabaseName = testName
            });
        }
    }
}
