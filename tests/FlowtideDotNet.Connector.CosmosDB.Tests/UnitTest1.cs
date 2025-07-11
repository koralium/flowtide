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

namespace FlowtideDotNet.Connector.CosmosDB.Tests
{
    public class UnitTest1
    {
        [Fact(Skip = "Requires local cosmosdb emulator")]
        public async Task TesWithPartitionKey()
        {
            CosmosDbTestStream testStream = new CosmosDbTestStream("test");
            testStream.Generate();

            await testStream.StartStream(@"
               INSERT INTO output 
                SELECT 
                    UserKey as id,
                    FirstName,
                    LastName,
                    UserKey as pk
                FROM users");

            await testStream.WaitForUpdate();
        }

        [Fact(Skip = "Requires local cosmosdb emulator")]
        public async Task TesWithoutPartitionKey()
        {
            CosmosDbTestStream testStream = new CosmosDbTestStream("testnopk");
            testStream.Generate();

            await testStream.StartStream(@"
               INSERT INTO output 
                SELECT 
                    UserKey as id,
                    FirstName,
                    LastName,
                    '1' as pk
                FROM users");

            await testStream.WaitForUpdate();
        }
    }
}