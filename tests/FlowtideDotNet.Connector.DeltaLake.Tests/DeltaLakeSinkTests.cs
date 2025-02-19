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
using Stowage;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DeltaLakeSinkTests
    {

        [Fact]
        public async Task TestCreateTable()
        {
            var storage = Files.Of.LocalDisk("./test");
            DeltaLakeSinkStream stream = new DeltaLakeSinkStream(nameof(TestCreateTable), storage);

            stream.Generate(10);

            await stream.StartStream(@"
                CREATE TABLE test (
                    Name STRING,
                    LastName STRING,
                    userkey INT
                );

                INSERT INTO test
                SELECT firstName as Name, lastName, userKey FROM users
            ");

            await WaitForVersion(storage, stream, 0);

            var firstUser = stream.Users[0];
            stream.DeleteUser(firstUser);

            stream.Generate(50);

            await WaitForVersion(storage, stream, 1);

            firstUser = stream.Users.Last();
            stream.DeleteUser(firstUser);

            await WaitForVersion(storage, stream, 2);

            await Task.Delay(1000);
            
        }

        private async Task WaitForVersion(IFileStorage storage, FlowtideTestStream stream, long version)
        {
            while (true)
            {
                try
                {
                    var exists = await storage.Exists($"/test/_delta_log/{version.ToString("D20")}.json");
                    if (exists)
                    {
                        break;
                    }
                }
                catch (Exception)
                {
                }
                
                await stream.SchedulerTick();
                await Task.Delay(100);
            }
        }
    }
}
