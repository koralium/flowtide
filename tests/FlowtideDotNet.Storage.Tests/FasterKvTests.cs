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

using FASTER.core;
using FlowtideDotNet.Storage.Persistence.FasterStorage;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Logging.Debug;
using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tests
{
    public class FasterKvTests
    {
        [Fact]
        public async Task WriteAbovePageSizeThrowError()
        {
            using var device = Devices.CreateLogDevice("./data/tmp/persistent/pagesize");
            using StateManager.StateManagerSync stateManager = new StateManager.StateManagerSync<object>(
                new StateManagerOptions()
                {
                    PersistentStorage = new FasterKvPersistentStorage(meta => new FasterKVSettings<long, SpanByte>()
                    {
                        LogDevice = device,
                        CheckpointDir = "./data/tmp/persistent/pagesize",
                        PageSize = 512,
                        MemorySize = 1024
                    })
                }, NullLogger.Instance, new Meter($"storage"), "storage");

            await stateManager.InitializeAsync();

            var nodeClient = stateManager.GetOrCreateClient("node1");
            var objState = await nodeClient.GetOrCreateObjectStateAsync<string>("test");

            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < 600; i++)
            {
                stringBuilder.Append("a");
            }

            objState.Value = stringBuilder.ToString();

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await objState.Commit();
            });
            Assert.Equal("Serialized data size 635 exceeds maximum page size 512", exception.Message);
        }
    }
}
