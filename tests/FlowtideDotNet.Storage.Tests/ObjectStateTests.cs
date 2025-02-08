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
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.Tests
{
    public class ObjectStateTests
    {
        [Fact]
        public async Task TestCommitedValueIsSaved()
        {
            var stateManager = new StateManager.StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 1000000,
                PersistentStorage = new FileCachePersistentStorage(new FileCacheOptions()
                {
                    DirectoryPath = "./data/tmp/" + nameof(TestCommitedValueIsSaved)
                }, true)
            }, new NullLogger<StateManagerSync>(), new Meter($"storage"), "storage");
            await stateManager.InitializeAsync();

            var stateClient = stateManager.GetOrCreateClient("stateClient");
            var objectState = await stateClient.GetOrCreateObjectStateAsync<bool>("state");

            objectState.Value = true;
            await objectState.Commit();

            await stateManager.CheckpointAsync();

            await stateManager.InitializeAsync();

            stateClient = stateManager.GetOrCreateClient("stateClient");
            objectState = await stateClient.GetOrCreateObjectStateAsync<bool>("state");

            Assert.True(objectState.Value);
        }

        [Fact]
        public async Task TestNonCommitedValueIsRestored()
        {
            var stateManager = new StateManager.StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 1000000,
                PersistentStorage = new FileCachePersistentStorage(new FileCacheOptions()
                {
                    DirectoryPath = "./data/tmp/" + nameof(TestNonCommitedValueIsRestored)
                }, true)
            }, new NullLogger<StateManagerSync>(), new Meter($"storage"), "storage");
            await stateManager.InitializeAsync();

            var stateClient = stateManager.GetOrCreateClient("stateClient");
            var objectState = await stateClient.GetOrCreateObjectStateAsync<bool>("state");

            objectState.Value = true;

            await stateManager.CheckpointAsync();

            await stateManager.InitializeAsync();

            stateClient = stateManager.GetOrCreateClient("stateClient");
            objectState = await stateClient.GetOrCreateObjectStateAsync<bool>("state");

            Assert.False(objectState.Value);
        }
    }
}
