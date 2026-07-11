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

using FlowtideDotNet.Storage.Comparers;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Storage.Tests
{
    public class StateExistsTests
    {
        private static async Task<StateManagerSync> CreateStateManager()
        {
            var stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 1000000,
                PersistentStorage = new FileCachePersistentStorage(new FileCacheOptions())
            }, NullLoggerFactory.Instance, new Meter("state_exists_test"), "state_exists_test", GlobalMemoryManager.Instance);
            await stateManager.InitializeAsync();
            return stateManager;
        }

        private static ValueTask<IBPlusTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>> CreateTree(IStateManagerClient client, string name)
        {
            return client.GetOrCreateTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>(name,
                new BPlusTreeOptions<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>()
                {
                    Comparer = new PrimitiveListComparer<long>(),
                    KeySerializer = new PrimitiveListKeyContainerSerializer<long>(GlobalMemoryManager.Instance),
                    ValueSerializer = new PrimitiveListValueContainerSerializer<long>(GlobalMemoryManager.Instance),
                    MemoryAllocator = GlobalMemoryManager.Instance
                });
        }

        [Fact]
        public async Task StateExistsFalseBeforeCreationTrueAfter()
        {
            var stateManager = await CreateStateManager();
            var client = stateManager.GetOrCreateClient("node1");

            Assert.False(client.StateExists("tree"));

            await CreateTree(client, "tree");

            Assert.True(client.StateExists("tree"));
            Assert.False(client.StateExists("othertree"));
        }

        [Fact]
        public async Task StateExistsIsScopedPerClient()
        {
            var stateManager = await CreateStateManager();
            var client1 = stateManager.GetOrCreateClient("node1");
            var client2 = stateManager.GetOrCreateClient("node2");

            await CreateTree(client1, "tree");

            Assert.True(client1.StateExists("tree"));
            Assert.False(client2.StateExists("tree"));

            var child = client1.GetChildManager("child");
            Assert.False(child.StateExists("tree"));
        }

        [Fact]
        public async Task StateExistsDoesNotMatchPrefixes()
        {
            var stateManager = await CreateStateManager();
            var client = stateManager.GetOrCreateClient("node1");

            await CreateTree(client, "persistent_v1");

            Assert.True(client.StateExists("persistent_v1"));
            Assert.False(client.StateExists("persistent"));
        }
    }
}
