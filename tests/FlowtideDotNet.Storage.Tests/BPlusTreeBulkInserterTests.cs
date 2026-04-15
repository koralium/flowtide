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
using FlowtideDotNet.Storage.Tree.Internal;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Storage.Tests
{
    public class BPlusTreeBulkInserterTests : IDisposable
    {
        private readonly BPlusTree<long, string, ListKeyContainer<long>, ListValueContainer<string>> _tree;
        private readonly StateManagerSync _stateManager;

        public BPlusTreeBulkInserterTests()
        {
            (_tree, _stateManager) = Init().GetAwaiter().GetResult();
        }

        private static async Task<(BPlusTree<long, string, ListKeyContainer<long>, ListValueContainer<string>>, StateManagerSync)> Init()
        {
            var stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 1000000,
                PersistentStorage = new FileCachePersistentStorage(new FileCacheOptions())
            }, NullLoggerFactory.Instance, new Meter("bulk_inserter_test"), "bulk_inserter_test");
            await stateManager.InitializeAsync();

            var nodeClient = stateManager.GetOrCreateClient("node1");
            var tree = await nodeClient.GetOrCreateTree<long, string, ListKeyContainer<long>, ListValueContainer<string>>("tree",
                new BPlusTreeOptions<long, string, ListKeyContainer<long>, ListValueContainer<string>>()
                {
                    BucketSize = 8,
                    Comparer = new BPlusTreeListComparer<long>(new LongComparer()),
                    KeySerializer = new KeyListSerializer<long>(new LongSerializer()),
                    ValueSerializer = new ValueListSerializer<string>(new StringSerializer()),
                    MemoryAllocator = GlobalMemoryManager.Instance
                });

            return ((BPlusTree<long, string, ListKeyContainer<long>, ListValueContainer<string>>)tree, stateManager);
        }

        [Fact]
        public async Task StartBatch_OnEmptyTree()
        {
            var bulkInserter = new BPlusTreeBulkInserter<long, string, ListKeyContainer<long>, ListValueContainer<string>>(_tree);

            var keys = new long[] { 3, 1, 2 };
            var values = new string[] { "three", "one", "two" };

            await bulkInserter.StartBatch(keys, values);
        }

        public void Dispose()
        {
            _stateManager?.Dispose();
        }
    }
}
