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
        private readonly BPlusTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>> _tree;
        private readonly StateManagerSync _stateManager;

        public BPlusTreeBulkInserterTests()
        {
            (_tree, _stateManager) = Init().GetAwaiter().GetResult();
        }

        private static async Task<(BPlusTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>, StateManagerSync)> Init()
        {
            var stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 1000000,
                PersistentStorage = new FileCachePersistentStorage(new FileCacheOptions())
            }, NullLoggerFactory.Instance, new Meter("bulk_inserter_test"), "bulk_inserter_test");
            await stateManager.InitializeAsync();
            
            var nodeClient = stateManager.GetOrCreateClient("node1");
            var tree = await nodeClient.GetOrCreateTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>("tree",
                new BPlusTreeOptions<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>()
                {
                    PageSizeBytes = 100,
                    UseByteBasedPageSizes = true,
                    Comparer = new PrimitiveListComparer<long>(),
                    KeySerializer = new PrimitiveListKeyContainerSerializer<long>(GlobalMemoryManager.Instance),
                    ValueSerializer = new PrimitiveListValueContainerSerializer<long>(GlobalMemoryManager.Instance),
                    MemoryAllocator = GlobalMemoryManager.Instance
                });

            return ((BPlusTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>)tree, stateManager);
        }

        private struct Mutator : IRowMutator<long, long>
        {
            public GenericWriteOperation Process(long key, bool exists, in long existingData, ref long incomingData)
            {
                return GenericWriteOperation.Upsert;
            }
        }

        [Fact]
        public async Task StartBatch_OnEmptyTree()
        {
            var bulkInserter = new BPlusTreeBulkInserter<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>(_tree);

            var count = 100;
            var keys = new long[count];
            var values = new long[count];

            for (int i = 0; i < count; i++)
            {
                keys[i] = i;
                values[i] = i;
            }
            await bulkInserter.ApplyBatch(keys, values, count, new Mutator());
            await bulkInserter.ApplyBatch(keys, values, count, new Mutator());
        }

        public void Dispose()
        {
            _stateManager?.Dispose();
        }
    }
}
