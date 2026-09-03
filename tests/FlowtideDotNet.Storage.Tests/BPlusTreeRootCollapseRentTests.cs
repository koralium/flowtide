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
using FlowtideDotNet.Storage.Tests.BPlusTreeByteBased;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Storage.Tree.Internal;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Storage.Tests
{
    public class BPlusTreeRootCollapseRentTests
    {
        private static async Task<StateManagerSync<object>> CreateStateManager(string name)
        {
            var stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 1000000,
                PersistentStorage = new FileCachePersistentStorage(new FileCacheOptions()
                {
                    DirectoryPath = $"./data/temp/{name}"
                })
            }, NullLoggerFactory.Instance, new Meter(name), name, GlobalMemoryManager.Instance);
            await stateManager.InitializeAsync();
            return stateManager;
        }

        /// <summary>
        /// Fetches the root, drops the fetch rent, and returns it holding only the cache rent.
        /// </summary>
        private static async Task<BaseNode<K, TKeyContainer>> RentedByCacheOnly<K, V, TKeyContainer, TValueContainer>(BPlusTree<K, V, TKeyContainer, TValueContainer> tree)
            where TKeyContainer : IKeyContainer<K>
            where TValueContainer : IValueContainer<V>
        {
            var root = Assert.IsType<InternalNode<K, V, TKeyContainer>>(await tree.m_stateClient.GetValue(tree.m_stateClient.Metadata!.Root));
            Assert.Equal(2, root.children.Count);
            root.Return();
            Assert.Equal(1, root.RentCount);
            return root;
        }

        /// <summary>
        /// Collapsing the root must return the rent the write took when it fetched it.
        /// </summary>
        [Fact]
        public async Task RootCollapseReturnsTheFetchRent()
        {
            using var stateManager = await CreateStateManager("root_collapse_rent");
            var tree = (BPlusTree<long, string, ListKeyContainer<long>, ListValueContainer<string>>)await stateManager.GetOrCreateClient("node1")
                .GetOrCreateTree<long, string, ListKeyContainer<long>, ListValueContainer<string>>("tree",
                new BPlusTreeOptions<long, string, ListKeyContainer<long>, ListValueContainer<string>>()
                {
                    BucketSize = 8,
                    Comparer = new BPlusTreeListComparer<long>(new LongComparer()),
                    KeySerializer = new KeyListSerializer<long>(new LongSerializer()),
                    ValueSerializer = new ValueListSerializer<string>(new StringSerializer()),
                    MemoryAllocator = GlobalMemoryManager.Instance
                });

            // Enough for one split, so the root is an internal node over two leaves.
            for (var i = 0; i < 10; i++)
            {
                await tree.Upsert(i, $"{i}");
            }
            var rootId = tree.m_stateClient.Metadata!.Root;
            var root = await RentedByCacheOnly(tree);

            // Deleting from the left leaf merges the leaves and collapses the root.
            for (var i = 0; i < 10 && tree.m_stateClient.Metadata.Root == rootId; i++)
            {
                await tree.Delete(i);
            }
            Assert.NotEqual(rootId, tree.m_stateClient.Metadata.Root);

            // Delete returned the cache rent, the fetch rent must be gone too.
            Assert.Equal(0, root.RentCount);
        }

        /// <summary>
        /// The byte based write has the same collapse path.
        /// </summary>
        [Fact]
        public async Task RootCollapseReturnsTheFetchRentByteBased()
        {
            using var stateManager = await CreateStateManager("root_collapse_rent_bytes");
            var tree = (BPlusTree<KeyValuePair<long, long>, string, ListKeyContainerWithSize, ListValueContainer<string>>)await stateManager.GetOrCreateClient("node1")
                .GetOrCreateTree<KeyValuePair<long, long>, string, ListKeyContainerWithSize, ListValueContainer<string>>("tree",
                new BPlusTreeOptions<KeyValuePair<long, long>, string, ListKeyContainerWithSize, ListValueContainer<string>>()
                {
                    BucketSize = 8,
                    Comparer = new ListWithSizeComparer(new LongComparer()),
                    KeySerializer = new ListKeyWithSizeSerializer(17000),
                    ValueSerializer = new ValueListSerializer<string>(new StringSerializer()),
                    UseByteBasedPageSizes = true,
                    MemoryAllocator = GlobalMemoryManager.Instance
                });

            // A byte based leaf splits once it is over the page size and past sixteen keys.
            var initialRoot = tree.m_stateClient.Metadata!.Root;
            var inserted = 0;
            while (tree.m_stateClient.Metadata.Root == initialRoot)
            {
                Assert.True(inserted < 40, "the root never split");
                await tree.Upsert(new KeyValuePair<long, long>(inserted, 33000), $"{inserted}");
                inserted++;
            }
            var rootId = tree.m_stateClient.Metadata.Root;
            var root = await RentedByCacheOnly(tree);

            for (var i = 0; i < inserted && tree.m_stateClient.Metadata.Root == rootId; i++)
            {
                await tree.Delete(new KeyValuePair<long, long>(i, 33000));
            }
            Assert.NotEqual(rootId, tree.m_stateClient.Metadata.Root);

            Assert.Equal(0, root.RentCount);
        }
    }
}
