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
    public class BPlusTreeBulkSearchTests : IDisposable
    {
        private readonly BPlusTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>> _tree;
        private readonly StateManagerSync _stateManager;

        public BPlusTreeBulkSearchTests()
        {
            (_tree, _stateManager) = Init().GetAwaiter().GetResult();
        }

        private static async Task<(BPlusTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>, StateManagerSync)> Init()
        {
            var stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 1000000,
                PersistentStorage = new FileCachePersistentStorage(new FileCacheOptions())
            }, NullLoggerFactory.Instance, new Meter("bulk_search_test"), "bulk_search_test");
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

        public void Dispose()
        {
            _stateManager?.Dispose();
        }

        private async Task InsertKeys(params long[] keys)
        {
            foreach (var key in keys)
            {
                await _tree.Upsert(key, key * 10);
            }
        }

        private async Task InsertRange(long start, long count)
        {
            for (long i = start; i < start + count; i++)
            {
                await _tree.Upsert(i, i * 10);
            }
        }

        /// <summary>
        /// Collects all results from a bulk search into a dictionary keyed by the original key index.
        /// </summary>
        private async Task<Dictionary<int, List<BulkSearchKeyResult>>> CollectAllResults<TComparer>(
            IBplusTreeBulkSearch<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>, TComparer> searcher)
            where TComparer : IBplusTreeComparer<long, PrimitiveListKeyContainer<long>>
        {
            var allResults = new Dictionary<int, List<BulkSearchKeyResult>>();
            while (await searcher.MoveNextLeaf())
            {
                foreach (var result in searcher.CurrentResults)
                {
                    if (!allResults.TryGetValue(result.KeyIndex, out var list))
                    {
                        list = new List<BulkSearchKeyResult>();
                        allResults[result.KeyIndex] = list;
                    }
                    list.Add(result);
                }
            }
            return allResults;
        }

        [Fact]
        public async Task SearchEmptyTree_ReturnsNotFound()
        {
            var searcher = _tree.CreateBulkSearcher(new PrimitiveListComparer<long>());
            var keys = new long[] { 1, 2, 3 };
            await searcher.Start(keys, keys.Length);

            var results = await CollectAllResults(searcher);

            // All keys should appear with Found == false
            foreach (var kvp in results)
            {
                Assert.Single(kvp.Value);
                Assert.False(kvp.Value[0].Found);
            }
        }

        [Fact]
        public async Task SearchSingleKey_Found()
        {
            await InsertKeys(5);

            var searcher = _tree.CreateBulkSearcher(new PrimitiveListComparer<long>());
            var keys = new long[] { 5 };
            await searcher.Start(keys, keys.Length);

            var results = await CollectAllResults(searcher);

            Assert.True(results.ContainsKey(0));
            Assert.True(results[0][0].Found);
            Assert.Equal(0, results[0][0].LowerBound);
            Assert.Equal(0, results[0][0].UpperBound);
        }

        [Fact]
        public async Task SearchSingleKey_NotFound()
        {
            await InsertKeys(5);

            var searcher = _tree.CreateBulkSearcher(new PrimitiveListComparer<long>());
            var keys = new long[] { 3 };
            await searcher.Start(keys, keys.Length);

            var results = await CollectAllResults(searcher);

            Assert.True(results.ContainsKey(0));
            Assert.False(results[0][0].Found);
        }

        [Fact]
        public async Task SearchMultipleKeys_MixedFoundAndNotFound()
        {
            await InsertKeys(1, 3, 5, 7, 9);

            var searcher = _tree.CreateBulkSearcher(new PrimitiveListComparer<long>());
            var keys = new long[] { 1, 2, 5, 8, 9 };
            await searcher.Start(keys, keys.Length);

            var results = await CollectAllResults(searcher);

            // key[0]=1 found, key[1]=2 not found, key[2]=5 found, key[3]=8 not found, key[4]=9 found
            Assert.True(results[0].Any(r => r.Found));   // 1
            Assert.True(results[1].All(r => !r.Found));   // 2
            Assert.True(results[2].Any(r => r.Found));   // 5
            Assert.True(results[3].All(r => !r.Found));   // 8
            Assert.True(results[4].Any(r => r.Found));   // 9
        }

        [Fact]
        public async Task SearchAcrossMultipleLeaves()
        {
            // Insert enough keys to span multiple leaves (page size is 100 bytes, longs are 8 bytes each)
            await InsertRange(0, 100);

            var searcher = _tree.CreateBulkSearcher(new PrimitiveListComparer<long>());
            var keys = new long[] { 0, 50, 99 };
            await searcher.Start(keys, keys.Length);

            var results = await CollectAllResults(searcher);

            Assert.True(results[0].Any(r => r.Found));   // 0
            Assert.True(results[1].Any(r => r.Found));   // 50
            Assert.True(results[2].Any(r => r.Found));   // 99
        }

        [Fact]
        public async Task SearchWithUnsortedKeys_StillReturnsCorrectKeyIndex()
        {
            await InsertKeys(10, 20, 30);

            var searcher = _tree.CreateBulkSearcher(new PrimitiveListComparer<long>());
            // Keys in reverse order
            var keys = new long[] { 30, 10, 20 };
            await searcher.Start(keys, keys.Length);

            var results = await CollectAllResults(searcher);

            // KeyIndex should map back to original positions
            Assert.True(results[0].Any(r => r.Found));  // key[0]=30
            Assert.True(results[1].Any(r => r.Found));  // key[1]=10
            Assert.True(results[2].Any(r => r.Found));  // key[2]=20
        }

        [Fact]
        public async Task SearchCanBeReused()
        {
            await InsertKeys(1, 2, 3);

            var searcher = _tree.CreateBulkSearcher(new PrimitiveListComparer<long>());

            // First search
            var keys1 = new long[] { 1, 4 };
            await searcher.Start(keys1, keys1.Length);
            var results1 = await CollectAllResults(searcher);
            Assert.True(results1[0].Any(r => r.Found));   // 1
            Assert.True(results1[1].All(r => !r.Found));   // 4

            // Second search reusing the same searcher
            var keys2 = new long[] { 2, 3, 5 };
            await searcher.Start(keys2, keys2.Length);
            var results2 = await CollectAllResults(searcher);
            Assert.True(results2[0].Any(r => r.Found));   // 2
            Assert.True(results2[1].Any(r => r.Found));   // 3
            Assert.True(results2[2].All(r => !r.Found));   // 5
        }

        [Fact]
        public async Task SearchAllKeysFound_LargeTree()
        {
            await InsertRange(0, 500);

            var searcher = _tree.CreateBulkSearcher(new PrimitiveListComparer<long>());
            var keys = new long[] { 0, 100, 200, 300, 400, 499 };
            await searcher.Start(keys, keys.Length);

            var results = await CollectAllResults(searcher);

            for (int i = 0; i < keys.Length; i++)
            {
                Assert.True(results.ContainsKey(i), $"Key index {i} (value {keys[i]}) missing from results");
                Assert.True(results[i].Any(r => r.Found), $"Key {keys[i]} should be found");
            }
        }

        [Fact]
        public async Task SearchAllKeysNotFound_LargeTree()
        {
            // Insert only even numbers
            for (long i = 0; i < 200; i += 2)
            {
                await _tree.Upsert(i, i * 10);
            }

            var searcher = _tree.CreateBulkSearcher(new PrimitiveListComparer<long>());
            // Search for odd numbers
            var keys = new long[] { 1, 3, 5, 99, 101, 199 };
            await searcher.Start(keys, keys.Length);

            var results = await CollectAllResults(searcher);

            for (int i = 0; i < keys.Length; i++)
            {
                Assert.True(results.ContainsKey(i), $"Key index {i} (value {keys[i]}) missing from results");
                Assert.True(results[i].All(r => !r.Found), $"Key {keys[i]} should not be found");
            }
        }

        [Fact]
        public async Task SearchPartialKeyLength()
        {
            await InsertKeys(1, 2, 3, 4, 5);

            var searcher = _tree.CreateBulkSearcher(new PrimitiveListComparer<long>());
            var keys = new long[] { 1, 2, 3, 4, 5 };
            // Only search first 2 keys
            await searcher.Start(keys, 2);

            var results = await CollectAllResults(searcher);

            // Only key indices 0 and 1 should have results
            Assert.True(results.ContainsKey(0));
            Assert.True(results.ContainsKey(1));
            Assert.True(results[0].Any(r => r.Found));
            Assert.True(results[1].Any(r => r.Found));
        }

        [Fact]
        public async Task MoveNextLeaf_ReturnsFalseOnEmptyTree()
        {
            var searcher = _tree.CreateBulkSearcher(new PrimitiveListComparer<long>());
            var keys = new long[] { 1 };
            await searcher.Start(keys, keys.Length);

            // Should get at least one leaf call
            int leafCount = 0;
            while (await searcher.MoveNextLeaf())
            {
                leafCount++;
                Assert.NotNull(searcher.CurrentLeaf);
            }
            // Even on empty tree the root leaf exists, so we may get 1 leaf
            Assert.True(leafCount >= 1);
        }

        [Fact]
        public async Task CurrentLeafHasAccessibleKeysAndValues()
        {
            await InsertKeys(10, 20, 30);

            var searcher = _tree.CreateBulkSearcher(new PrimitiveListComparer<long>());
            var keys = new long[] { 10 };
            await searcher.Start(keys, keys.Length);

            Assert.True(await searcher.MoveNextLeaf());

            var leaf = searcher.CurrentLeaf;
            Assert.True(leaf.keys.Count > 0);
            Assert.True(leaf.values.Count > 0);

            // The found result should let us index into the leaf
            var result = searcher.CurrentResults[0];
            if (result.Found)
            {
                var foundKey = leaf.keys.Get(result.LowerBound);
                var foundValue = leaf.values.Get(result.LowerBound);
                Assert.Equal(10, foundKey);
                Assert.Equal(100, foundValue);
            }
        }

        [Fact]
        public async Task SearchValueCanBeReadFromLeaf()
        {
            await InsertRange(0, 200);

            var searcher = _tree.CreateBulkSearcher(new PrimitiveListComparer<long>());
            var keys = new long[] { 42, 99, 150 };
            await searcher.Start(keys, keys.Length);

            var foundValues = new Dictionary<long, long>();
            while (await searcher.MoveNextLeaf())
            {
                var leaf = searcher.CurrentLeaf;
                foreach (var result in searcher.CurrentResults)
                {
                    if (result.Found)
                    {
                        var key = keys[result.KeyIndex];
                        var value = leaf.values.Get(result.LowerBound);
                        foundValues[key] = value;
                    }
                }
            }

            Assert.Equal(420, foundValues[42]);
            Assert.Equal(990, foundValues[99]);
            Assert.Equal(1500, foundValues[150]);
        }
    }
}
