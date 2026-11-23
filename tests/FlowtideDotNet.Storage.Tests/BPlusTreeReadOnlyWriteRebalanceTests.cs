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
    public class BPlusTreeReadOnlyWriteRebalanceTests
    {
        private struct UpsertMutator : IRowMutator<long, long>
        {
            public void GetSizePrefixSum(long[] keys, ReadOnlySpan<int> indices, Span<int> sizes)
            {
                for (int i = 0; i < indices.Length; i++)
                {
                    sizes[i] = 16 * (i + 1);
                }
            }

            public GenericWriteOperation Process(long key, bool exists, in long existingData, ref long incomingData, int sortedIndex)
            {
                return GenericWriteOperation.Upsert;
            }
        }

        private struct DeleteMutator : IRowMutator<long, long>
        {
            public void GetSizePrefixSum(long[] keys, ReadOnlySpan<int> indices, Span<int> sizes)
            {
                for (int i = 0; i < indices.Length; i++)
                {
                    sizes[i] = 16 * (i + 1);
                }
            }

            public GenericWriteOperation Process(long key, bool exists, in long existingData, ref long incomingData, int sortedIndex)
            {
                return exists ? GenericWriteOperation.Delete : GenericWriteOperation.None;
            }
        }

        private static async Task<StateManagerSync<object>> CreateStateManager(string name)
        {
            var stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 1000000,
                PersistentStorage = new FileCachePersistentStorage(new FileCacheOptions())
            }, NullLoggerFactory.Instance, new Meter(name), name, GlobalMemoryManager.Instance);
            await stateManager.InitializeAsync();
            return stateManager;
        }

        private static async Task<BPlusTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>> CreateLongTree(StateManagerSync<object> stateManager)
        {
            return (BPlusTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>)await stateManager.GetOrCreateClient("node1")
                .GetOrCreateTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>("tree",
                new BPlusTreeOptions<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>()
                {
                    PageSizeBytes = 100,
                    UseByteBasedPageSizes = true,
                    Comparer = new PrimitiveListComparer<long>(),
                    KeySerializer = new PrimitiveListKeyContainerSerializer<long>(GlobalMemoryManager.Instance),
                    ValueSerializer = new PrimitiveListValueContainerSerializer<long>(GlobalMemoryManager.Instance),
                    MemoryAllocator = GlobalMemoryManager.Instance
                });
        }

        private static async Task<(int leaves, int emptyLeaves)> CountLeaves<K, V, TKeyContainer, TValueContainer>(BPlusTree<K, V, TKeyContainer, TValueContainer> tree, long pageId)
            where TKeyContainer : IKeyContainer<K>
            where TValueContainer : IValueContainer<V>
        {
            var node = (await tree.m_stateClient.GetValue(pageId))!;
            try
            {
                if (node is LeafNode<K, V, TKeyContainer, TValueContainer> leaf)
                {
                    return (1, leaf.keys.Count == 0 ? 1 : 0);
                }
                var internalNode = (InternalNode<K, V, TKeyContainer>)node;
                int leaves = 0;
                int empty = 0;
                for (int i = 0; i < internalNode.children.Count; i++)
                {
                    var (l, e) = await CountLeaves(tree, internalNode.children[i]);
                    leaves += l;
                    empty += e;
                }
                return (leaves, empty);
            }
            finally
            {
                node.Return();
            }
        }

        [Fact]
        public async Task BulkDeleteOfEveryKeyMergesTheEmptiedLeaves()
        {
            using var stateManager = await CreateStateManager("rebalance_bulk_delete");
            var tree = await CreateLongTree(stateManager);
            var bulkInserter = new BPlusTreeBulkInserter<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>(tree);

            var count = 400;
            var keys = new long[count];
            var values = new long[count];
            for (int i = 0; i < count; i++)
            {
                keys[i] = i;
                values[i] = i;
            }
            await bulkInserter.ApplyBatch(keys, values, count, new UpsertMutator(), count * 16);
            var (leavesBefore, _) = await CountLeaves(tree, tree.m_stateClient.Metadata!.Root);
            Assert.True(leavesBefore > 20);

            await bulkInserter.ApplyBatch(keys, values, count, new DeleteMutator(), count * 16);

            var (leavesAfter, emptyAfter) = await CountLeaves(tree, tree.m_stateClient.Metadata!.Root);
            Assert.True(leavesAfter <= 2, $"{leavesAfter} leaves, {emptyAfter} empty, {leavesBefore} before the delete");
        }

        [Fact]
        public async Task BulkChurnAtConstantLiveSizeKeepsTheLeafCountBounded()
        {
            using var stateManager = await CreateStateManager("rebalance_bulk_churn");
            var tree = await CreateLongTree(stateManager);
            var bulkInserter = new BPlusTreeBulkInserter<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>(tree);

            var batch = 50;
            var keys = new long[batch];
            var values = new long[batch];
            long next = 0;
            int maxLeaves = 0;
            for (int round = 0; round < 40; round++)
            {
                for (int i = 0; i < batch; i++)
                {
                    keys[i] = next + i;
                    values[i] = next + i;
                }
                await bulkInserter.ApplyBatch(keys, values, batch, new UpsertMutator(), batch * 16);
                await bulkInserter.ApplyBatch(keys, values, batch, new DeleteMutator(), batch * 16);
                next += batch;
                var (leaves, _) = await CountLeaves(tree, tree.m_stateClient.Metadata!.Root);
                maxLeaves = Math.Max(maxLeaves, leaves);
            }

            var (leavesAfter, emptyAfter) = await CountLeaves(tree, tree.m_stateClient.Metadata!.Root);
            Assert.True(leavesAfter <= 2, $"{leavesAfter} leaves, {emptyAfter} empty, live rows 0");
        }

        private static async Task<BPlusTree<KeyValuePair<long, long>, string, ListKeyContainerWithSize, ListValueContainer<string>>> CreateSizedTree(StateManagerSync<object> stateManager)
        {
            return (BPlusTree<KeyValuePair<long, long>, string, ListKeyContainerWithSize, ListValueContainer<string>>)await stateManager.GetOrCreateClient("node1")
                .GetOrCreateTree<KeyValuePair<long, long>, string, ListKeyContainerWithSize, ListValueContainer<string>>("tree",
                new BPlusTreeOptions<KeyValuePair<long, long>, string, ListKeyContainerWithSize, ListValueContainer<string>>()
                {
                    BucketSize = 8,
                    PageSizeBytes = 32 * 1024,
                    Comparer = new ListWithSizeComparer(new LongComparer()),
                    KeySerializer = new ListKeyWithSizeSerializer(10),
                    ValueSerializer = new ValueListSerializer<string>(new StringSerializer()),
                    UseByteBasedPageSizes = true,
                    MemoryAllocator = GlobalMemoryManager.Instance
                });
        }

        [Fact]
        public async Task SavePageWithResizeSplitsAGrownRootLeaf()
        {
            using var stateManager = await CreateStateManager("rebalance_save_page_root");
            var tree = await CreateSizedTree(stateManager);
            for (long i = 0; i < 20; i++)
            {
                await tree.Upsert(new KeyValuePair<long, long>(i, 10), $"{i}");
            }
            var depthBefore = tree.m_stateClient.Metadata!.Depth;
            var (leavesBefore, _) = await CountLeaves(tree, tree.m_stateClient.Metadata!.Root);
            Assert.Equal(1, leavesBefore);

            using (var iterator = tree.CreateIterator())
            {
                await iterator.SeekFirst();
                await foreach (var page in iterator)
                {
                    for (int i = 0; i < page.Keys.Count; i++)
                    {
                        page.Keys.Update(i, new KeyValuePair<long, long>(page.Keys.Get(i).Key, 2000));
                    }
                    await page.SavePage(true);
                    break;
                }
            }

            var (leavesAfter, _) = await CountLeaves(tree, tree.m_stateClient.Metadata!.Root);
            Assert.True(leavesAfter > 1, $"{leavesAfter} leaves after, depth {depthBefore} before and {tree.m_stateClient.Metadata!.Depth} after");
        }

        [Fact]
        public async Task SavePageWithResizeSplitsAGrownLeafBelowTheRoot()
        {
            using var stateManager = await CreateStateManager("rebalance_save_page_leaf");
            var tree = await CreateSizedTree(stateManager);
            for (long i = 0; i < 200; i++)
            {
                await tree.Upsert(new KeyValuePair<long, long>(i, 400), $"{i}");
            }
            var (leavesBefore, _) = await CountLeaves(tree, tree.m_stateClient.Metadata!.Root);
            Assert.True(leavesBefore > 1, $"{leavesBefore} leaves before");

            using (var iterator = tree.CreateIterator())
            {
                await iterator.SeekFirst();
                await foreach (var page in iterator)
                {
                    for (int i = 0; i < page.Keys.Count; i++)
                    {
                        page.Keys.Update(i, new KeyValuePair<long, long>(page.Keys.Get(i).Key, 4000));
                    }
                    await page.SavePage(true);
                    break;
                }
            }

            var (leavesAfter, _) = await CountLeaves(tree, tree.m_stateClient.Metadata!.Root);
            Assert.True(leavesAfter > leavesBefore, $"{leavesAfter} leaves after, {leavesBefore} before");
        }
    }
}
