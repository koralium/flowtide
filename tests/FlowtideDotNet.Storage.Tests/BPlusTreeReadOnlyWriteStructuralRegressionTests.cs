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

namespace FlowtideDotNet.Storage.Tests;

public class BPlusTreeReadOnlyWriteStructuralRegressionTests
{
    private readonly struct UpsertMutator : IRowMutator<long, long>
    {
        public void GetSizePrefixSum(long[] keys, ReadOnlySpan<int> indices, Span<int> sizes)
        {
            for (int i = 0; i < indices.Length; i++)
            {
                sizes[i] = (i + 1) * 16;
            }
        }

        public GenericWriteOperation Process(long key, bool exists, in long existingData, ref long incomingData, int sortedIndex)
            => GenericWriteOperation.Upsert;
    }

    private readonly struct DeleteMutator : IRowMutator<long, long>
    {
        public void GetSizePrefixSum(long[] keys, ReadOnlySpan<int> indices, Span<int> sizes)
        {
            for (int i = 0; i < indices.Length; i++)
            {
                sizes[i] = (i + 1) * 16;
            }
        }

        public GenericWriteOperation Process(long key, bool exists, in long existingData, ref long incomingData, int sortedIndex)
            => exists ? GenericWriteOperation.Delete : GenericWriteOperation.None;
    }

    [Fact]
    public async Task BulkDeleteDoesNotRetainLeavesWithTotalChurn()
    {
        using var storage = new FileCachePersistentStorage(new FileCacheOptions()
        {
            DirectoryPath = "./data/temp/reviewer_a_bulk_churn"
        });
        using var manager = new StateManagerSync<object>(new StateManagerOptions()
        {
            CachePageCount = 1000000,
            PersistentStorage = storage
        }, NullLoggerFactory.Instance, new Meter("reviewer_a_bulk_churn"), "reviewer_a_bulk_churn", GlobalMemoryManager.Instance);
        await manager.InitializeAsync();
        var tree = await manager.GetOrCreateClient("node").GetOrCreateTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>("tree",
            new BPlusTreeOptions<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>()
            {
                PageSizeBytes = 1024,
                UseByteBasedPageSizes = true,
                Comparer = new PrimitiveListComparer<long>(),
                KeySerializer = new PrimitiveListKeyContainerSerializer<long>(GlobalMemoryManager.Instance),
                ValueSerializer = new PrimitiveListValueContainerSerializer<long>(GlobalMemoryManager.Instance),
                MemoryAllocator = GlobalMemoryManager.Instance
            });
        var bulk = tree.CreateBulkInserter();
        var pageCounts = new List<int>();
        for (int cycle = 0; cycle < 6; cycle++)
        {
            var keys = new long[100];
            for (int i = 0; i < keys.Length; i++)
            {
                keys[i] = cycle * 100 + i;
            }
            await bulk.ApplyBatch(keys, keys.ToArray(), keys.Length, new UpsertMutator(), keys.Length * 16);
            await bulk.ApplyBatch(keys, new long[keys.Length], keys.Length, new DeleteMutator(), keys.Length * 16);
            using var iterator = tree.CreateIterator();
            await iterator.SeekFirst();
            int pages = 0;
            int rows = 0;
            await foreach (var page in iterator)
            {
                pages++;
                rows += page.Keys.Count;
            }
            Assert.Equal(0, rows);
            pageCounts.Add(pages);
        }
        Assert.True(pageCounts[^1] <= pageCounts[0], $"Live rows remain zero; leaf counts after each cycle: {string.Join(", ", pageCounts)}");
    }

    [Theory]
    [InlineData(20, false)]
    [InlineData(20, true)]
    [InlineData(100, false)]
    [InlineData(100, true)]
    public async Task SavePageWithResizeSplitsAGrownLeaf(int rows, bool backward)
    {
        var name = $"reviewer_a_page_resize_{rows}_{backward}";
        using var storage = new FileCachePersistentStorage(new FileCacheOptions() { DirectoryPath = $"./data/temp/{name}" });
        using var manager = new StateManagerSync<object>(new StateManagerOptions()
        {
            CachePageCount = 1000000,
            PersistentStorage = storage
        }, NullLoggerFactory.Instance, new Meter(name), name, GlobalMemoryManager.Instance);
        await manager.InitializeAsync();
        var tree = await manager.GetOrCreateClient("node").GetOrCreateTree<KeyValuePair<long, long>, string, ListKeyContainerWithSize, ListValueContainer<string>>("tree",
            new BPlusTreeOptions<KeyValuePair<long, long>, string, ListKeyContainerWithSize, ListValueContainer<string>>()
            {
                PageSizeBytes = 10000,
                UseByteBasedPageSizes = true,
                UsePreviousPointers = true,
                Comparer = new ListWithSizeComparer(new LongComparer()),
                KeySerializer = new ListKeyWithSizeSerializer(200),
                ValueSerializer = new ValueListSerializer<string>(new StringSerializer()),
                MemoryAllocator = GlobalMemoryManager.Instance
            });
        for (int i = 0; i < rows; i++)
        {
            await tree.Upsert(new KeyValuePair<long, long>(i, 200), i.ToString());
        }
        using var iterator = backward ? tree.CreateBackwardIterator() : tree.CreateIterator();
        await iterator.Seek(new KeyValuePair<long, long>(0, 200));
        await using var enumerator = iterator.GetAsyncEnumerator();
        Assert.True(await enumerator.MoveNextAsync());
        var page = enumerator.Current;
        var originalCount = page.Keys.Count;
        Assert.True(originalCount > 16);
        for (int i = 0; i < originalCount; i++)
        {
            var key = page.Keys.Get(i);
            page.Keys.Update(i, new KeyValuePair<long, long>(key.Key, 2000));
        }
        await page.SavePage(true);
        Assert.True(page.Keys.Count < originalCount, $"SavePage(true) left all {originalCount} rows in the oversized leaf");
    }
}
