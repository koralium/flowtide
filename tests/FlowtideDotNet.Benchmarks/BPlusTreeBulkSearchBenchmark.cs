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

using BenchmarkDotNet.Attributes;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Comparers;
using FlowtideDotNet.Storage.FileCache;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Storage.Tree.Internal;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Benchmarks
{
    [MemoryDiagnoser]
    public class BPlusTreeBulkSearchBenchmark
    {
        private StateManagerSync _stateManager = null!;
        private IStateManagerClient _nodeClient = null!;
        private BPlusTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>> _tree = null!;
        private long[] _searchKeys = null!;
        private long[] _inOrderSearchKeys = null!;
        private IBplusTreeBulkSearch<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>, PrimitiveListComparer<long>> _bulkSearcher = null!;
        private int[] _presortedIndices = null!;


        [Params(10_000)]
        public int SearchKeyCount { get; set; }

        [Params(1_000_000)]
        public int TreeSize { get; set; }

        [GlobalSetup]
        public void GlobalSetup()
        {
            _stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 1_000_000,
                PersistentStorage = new FileCachePersistentStorage(new FileCacheOptions())
            }, NullLoggerFactory.Instance, new Meter("bulk_search_bench"), "bulk_search_bench");

            _stateManager.InitializeAsync().GetAwaiter().GetResult();
            _nodeClient = _stateManager.GetOrCreateClient("node1");

            _tree = (BPlusTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>)
                _nodeClient.GetOrCreateTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>("tree",
                    new BPlusTreeOptions<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>()
                    {
                        UseByteBasedPageSizes = true,
                        Comparer = new PrimitiveListComparer<long>(),
                        KeySerializer = new PrimitiveListKeyContainerSerializer<long>(GlobalMemoryManager.Instance),
                        ValueSerializer = new PrimitiveListValueContainerSerializer<long>(GlobalMemoryManager.Instance),
                        MemoryAllocator = GlobalMemoryManager.Instance
                    }).GetAwaiter().GetResult();

            var inserter = _tree.CreateBulkInserter();
            _bulkSearcher = _tree.CreateBulkSearcher(new PrimitiveListComparer<long>());

            // Populate tree
            for (long i = 0; i < TreeSize; i++)
            {
                _tree.Upsert(i, i * 10).GetAwaiter().GetResult();
            }

            // Generate search keys spread across the tree
            var rng = new Random(42);
            _searchKeys = new long[SearchKeyCount];
            for (int i = 0; i < SearchKeyCount; i++)
            {
                _searchKeys[i] = rng.NextInt64(0, TreeSize);
            }

            _presortedIndices = inserter.SortAndGetIndices(_searchKeys, _searchKeys.Length);

            _inOrderSearchKeys = new long[SearchKeyCount];

            for (int i = 0; i < SearchKeyCount; i++)
            {
                _inOrderSearchKeys[i] = TreeSize - SearchKeyCount + i;
            }
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            _stateManager?.Dispose();
        }

        //[Benchmark(Baseline = true)]
        //public async Task IteratorSeek()
        //{
        //    var comparer = new PrimitiveListComparer<long>();
        //    using var iterator = _tree.CreateIterator();

        //    for (int i = 0; i < _searchKeys.Length; i++)
        //    {
        //        await iterator.Seek(_searchKeys[i], comparer);

        //        await foreach (var page in iterator)
        //        {
        //            // Just consume the first page to match bulk search behavior
        //            break;
        //        }
        //    }
        //}

        //[Benchmark()]
        //public async Task IteratorSeekInOrder()
        //{
        //    var comparer = new PrimitiveListComparer<long>();
        //    using var iterator = _tree.CreateIterator();

        //    for (int i = 0; i < _inOrderSearchKeys.Length; i++)
        //    {
        //        await iterator.Seek(_inOrderSearchKeys[i], comparer);

        //        await foreach (var page in iterator)
        //        {
        //            // Just consume the first page to match bulk search behavior
        //            break;
        //        }
        //    }
        //}

        //[Benchmark]
        //public async Task BulkSearchInOrder()
        //{
        //    await _bulkSearcher.Start(_inOrderSearchKeys, _inOrderSearchKeys.Length);

        //    while (await _bulkSearcher.MoveNextLeaf())
        //    {
        //        // Consume results
        //        var _ = _bulkSearcher.CurrentResults;
        //    }
        //}

        [Benchmark]
        public async Task BulkSearchPreSorted()
        {
            await _bulkSearcher.Start(_searchKeys, _searchKeys.Length, _presortedIndices);

            while (await _bulkSearcher.MoveNextLeaf())
            {
                // Consume results
                var _ = _bulkSearcher.CurrentResults;
            }
        }

        [Benchmark]
        public async Task BulkSearch()
        {
            await _bulkSearcher.Start(_searchKeys, _searchKeys.Length);

            while (await _bulkSearcher.MoveNextLeaf())
            {
                // Consume results
                var _ = _bulkSearcher.CurrentResults;
            }
        }
    }
}
