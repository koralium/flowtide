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
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Storage.Tree.Internal;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using static SqlParser.Ast.FetchDirection;

namespace DifferntialCompute.Benchmarks
{
    [MemoryDiagnoser]
    public class BPlusTreeBulkInsertBenchmark
    {
        private StateManagerSync _stateManager = null!;
        private IStateManagerClient _nodeClient = null!;
        private BPlusTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>> _tree = null!;
        private SortedDictionary<long, long> _sortedlist = new SortedDictionary<long, long>();

        [Params(1_000_000)] //, 1_000_000)]
        public int ElementCount { get; set; }

        [GlobalSetup]
        public void GlobalSetup()
        {
            _stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 1_000_000,
                PersistentStorage = new FileCachePersistentStorage(new FileCacheOptions())
            }, NullLoggerFactory.Instance, new Meter("bulk_insert_bench"), "bulk_insert_bench");

            _stateManager.InitializeAsync().GetAwaiter().GetResult();
            _nodeClient = _stateManager.GetOrCreateClient("node1");
        }

        Random? r;

        [IterationSetup]
        public void IterationSetup()
        {
            r = new Random(123);
            _tree = (BPlusTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>)
                _nodeClient.GetOrCreateTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>("tree",
                    new BPlusTreeOptions<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>()
                    {
                        PageSizeBytes = 4096,
                        UseByteBasedPageSizes = true,
                        Comparer = new PrimitiveListComparer<long>(),
                        KeySerializer = new PrimitiveListKeyContainerSerializer<long>(GlobalMemoryManager.Instance),
                        ValueSerializer = new PrimitiveListValueContainerSerializer<long>(GlobalMemoryManager.Instance),
                        MemoryAllocator = GlobalMemoryManager.Instance
                    }).GetAwaiter().GetResult();
            _tree.Clear();
            _sortedlist.Clear();
        }

        [Benchmark]
        public void SortedList()
        {
            for (int i = 0; i < ElementCount; i++)
            {
                var key = i;//r.Next();

                if (!_sortedlist.ContainsKey(key))
                {
                    _sortedlist.Add(key, i);
                }
                else
                {
                    _sortedlist[key] = i;
                }
            }
        }

        [Benchmark(Baseline = true)]
        public async Task RMWNoResult_SingleInsert()
        {
            for (int i = 0; i < ElementCount; i++)
            {
                await _tree.RMWNoResult(i, i, static (input, current, exists) => (input, GenericWriteOperation.Upsert));
            }
        }

        [Benchmark]
        public async Task BulkInsert_Batched()
        {
            var bulkInserter = new BPlusTreeBulkInserter<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>(_tree);
            var batchSize = 1000;

            var keys = new long[batchSize];
            var values = new long[batchSize];

            for (int offset = 0; offset < ElementCount; offset += batchSize)
            {
                var count = Math.Min(batchSize, ElementCount - offset);
                

                for (int i = 0; i < count; i++)
                {
                    keys[i] = offset + i;
                    values[i] = offset + i;
                }

                await bulkInserter.ApplyBatch(keys, values, count, new UpsertMutator());
            }
        }

        private struct UpsertMutator : IRowMutator<long, long>
        {
            public GenericWriteOperation Process(long key, bool exists, in long existingData, ref long incomingData)
            {
                return GenericWriteOperation.Upsert;
            }
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            _stateManager?.Dispose();
        }
    }
}
