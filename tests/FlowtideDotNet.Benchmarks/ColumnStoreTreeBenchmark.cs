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
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.Set;
using FlowtideDotNet.Core.Storage;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using FlowtideDotNet.Storage.Tree.Internal;
using Microsoft.Extensions.Logging.Abstractions;
using SqlParser.Ast;
using System.Diagnostics;

namespace FlowtideDotNet.Benchmarks
{
    public class ColumnStoreTreeBenchmark
    {
        private IStateManagerClient? nodeClient;
        private IBPlusTree<ColumnRowReference, long, ColumnKeyStorageContainer, PrimitiveListValueContainer<long>>? tree;
        private IBPlusTree<RowEvent, string, ListKeyContainer<RowEvent>, ListValueContainer<string>>? flxTree;
        private BPlusTreeBulkInserter<ColumnRowReference, long, ColumnKeyStorageContainer, PrimitiveListValueContainer<long>>? _bulkInserter;

        private EventBatchData data = new EventBatchData(
        [
            new Column(GlobalMemoryManager.Instance)
        ]);

        private List<RowEvent> rowEvents = new List<RowEvent>();

        [GlobalSetup]
        public void GlobalSetup()
        {
            StateManagerSync stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 1_000_000
            }, NullLoggerFactory.Instance, new System.Diagnostics.Metrics.Meter("storage"), "storage");

            stateManager.InitializeAsync().GetAwaiter().GetResult();

            nodeClient = stateManager.GetOrCreateClient("node1");

            for (int i = 0; i < 1_000_000; i++)
            {
                data.Columns[0].Add(new StringValue(i.ToString()));
                rowEvents.Add(RowEvent.Create(1, 0, b =>
                {
                    b.Add(i);
                }));
            }
        }

        [IterationSetup]
        public void IterationSetup()
        {
            Debug.Assert(nodeClient != null);
            tree = nodeClient.GetOrCreateTree("tree", new BPlusTreeOptions<ColumnRowReference, long, ColumnKeyStorageContainer, PrimitiveListValueContainer<long>>()
            {
                UseByteBasedPageSizes = true,
                Comparer = new ColumnComparer(1),
                KeySerializer = new ColumnStoreSerializer(1, GlobalMemoryManager.Instance),
                ValueSerializer = new PrimitiveListValueContainerSerializer<long>(GlobalMemoryManager.Instance),
                MemoryAllocator = GlobalMemoryManager.Instance
            }).GetAwaiter().GetResult();

            var casted = (BPlusTree<ColumnRowReference, long, ColumnKeyStorageContainer, PrimitiveListValueContainer<long>>)tree;
            _bulkInserter = new BPlusTreeBulkInserter<ColumnRowReference, long, ColumnKeyStorageContainer, PrimitiveListValueContainer<long>>(casted);
            tree.Clear();
            flxTree = nodeClient.GetOrCreateTree("flxtree", new BPlusTreeOptions<RowEvent, string, ListKeyContainer<RowEvent>, ListValueContainer<string>>()
            {
                BucketSize = 1024,
                Comparer = new BPlusTreeListComparer<RowEvent>(new BPlusTreeStreamEventComparer()),
                KeySerializer = new KeyListSerializer<RowEvent>(new StreamEventBPlusTreeSerializer()),
                ValueSerializer = new ValueListSerializer<string>(new StringSerializer()),
                MemoryAllocator = GlobalMemoryManager.Instance,
                UseByteBasedPageSizes = true,
                PageSizeBytes = 32 * 1024
            }).GetAwaiter().GetResult();
        }

        [Benchmark]
        public async Task ColumnarInsertInOrder()
        {
            Debug.Assert(tree != null);
            for (int i = 0; i < 1_000_000; i++)
            {
                await tree.Upsert(new ColumnRowReference()
                {
                    referenceBatch = data,
                    RowIndex = i
                }, i);
            }
        }

        private struct UpsertMutator : IRowMutator<ColumnRowReference, long>
        {
            public void GetSizePrefixSum(ColumnRowReference[] keys, ReadOnlySpan<int> indices, Span<int> sizes)
            {
                var batch = keys[0].referenceBatch;
                for (int i = 0; i < batch.Columns.Count; i++)
                {
                    batch.Columns[i].GetPrefixSumByteSizes(indices, sizes);
                }
            }

            public GenericWriteOperation Process(ColumnRowReference key, bool exists, in long existingData, ref long incomingData)
            {
                return GenericWriteOperation.Upsert;
            }
        }

        [Benchmark]
        public async Task ColumnarInsertBulkInOrder()
        {
            Debug.Assert(tree != null);
            Debug.Assert(_bulkInserter != null);
            int batchSize = 1_000;
            ColumnRowReference[] keys = new ColumnRowReference[batchSize];
            long[] values = new long[batchSize];
            for (int i = 0; i < 1_000_000; i++)
            {
                var count = Math.Min(batchSize, 1_000_000 - i);
                for (int k = 0; k < count; k++)
                {
                    keys[k] = new ColumnRowReference()
                    {
                        referenceBatch = data,
                        RowIndex = i,
                    };
                    values[k] = i;
                    i++;
                }
                await _bulkInserter.ApplyBatch(keys, values, count, new UpsertMutator(), count * 8);
            }
        }
    }
}
