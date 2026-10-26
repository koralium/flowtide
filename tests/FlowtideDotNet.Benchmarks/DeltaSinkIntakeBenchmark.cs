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
using BenchmarkDotNet.Engines;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.Sort;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Benchmarks
{
    [MemoryDiagnoser]
    [MedianColumn, MinColumn, MaxColumn]
    [SimpleJob(RunStrategy.Monitoring, launchCount: 1, warmupCount: 1, iterationCount: 5)]
    public class DeltaSinkIntakeBenchmark
    {
        private const int Rows = 1_000_000;
        private const int Columns = 4;
        private static readonly TimeSpan s_didNotFinish = TimeSpan.FromSeconds(30);

        private readonly BatchSorter _sorter = new BatchSorter(Columns);
        private readonly IColumn[] _sortColumns = new IColumn[Columns];
        private FileCachePersistentStorage _storage = null!;
        private StateManagerSync _stateManager = null!;
        private IBPlusTree<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>> _tree = null!;
        private IBPlusTreeBulkInserter<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>> _inserter = null!;
        private List<(EventBatchData Data, int[] Weights)> _batches = null!;
        private ColumnRowReference[] _keys = null!;
        private int[] _values = null!;
        private int[] _sorted = null!;
        private int[] _tags = null!;

        [Params(100, 1_000, 10_000)]
        public int BatchSize { get; set; }

        // In memory, or spilled to disk
        [ParamsSource(nameof(CacheSizes))]
        public int CachePages { get; set; }

        // FLOWTIDE_INTAKE_CACHE runs one cache size, spilled cases can run apart
        public static IEnumerable<int> CacheSizes()
        {
            var only = Environment.GetEnvironmentVariable("FLOWTIDE_INTAKE_CACHE");
            return only != null ? new[] { int.Parse(only) } : new[] { 1_000_000, 64 };
        }

        [GlobalSetup]
        public void GlobalSetup()
        {
            // The state manager does not dispose a given storage
            _storage = new FileCachePersistentStorage(new FileCacheOptions());
            _stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = CachePages,
                MinCachePageCount = Math.Min(CachePages, 1000),
                PersistentStorage = _storage
            }, NullLoggerFactory.Instance, new Meter("delta_sink_intake"), "delta_sink_intake", GlobalMemoryManager.Instance);
            _stateManager.InitializeAsync().GetAwaiter().GetResult();
            // Built like the sink's temporary tree, one per process
            _tree = _stateManager.GetOrCreateClient("sink").GetOrCreateTree("temporary", new BPlusTreeOptions<ColumnRowReference, int, ColumnKeyStorageContainer, PrimitiveListValueContainer<int>>()
            {
                Comparer = new ColumnComparer(Columns),
                KeySerializer = new ColumnStoreSerializer(Columns, GlobalMemoryManager.Instance),
                ValueSerializer = new PrimitiveListValueContainerSerializer<int>(GlobalMemoryManager.Instance),
                MemoryAllocator = GlobalMemoryManager.Instance,
                UseByteBasedPageSizes = true,
            }).GetAwaiter().GetResult();
            _inserter = _tree.CreateBulkInserter();
            _batches = CreateBatches(BatchSize);
            _keys = new ColumnRowReference[BatchSize];
            _values = new int[BatchSize];
            _sorted = new int[BatchSize];
            _tags = new int[BatchSize];
        }

        // Every method gets the same batches in the same order
        private static List<(EventBatchData Data, int[] Weights)> CreateBatches(int batchSize)
        {
            var random = new Random(123);
            var written = new List<IDataValue[]>();
            var batches = new List<(EventBatchData, int[])>();
            for (int offset = 0; offset < Rows; offset += batchSize)
            {
                var count = Math.Min(batchSize, Rows - offset);
                var columns = Enumerable.Range(0, Columns).Select(_ => new Column(GlobalMemoryManager.Instance)).ToArray();
                var weights = new int[count];
                var batchStart = written.Count;
                for (int i = 0; i < count; i++)
                {
                    IDataValue[] row;
                    var kind = random.Next(100);
                    if (kind < 10 && written.Count > 0)
                    {
                        // A retraction of an earlier row
                        row = written[random.Next(written.Count)];
                        weights[i] = -1;
                    }
                    else if (kind < 15 && written.Count > batchStart)
                    {
                        // A duplicate inside the batch
                        row = written[random.Next(batchStart, written.Count)];
                        weights[i] = 1;
                    }
                    else
                    {
                        row = new IDataValue[]
                        {
                            new Int64Value(random.NextInt64()),
                            new StringValue($"s{random.Next(1_000_000)}"),
                            new DoubleValue(random.NextDouble()),
                            new TimestampTzValue(DateTime.UnixEpoch.AddSeconds(random.Next(1_000_000_000)))
                        };
                        written.Add(row);
                        weights[i] = 1;
                    }
                    for (int c = 0; c < Columns; c++)
                    {
                        columns[c].Add(row[c]);
                    }
                }
                batches.Add((new EventBatchData(columns), weights));
            }
            return batches;
        }

        [IterationCleanup]
        public void IterationCleanup()
        {
            _tree.Clear().GetAwaiter().GetResult();
        }

        [Benchmark(Baseline = true)]
        public async Task PerRow()
        {
            var watch = Stopwatch.StartNew();
            foreach (var (data, weights) in _batches)
            {
                ThrowIfLate(watch);
                for (int i = 0; i < weights.Length; i++)
                {
                    var rowRef = new ColumnRowReference() { referenceBatch = data, RowIndex = i };
                    // The sink's own lambda
                    await _tree.RMWNoResult(in rowRef, weights[i], (input, current, exists) =>
                    {
                        if (exists)
                        {
                            var newWeight = current + input;
                            if (newWeight == 0)
                            {
                                return (0, GenericWriteOperation.Delete);
                            }
                            return (newWeight, GenericWriteOperation.Upsert);
                        }
                        return (input, GenericWriteOperation.Upsert);
                    });
                }
            }
            ThrowIfLate(watch);
        }

        [Benchmark]
        public async Task Bulk()
        {
            var watch = Stopwatch.StartNew();
            foreach (var (data, weights) in _batches)
            {
                ThrowIfLate(watch);
                Fill(data, weights);
                await _inserter.ApplyBatch(_keys, _values, weights.Length, new WeightMutator(), data.GetByteSize() + weights.Length * sizeof(int));
            }
            ThrowIfLate(watch);
        }

        [Benchmark]
        public async Task BulkPresorted()
        {
            var watch = Stopwatch.StartNew();
            foreach (var (data, weights) in _batches)
            {
                ThrowIfLate(watch);
                Fill(data, weights);
                Sort(data, weights.Length);
                await _inserter.ApplyBatch(_keys, _values, weights.Length, _sorted, _tags, new WeightMutator(), data.GetByteSize() + weights.Length * sizeof(int));
            }
            ThrowIfLate(watch);
        }

        // Full row sort with duplicate tags, like the set operator
        private void Sort(EventBatchData data, int count)
        {
            for (int c = 0; c < Columns; c++)
            {
                _sortColumns[c] = data.Columns[c];
            }
            for (int i = 0; i < count; i++)
            {
                _sorted[i] = i;
            }
            var indices = _sorted.AsSpan(0, count);
            var tags = _tags.AsSpan(0, count);
            _sorter.SortDataWithTags(_sortColumns, ref indices, ref tags);
        }

        // Past 30 s the case stops and reports a failure
        private static void ThrowIfLate(Stopwatch watch)
        {
            if (watch.Elapsed > s_didNotFinish)
            {
                throw new TimeoutException($"Did not finish within {s_didNotFinish.TotalSeconds:0} s");
            }
        }

        // Row index is the key, required by the inserter
        private void Fill(EventBatchData data, int[] weights)
        {
            for (int i = 0; i < weights.Length; i++)
            {
                _keys[i] = new ColumnRowReference() { referenceBatch = data, RowIndex = i };
                _values[i] = weights[i];
            }
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            _stateManager.Dispose();
            _storage.Dispose();
            foreach (var (data, _) in _batches)
            {
                data.Dispose();
            }
        }

        // Sums weights like the sink, zero removes the key
        private readonly struct WeightMutator : IRowMutator<ColumnRowReference, int>
        {
            public void GetSizePrefixSum(ColumnRowReference[] keys, ReadOnlySpan<int> indices, Span<int> sizes)
            {
                // All keys come from the same batch
                var columns = keys[0].referenceBatch.GetColumns_Unsafe();
                for (int i = 0; i < columns.Length; i++)
                {
                    columns[i].GetPrefixSumByteSizes(indices, sizes);
                }
                var valueBytes = 0;
                for (int i = 0; i < indices.Length; i++)
                {
                    valueBytes += sizeof(int);
                    sizes[i] += valueBytes;
                }
            }

            public GenericWriteOperation Process(ColumnRowReference key, bool exists, in int existingData, ref int incomingData, int sortedIndex)
            {
                if (exists)
                {
                    incomingData += existingData;
                    return incomingData == 0 ? GenericWriteOperation.Delete : GenericWriteOperation.Upsert;
                }
                return incomingData == 0 ? GenericWriteOperation.None : GenericWriteOperation.Upsert;
            }
        }
    }
}
