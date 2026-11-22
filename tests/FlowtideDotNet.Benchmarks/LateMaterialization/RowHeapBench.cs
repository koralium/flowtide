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

using FlowtideDotNet.Benchmarks.ReservoirReaders;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Operators.Join.MultiJoin;
using FlowtideDotNet.Core.Operators.LateMaterialization;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Text;
using System.Text.Json;

namespace FlowtideDotNet.Benchmarks.LateMaterialization
{
    /// <summary>
    /// The row heap against multi join's own storage, run with --row-heap.
    /// Options: --mode gate|gather|churn --arm heap|multijoin --rows --batch --order append|local|shuffled
    /// --shape wide|mixed|nested --repeat --cache --compression zstd|none --rounds --seed --page-bytes --queue
    /// --keys shuffled|sorted --permute 0|1 --change (percent of the rows a round) --survivors none|page|chunk
    /// --merge 0|1 (0 leaves the merging out, as a baseline) --stored-ratio (default 1.5)
    /// --store 0|1 (1 retires through a keyed row store)
    /// </summary>
    internal static class RowHeapBench
    {
        private static readonly string[] Words = new string[]
        {
            "order", "customer", "delivery", "pending", "shipped", "invoice", "account", "region", "north", "south",
            "priority", "standard", "express", "returned", "warehouse", "supplier", "contract", "payment", "credit", "debit",
            "monthly", "yearly", "balance", "address", "street", "postal", "country", "product", "category", "discount"
        };

        private static readonly StructHeader Fields = StructHeader.Create("number", "text");

        public static async Task<int> Run(string[] args)
        {
            string mode = ReservoirReaderBench.Option(args, "mode", "gate");
            string arm = ReservoirReaderBench.Option(args, "arm", "heap");
            int rows = int.Parse(ReservoirReaderBench.Option(args, "rows", "100000"), CultureInfo.InvariantCulture);
            int batch = int.Parse(ReservoirReaderBench.Option(args, "batch", "1000"), CultureInfo.InvariantCulture);
            string order = ReservoirReaderBench.Option(args, "order", "append");
            string shape = ReservoirReaderBench.Option(args, "shape", "wide");
            int repeat = int.Parse(ReservoirReaderBench.Option(args, "repeat", "1"), CultureInfo.InvariantCulture);
            int cache = int.Parse(ReservoirReaderBench.Option(args, "cache", "1000000"), CultureInfo.InvariantCulture);
            string compression = ReservoirReaderBench.Option(args, "compression", "zstd");
            int rounds = int.Parse(ReservoirReaderBench.Option(args, "rounds", "5"), CultureInfo.InvariantCulture);
            int seed = int.Parse(ReservoirReaderBench.Option(args, "seed", "1"), CultureInfo.InvariantCulture);
            int pageBytes = int.Parse(ReservoirReaderBench.Option(args, "page-bytes", "32768"), CultureInfo.InvariantCulture);
            string directory = Path.Join(ReservoirReaderBench.Option(args, "state-dir", Path.GetTempPath()), $"row-heap-{Guid.NewGuid():N}");
            rows -= rows % batch;

            var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
            using var manager = new StateManagerSync<object>(
                new StateManagerOptions()
                {
                    PersistentStorage = storage,
                    CachePageCount = cache,
                    MinCachePageCount = Math.Min(1000, cache),
                    SerializeOptions = new StateSerializeOptions()
                    {
                        CompressionType = compression == "zstd" ? CompressionType.Zstd : CompressionType.None,
                        CompressionMethod = CompressionMethod.Page
                    },
                    TemporaryStorageOptions = new FileCacheOptions() { DirectoryPath = Path.Join(directory, "tmp") }
                },
                NullLoggerFactory.Instance,
                new Meter("rowHeapBench"),
                "rowHeapBench",
                GlobalMemoryManager.Instance);
            await manager.InitializeAsync();
            var client = manager.GetOrCreateClient("bench");
            int queue = int.Parse(ReservoirReaderBench.Option(args, "queue", "16"), CultureInfo.InvariantCulture);
            string keys = ReservoirReaderBench.Option(args, "keys", "shuffled");
            bool permute = ReservoirReaderBench.Option(args, "permute", "0") == "1";
            double storedRatio = double.Parse(ReservoirReaderBench.Option(args, "stored-ratio", "1.5"), CultureInfo.InvariantCulture);
            int mergePages = int.Parse(ReservoirReaderBench.Option(args, "merge-pages", "256"), CultureInfo.InvariantCulture);
            var options = new RowHeapOptions() { PageTargetBytes = pageBytes, WriteQueueCapacity = queue, MaxStoredRatio = storedRatio, CompactPagesPerCall = mergePages };

            if (mode == "churn")
            {
                int change = int.Parse(ReservoirReaderBench.Option(args, "change", "10"), CultureInfo.InvariantCulture);
                string survivors = ReservoirReaderBench.Option(args, "survivors", "none");
                bool merge = ReservoirReaderBench.Option(args, "merge", "1") == "1";
                bool drain = ReservoirReaderBench.Option(args, "compact", "all") == "all";
                bool keyed = ReservoirReaderBench.Option(args, "store", "0") == "1";
                return await RunChurn(manager, client, options, shape, rows, batch, rounds, change, survivors, merge, drain, keyed, seed, new { Store = keyed, Mode = mode, Shape = shape, Change = change, Survivors = survivors, Merge = merge, Drain = drain, MergePages = mergePages, StoredRatio = storedRatio, Rounds = rounds, Compression = compression, PageBytes = pageBytes });
            }
            var data = new Data(rows, batch, shape, seed, keys == "sorted");
            var picks = Picks(rows, repeat, order, seed);
            if (mode == "gather")
            {
                return await RunGather(manager, client, options, data, picks, rounds, new { Mode = mode, Shape = shape, Order = order, Repeat = repeat, Cache = cache, Compression = compression, PageBytes = pageBytes });
            }
            if (arm == "multijoin")
            {
                return await RunMultiJoin(manager, client, data, picks, new { Mode = mode, Arm = arm, Shape = shape, Order = order, Compression = compression, Keys = keys });
            }
            return await RunHeap(manager, client, options, data, picks, permute, new { Mode = mode, Arm = arm, Shape = shape, Order = order, Compression = compression, PageBytes = pageBytes, Queue = queue, Permute = permute });
        }

        // The rows to fetch, as positions in append order
        private static int[] Picks(int rows, int repeat, string order, int seed)
        {
            var picks = new int[rows * repeat];
            for (int i = 0; i < picks.Length; i++)
            {
                picks[i] = i / repeat;
            }
            var random = new Random(seed + 7);
            if (order == "shuffled")
            {
                random.Shuffle(picks);
            }
            else if (order == "local")
            {
                // Shuffled inside runs of 200 rows, a few pages each
                for (int start = 0; start < picks.Length; start += 200)
                {
                    random.Shuffle(picks.AsSpan(start, Math.Min(200, picks.Length - start)));
                }
            }
            return picks;
        }

        private sealed class Data
        {
            public Data(int rows, int batch, string shape, int seed, bool sortedKeys)
            {
                Rows = rows;
                Batch = batch;
                var random = new Random(seed);
                var keys = new long[rows];
                for (int i = 0; i < rows; i++)
                {
                    keys[i] = i;
                }
                // Sorted keys arrive in tree order, the best case for a tree
                if (!sortedKeys)
                {
                    random.Shuffle(keys);
                }
                Keys = keys;
                int batches = rows / batch;
                Payload = new EventBatchData[batches];
                Keyed = new EventBatchData[batches];
                for (int b = 0; b < batches; b++)
                {
                    var payload = PayloadColumns(shape, random, batch);
                    var keyed = new IColumn[payload.Length + 1];
                    var key = Column.Create(GlobalMemoryManager.Instance);
                    for (int i = 0; i < batch; i++)
                    {
                        key.Add(new Int64Value(keys[b * batch + i]));
                    }
                    keyed[0] = key;
                    Array.Copy(payload, 0, keyed, 1, payload.Length);
                    Payload[b] = new EventBatchData(payload);
                    Keyed[b] = new EventBatchData(keyed);
                }
                PayloadColumnCount = Payload[0].Columns.Count;
                PayloadBytes = 0;
                for (int b = 0; b < batches; b++)
                {
                    PayloadBytes += Payload[b].GetByteSize();
                }
            }

            public int Rows { get; }

            public int Batch { get; }

            public long[] Keys { get; }

            public EventBatchData[] Payload { get; }

            public EventBatchData[] Keyed { get; }

            public int PayloadColumnCount { get; }

            public long PayloadBytes { get; }
        }

        private static string Text(Random random, int bytes)
        {
            var text = new StringBuilder(bytes + 12);
            while (text.Length < bytes)
            {
                text.Append(Words[random.Next(Words.Length)]).Append(' ');
            }
            return text.ToString(0, bytes);
        }

        // About 1 KB a row in every shape
        private static IColumn[] PayloadColumns(string shape, Random random, int rows)
        {
            int count = shape == "nested" ? 5 : 8;
            var columns = new IColumn[count];
            for (int c = 0; c < count; c++)
            {
                columns[c] = Column.Create(GlobalMemoryManager.Instance);
            }
            for (int i = 0; i < rows; i++)
            {
                if (shape == "nested")
                {
                    columns[0].Add(new ListValue(new StringValue(Text(random, 110)), new StringValue(Text(random, 110)), new StringValue(Text(random, 110))));
                    columns[1].Add(new MapValue(new KeyValuePair<IDataValue, IDataValue>(new StringValue(Text(random, 20)), new StringValue(Text(random, 200)))));
                    columns[2].Add(new StructValue(Fields, new Int64Value(random.Next()), new StringValue(Text(random, 220))));
                    columns[3].Add(new StringValue(Text(random, 200)));
                    columns[4].Add(new Int64Value(random.Next()));
                    continue;
                }
                for (int c = 0; c < 4; c++)
                {
                    bool mixed = shape == "mixed";
                    if (shape == "skewed" && c == 0 && i % 10 == 0)
                    {
                        // One row in ten is far larger than the others
                        columns[c].Add(new StringValue(Text(random, 12_000)));
                    }
                    else if (shape == "varied")
                    {
                        // Rows from a quarter to seven quarters of a kilobyte
                        columns[c].Add(new StringValue(Text(random, 60 + random.Next(380))));
                    }
                    else if (mixed && c == 1 && random.Next(4) == 0)
                    {
                        columns[c].Add(NullValue.Instance);
                    }
                    else if (mixed && c == 2 && random.Next(3) == 0)
                    {
                        columns[c].Add(new Int64Value(random.Next()));
                    }
                    else
                    {
                        columns[c].Add(new StringValue(Text(random, 235)));
                    }
                }
                columns[4].Add(new Int64Value(random.Next()));
                columns[5].Add(new Int64Value(random.Next(1000)));
                columns[6].Add(new DoubleValue(random.NextDouble()));
                columns[7].Add(new DecimalValue(random.Next(1_000_000) / 100m));
            }
            return columns;
        }

        private static async Task<int> RunHeap(StateManagerSync<object> manager, IStateManagerClient client, RowHeapOptions options, Data data, int[] picks, bool permute, object settings)
        {
            using var heap = await RowHeap.CreateAsync(client, "heap", data.PayloadColumnCount, options, GlobalMemoryManager.Instance);
            var references = new ulong[data.Rows];
            var cpu = Process.GetCurrentProcess().TotalProcessorTime;
            int gen0 = GC.CollectionCount(0);
            var watch = Stopwatch.StartNew();
            for (int b = 0; b < data.Payload.Length; b++)
            {
                using var column = Column.Create(GlobalMemoryManager.Instance);
                await heap.AppendAsync(data.Payload[b], 0, data.Batch, column);
                for (int i = 0; i < data.Batch; i++)
                {
                    references[b * data.Batch + i] = (ulong)column.GetValueAt(i, default).AsLong;
                }
            }
            double store = watch.Elapsed.TotalSeconds;
            await heap.CheckpointAsync();
            await manager.CheckpointAsync();
            double checkpoint = watch.Elapsed.TotalSeconds - store;

            var probes = ReferenceBatches(references, picks, data.Batch);
            var columns = new int[data.PayloadColumnCount];
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = i;
            }
            using var reader = heap.CreateReader();
            var rowOrder = new int[data.Batch];
            var positions = new int[data.Batch];
            var weights = new int[data.Batch];
            var orderedWeights = new int[data.Batch];
            var copyWatch = Stopwatch.StartNew();
            long copied = 0;
            for (int b = 0; b < probes.Length; b++)
            {
                var output = new Column[columns.Length];
                for (int i = 0; i < output.Length; i++)
                {
                    output[i] = Column.Create(GlobalMemoryManager.Instance);
                }
                int count = probes[b].Count;
                await reader.GatherAsync(probes[b], count, columns, output, rowOrder);
                if (permute)
                {
                    copied += ReorderNarrowSide(data.Keyed[b % data.Keyed.Length].Columns[0], probes[b], rowOrder, positions, weights, orderedWeights, count);
                }
                copied += output[0].Count;
                for (int i = 0; i < output.Length; i++)
                {
                    output[i].Dispose();
                }
            }
            double copy = copyWatch.Elapsed.TotalSeconds;
            double cpuSeconds = (Process.GetCurrentProcess().TotalProcessorTime - cpu).TotalSeconds;
            Report(settings, data, picks.Length, store, checkpoint, copy, cpuSeconds, copied, GC.CollectionCount(0) - gen0, heap.PageCount, heap.Client.QueueFullWaits);
            return copied == picks.Length ? 0 : 1;
        }

        // What a fetch does to its narrow side: two columns and the weights follow the row order
        private static int ReorderNarrowSide(IColumn keys, IColumn references, int[] rowOrder, int[] positions, int[] weights, int[] orderedWeights, int count)
        {
            using var key = Column.Create(GlobalMemoryManager.Instance);
            using var reference = Column.Create(GlobalMemoryManager.Instance);
            ReadOnlySpan<int> order = rowOrder.AsSpan(0, count);
            ReadOnlySpan<int> at = positions.AsSpan(0, count);
            key.InsertFrom(keys, in order, in at, ColumnWithOffset.NullValueIndex);
            reference.InsertFrom(references, in order, in at, ColumnWithOffset.NullValueIndex);
            for (int i = 0; i < count; i++)
            {
                orderedWeights[i] = weights[rowOrder[i]];
            }
            return key.Count + reference.Count - 2 * count;
        }

        private static Column[] ReferenceBatches(ulong[] references, int[] picks, int batch)
        {
            var probes = new Column[(picks.Length + batch - 1) / batch];
            for (int b = 0; b < probes.Length; b++)
            {
                var column = Column.Create(GlobalMemoryManager.Instance);
                int end = Math.Min(picks.Length, (b + 1) * batch);
                for (int i = b * batch; i < end; i++)
                {
                    column.Add(new RowRefValue(references[picks[i]]));
                }
                probes[b] = column;
            }
            return probes;
        }

        private static async Task<int> RunMultiJoin(StateManagerSync<object> manager, IStateManagerClient client, Data data, int[] picks, object settings)
        {
            int columnCount = data.PayloadColumnCount + 1;
            var writer = await MultiJoinInputState.CreateWriter(client, "rows", new List<int>() { 0 }, columnCount, GlobalMemoryManager.Instance);
            var changes = new sbyte[data.Batch];
            var cpu = Process.GetCurrentProcess().TotalProcessorTime;
            int gen0 = GC.CollectionCount(0);
            var watch = Stopwatch.StartNew();
            for (int b = 0; b < data.Keyed.Length; b++)
            {
                // The same steps as MultiJoinInputState.Insert takes for its primary tree
                var rows = data.Keyed[b];
                Array.Clear(changes, 0, data.Batch);
                writer.Prepare(rows, data.Batch);
                for (int i = 0; i < data.Batch; i++)
                {
                    writer.Values[i] = new MultiJoinWeight(1);
                }
                await writer.Inserter.ApplyBatch(
                    writer.Refs,
                    writer.Values,
                    data.Batch,
                    writer.Sorted,
                    writer.Tags,
                    new MultiJoinPrimaryMutator(writer.ColumnCount, changes),
                    rows.GetByteSize() + data.Batch * sizeof(int));
                writer.Release(data.Batch);
            }
            double store = watch.Elapsed.TotalSeconds;
            await writer.Tree.Commit();
            await manager.CheckpointAsync();
            double checkpoint = watch.Elapsed.TotalSeconds - store;

            var probes = new Column[(picks.Length + data.Batch - 1) / data.Batch];
            for (int b = 0; b < probes.Length; b++)
            {
                var column = Column.Create(GlobalMemoryManager.Instance);
                int end = Math.Min(picks.Length, (b + 1) * data.Batch);
                for (int i = b * data.Batch; i < end; i++)
                {
                    column.Add(new Int64Value(data.Keys[picks[i]]));
                }
                probes[b] = column;
            }
            var copies = new int[data.PayloadColumnCount];
            for (int i = 0; i < copies.Length; i++)
            {
                copies[i] = i + 1;
            }
            var lookup = new MultiJoinTreeLookup(writer.Tree, new List<int>() { 0 });
            var frame = new MultiJoinFrame();
            var copyWatch = Stopwatch.StartNew();
            long copied = 0;
            for (int b = 0; b < probes.Length; b++)
            {
                lookup.KeyColumns[0] = probes[b];
                await lookup.Start(probes[b].Count);
                while (await lookup.NextPart(frame, copies, data.Batch, GlobalMemoryManager.Instance))
                {
                    copied += frame.Count;
                    for (int i = 0; i < frame.Columns.Length; i++)
                    {
                        frame.Columns[i]?.Dispose();
                    }
                    Array.Clear(frame.Columns);
                }
            }
            double copy = copyWatch.Elapsed.TotalSeconds;
            double cpuSeconds = (Process.GetCurrentProcess().TotalProcessorTime - cpu).TotalSeconds;
            Report(settings, data, picks.Length, store, checkpoint, copy, cpuSeconds, copied, GC.CollectionCount(0) - gen0, 0, 0);
            return copied == picks.Length ? 0 : 1;
        }

        private static void Report(object settings, Data data, int fetched, double store, double checkpoint, double copy, double cpu, long copied, int gen0, long pages, long queueFullWaits)
        {
            Console.WriteLine(JsonSerializer.Serialize(new
            {
                Settings = settings,
                data.Rows,
                Fetched = fetched,
                Copied = copied,
                BytesPerRow = data.PayloadBytes / data.Rows,
                StoreSeconds = store,
                CheckpointSeconds = checkpoint,
                CopySeconds = copy,
                TotalSeconds = store + checkpoint + copy,
                CpuSeconds = cpu,
                StoreNanosPerRow = store * 1e9 / data.Rows,
                CheckpointNanosPerRow = checkpoint * 1e9 / data.Rows,
                CopyNanosPerRow = copy * 1e9 / fetched,
                Gen0 = gen0,
                Pages = pages,
                QueueFullWaits = queueFullWaits
            }));
        }

        private static long Footprint(RowHeap heap)
        {
            return heap.StoredBytes + heap.DirectoryBytes + heap.PageTableBytes + heap.ReleasedSlotBytes + heap.MergeQueueBytes;
        }

        private static async Task Checkpoint(StateManagerSync<object> manager, RowHeap heap, RowStore? store = null)
        {
            if (store != null)
            {
                // Barrier and commit together, no reader is in between here
                await store.BeginCheckpointAsync();
                await store.CheckpointAsync();
            }
            else
            {
                await heap.CheckpointAsync();
            }
            await manager.CheckpointAsync();
        }

        // Slot map bytes of the pages that hold live rows
        private static async Task<long> SlotMapBytes(RowHeap heap, ulong[] live)
        {
            var pages = new HashSet<long>();
            for (int i = 0; i < live.Length; i++)
            {
                if (live[i] != 0)
                {
                    pages.Add(RowHeap.PageOf(heap.Directory.Get(RowReference.Sequence(live[i]))));
                }
            }
            using var window = heap.Client.CreateWindow();
            long bytes = 0;
            foreach (long page in pages)
            {
                var loaded = await heap.Client.GetAsync(heap.Client.FirstPageId + page, window);
                bytes += loaded.Map?.ByteSize ?? 0;
                window.Release(loaded);
            }
            return bytes;
        }

        // Steady churn, or one row kept of every page or chunk
        private static async Task<int> RunChurn(StateManagerSync<object> manager, IStateManagerClient client, RowHeapOptions options, string shape, int rows, int batch, int rounds, int change, string survivors, bool merge, bool drain, bool keyed, int seed, object settings)
        {
            var random = new Random(seed);
            using var pool = new EventBatchData(PayloadColumns(shape, random, rows));
            // A keyed store releases its rows at the checkpoint
            using var store = keyed ? await RowStore.CreateAsync(client, "heap", pool.Columns.Count, new RowStoreOptions() { Mode = RowStoreMode.Keyed, Heap = options }, GlobalMemoryManager.Instance) : null;
            using var ownHeap = keyed ? null : await RowHeap.CreateAsync(client, "heap", pool.Columns.Count, options, GlobalMemoryManager.Instance);
            var heap = store != null ? store.Heap : ownHeap!;
            var live = new ulong[rows];
            double longestCompact = 0;
            int compactCalls = 0;
            int lowestBudget = int.MaxValue;
            // A host calls again while more is left, or waits
            async Task CompactAll()
            {
                bool more = true;
                while (more)
                {
                    var call = Stopwatch.StartNew();
                    more = await heap.CompactAsync() && drain;
                    longestCompact = Math.Max(longestCompact, call.Elapsed.TotalSeconds);
                    compactCalls++;
                }
            }
            for (int start = 0; start < rows; start += batch)
            {
                using var column = Column.Create(GlobalMemoryManager.Instance);
                await heap.AppendAsync(pool, start, batch, column);
                for (int i = 0; i < batch; i++)
                {
                    live[start + i] = (ulong)column.GetValueAt(i, default).AsLong;
                }
                if (merge)
                {
                    // As a host does after every batch
                    await CompactAll();
                }
                if ((start / batch) % 50 == 49)
                {
                    // A stream takes checkpoints while it loads, the budget follows
                    await Checkpoint(manager, heap, store);
                }
            }
            for (int i = 0; i < 3; i++)
            {
                await Checkpoint(manager, heap, store);
                if (merge)
                {
                    await CompactAll();
                }
            }
            await Checkpoint(manager, heap, store);
            longestCompact = 0;
            long loadedBytes = heap.LiveBytes;
            long rowsMergedAtLoad = heap.RowsMerged;
            long loadedPages = heap.PageCount;
            long liveRows = rows;
            long highest = Footprint(heap);
            long highestBefore = highest;
            long changed = 0;
            long changedPerRound = 0;
            double changeSeconds = 0;
            double compactSeconds = 0;
            double checkpointSeconds = 0;
            if (survivors != "none")
            {
                // The first row of every page stays, or of every 4,096th page
                long step = survivors == "chunk" ? RowHeapDirectory.ChunkSize : 1;
                var watch = Stopwatch.StartNew();
                for (int i = 0; i < rows; i++)
                {
                    if (RowReference.Slot(live[i]) == 0 && RowReference.Sequence(live[i]) % step == 0)
                    {
                        continue;
                    }
                    if (store != null)
                    {
                        await store.RetireAsync(live[i]);
                    }
                    else
                    {
                        heap.Release(live[i]);
                    }
                    live[i] = 0;
                    changed++;
                }
                changeSeconds = watch.Elapsed.TotalSeconds;
                await CompactAll();
                compactSeconds = watch.Elapsed.TotalSeconds - changeSeconds;
                highestBefore = Footprint(heap);
                // A burst is merged at the rate the checkpoints allow
                for (int i = 0; i < 4; i++)
                {
                    await Checkpoint(manager, heap, store);
                    await CompactAll();
                }
                await Checkpoint(manager, heap, store);
                await Checkpoint(manager, heap, store);
                checkpointSeconds = watch.Elapsed.TotalSeconds - changeSeconds - compactSeconds;
                liveRows = rows - changed;
                highest = Footprint(heap);
            }
            else
            {
                changedPerRound = (long)rows * change / 100;
                var order = new int[rows];
                for (int i = 0; i < rows; i++)
                {
                    order[i] = i;
                }
                int poolAt = 0;
                for (int round = 0; round < rounds; round++)
                {
                    for (int i = 0; i < changedPerRound; i++)
                    {
                        int other = i + random.Next(rows - i);
                        (order[i], order[other]) = (order[other], order[i]);
                    }
                    var watch = Stopwatch.StartNew();
                    for (int start = 0; start < changedPerRound; start += batch)
                    {
                        int count = (int)Math.Min(batch, changedPerRound - start);
                        if (poolAt + count > rows)
                        {
                            poolAt = 0;
                        }
                        using var column = Column.Create(GlobalMemoryManager.Instance);
                        await heap.AppendAsync(pool, poolAt, count, column);
                        for (int i = 0; i < count; i++)
                        {
                            int row = order[start + i];
                            if (store != null)
                            {
                                await store.RetireAsync(live[row]);
                            }
                            else
                            {
                                heap.Release(live[row]);
                            }
                            live[row] = (ulong)column.GetValueAt(i, default).AsLong;
                        }
                        poolAt += count;
                        if (merge)
                        {
                            var compact = Stopwatch.StartNew();
                            await CompactAll();
                            compactSeconds += compact.Elapsed.TotalSeconds;
                        }
                    }
                    changeSeconds += watch.Elapsed.TotalSeconds;
                    lowestBudget = Math.Min(lowestBudget, round < 2 ? int.MaxValue : heap.CompactBudget);
                    // Storage peaks right before a checkpoint deletes pages
                    highestBefore = Math.Max(highestBefore, Footprint(heap));
                    var checkpoint = Stopwatch.StartNew();
                    await Checkpoint(manager, heap, store);
                    checkpointSeconds += checkpoint.Elapsed.TotalSeconds;
                    changed += changedPerRound;
                    highest = Math.Max(highest, Footprint(heap));
                }
            }
            long liveBytes = loadedBytes * liveRows / rows;
            long churnBytes = loadedBytes * changedPerRound / rows;
            long slotMapBytes = await SlotMapBytes(heap, live);
            Console.WriteLine(JsonSerializer.Serialize(new
            {
                Settings = settings,
                Rows = rows,
                LiveRows = liveRows,
                LiveBytes = liveBytes,
                ChurnBytesPerCheckpoint = churnBytes,
                HighestBytes = highest,
                HighestOverLive = highest / (double)liveBytes,
                HighestBeforeCheckpointOverLive = highestBefore / (double)liveBytes,
                Bound = (liveBytes * 1.5 + churnBytes) / liveBytes,
                heap.StoredBytes,
                HeapLiveBytes = heap.LiveBytes,
                heap.DirectoryBytes,
                heap.PageTableBytes,
                heap.ReleasedSlotBytes,
                heap.MergeQueueBytes,
                heap.PagesMeasured,
                heap.PagesRead,
                LowestBudgetLeft = lowestBudget == int.MaxValue ? -1 : lowestBudget,
                LongestCompactMillis = longestCompact * 1000,
                CompactCalls = compactCalls,
                SlotMapBytes = slotMapBytes,
                LoadedPages = loadedPages,
                Pages = heap.PageCount,
                Sequences = heap.SequenceCount,
                heap.RowsReleased,
                heap.RowsMerged,
                RowsMergedAtLoad = rowsMergedAtLoad,
                MergedPerReleased = (heap.RowsMerged - rowsMergedAtLoad) / (double)Math.Max(1, heap.RowsReleased),
                heap.PagesMerged,
                heap.PagesDeleted,
                ChangeNanosPerRow = changeSeconds * 1e9 / Math.Max(1, changed),
                CompactNanosPerRow = compactSeconds * 1e9 / Math.Max(1, changed),
                CheckpointSeconds = checkpointSeconds
            }));
            return heap.RowCount == liveRows ? 0 : 1;
        }

        // Times the gather alone, several rounds over the same heap
        private static async Task<int> RunGather(StateManagerSync<object> manager, IStateManagerClient client, RowHeapOptions options, Data data, int[] picks, int rounds, object settings)
        {
            using var heap = await RowHeap.CreateAsync(client, "heap", data.PayloadColumnCount, options, GlobalMemoryManager.Instance);
            var references = new ulong[data.Rows];
            for (int b = 0; b < data.Payload.Length; b++)
            {
                using var column = Column.Create(GlobalMemoryManager.Instance);
                await heap.AppendAsync(data.Payload[b], 0, data.Batch, column);
                for (int i = 0; i < data.Batch; i++)
                {
                    references[b * data.Batch + i] = (ulong)column.GetValueAt(i, default).AsLong;
                }
            }
            await heap.CheckpointAsync();
            await manager.CheckpointAsync();
            var probes = ReferenceBatches(references, picks, data.Batch);
            var columns = new int[data.PayloadColumnCount];
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = i;
            }
            using var reader = heap.CreateReader();
            var rowOrder = new int[data.Batch];
            var times = new List<double>();
            long copied = 0;
            for (int round = 0; round <= rounds; round++)
            {
                var watch = Stopwatch.StartNew();
                for (int b = 0; b < probes.Length; b++)
                {
                    var output = new Column[columns.Length];
                    for (int i = 0; i < output.Length; i++)
                    {
                        output[i] = Column.Create(GlobalMemoryManager.Instance);
                    }
                    await reader.GatherAsync(probes[b], probes[b].Count, columns, output, rowOrder);
                    copied += output[0].Count;
                    for (int i = 0; i < output.Length; i++)
                    {
                        output[i].Dispose();
                    }
                }
                // The first round warms up and is not kept
                if (round > 0)
                {
                    times.Add(watch.Elapsed.TotalSeconds * 1e9 / picks.Length);
                }
            }
            times.Sort();
            Console.WriteLine(JsonSerializer.Serialize(new
            {
                Settings = settings,
                data.Rows,
                Fetched = picks.Length,
                BytesPerRow = data.PayloadBytes / data.Rows,
                BestNanosPerRow = times[0],
                MedianNanosPerRow = times[times.Count / 2],
                Pages = heap.PageCount,
                ColdLoads = reader.ColdLoads
            }));
            return copied == (long)picks.Length * (rounds + 1) ? 0 : 1;
        }
    }
}
