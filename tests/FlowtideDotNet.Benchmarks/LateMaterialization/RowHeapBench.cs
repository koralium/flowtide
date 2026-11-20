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
    /// Options: --mode gate|gather --arm heap|multijoin --rows --batch --order append|local|shuffled
    /// --shape wide|mixed|nested --repeat --cache --compression zstd|none --rounds --seed --page-bytes --queue
    /// --keys shuffled|sorted --permute 0|1
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
            var options = new RowHeapOptions() { PageTargetBytes = pageBytes, WriteQueueCapacity = queue };

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
                    if (mixed && c == 1 && random.Next(4) == 0)
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
