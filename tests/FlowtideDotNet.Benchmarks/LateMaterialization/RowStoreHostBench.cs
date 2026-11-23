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
using FlowtideDotNet.Core.ColumnStore.Comparers;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.LateMaterialization;
using FlowtideDotNet.Core.Operators.Normalization;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Text.Json;

namespace FlowtideDotNet.Benchmarks.LateMaterialization
{
    /// <summary>
    /// A keyed source's state: whole rows against a hosted store.
    /// Run with --row-host --arm tree|host|temp-rows|temp-keys.
    /// Options: --rows --batch --rounds --change --wide --cache --compression --seed --page-bytes
    /// </summary>
    internal static class RowStoreHostBench
    {
        private const int KeptColumns = 2;
        private const int WideColumns = 4;
        private const int Columns = 1 + KeptColumns + WideColumns;

        public static async Task<int> Run(string[] args)
        {
            string arm = ReservoirReaderBench.Option(args, "arm", "host");
            int rows = int.Parse(ReservoirReaderBench.Option(args, "rows", "100000"), CultureInfo.InvariantCulture);
            int batch = int.Parse(ReservoirReaderBench.Option(args, "batch", "1000"), CultureInfo.InvariantCulture);
            int rounds = int.Parse(ReservoirReaderBench.Option(args, "rounds", "10"), CultureInfo.InvariantCulture);
            int change = int.Parse(ReservoirReaderBench.Option(args, "change", "10"), CultureInfo.InvariantCulture);
            int wide = int.Parse(ReservoirReaderBench.Option(args, "wide", "400"), CultureInfo.InvariantCulture);
            int cache = int.Parse(ReservoirReaderBench.Option(args, "cache", "1000000"), CultureInfo.InvariantCulture);
            string compression = ReservoirReaderBench.Option(args, "compression", "zstd");
            int seed = int.Parse(ReservoirReaderBench.Option(args, "seed", "1"), CultureInfo.InvariantCulture);
            int pageBytes = int.Parse(ReservoirReaderBench.Option(args, "page-bytes", "32768"), CultureInfo.InvariantCulture);
            // packed: batches of changed rows only; scattered: every row, some changed
            bool scattered = ReservoirReaderBench.Option(args, "churn", "packed") == "scattered";
            string directory = Path.Join(Path.GetTempPath(), $"row-host-{Guid.NewGuid():N}");
            rows -= rows % batch;

            var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
            var manager = new StateManagerSync<object>(
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
                new Meter("rowHostBench"),
                "rowHostBench",
                GlobalMemoryManager.Instance);
            await manager.InitializeAsync();
            var client = manager.GetOrCreateClient("bench");

            // The key of each row in load order, and its version
            var keys = new long[rows];
            for (int i = 0; i < rows; i++)
            {
                keys[i] = i;
            }
            new Random(seed).Shuffle(keys);
            var versions = new int[rows];

            if (arm.StartsWith("temp"))
            {
                return await RunTemp(client, arm == "temp-keys", keys, versions, batch, wide, rounds);
            }

            IState state = arm == "tree"
                ? await TreeState.CreateAsync(client)
                : await HostState.CreateAsync(client, pageBytes);

            long sent = 0;
            double load = await Pass(state, manager, keys, versions, batch, wide, null, x => sent += x);
            double loadCheckpoint = await Checkpoint(state, manager);
            string loadSteps = state.Steps();
            long loadSent = sent;
            sent = 0;
            double same = await Pass(state, manager, keys, versions, batch, wide, null, x => sent += x);
            double sameCheckpoint = await Checkpoint(state, manager);
            string sameSteps = state.Steps();
            long sameSent = sent;

            sent = 0;
            double churn = 0;
            double churnCheckpoint = 0;
            long changedRows = 0;
            var random = new Random(seed + 1);
            int perRound = Math.Max(batch, rows * change / 100 / batch * batch);
            var picked = new long[perRound];
            for (int round = 0; round < rounds; round++)
            {
                for (int i = 0; i < perRound; i++)
                {
                    int row = random.Next(rows);
                    picked[i] = row;
                }
                Array.Sort(picked);
                int distinct = 0;
                for (int i = 0; i < perRound; i++)
                {
                    if (i == 0 || picked[i] != picked[i - 1])
                    {
                        versions[picked[i]]++;
                        picked[distinct++] = picked[i];
                    }
                }
                long[]? subset = null;
                if (!scattered)
                {
                    distinct -= distinct % batch;
                    subset = new long[distinct];
                    Array.Copy(picked, subset, distinct);
                    new Random(seed + round).Shuffle(subset);
                }
                churn += await Pass(state, manager, keys, versions, batch, wide, subset, x => sent += x);
                churnCheckpoint += await Checkpoint(state, manager);
                changedRows += distinct;
            }
            long allocated = GC.GetTotalAllocatedBytes();
            Console.WriteLine(JsonSerializer.Serialize(new
            {
                Arm = arm,
                Rows = rows,
                Batch = batch,
                Wide = wide,
                Change = change,
                Churn = scattered ? "scattered" : "packed",
                Rounds = rounds,
                LoadMicrosPerRow = Math.Round(load * 1e6 / rows, 3),
                LoadCheckpointSeconds = Math.Round(loadCheckpoint, 3),
                SameMicrosPerRow = Math.Round(same * 1e6 / rows, 3),
                SameCheckpointSeconds = Math.Round(sameCheckpoint, 3),
                ChurnMicrosPerChangedRow = changedRows == 0 ? 0 : Math.Round(churn * 1e6 / changedRows, 3),
                ChurnCheckpointMicrosPerChangedRow = changedRows == 0 ? 0 : Math.Round(churnCheckpoint * 1e6 / changedRows, 3),
                LoadSent = loadSent,
                SameSent = sameSent,
                ChurnSent = sent,
                ChangedRows = changedRows,
                LoadStepsMs = loadSteps,
                SameStepsMs = sameSteps,
                ChurnStepsMs = state.Steps(),
                StoredPages = manager.PageCount,
                StoredBytes = state.StoredBytes,
                AllocatedMegabytes = allocated / (1024 * 1024)
            }));
            state.Dispose();
            manager.Dispose();
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, true);
            }
            return (loadSent == rows && sameSent == 0 && sent == 2 * changedRows) ? 0 : 1;
        }

        // A reload's temp tree: fill, walk the keys, clear
        private static async Task<int> RunTemp(IStateManagerClient client, bool keyOnly, long[] keys, int[] versions, int batch, int wide, int rounds)
        {
            var keyColumns = new List<int>() { 0 };
            var otherColumns = keyOnly ? new List<int>() : Enumerable.Range(1, Columns - 1).ToList();
            var tree = await client.GetOrCreateTree("full_load_temp",
                new BPlusTreeOptions<ColumnRowReference, ColumnRowReference, NormalizeKeyStorage, NormalizeValueStorage>()
                {
                    Comparer = new NormalizeTreeComparer(keyColumns),
                    KeySerializer = new NormalizeKeyStorageSerializer(keyColumns, GlobalMemoryManager.Instance),
                    ValueSerializer = new NormalizeValueSerializer(otherColumns, GlobalMemoryManager.Instance),
                    MemoryAllocator = GlobalMemoryManager.Instance,
                    UseByteBasedPageSizes = true
                });
            long ticks = 0;
            long walked = 0;
            for (int round = 0; round < rounds; round++)
            {
                for (int start = 0; start < keys.Length; start += batch)
                {
                    var (data, weights) = Batch(keys, versions, null, start, batch, wide);
                    long mark = Stopwatch.GetTimestamp();
                    for (int i = 0; i < batch; i++)
                    {
                        var row = new ColumnRowReference() { referenceBatch = data, RowIndex = i };
                        await tree.Upsert(row, row);
                    }
                    ticks += Stopwatch.GetTimestamp() - mark;
                    weights.Dispose();
                    data.Dispose();
                }
                long walk = Stopwatch.GetTimestamp();
                using (var iterator = tree.CreateIterator())
                {
                    await iterator.SeekFirst();
                    await foreach (var page in iterator)
                    {
                        foreach (var kv in page)
                        {
                            walked += kv.Key.RowIndex >= 0 ? 1 : 0;
                        }
                    }
                }
                await tree.Clear();
                ticks += Stopwatch.GetTimestamp() - walk;
            }
            Console.WriteLine(JsonSerializer.Serialize(new
            {
                Arm = keyOnly ? "temp-keys" : "temp-rows",
                Rows = keys.Length,
                Wide = wide,
                Rounds = rounds,
                MicrosPerRow = Math.Round((double)ticks / Stopwatch.Frequency * 1e6 / walked, 3),
                AllocatedMegabytes = GC.GetTotalAllocatedBytes() / (1024 * 1024)
            }));
            return walked == (long)keys.Length * rounds ? 0 : 1;
        }

        private static string Text(long key, int version, int column, int bytes)
        {
            var random = new Random(HashCode.Combine(key, version, column));
            return string.Create(bytes, random, (span, r) =>
            {
                for (int i = 0; i < span.Length; i++)
                {
                    span[i] = (char)('a' + r.Next(26));
                }
            });
        }

        private static (EventBatchData batch, PrimitiveList<int> weights) Batch(long[] keys, int[] versions, long[]? subset, int start, int count, int wide)
        {
            var allocator = GlobalMemoryManager.Instance;
            var columns = new IColumn[Columns];
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = Column.Create(allocator);
            }
            var weights = new PrimitiveList<int>(allocator);
            for (int i = 0; i < count; i++)
            {
                long row = subset == null ? start + i : subset[start + i];
                long key = keys[row];
                int version = versions[row];
                columns[0].Add(new Int64Value(key));
                columns[1].Add(new Int64Value(key % 1000));
                columns[2].Add(new Int64Value(key * 7));
                for (int c = 0; c < WideColumns; c++)
                {
                    columns[1 + KeptColumns + c].Add(new StringValue(Text(key, version, c, wide / WideColumns)));
                }
                weights.Add(1);
            }
            return (new EventBatchData(columns), weights);
        }

        // Applies every row, the time is the state's alone
        private static async Task<double> Pass(IState state, StateManagerSync<object> manager, long[] keys, int[] versions, int batch, int wide, long[]? subset, Action<long> sent)
        {
            int total = subset == null ? keys.Length : subset.Length;
            long ticks = 0;
            for (int start = 0; start < total; start += batch)
            {
                var (data, weights) = Batch(keys, versions, subset, start, batch, wide);
                long mark = Stopwatch.GetTimestamp();
                sent(await state.ApplyAsync(data, weights));
                ticks += Stopwatch.GetTimestamp() - mark;
                weights.Dispose();
                data.Dispose();
            }
            return (double)ticks / Stopwatch.Frequency;
        }

        private static async Task<double> Checkpoint(IState state, StateManagerSync<object> manager)
        {
            long mark = Stopwatch.GetTimestamp();
            await state.BeginCheckpointAsync();
            await state.CheckpointAsync();
            await manager.CheckpointAsync();
            return (double)(Stopwatch.GetTimestamp() - mark) / Stopwatch.Frequency;
        }

        private interface IState : IDisposable
        {
            long StoredBytes { get; }

            // Milliseconds per step since the last call
            string Steps();

            ValueTask<long> ApplyAsync(EventBatchData batch, PrimitiveList<int> weights);

            Task BeginCheckpointAsync();

            Task CheckpointAsync();
        }

        // What the read base and the normalization do today
        private sealed class TreeState : IState
        {
            private readonly IBPlusTree<ColumnRowReference, ColumnRowReference, NormalizeKeyStorage, NormalizeValueStorage> _tree;
            private readonly List<int> _otherColumns;

            private TreeState(IBPlusTree<ColumnRowReference, ColumnRowReference, NormalizeKeyStorage, NormalizeValueStorage> tree, List<int> otherColumns)
            {
                _tree = tree;
                _otherColumns = otherColumns;
            }

            public long StoredBytes => 0;

            public string Steps() => "";

            public static async Task<TreeState> CreateAsync(IStateManagerClient client)
            {
                var keyColumns = new List<int>() { 0 };
                var otherColumns = Enumerable.Range(1, Columns - 1).ToList();
                var tree = await client.GetOrCreateTree("persistent",
                    new BPlusTreeOptions<ColumnRowReference, ColumnRowReference, NormalizeKeyStorage, NormalizeValueStorage>()
                    {
                        Comparer = new NormalizeTreeComparer(keyColumns),
                        KeySerializer = new NormalizeKeyStorageSerializer(keyColumns, GlobalMemoryManager.Instance),
                        ValueSerializer = new NormalizeValueSerializer(otherColumns, GlobalMemoryManager.Instance),
                        MemoryAllocator = GlobalMemoryManager.Instance,
                        UseByteBasedPageSizes = true
                    });
                return new TreeState(tree, otherColumns);
            }

            public async ValueTask<long> ApplyAsync(EventBatchData batch, PrimitiveList<int> weights)
            {
                long sent = 0;
                // The deleted rows are copied out as the operators do
                var deleted = new Column[_otherColumns.Count];
                for (int i = 0; i < deleted.Length; i++)
                {
                    deleted[i] = Column.Create(GlobalMemoryManager.Instance);
                }
                for (int i = 0; i < weights.Count; i++)
                {
                    var row = new ColumnRowReference() { referenceBatch = batch, RowIndex = i };
                    await _tree.RMWNoResult(row, row, (input, current, exists) =>
                    {
                        if (exists)
                        {
                            for (int k = 0; k < _otherColumns.Count; k++)
                            {
                                int compare = DataValueComparer.Instance.Compare(
                                    input.referenceBatch.Columns[_otherColumns[k]].GetValueAt(input.RowIndex, default),
                                    current.referenceBatch.Columns[k].GetValueAt(current.RowIndex, default));
                                if (compare != 0)
                                {
                                    for (int c = 0; c < _otherColumns.Count; c++)
                                    {
                                        deleted[c].Add(current.referenceBatch.Columns[c].GetValueAt(current.RowIndex, default));
                                    }
                                    sent += 2;
                                    return (input, GenericWriteOperation.Upsert);
                                }
                            }
                            return (current, GenericWriteOperation.None);
                        }
                        sent++;
                        return (input, GenericWriteOperation.Upsert);
                    });
                }
                for (int i = 0; i < deleted.Length; i++)
                {
                    deleted[i].Dispose();
                }
                return sent;
            }

            public async Task BeginCheckpointAsync()
            {
                await _tree.Commit();
            }

            public Task CheckpointAsync()
            {
                return Task.CompletedTask;
            }

            public void Dispose()
            {
            }
        }

        private sealed class HostState : IState
        {
            private readonly RowStoreHost _host;

            private HostState(RowStoreHost host)
            {
                _host = host;
            }

            public long StoredBytes => _host.Store.Heap.StoredBytes;

            public string Steps()
            {
                string[] names = ["hash+columns", "classify", "verify", "append", "write", "compact", "hash+columns+keys", "sort"];
                var parts = new List<string>();
                for (int i = 0; i < names.Length; i++)
                {
                    parts.Add($"{names[i]} {_host.StepTicks[i] * 1000.0 / Stopwatch.Frequency:F0}");
                    _host.StepTicks[i] = 0;
                }
                parts.Add($"verified rows {_host.VerifiedRows}, pages visited {_host.VerifyReads}, cold loads {_host.ColdLoads}");
                return string.Join(", ", parts);
            }

            public static async Task<HostState> CreateAsync(IStateManagerClient client, int pageBytes)
            {
                var layout = new RowStoreHostLayout(
                    Columns,
                    new List<int>() { 0 },
                    Enumerable.Range(1, KeptColumns).ToList(),
                    Enumerable.Range(1 + KeptColumns, WideColumns).ToList());
                var options = new RowStoreOptions() { Mode = RowStoreMode.Keyed, Heap = new RowHeapOptions() { PageTargetBytes = pageBytes } };
                return new HostState(await RowStoreHost.CreateAsync(client, "persistent_keyed", "rows", layout, options, GlobalMemoryManager.Instance));
            }

            public async ValueTask<long> ApplyAsync(EventBatchData batch, PrimitiveList<int> weights)
            {
                using var result = await _host.ApplyAsync(batch, weights, null);
                return result.InsertRows.Count + result.DeleteRows.Count;
            }

            public Task BeginCheckpointAsync()
            {
                return _host.BeginCheckpointAsync();
            }

            public Task CheckpointAsync()
            {
                return _host.CheckpointAsync();
            }

            public void Dispose()
            {
                _host.Dispose();
            }
        }
    }
}
