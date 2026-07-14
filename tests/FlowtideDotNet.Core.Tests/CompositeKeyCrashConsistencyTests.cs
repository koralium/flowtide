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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Compute.Columnar.Functions.StatefulAggregations;
using FlowtideDotNet.Core.Compute.Columnar.Functions.StatefulAggregations.ListAgg;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics.Metrics;
using Xunit;

namespace FlowtideDotNet.Core.Tests
{
    /// <summary>
    /// Crash-consistency stress for the list_union_distinct_agg tree structure.
    /// A composite EventBatchData key with byte-based pages, driven through the real ListAgg
    /// serializer and comparer under maximal eviction while a crash reverts to the checkpoint.
    /// The recovered tree must exactly match the last checkpoint.
    /// </summary>
    public class CompositeKeyCrashConsistencyTests
    {
        [Fact]
        public async Task ListUnionTreeRecoversUnderConcurrentEviction()
        {
            var allocator = GlobalMemoryManager.Instance;
            var tempDir = $"./data/compositeKeyCrash/{Guid.NewGuid():N}";
            var stateManager = new StateManagerSync<object>(
                new StateManagerOptions()
                {
                    PersistentStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions()
                    {
                        FileProvider = new MemoryFileProvider()
                    }),
                    TemporaryStorageOptions = new FlowtideDotNet.Storage.FileCacheOptions()
                    {
                        DirectoryPath = $"{tempDir}/tmp"
                    },
                    CachePageCount = 0,
                    MinCachePageCount = 0,
                }, NullLoggerFactory.Instance, new Meter(Guid.NewGuid().ToString()), "storage", allocator);

            await stateManager.InitializeAsync();
            await stateManager.CacheTable.StopCleanupTask();

            var client = stateManager.GetOrCreateClient("node1");

            async Task<IBPlusTree<ListAggColumnRowReference, int, ListAggKeyStorageContainer, PrimitiveListValueContainer<int>>> GetTree()
            {
                return await client.GetOrCreateTree("listtree",
                    new BPlusTreeOptions<ListAggColumnRowReference, int, ListAggKeyStorageContainer, PrimitiveListValueContainer<int>>()
                    {
                        Comparer = new ListAggInsertComparer(1),
                        KeySerializer = new ListAggKeyStorageSerializer(1, allocator),
                        ValueSerializer = new PrimitiveListValueContainerSerializer<int>(allocator),
                        UseByteBasedPageSizes = true,
                        PageSizeBytes = 256,
                        MemoryAllocator = allocator
                    });
            }

            var tree = await GetTree();

            // Applies a weighted (group, item) update, mirroring DoListUnionDistinctAgg.
            async ValueTask Rmw(string group, string item, int weight)
            {
                var groupCol = Column.Create(allocator);
                groupCol.Add(new StringValue(group));
                var batch = new EventBatchData(new IColumn[] { groupCol });
                var key = new ListAggColumnRowReference() { batch = batch, index = 0, insertValue = new StringValue(item) };
                await tree.RMWNoResult(key, weight, (input, current, exists) =>
                {
                    if (exists)
                    {
                        current += input;
                        if (current == 0)
                        {
                            return (0, GenericWriteOperation.Delete);
                        }
                        return (current, GenericWriteOperation.Upsert);
                    }
                    return (input, GenericWriteOperation.Upsert);
                });
                batch.Dispose();
            }

            async Task<Dictionary<(string, string), int>> ReadAll()
            {
                var result = new Dictionary<(string, string), int>();
                var it = tree.CreateIterator();
                await it.SeekFirst();
                await foreach (var page in it)
                {
                    foreach (var kv in page)
                    {
                        var group = kv.Key.batch.Columns[0].GetValueAt(kv.Key.index, default).AsString.ToString();
                        var item = kv.Key.batch.Columns[1].GetValueAt(kv.Key.index, default).AsString.ToString();
                        result[(group, item)] = kv.Value;
                    }
                }
                return result;
            }

            // Reads a single group's items via Seek + range, exactly like ListAggUnionDistinctGetValue.
            async Task<List<string>> GetGroupItems(string group)
            {
                var groupCol = Column.Create(allocator);
                groupCol.Add(new StringValue(group));
                var batch = new EventBatchData(new IColumn[] { groupCol });
                var rowRef = new ListAggColumnRowReference() { batch = batch, index = 0 };
                var searchComparer = new ListAggSearchComparer(1);
                var items = new List<string>();
                var iterator = tree.CreateIterator();
                await iterator.Seek(rowRef, searchComparer);
                if (!searchComparer.noMatch)
                {
                    bool firstPage = true;
                    await foreach (var page in iterator)
                    {
                        if (!firstPage)
                        {
                            searchComparer.FindIndex(rowRef, page.Keys!);
                            if (searchComparer.noMatch)
                            {
                                break;
                            }
                        }
                        firstPage = false;
                        for (int i = searchComparer.start; i <= searchComparer.end; i++)
                        {
                            var itemVal = page.Keys!._data.Columns[1].GetValueAt(i, default);
                            items.Add(itemVal.AsString.ToString());
                        }
                    }
                }
                batch.Dispose();
                items.Sort();
                return items;
            }

            // Verifies each group by the same Seek-based read path the aggregate uses.
            async Task VerifyBySeek(Dictionary<(string, string), int> model, int crashIndex)
            {
                var expectedByGroup = model.Keys.GroupBy(k => k.Item1)
                    .ToDictionary(g => g.Key, g => g.Select(k => k.Item2).OrderBy(x => x).ToList());
                foreach (var g in expectedByGroup)
                {
                    var actual = await GetGroupItems(g.Key);
                    if (!g.Value.SequenceEqual(actual))
                    {
                        throw new Exception($"Seek mismatch for group {g.Key} after crash {crashIndex}: expected=[{string.Join(",", g.Value)}] actual=[{string.Join(",", actual)}]");
                    }
                }
            }

            var crashGate = new SemaphoreSlim(1);
            var stop = new CancellationTokenSource();
            var evictor = Task.Run(async () =>
            {
                while (!stop.IsCancellationRequested)
                {
                    await crashGate.WaitAsync();
                    try
                    {
                        await stateManager.CacheTable.ForceCleanup();
                    }
                    catch { }
                    finally
                    {
                        crashGate.Release();
                    }
                    await Task.Delay(1);
                }
            });

            var rnd = new Random(7);
            var live = new Dictionary<(string, string), int>();
            var committed = new Dictionary<(string, string), int>();
            // The events applied since the last checkpoint. On crash the tree reverts to the
            // checkpoint and the stream replays exactly these, the harness does the same.
            var sinceCheckpoint = new List<(string group, string item, int weight)>();
            string Group(int g) => "co_" + g.ToString("D4");
            string Item(int p) => "p" + p.ToString("D2");

            var sw = System.Diagnostics.Stopwatch.StartNew();
            var deadline = TimeSpan.FromSeconds(20);
            int crashes = 0, checkpoints = 0;

            while (sw.Elapsed < deadline)
            {
                int ops = rnd.Next(20, 80);
                for (int o = 0; o < ops; o++)
                {
                    var group = Group(rnd.Next(60));
                    var item = Item(rnd.Next(15));
                    var kkey = (group, item);
                    int weight = (live.TryGetValue(kkey, out var w) && rnd.Next(2) == 0) ? -1 : 1;
                    await Rmw(group, item, weight);
                    sinceCheckpoint.Add((group, item, weight));
                    var nw = (live.TryGetValue(kkey, out var cur) ? cur : 0) + weight;
                    if (nw == 0) live.Remove(kkey); else live[kkey] = nw;
                }

                if (rnd.Next(3) != 0)
                {
                    await tree.Commit();
                    await stateManager.CheckpointAsync();
                    committed = new Dictionary<(string, string), int>(live);
                    sinceCheckpoint.Clear();
                    checkpoints++;
                }
                else
                {
                    await crashGate.WaitAsync();
                    try
                    {
                        await stateManager.InitializeAsync();
                        tree = await GetTree();
                        // Replay the post-checkpoint events, as the stream's source does.
                        foreach (var (g, it, wt) in sinceCheckpoint)
                        {
                            await Rmw(g, it, wt);
                        }
                        var actual = await ReadAll();
                        AssertEqual(live, actual, crashes);
                        await VerifyBySeek(live, crashes);
                        crashes++;
                    }
                    finally
                    {
                        crashGate.Release();
                    }
                }
            }

            stop.Cancel();
            await evictor;

            await stateManager.InitializeAsync();
            tree = await GetTree();
            foreach (var (g, it, wt) in sinceCheckpoint)
            {
                await Rmw(g, it, wt);
            }
            AssertEqual(live, await ReadAll(), crashes);

            Assert.True(crashes > 3, $"Only {crashes} crashes");
            Assert.True(checkpoints > 6, $"Only {checkpoints} checkpoints");

            stateManager.Dispose();
            try { Directory.Delete(tempDir, recursive: true); } catch { }
        }

        private static void AssertEqual(Dictionary<(string, string), int> expected, Dictionary<(string, string), int> actual, int crashIndex)
        {
            if (expected.Count != actual.Count)
            {
                var missing = expected.Keys.Where(k => !actual.ContainsKey(k)).Take(8).ToList();
                var extra = actual.Keys.Where(k => !expected.ContainsKey(k)).Take(8).ToList();
                throw new Exception($"Count mismatch after crash {crashIndex}: expected {expected.Count} actual {actual.Count}. missing=[{string.Join(";", missing)}] extra=[{string.Join(";", extra)}]");
            }
            foreach (var kv in expected)
            {
                if (!actual.TryGetValue(kv.Key, out var v))
                {
                    throw new Exception($"Missing key ({kv.Key.Item1},{kv.Key.Item2}) after crash {crashIndex}");
                }
                if (v != kv.Value)
                {
                    throw new Exception($"Wrong weight for ({kv.Key.Item1},{kv.Key.Item2}) after crash {crashIndex}: expected {kv.Value} actual {v}");
                }
            }
        }
    }
}
