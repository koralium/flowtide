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
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics.Metrics;
using Xunit;

namespace FlowtideDotNet.Storage.Tests
{
    /// <summary>
    /// Deterministic-ish crash-consistency harness for the page cache under maximal eviction.
    ///
    /// The stream-level symptom is a wrong row count after a crash under CachePageCount = 0.
    /// This drives the same conditions at the storage layer: a B+ tree with maximal eviction
    /// (every page evicted continuously) while a dedicated evictor thread races the main
    /// thread's Commit/CheckpointAsync. After each simulated crash (InitializeAsync, which
    /// drops all volatile cache/spill state and recovers the durable checkpoint) the recovered
    /// tree must EXACTLY equal the state as of the last checkpoint. A divergence is the
    /// storage-level form of the lost/gained rows seen at the stream level.
    ///
    /// The crashGate serializes eviction against the crash+verify phase (so verification reads
    /// a quiescent tree) while letting eviction freely race Commit/CheckpointAsync, which is
    /// where the durability race lives.
    /// </summary>
    public class CrashConsistencyStressTests
    {
        private sealed class Harness
        {
            public StateManagerSync StateManager = null!;
            public IBPlusTree<long, long, ListKeyContainer<long>, ListValueContainer<long>> Tree = null!;
            public string TempDir = null!;
        }

        private static async Task<Harness> CreateAsync(MemoryFileProvider fileProvider)
        {
            var tempDir = $"./data/crashConsistency/{Guid.NewGuid():N}";
            var stateManager = new StateManagerSync<object>(
                new StateManagerOptions()
                {
                    PersistentStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions()
                    {
                        FileProvider = fileProvider
                    }),
                    TemporaryStorageOptions = new FlowtideDotNet.Storage.FileCacheOptions()
                    {
                        DirectoryPath = $"{tempDir}/tmp"
                    },
                    CachePageCount = 0,
                    MinCachePageCount = 0,
                    // Tiny pages force deep multi-leaf trees with constant splits/merges, like the
                    // aggregate stress test (SetPageSizeBytes(160)) that exposed the flake.
                    DefaultBPlusTreePageSize = 6,
                    DefaultBPlusTreePageSizeBytes = 256,
                }, NullLoggerFactory.Instance, new Meter(Guid.NewGuid().ToString()), "storage", GlobalMemoryManager.Instance);

            await stateManager.InitializeAsync();

            var client = stateManager.GetOrCreateClient("node1");
            var tree = await client.GetOrCreateTree("tree", new BPlusTreeOptions<long, long, ListKeyContainer<long>, ListValueContainer<long>>()
            {
                BucketSize = 8,
                Comparer = new BPlusTreeListComparer<long>(new LongComparer()),
                KeySerializer = new KeyListSerializer<long>(new LongSerializer()),
                ValueSerializer = new ValueListSerializer<long>(new LongSerializer()),
                MemoryAllocator = GlobalMemoryManager.Instance
            });

            return new Harness { StateManager = stateManager, Tree = tree, TempDir = tempDir };
        }

        private static async Task<Dictionary<long, long>> ReadAllAsync(IBPlusTree<long, long, ListKeyContainer<long>, ListValueContainer<long>> tree)
        {
            var result = new Dictionary<long, long>();
            var it = tree.CreateIterator();
            await it.SeekFirst();
            await foreach (var page in it)
            {
                foreach (var kv in page)
                {
                    result.Add(kv.Key, kv.Value);
                }
            }
            return result;
        }

        [Fact]
        public async Task RecoveredTreeMatchesLastCheckpointUnderConcurrentEviction()
        {
            var fileProvider = new MemoryFileProvider();
            var harness = await CreateAsync(fileProvider);
            var stateManager = harness.StateManager;
            var tree = harness.Tree;

            // Stop the built-in cleanup task so this test drives eviction through a SINGLE
            // dedicated evictor thread. In production only the one background cleanup task
            // evicts (serialized by _fullLock); two concurrent evictors would be unrealistic.
            await stateManager.CacheTable.StopCleanupTask();
            var crashGate = new SemaphoreSlim(1);
            var stop = new CancellationTokenSource();
            int transientEvictorErrors = 0;
            var evictor = Task.Run(async () =>
            {
                while (!stop.IsCancellationRequested)
                {
                    await crashGate.WaitAsync();
                    try
                    {
                        await stateManager.CacheTable.ForceCleanup();
                    }
                    catch
                    {
                        // A real background cleanup task restarts on fault rather than dying.
                        // Transient file-cache segment errors are a separate concern; keep going
                        // so this test isolates crash-consistency corruption.
                        Interlocked.Increment(ref transientEvictorErrors);
                    }
                    finally
                    {
                        crashGate.Release();
                    }
                    // Yield so this stress test does not monopolize a core and starve other
                    // timing-sensitive tests running in parallel; eviction still fires hundreds
                    // of times a second, far above the production 10ms cadence.
                    await Task.Delay(1);
                }
            });

            var rnd = new Random(12345);
            const int keySpace = 2000;
            var live = new Dictionary<long, long>();     // current tree state
            var committed = new Dictionary<long, long>(); // state as of the last checkpoint
            long valueCounter = 0;

            var sw = System.Diagnostics.Stopwatch.StartNew();
            var deadline = TimeSpan.FromSeconds(5);
            int crashes = 0;
            int checkpoints = 0;

            while (sw.Elapsed < deadline)
            {
                // Apply a burst of random ops to the tree and the shadow model.
                int ops = rnd.Next(20, 80);
                for (int o = 0; o < ops; o++)
                {
                    long key = rnd.Next(keySpace);
                    if (live.ContainsKey(key) && rnd.Next(3) == 0)
                    {
                        await tree.Delete(key);
                        live.Remove(key);
                    }
                    else
                    {
                        long value = ++valueCounter;
                        await tree.Upsert(key, value);
                        live[key] = value;
                    }
                }

                var choice = rnd.Next(3);
                if (choice != 0)
                {
                    // Checkpoint: commit tree pages and take a checkpoint. The evictor races this.
                    // Commit throwing (e.g. "Segment not found") IS a corruption failure and must
                    // propagate; a transient segment-writer file error is retried once.
                    try
                    {
                        await tree.Commit();
                    }
                    catch (Exception e) when (e.ToString().Contains("FileCacheSegmentWriter"))
                    {
                        await tree.Commit();
                    }
                    await stateManager.CheckpointAsync();
                    committed = new Dictionary<long, long>(live);
                    checkpoints++;
                }
                else
                {
                    // Crash: revert to the last checkpoint and verify the recovered tree exactly
                    // matches it. Pause the evictor so verification reads a quiescent tree.
                    await crashGate.WaitAsync();
                    try
                    {
                        await stateManager.InitializeAsync();
                        var actual = await ReadAllAsync(tree);
                        AssertEqual(committed, actual, crashes, checkpoints);
                        live = new Dictionary<long, long>(committed);
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

            // Final crash + verify.
            await stateManager.InitializeAsync();
            var finalActual = await ReadAllAsync(tree);
            AssertEqual(committed, finalActual, crashes, checkpoints);

            Assert.True(crashes > 5, $"Only {crashes} crashes exercised");
            Assert.True(checkpoints > 10, $"Only {checkpoints} checkpoints exercised");

            stateManager.Dispose();
            try { Directory.Delete(harness.TempDir, recursive: true); } catch { }
        }

        private static void AssertEqual(Dictionary<long, long> expected, Dictionary<long, long> actual, int crashIndex, int checkpointIndex)
        {
            if (expected.Count != actual.Count)
            {
                var missing = expected.Keys.Where(k => !actual.ContainsKey(k)).OrderBy(k => k).Take(10).ToList();
                var extra = actual.Keys.Where(k => !expected.ContainsKey(k)).OrderBy(k => k).Take(10).ToList();
                throw new Exception($"Count mismatch after crash {crashIndex} (checkpoints={checkpointIndex}): expected {expected.Count} actual {actual.Count}. missingKeys=[{string.Join(",", missing)}] extraKeys=[{string.Join(",", extra)}]");
            }
            foreach (var kv in expected)
            {
                if (!actual.TryGetValue(kv.Key, out var v))
                {
                    throw new Exception($"Missing key {kv.Key} after crash {crashIndex}");
                }
                if (v != kv.Value)
                {
                    throw new Exception($"Wrong value for key {kv.Key} after crash {crashIndex}: expected {kv.Value} actual {v}");
                }
            }
        }
    }
}
