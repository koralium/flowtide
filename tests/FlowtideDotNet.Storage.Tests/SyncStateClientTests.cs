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
using FlowtideDotNet.Storage.StateManager.Internal;
using FlowtideDotNet.Storage.Tree;
using Microsoft.Extensions.Logging.Abstractions;

namespace FlowtideDotNet.Storage.Tests
{
    public class SyncStateClientTests
    {
        /// <summary>
        /// Wraps a file cache and records writes that arrive after Dispose has started.
        /// Dispose deliberately holds the state-client teardown open long enough for the
        /// cache table's 10ms background cleanup task to run an eviction into the window,
        /// making the dispose-ordering race deterministic instead of timing-dependent.
        /// </summary>
        private class DisposeTrackingFileCache : FlowtideDotNet.Storage.FileCache.IFileCache
        {
            private readonly FlowtideDotNet.Storage.FileCache.IFileCache _inner;
            private int _disposeStarted;
            private int _writesAfterDisposeStarted;

            public DisposeTrackingFileCache(FlowtideDotNet.Storage.FileCache.IFileCache inner)
            {
                _inner = inner;
            }

            public int WritesAfterDisposeStarted => Volatile.Read(ref _writesAfterDisposeStarted);

            public void Write(long id, SerializableObject serializableObject)
            {
                if (Volatile.Read(ref _disposeStarted) == 1)
                {
                    Interlocked.Increment(ref _writesAfterDisposeStarted);
                }
                _inner.Write(id, serializableObject);
            }

            public ValueTask<ReadOnlyMemory<byte>> Read(long pageKey) => _inner.Read(pageKey);

            public ValueTask<T> Read<T>(long pageKey, StateManager.Internal.IStateSerializer<T> serializer)
                where T : StateManager.Internal.ICacheObject => _inner.Read(pageKey, serializer);

            public void Free(in long pageKey) => _inner.Free(pageKey);

            public void FreeAll(IEnumerable<long> keys) => _inner.FreeAll(keys);

            public void Flush() => _inner.Flush();

            public void ClearTemporaryAllocations() => _inner.ClearTemporaryAllocations();

            public void Dispose()
            {
                Volatile.Write(ref _disposeStarted, 1);
                Thread.Sleep(250);
                _inner.Dispose();
            }
        }

        private class DisposeTrackingFileCacheFactory : FlowtideDotNet.Storage.FileCache.IFileCacheFactory
        {
            private readonly FlowtideDotNet.Storage.FileCache.IFileCacheFactory _inner;

            public DisposeTrackingFileCacheFactory(FlowtideDotNet.Storage.FileCache.IFileCacheFactory inner)
            {
                _inner = inner;
            }

            public List<DisposeTrackingFileCache> Created { get; } = new List<DisposeTrackingFileCache>();

            public FlowtideDotNet.Storage.FileCache.IFileCache Create(string name, IMemoryAllocator memoryAllocator)
            {
                var cache = new DisposeTrackingFileCache(_inner.Create(name, memoryAllocator));
                lock (Created)
                {
                    Created.Add(cache);
                }
                return cache;
            }
        }

        /// <summary>
        /// Disposing the state manager must stop the cache table's background cleanup task
        /// BEFORE the state clients are torn down; otherwise an in-flight eviction writes
        /// through the client's already-disposed file cache and serializer.
        /// </summary>
        [Fact]
        public async Task DisposeStopsEvictionBeforeStateClientsAreDisposed()
        {
            var persist = new FileCachePersistentStorage(new FileCacheOptions()
            {
                DirectoryPath = "./disposeStopsEviction"
            });
            var factory = new DisposeTrackingFileCacheFactory(new FlowtideDotNet.Storage.FileCache.DefaultFileCacheFactory(new FileCacheOptions()
            {
                DirectoryPath = "./disposeStopsEvictionTmp"
            }));
            var a = new StateManagerOptions()
            {
                PersistentStorage = persist,
                CachePageCount = 0,
                MinCachePageCount = 0,
                FileCacheFactory = factory
            };
            var manager = new StateManagerSync<StateManagerMetadata>(a, NullLoggerFactory.Instance, new System.Diagnostics.Metrics.Meter("tmp3"), "test", GlobalMemoryManager.Instance);
            // The background cleanup task keeps running until Dispose.
            await manager.InitializeAsync();

            var client = manager.GetOrCreateClient("client");
            var tree = await client.GetOrCreateTree("tree",
                new BPlusTreeOptions<long, int, ListKeyContainer<long>, ListValueContainer<int>>()
                {
                    Comparer = new BPlusTreeListComparer<long>(new LongComparer()),
                    KeySerializer = new KeyListSerializer<long>(new LongSerializer()),
                    ValueSerializer = new ValueListSerializer<int>(new IntSerializer()),
                    MemoryAllocator = GlobalMemoryManager.Instance
                });

            // A burst of dirty pages immediately before Dispose so the cache is above its
            // eviction threshold when the teardown window opens.
            for (long i = 0; i < 500; i++)
            {
                await tree.Upsert(i, (int)i);
            }

            manager.Dispose();

            var writesAfterDispose = factory.Created.Sum(c => c.WritesAfterDisposeStarted);
            Assert.Equal(0, writesAfterDispose);
        }
        /// <summary>
        /// Tests an edge case where a version 1 is evicted and then a two writes are done which becomes version 1 after a commit.
        /// If this is evicted it should overwrite the previous evicted version 1.
        /// </summary>
        /// <returns></returns>
        [Fact]
        public async Task TestEvictVersionZeroAndWriteEvict()
        {
            var persist = new FileCachePersistentStorage(new FileCacheOptions()
            {
                DirectoryPath = "./testEvictVersionZeroAndWriteEvict"
            });
            var a = new StateManagerOptions()
            {
                PersistentStorage = persist,
                CachePageCount = 0,
                UseReadCache = true
            };
            var manager = new StateManagerSync<StateManagerMetadata>(a, NullLoggerFactory.Instance, new System.Diagnostics.Metrics.Meter("tmp"), "test", GlobalMemoryManager.Instance);
            await manager.InitializeAsync();
            await manager.CacheTable.StopCleanupTask();

            var client = manager.GetOrCreateClient("client");
            var tree = await client.GetOrCreateTree("tree",
                new BPlusTreeOptions<long, int, ListKeyContainer<long>, ListValueContainer<int>>()
                {
                    Comparer = new BPlusTreeListComparer<long>(new LongComparer()),
                    KeySerializer = new KeyListSerializer<long>(new LongSerializer()),
                    ValueSerializer = new ValueListSerializer<int>(new IntSerializer()),
                    MemoryAllocator = GlobalMemoryManager.Instance
                });
            //Version 0
            await tree.Upsert(1, 1);

            await manager.CacheTable.ForceCleanup();

            //Version 1
            await tree.Upsert(2, 2);
            await tree.Commit();
            //Version 0
            await tree.Upsert(3, 3);
            //Version 1
            await tree.Upsert(4, 4);
            await manager.CacheTable.ForceCleanup();
            var val = await tree.GetValue(2);
            Assert.True(val.found);
            Assert.Equal(2, val.value);
        }

        /// <summary>
        /// Regression net for the GetValue_Persistent rent ordering: pages loaded from
        /// persistent storage must survive continuous eviction pressure racing the read.
        /// With CachePageCount = 0 every cleanup evicts everything, so each read round
        /// re-loads pages from storage while the evictor concurrently tears them out.
        /// </summary>
        [Fact]
        public async Task PersistentReadsSurviveContinuousEviction()
        {
            var persist = new FileCachePersistentStorage(new FileCacheOptions()
            {
                DirectoryPath = "./persistentReadsSurviveContinuousEviction"
            });
            var a = new StateManagerOptions()
            {
                PersistentStorage = persist,
                CachePageCount = 0,
                MinCachePageCount = 0
            };
            var manager = new StateManagerSync<StateManagerMetadata>(a, NullLoggerFactory.Instance, new System.Diagnostics.Metrics.Meter("tmp2"), "test", GlobalMemoryManager.Instance);
            await manager.InitializeAsync();
            await manager.CacheTable.StopCleanupTask();

            var client = manager.GetOrCreateClient("client");
            var tree = await client.GetOrCreateTree("tree",
                new BPlusTreeOptions<long, int, ListKeyContainer<long>, ListValueContainer<int>>()
                {
                    Comparer = new BPlusTreeListComparer<long>(new LongComparer()),
                    KeySerializer = new KeyListSerializer<long>(new LongSerializer()),
                    ValueSerializer = new ValueListSerializer<int>(new IntSerializer()),
                    MemoryAllocator = GlobalMemoryManager.Instance
                });
            for (long i = 0; i < 200; i++)
            {
                await tree.Upsert(i, (int)i);
            }
            await tree.Commit();

            using var stop = new CancellationTokenSource(TimeSpan.FromSeconds(1.5));
            var evictor = Task.Run(async () =>
            {
                while (!stop.IsCancellationRequested)
                {
                    await manager.CacheTable.ForceCleanup();
                }
            });

            while (!stop.IsCancellationRequested)
            {
                for (long i = 0; i < 200; i++)
                {
                    var val = await tree.GetValue(i);
                    Assert.True(val.found);
                    Assert.Equal((int)i, val.value);
                }
            }
            await evictor;
        }
    }
}
