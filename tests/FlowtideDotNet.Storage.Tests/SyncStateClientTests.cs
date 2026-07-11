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
