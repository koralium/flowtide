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
            var manager = new StateManagerSync<StateManagerMetadata>(a, NullLoggerFactory.Instance, new System.Diagnostics.Metrics.Meter("tmp"), "test");
            await manager.InitializeAsync();
            await manager.LruTable.StopCleanupTask();

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

            await manager.LruTable.ForceCleanup();

            //Version 1
            await tree.Upsert(2, 2);
            await tree.Commit();
            //Version 0
            await tree.Upsert(3, 3);
            //Version 1
            await tree.Upsert(4, 4);
            await manager.LruTable.ForceCleanup();
            var val = await tree.GetValue(2);
            Assert.True(val.found);
            Assert.Equal(2, val.value);
        }

        /// <summary>
        /// Stress tests the JIT pre-serialization mechanism by simulating rapid concurrent modifications
        /// immediately after a commit is requested, ensuring the background thread does not lose or corrupt data.
        /// </summary>
        [Fact]
        public async Task TestJitPreserializationDuringCommit()
        {
            var persist = new FileCachePersistentStorage(new FileCacheOptions()
            {
                DirectoryPath = "./testJitPreserializationDuringCommit"
            });
            var a = new StateManagerOptions()
            {
                PersistentStorage = persist,
                CachePageCount = 0,
                UseReadCache = true,
                FileCacheOptions = new FileCacheOptions()
                {
                    DirectoryPath = "./testJitPreserializationDuringCommit_cache"
                }
            };
            var manager = new StateManagerSync<StateManagerMetadata>(a, NullLoggerFactory.Instance, new System.Diagnostics.Metrics.Meter("tmp"), "test");
            await manager.InitializeAsync();
            await manager.LruTable.StopCleanupTask();

            var client = manager.GetOrCreateClient("client");
            var tree = await client.GetOrCreateTree("tree",
                new BPlusTreeOptions<long, int, ListKeyContainer<long>, ListValueContainer<int>>()
                {
                    Comparer = new BPlusTreeListComparer<long>(new LongComparer()),
                    KeySerializer = new KeyListSerializer<long>(new LongSerializer()),
                    ValueSerializer = new ValueListSerializer<int>(new IntSerializer()),
                    MemoryAllocator = GlobalMemoryManager.Instance
                });

            // Populate multiple pages
            for (int i = 0; i < 2000; i++)
            {
                await tree.Upsert(i, i);
            }

            await tree.Commit();

            // Modify exactly before and after commit to force JIT serialization races
            for (int i = 0; i < 2000; i++)
            {
                await tree.Upsert(i, i + 10);
            }
            
            var commitTask = tree.Commit();

            // Hit the tree while the commit is backgrounding to trigger the pre-serializer lock logic
            for (int i = 0; i < 2000; i++)
            {
                await tree.Upsert(i, i + 20);
            }

            await commitTask;

            await manager.LruTable.ForceCleanup();

            for (int i = 0; i < 2000; i++)
            {
                var val = await tree.GetValue(i);
                Assert.True(val.found);
                Assert.Equal(i + 20, val.value);
            }

            manager.Dispose();
            persist.Dispose();
        }

        [Fact]
        public async Task TestWithoutReadCache()
        {
            var persist = new FileCachePersistentStorage(new FileCacheOptions()
            {
                DirectoryPath = "./TestWithoutReadCache"
            });
            var a = new StateManagerOptions()
            {
                PersistentStorage = persist,
                CachePageCount = 0,
                UseReadCache = false,
                FileCacheOptions = new FileCacheOptions()
                {
                    DirectoryPath = "./TestWithoutReadCache_cache"
                }
            };
            var manager = new StateManagerSync<StateManagerMetadata>(a, NullLoggerFactory.Instance, new System.Diagnostics.Metrics.Meter("tmp"), "test");
            await manager.InitializeAsync();
            await manager.LruTable.StopCleanupTask();

            var client = manager.GetOrCreateClient("client");
            var tree = await client.GetOrCreateTree("tree",
                new BPlusTreeOptions<long, int, ListKeyContainer<long>, ListValueContainer<int>>()
                {
                    Comparer = new BPlusTreeListComparer<long>(new LongComparer()),
                    KeySerializer = new KeyListSerializer<long>(new LongSerializer()),
                    ValueSerializer = new ValueListSerializer<int>(new IntSerializer()),
                    MemoryAllocator = GlobalMemoryManager.Instance
                });

            for (int i = 0; i < 1000; i++)
            {
                await tree.Upsert(i, i);
            }
            await tree.Commit();

            for (int i = 0; i < 1000; i++)
            {
                await tree.Upsert(i, i + 10);
            }

            var commitTask = tree.Commit();

            for (int i = 0; i < 1000; i++)
            {
                await tree.Upsert(i, i + 20);
            }

            await commitTask;
            await manager.LruTable.ForceCleanup();

            for (int i = 0; i < 1000; i++)
            {
                var val = await tree.GetValue(i);
                Assert.True(val.found);
                Assert.Equal(i + 20, val.value);
            }

            manager.Dispose();
            persist.Dispose();
        }
    }
}
