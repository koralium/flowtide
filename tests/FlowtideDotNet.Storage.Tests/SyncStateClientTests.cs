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
using FlowtideDotNet.Storage.FileCache;
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
                FileCacheFactory = new DefaultFileCacheFactory(new FileCacheOptions()
                {
                    DirectoryPath = "./testJitPreserializationDuringCommit_cache"
                })
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
                FileCacheFactory = new DefaultFileCacheFactory(new FileCacheOptions()
                {
                    DirectoryPath = "./TestWithoutReadCache_cache"
                })
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

        /// <summary>
        /// Tests that deleting keys while a background commit is in progress
        /// does not corrupt the file cache or cause "Segment not found" errors.
        /// </summary>
        [Fact]
        public async Task TestDeleteDuringBackgroundCommit()
        {
            var persist = new FileCachePersistentStorage(new FileCacheOptions()
            {
                DirectoryPath = "./TestDeleteDuringBackgroundCommit"
            });
            var a = new StateManagerOptions()
            {
                PersistentStorage = persist,
                CachePageCount = 0,
                UseReadCache = true,
                FileCacheFactory = new DefaultFileCacheFactory(new FileCacheOptions()
                {
                    DirectoryPath = "./TestDeleteDuringBackgroundCommit_cache"
                })
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

            for (int i = 0; i < 500; i++)
            {
                await tree.Upsert(i, i);
            }
            await tree.Commit();

            // Modify all keys so they are tracked for the next commit
            for (int i = 0; i < 500; i++)
            {
                await tree.Upsert(i, i + 100);
            }

            var commitTask = tree.Commit();

            // Delete some keys while background commit is running
            for (int i = 0; i < 250; i++)
            {
                await tree.Delete(i);
            }

            await commitTask;
            await manager.LruTable.ForceCleanup();

            // Deleted keys should not be found
            for (int i = 0; i < 250; i++)
            {
                var val = await tree.GetValue(i);
                Assert.False(val.found);
            }

            // Remaining keys should have the correct value
            for (int i = 250; i < 500; i++)
            {
                var val = await tree.GetValue(i);
                Assert.True(val.found);
                Assert.Equal(i + 100, val.value);
            }

            manager.Dispose();
            persist.Dispose();
        }

        /// <summary>
        /// Tests that calling Commit() twice back-to-back works correctly:
        /// the second commit waits for the first background task to finish,
        /// then starts its own background task.
        /// </summary>
        [Fact]
        public async Task TestBackToBackCommits()
        {
            var persist = new FileCachePersistentStorage(new FileCacheOptions()
            {
                DirectoryPath = "./TestBackToBackCommits"
            });
            var a = new StateManagerOptions()
            {
                PersistentStorage = persist,
                CachePageCount = 0,
                UseReadCache = true,
                FileCacheFactory = new DefaultFileCacheFactory(new FileCacheOptions()
                {
                    DirectoryPath = "./TestBackToBackCommits_cache"
                })
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

            // Round 1: populate and commit
            for (int i = 0; i < 200; i++)
            {
                await tree.Upsert(i, i);
            }
            await tree.Commit();

            // Round 2: modify and commit (background fires)
            for (int i = 0; i < 200; i++)
            {
                await tree.Upsert(i, i + 10);
            }
            var commit1 = tree.Commit();

            // Round 3: modify again and immediately commit again
            // This second Commit() should await the first background task before starting its own
            for (int i = 0; i < 200; i++)
            {
                await tree.Upsert(i, i + 20);
            }
            var commit2 = tree.Commit();

            // Round 4: modify once more while second background runs
            for (int i = 0; i < 200; i++)
            {
                await tree.Upsert(i, i + 30);
            }

            await commit1;
            await commit2;
            await manager.LruTable.ForceCleanup();

            // Final values should be the last round of updates
            for (int i = 0; i < 200; i++)
            {
                var val = await tree.GetValue(i);
                Assert.True(val.found);
                Assert.Equal(i + 30, val.value);
            }

            manager.Dispose();
            persist.Dispose();
        }

        /// <summary>
        /// Tests that two different trees on the same state manager client can both
        /// commit concurrently without interfering with each other.
        /// </summary>
        [Fact]
        public async Task TestMultiTreeConcurrentCommit()
        {
            var persist = new FileCachePersistentStorage(new FileCacheOptions()
            {
                DirectoryPath = "./TestMultiTreeConcurrentCommit"
            });
            var a = new StateManagerOptions()
            {
                PersistentStorage = persist,
                CachePageCount = 0,
                UseReadCache = true,
                FileCacheFactory = new DefaultFileCacheFactory(new FileCacheOptions()
                {
                    DirectoryPath = "./TestMultiTreeConcurrentCommit_cache"
                })
            };
            var manager = new StateManagerSync<StateManagerMetadata>(a, NullLoggerFactory.Instance, new System.Diagnostics.Metrics.Meter("tmp"), "test");
            await manager.InitializeAsync();
            await manager.LruTable.StopCleanupTask();

            var client = manager.GetOrCreateClient("client");
            var treeOpts = new BPlusTreeOptions<long, int, ListKeyContainer<long>, ListValueContainer<int>>()
            {
                Comparer = new BPlusTreeListComparer<long>(new LongComparer()),
                KeySerializer = new KeyListSerializer<long>(new LongSerializer()),
                ValueSerializer = new ValueListSerializer<int>(new IntSerializer()),
                MemoryAllocator = GlobalMemoryManager.Instance
            };

            var leftTree = await client.GetOrCreateTree("left", treeOpts);
            var rightTree = await client.GetOrCreateTree("right", treeOpts);

            // Initial population
            for (int i = 0; i < 500; i++)
            {
                await leftTree.Upsert(i, i);
                await rightTree.Upsert(i, i + 1000);
            }
            await leftTree.Commit();
            await rightTree.Commit();

            // Modify both trees
            for (int i = 0; i < 500; i++)
            {
                await leftTree.Upsert(i, i + 10);
                await rightTree.Upsert(i, i + 1010);
            }

            // Commit both — each fires its own background task
            var leftCommit = leftTree.Commit();
            var rightCommit = rightTree.Commit();

            // Modify both again while commits are running
            for (int i = 0; i < 500; i++)
            {
                await leftTree.Upsert(i, i + 20);
                await rightTree.Upsert(i, i + 1020);
                
                await leftTree.Upsert(i + 5000, i + 20);
                await rightTree.Upsert(i + 5000, i + 1020);
            }

            await leftCommit;
            await rightCommit;
            await manager.LruTable.ForceCleanup();

            for (int i = 0; i < 500; i++)
            {
                var leftVal = await leftTree.GetValue(i);
                Assert.True(leftVal.found);
                Assert.Equal(i + 20, leftVal.value);

                var rightVal = await rightTree.GetValue(i);
                Assert.True(rightVal.found);
                Assert.Equal(i + 1020, rightVal.value);
            }

            manager.Dispose();
            persist.Dispose();
        }

        /// <summary>
        /// Tests that eviction triggered during a background commit does not corrupt data.
        /// The LRU cleanup task runs on a separate thread, so eviction and background
        /// commit can race on the file cache and version tracking.
        /// </summary>
        [Fact]
        public async Task TestEvictionDuringBackgroundCommit()
        {
            var persist = new FileCachePersistentStorage(new FileCacheOptions()
            {
                DirectoryPath = "./TestEvictionDuringBackgroundCommit"
            });
            var a = new StateManagerOptions()
            {
                PersistentStorage = persist,
                CachePageCount = 100,
                UseReadCache = true,
                FileCacheFactory = new DefaultFileCacheFactory(new FileCacheOptions()
                {
                    DirectoryPath = "./TestEvictionDuringBackgroundCommit_cache"
                })
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

            // Populate enough keys to fill the cache and force evictions
            for (int i = 0; i < 1000; i++)
            {
                await tree.Upsert(i, i);
            }
            await tree.Commit();

            // Force eviction so pages go to file cache
            await manager.LruTable.ForceCleanup();

            // Modify all keys
            for (int i = 0; i < 1000; i++)
            {
                await tree.Upsert(i, i + 10);
            }

            var commitTask = tree.Commit();

            // Force eviction while background commit is running
            await manager.LruTable.ForceCleanup();

            // Modify some keys again
            for (int i = 0; i < 500; i++)
            {
                await tree.Upsert(i, i + 20);
            }

            await commitTask;

            // Force another eviction and cleanup
            await manager.LruTable.ForceCleanup();

            // Keys 0-499 should have value i+20
            for (int i = 0; i < 500; i++)
            {
                var val = await tree.GetValue(i);
                Assert.True(val.found);
                Assert.Equal(i + 20, val.value);
            }

            // Keys 500-999 should have value i+10
            for (int i = 500; i < 1000; i++)
            {
                var val = await tree.GetValue(i);
                Assert.True(val.found);
                Assert.Equal(i + 10, val.value);
            }

            manager.Dispose();
            persist.Dispose();
        }

        /// <summary>
        /// Tests the deferred file cache cleanup race under !useReadCache.
        /// When the background commit finishes, it frees file cache segments in the post-commit
        /// block under m_lock. A foreground GetValue call that was dispatched to GetValue_FromCache
        /// (because m_fileCacheVersion contained the key at dispatch time) must detect that the
        /// segment was freed and fall back to reading from persistent storage.
        /// </summary>
        [Fact]
        public async Task TestWithoutReadCachePostCommitCleanupRace()
        {
            var persist = new FileCachePersistentStorage(new FileCacheOptions()
            {
                DirectoryPath = "./TestWithoutReadCachePostCommitCleanupRace"
            });
            var a = new StateManagerOptions()
            {
                PersistentStorage = persist,
                CachePageCount = 100,
                UseReadCache = false,
                FileCacheFactory = new DefaultFileCacheFactory(new FileCacheOptions()
                {
                    DirectoryPath = "./TestWithoutReadCachePostCommitCleanupRace_cache"
                })
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

            // Phase 1: Populate and persist so values are in persistent storage
            for (int i = 0; i < 1000; i++)
            {
                await tree.Upsert(i, i);
            }
            await tree.Commit();

            // Phase 2: Evict to file cache, then modify to dirty the pages
            await manager.LruTable.ForceCleanup();
            for (int i = 0; i < 1000; i++)
            {
                await tree.Upsert(i, i + 10);
            }

            // Phase 3: Commit and wait — the background task will read file cache segments,
            // write to persistent storage, and then free file cache entries in post-commit cleanup
            await tree.Commit();

            // Phase 4: Evict again so values leave the LRU cache
            await manager.LruTable.ForceCleanup();

            // Phase 5: Read all values — since UseReadCache=false, the file cache entries
            // were freed by the post-commit cleanup. GetValue must detect this via the
            // segmentFreed check and fall back to persistent storage.
            for (int i = 0; i < 1000; i++)
            {
                var val = await tree.GetValue(i);
                Assert.True(val.found);
                Assert.Equal(i + 10, val.value);
            }

            // Phase 6: Do another round to verify the cycle is repeatable
            for (int i = 0; i < 1000; i++)
            {
                await tree.Upsert(i, i + 20);
            }

            var commitTask = tree.Commit();

            // Read while background commit is running
            for (int i = 500; i < 1000; i++)
            {
                await tree.Upsert(i, i + 30);
            }

            await commitTask;
            await manager.LruTable.ForceCleanup();

            for (int i = 0; i < 500; i++)
            {
                var val = await tree.GetValue(i);
                Assert.True(val.found);
                Assert.Equal(i + 20, val.value);
            }
            for (int i = 500; i < 1000; i++)
            {
                var val = await tree.GetValue(i);
                Assert.True(val.found);
                Assert.Equal(i + 30, val.value);
            }

            manager.Dispose();
            persist.Dispose();
        }

        /// <summary>
        /// Tests that Evict does not create a stale m_fileCacheVersion entry when the
        /// linked list node's value is already removed. Before the fix, m_fileCacheVersion
        /// was set unconditionally even when m_fileCache.Write was skipped, causing
        /// GetValue_FromCache to attempt reading a segment that doesn't exist.
        /// </summary>
        [Fact]
        public async Task TestEvictSkippedWriteDesync()
        {
            var persist = new FileCachePersistentStorage(new FileCacheOptions()
            {
                DirectoryPath = "./TestEvictSkippedWriteDesync"
            });
            var a = new StateManagerOptions()
            {
                PersistentStorage = persist,
                CachePageCount = 100,
                UseReadCache = true,
                FileCacheFactory = new DefaultFileCacheFactory(new FileCacheOptions()
                {
                    DirectoryPath = "./TestEvictSkippedWriteDesync_cache"
                })
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

            // Populate data
            for (int i = 0; i < 500; i++)
            {
                await tree.Upsert(i, i);
            }
            await tree.Commit();

            // Modify values and start background commit
            for (int i = 0; i < 500; i++)
            {
                await tree.Upsert(i, i + 100);
            }
            var commitTask = tree.Commit();

            // Modify again while commit is in progress — causes the loadedFromCache cleanup
            // to free file cache entries, which can leave nodes in a "removed" state
            for (int i = 0; i < 500; i++)
            {
                await tree.Upsert(i, i + 200);
            }

            await commitTask;

            // Evict everything — some nodes may be "removed" from LRU and Evict should NOT
            // set m_fileCacheVersion for those nodes since m_fileCache.Write is skipped
            await manager.LruTable.ForceCleanup();

            // Read all values — if the Evict desync was present, this would crash with
            // "Segment not found" since m_fileCacheVersion says there's a segment but there isn't
            for (int i = 0; i < 500; i++)
            {
                var val = await tree.GetValue(i);
                Assert.True(val.found);
                Assert.Equal(i + 200, val.value);
            }

            manager.Dispose();
            persist.Dispose();
        }

        /// <summary>
        /// Tests that a tree structural change (via UpdateRoot which replaces the metadata object)
        /// during a background commit does not lose the metadata Updated flag.
        /// The background task should only clear Updated if the metadata reference has not been
        /// replaced by the foreground thread.
        /// </summary>
        [Fact]
        public async Task TestMetadataUpdatedFlagSurvivesTreeSplit()
        {
            var persist = new FileCachePersistentStorage(new FileCacheOptions()
            {
                DirectoryPath = "./TestMetadataUpdatedFlagSurvivesTreeSplit"
            });
            var a = new StateManagerOptions()
            {
                PersistentStorage = persist,
                CachePageCount = 100,
                UseReadCache = true,
                FileCacheFactory = new DefaultFileCacheFactory(new FileCacheOptions()
                {
                    DirectoryPath = "./TestMetadataUpdatedFlagSurvivesTreeSplit_cache"
                })
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

            // Initial population — enough data to create multiple pages
            for (int i = 0; i < 1000; i++)
            {
                await tree.Upsert(i, i);
            }
            await tree.Commit();

            // Modify some values to dirty pages
            for (int i = 0; i < 500; i++)
            {
                await tree.Upsert(i, i + 10);
            }
            var commitTask = tree.Commit();

            // While the background commit runs, insert many new keys to force tree splits.
            // Splits call UpdateRoot which replaces the metadata object, setting Updated = true.
            // The background's WriteMetadata must NOT clear this Updated flag.
            for (int i = 5000; i < 8000; i++)
            {
                await tree.Upsert(i, i);
            }

            await commitTask;

            // Commit the split — this must persist the new root metadata
            await tree.Commit();
            await manager.LruTable.ForceCleanup();

            // Verify all data survived — if metadata was lost, the tree would be corrupted
            for (int i = 0; i < 500; i++)
            {
                var val = await tree.GetValue(i);
                Assert.True(val.found);
                Assert.Equal(i + 10, val.value);
            }
            for (int i = 500; i < 1000; i++)
            {
                var val = await tree.GetValue(i);
                Assert.True(val.found);
                Assert.Equal(i, val.value);
            }
            for (int i = 5000; i < 8000; i++)
            {
                var val = await tree.GetValue(i);
                Assert.True(val.found);
                Assert.Equal(i, val.value);
            }

            manager.Dispose();
            persist.Dispose();
        }

        /// <summary>
        /// Stress test with rapid commit-evict-read cycles verifying all three fixes:
        /// 1. Deferred !useReadCache cleanup (no segment-not-found)
        /// 2. Metadata Updated flag preservation across concurrent commits  
        /// 3. Evict version tracking only when write actually occurs
        /// </summary>
        [Fact]
        public async Task TestRapidCommitEvictReadCycles()
        {
            var persist = new FileCachePersistentStorage(new FileCacheOptions()
            {
                DirectoryPath = "./TestRapidCommitEvictReadCycles"
            });
            var a = new StateManagerOptions()
            {
                PersistentStorage = persist,
                CachePageCount = 50,
                UseReadCache = true,
                FileCacheFactory = new DefaultFileCacheFactory(new FileCacheOptions()
                {
                    DirectoryPath = "./TestRapidCommitEvictReadCycles_cache"
                })
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

            // Populate initial data
            for (int i = 0; i < 500; i++)
            {
                await tree.Upsert(i, i);
            }
            await tree.Commit();

            // Run 5 rapid cycles of modify-commit-modify-evict-read
            for (int cycle = 1; cycle <= 5; cycle++)
            {
                int offset = cycle * 100;

                // Modify all keys
                for (int i = 0; i < 500; i++)
                {
                    await tree.Upsert(i, i + offset);
                }

                // Start background commit
                var commitTask = tree.Commit();

                // Modify a subset while commit is running
                for (int i = 0; i < 250; i++)
                {
                    await tree.Upsert(i, i + offset + 50);
                }

                await commitTask;

                // Evict to force file cache interaction
                await manager.LruTable.ForceCleanup();

                // Verify data integrity
                for (int i = 0; i < 250; i++)
                {
                    var val = await tree.GetValue(i);
                    Assert.True(val.found, $"Cycle {cycle}: key {i} not found");
                    Assert.Equal(i + offset + 50, val.value);
                }
                for (int i = 250; i < 500; i++)
                {
                    var val = await tree.GetValue(i);
                    Assert.True(val.found, $"Cycle {cycle}: key {i} not found");
                    Assert.Equal(i + offset, val.value);
                }
            }

            manager.Dispose();
            persist.Dispose();
        }

        /// <summary>
        /// Tests the needsPreserialize race: the foreground enters GetValue_FromCache and
        /// determines needsPreserialize=true (key in m_committing, not in m_committing_preserialized).
        /// After releasing m_lock, the background's loadedFromCache path serializes the value from
        /// the LRU cache and frees the file cache entry. The foreground must detect this and use
        /// the already-preserialized data or fall back to persistent storage.
        /// </summary>
        [Fact]
        public async Task TestNeedsPreserializeRaceWithLoadedFromCache()
        {
            var persist = new FileCachePersistentStorage(new FileCacheOptions()
            {
                DirectoryPath = "./TestNeedsPreserializeRace"
            });
            var a = new StateManagerOptions()
            {
                PersistentStorage = persist,
                CachePageCount = 100,
                UseReadCache = true,
                FileCacheFactory = new DefaultFileCacheFactory(new FileCacheOptions()
                {
                    DirectoryPath = "./TestNeedsPreserializeRace_cache"
                })
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

            // Phase 1: Populate and commit
            for (int i = 0; i < 1000; i++)
            {
                await tree.Upsert(i, i);
            }
            await tree.Commit();

            // Phase 2: Run multiple cycles that stress the needsPreserialize path.
            // Each cycle: modify -> commit (background) -> read while commit is running.
            // The reads trigger GetValue_FromCache with needsPreserialize=true when the key
            // is in m_committing. The background's loadedFromCache cleanup can free the file
            // cache segment, creating the race condition.
            for (int cycle = 1; cycle <= 3; cycle++)
            {
                int offset = cycle * 100;

                for (int i = 0; i < 1000; i++)
                {
                    await tree.Upsert(i, i + offset);
                }
                
                var commitTask = tree.Commit();

                // Interleave reads while commit is running — triggers needsPreserialize path
                for (int i = 0; i < 1000; i++)
                {
                    var val = await tree.GetValue(i);
                    Assert.True(val.found, $"Cycle {cycle}: key {i} not found during commit");
                    // Value could be either the current cycle's or previous, depending on timing
                    Assert.True(val.value >= i, $"Cycle {cycle}: key {i} has unexpected value {val.value}");
                }

                await commitTask;
                await manager.LruTable.ForceCleanup();

                // Final verification after commit and eviction
                for (int i = 0; i < 1000; i++)
                {
                    var val = await tree.GetValue(i);
                    Assert.True(val.found, $"Cycle {cycle}: key {i} not found post-commit");
                    Assert.Equal(i + offset, val.value);
                }
            }

            manager.Dispose();
            persist.Dispose();
        }

        /// <summary>
        /// Tests that !useReadCache does not double-free file cache entries when
        /// loadedFromCache inline cleanup runs and the post-commit block also processes the same key.
        /// </summary>
        [Fact]
        public async Task TestNoDoubleFreeUnderNoReadCache()
        {
            var persist = new FileCachePersistentStorage(new FileCacheOptions()
            {
                DirectoryPath = "./TestNoDoubleFreeUnderNoReadCache"
            });
            var a = new StateManagerOptions()
            {
                PersistentStorage = persist,
                CachePageCount = 100,
                UseReadCache = false,
                FileCacheFactory = new DefaultFileCacheFactory(new FileCacheOptions()
                {
                    DirectoryPath = "./TestNoDoubleFreeUnderNoReadCache_cache"
                })
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

            // Populate and commit
            for (int i = 0; i < 500; i++)
            {
                await tree.Upsert(i, i);
            }
            await tree.Commit();

            // Run several cycles. In !useReadCache mode, the loadedFromCache inline cleanup
            // frees file cache entries and marks them via sentinel. The post-commit block
            // must skip those to avoid double-freeing.
            for (int cycle = 1; cycle <= 5; cycle++)
            {
                for (int i = 0; i < 500; i++)
                {
                    await tree.Upsert(i, i + cycle * 10);
                }

                var commitTask = tree.Commit();

                // Modify a subset while commit runs so loadedFromCache cleanup sees
                // some keys as not re-modified (triggers the inline Free path)
                for (int i = 250; i < 500; i++)
                {
                    await tree.Upsert(i, i + cycle * 10 + 5);
                }

                await commitTask;
                await manager.LruTable.ForceCleanup();

                // Verify data
                for (int i = 0; i < 250; i++)
                {
                    var val = await tree.GetValue(i);
                    Assert.True(val.found, $"Cycle {cycle}: key {i} not found");
                    Assert.Equal(i + cycle * 10, val.value);
                }
                for (int i = 250; i < 500; i++)
                {
                    var val = await tree.GetValue(i);
                    Assert.True(val.found, $"Cycle {cycle}: key {i} not found");
                    Assert.Equal(i + cycle * 10 + 5, val.value);
                }
            }

            manager.Dispose();
            persist.Dispose();
        }
    }
}
