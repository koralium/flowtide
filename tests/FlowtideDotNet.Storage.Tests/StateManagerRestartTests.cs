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
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.Serializers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Storage.Tree;
using Microsoft.Extensions.Logging.Abstractions;
using System.Diagnostics.Metrics;

namespace FlowtideDotNet.Storage.Tests
{
    /// <summary>
    /// The engine keeps one state manager per stream and disposes it on stop, then calls
    /// InitializeAsync again if the stream starts back up.
    /// </summary>
    public class StateManagerRestartTests
    {
        private static StateManagerSync<object> CreateStateManager(string name)
        {
            return new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 1000,
                MinCachePageCount = 100,
                TemporaryStorageOptions = new FileCacheOptions() { DirectoryPath = $"./data/{name}/temp" },
                PersistentStorage = new FileCachePersistentStorage(new FileCacheOptions() { DirectoryPath = $"./data/{name}/persist" })
            }, NullLoggerFactory.Instance, new Meter(name), name, GlobalMemoryManager.Instance);
        }

        private static ValueTask<IBPlusTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>> CreateTree(IStateManagerClient client, string name)
        {
            return client.GetOrCreateTree<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>(name,
                new BPlusTreeOptions<long, long, PrimitiveListKeyContainer<long>, PrimitiveListValueContainer<long>>()
                {
                    Comparer = new PrimitiveListComparer<long>(),
                    KeySerializer = new PrimitiveListKeyContainerSerializer<long>(GlobalMemoryManager.Instance),
                    ValueSerializer = new PrimitiveListValueContainerSerializer<long>(GlobalMemoryManager.Instance),
                    MemoryAllocator = GlobalMemoryManager.Instance
                });
        }

        [Fact]
        public async Task InitializeAfterDisposeRestartsTheManager()
        {
            using var stateManager = CreateStateManager("restart_after_dispose");

            await stateManager.InitializeAsync();
            stateManager.Dispose();

            await stateManager.InitializeAsync();

            Assert.True(stateManager.Initialized);
        }

        [Fact]
        public async Task RestartedManagerServesReadsAndWrites()
        {
            using var stateManager = CreateStateManager("restart_serves_io");

            await stateManager.InitializeAsync();
            stateManager.Dispose();

            await stateManager.InitializeAsync();

            var client = stateManager.GetOrCreateClient("node1");
            var tree = await CreateTree(client, "tree");
            for (var i = 0; i < 500; i++)
            {
                await tree.Upsert(i, i * 2);
            }
            await tree.Commit();

            for (var i = 0; i < 500; i++)
            {
                var (found, value) = await tree.GetValue(i);
                Assert.True(found);
                Assert.Equal(i * 2, value);
            }
        }

        [Fact]
        public async Task RepeatedStopStartCyclesKeepWorking()
        {
            using var stateManager = CreateStateManager("restart_repeated_cycles");

            for (var cycle = 0; cycle < 3; cycle++)
            {
                await stateManager.InitializeAsync();

                var client = stateManager.GetOrCreateClient("node1");
                var tree = await CreateTree(client, $"tree{cycle}");
                await tree.Upsert(cycle, cycle);
                await tree.Commit();

                stateManager.Dispose();
            }

            await stateManager.InitializeAsync();
            Assert.True(stateManager.Initialized);
        }

        /// <summary>
        /// A second stop must release what the restart created, so the cache table cannot be
        /// left behind with a running cleanup task.
        /// </summary>
        [Fact]
        public async Task DisposeAfterRestartReleasesTheCacheTable()
        {
            using var stateManager = CreateStateManager("restart_second_dispose");

            await stateManager.InitializeAsync();
            var firstTable = stateManager.CacheTable;
            stateManager.Dispose();

            await stateManager.InitializeAsync();
            var secondTable = stateManager.CacheTable;
            Assert.NotSame(firstTable, secondTable);

            stateManager.Dispose();
            Assert.Throws<InvalidOperationException>(() => stateManager.CacheTable);
        }

        /// <summary>
        /// Repeated stop and start must not grow allocated memory. Catches a page rent that a
        /// stop leaves behind, where the containers are only reclaimed by a finalizer.
        /// </summary>
        [Fact]
        public async Task RepeatedStopStartCyclesDoNotGrowAllocatedMemory()
        {
            using var stateManager = CreateStateManager("restart_no_growth");

            // The first cycle allocates the structures every later cycle reuses.
            await RunOneCycle(stateManager, 0);
            var baseline = GlobalMemoryManager.Instance.GetAllocatedMemory();

            for (var cycle = 1; cycle <= 8; cycle++)
            {
                await RunOneCycle(stateManager, cycle);
            }

            var afterCycles = GlobalMemoryManager.Instance.GetAllocatedMemory();
            var growth = afterCycles - baseline;

            // A stranded rent per cycle would grow this without bound, the tolerance only
            // covers the pages the final cycle legitimately still holds.
            Assert.True(growth <= baseline, $"Allocated memory grew by {growth} bytes over 8 stop and start cycles, baseline was {baseline}.");
        }

        private static async Task RunOneCycle(StateManagerSync<object> stateManager, int cycle)
        {
            await stateManager.InitializeAsync();
            var tree = await CreateTree(stateManager.GetOrCreateClient("node1"), "tree");
            for (var i = 0; i < 2000; i++)
            {
                await tree.Upsert(i, i + cycle);
            }
            await tree.Commit();
            stateManager.Dispose();
        }

        /// <summary>
        /// A supplied storage is owned by the caller, so a stop must not throw its data away.
        /// Without this the restart silently starts from empty instead of resuming.
        /// </summary>
        [Fact]
        public async Task RestartRecoversCheckpointedStateFromSuppliedStorage()
        {
            using var stateManager = new StateManagerSync<object>(new StateManagerOptions()
            {
                CachePageCount = 1000,
                MinCachePageCount = 100,
                TemporaryStorageOptions = new FileCacheOptions() { DirectoryPath = "./data/restart_resume/temp" },
                PersistentStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions()
                {
                    FileProvider = new MemoryFileProvider()
                })
            }, NullLoggerFactory.Instance, new Meter("restart_resume"), "restart_resume", GlobalMemoryManager.Instance);

            await stateManager.InitializeAsync();
            var tree = await CreateTree(stateManager.GetOrCreateClient("node1"), "tree");
            for (var i = 0; i < 500; i++)
            {
                await tree.Upsert(i, i * 3);
            }
            await tree.Commit();
            await stateManager.CheckpointAsync();
            var checkpointedVersion = stateManager.LastCompletedCheckpointVersion;

            stateManager.Dispose();
            await stateManager.InitializeAsync();

            Assert.Equal(checkpointedVersion, stateManager.LastCompletedCheckpointVersion);

            var recoveredTree = await CreateTree(stateManager.GetOrCreateClient("node1"), "tree");
            for (var i = 0; i < 500; i++)
            {
                var (found, value) = await recoveredTree.GetValue(i);
                Assert.True(found, $"Key {i} was lost across the restart");
                Assert.Equal(i * 3, value);
            }
        }
    }
}
