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

using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using Microsoft.Extensions.Logging.Abstractions;

namespace FlowtideDotNet.Storage.Tests
{
    /// <summary>
    /// Rule 2 says a rollback version is available on every substream so the rollback is clean.
    /// That only holds if the storage a substream runs on actually rolls back when it is asked to.
    /// </summary>
    public class StorageRollbackContractTests
    {
        /// <summary>
        /// Reservoir rolls back, this is the behaviour the other storages are measured against.
        /// </summary>
        [Fact]
        public async Task ReservoirRollsBackToTheRequestedVersion()
        {
            var provider = new MemoryFileProvider();
            var storage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions() { FileProvider = provider });
            await storage.InitializeAsync(new StorageInitializationMetadata("a", NullLoggerFactory.Instance, GlobalMemoryManager.Instance));

            var session = storage.CreateSession();
            await session.Write(100, new SerializableObject(new byte[] { 1 }));
            await session.Commit();
            await storage.CheckpointAsync(new byte[] { 1 }, false); // version 1

            await session.Write(100, new SerializableObject(new byte[] { 2 }));
            await session.Commit();
            await storage.CheckpointAsync(new byte[] { 2 }, false); // version 2

            var recovered = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions() { FileProvider = provider });
            await recovered.InitializeAsync(new StorageInitializationMetadata("a", NullLoggerFactory.Instance, GlobalMemoryManager.Instance));
            await recovered.RecoverAsync(1);

            var data = await recovered.CreateSession().Read(100);
            Assert.Equal(new byte[] { 1 }, data.ToArray());
        }

        /// <summary>
        /// The default storage must roll back too. Its RecoverAsync is currently a no-op while the
        /// state manager still records the requested version as the one it restored to, so a
        /// negotiated rollback is reported as applied against state that never moved.
        /// </summary>
        [Fact]
        public async Task FileCacheRollsBackToTheRequestedVersion()
        {
            var storage = new FileCachePersistentStorage(new FileCacheOptions());
            await storage.InitializeAsync(new StorageInitializationMetadata("a", NullLoggerFactory.Instance, GlobalMemoryManager.Instance));

            var session = storage.CreateSession();
            await session.Write(100, new SerializableObject(new byte[] { 1 }));
            await session.Commit();
            await storage.CheckpointAsync(new byte[] { 1 }, false); // version 1

            await session.Write(100, new SerializableObject(new byte[] { 2 }));
            await session.Commit();
            await storage.CheckpointAsync(new byte[] { 2 }, false); // version 2

            await storage.RecoverAsync(1);

            var data = await storage.CreateSession().Read(100);
            Assert.Equal(new byte[] { 1 }, data.ToArray());
        }
    }
}
